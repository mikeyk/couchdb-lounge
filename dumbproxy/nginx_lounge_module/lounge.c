/*
Copyright 2009 Meebo, Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

#include <ngx_config.h>
#include <ngx_core.h>
#include <ngx_http.h>
#include <json.h>

#define LOUNGE_PROXY_NODES_KEY "nodes"
#define LOUNGE_PROXY_SHARDMAP_KEY "shard_map"

#define FAILED_NODE_MAX_RETRY 60 * 10

enum {
	LOUNGE_PROXY_HOST_INDEX = 0,
	LOUNGE_PROXY_PORT_INDEX
};

typedef struct {
	ngx_flag_t 						enabled;
} lounge_loc_conf_t;

typedef struct {
	ngx_uint_t uri_sharded;
	int        shard_id;
} lounge_req_ctx_t;

typedef struct {
	ngx_peer_addr_t      peer;
	time_t               fail_retry_time;
	ngx_uint_t           fail_count;
} lounge_peer_t;

typedef struct {
	ngx_uint_t           shard_id;	
	ngx_uint_t           current_host_id;
    ngx_uint_t           try_i;
	lounge_peer_t        **addrs;
	ngx_uint_t           num_peers;
} lounge_proxy_peer_data_t;

typedef struct {
	ngx_uint_t 						*num_peers;
	time_t  						*fail_times;
	ngx_uint_t 						*fail_counts;
	lounge_peer_t					***shard_id_peers;
} lounge_shard_lookup_table_t;

typedef struct {
	ngx_int_t                    num_shards;
	ngx_str_t                    json_filename;
	lounge_peer_t                *couch_nodes;
	ngx_uint_t                   num_couch_nodes;
	lounge_shard_lookup_table_t  lookup_table;
} lounge_main_conf_t;

static void *lounge_create_loc_conf(ngx_conf_t *cf);
static char *lounge_merge_loc_conf(ngx_conf_t *cf, void *parent, void *child);
static ngx_int_t lounge_init(ngx_conf_t *cf);
static char *lounge(ngx_conf_t *cf, ngx_command_t *cmd, void *conf);
static ngx_int_t ngx_http_upstream_init_couch_proxy_peer(ngx_http_request_t *r, ngx_http_upstream_srv_conf_t *us);
static ngx_int_t ngx_http_upstream_get_couch_proxy_peer(ngx_peer_connection_t *pc, void *data);
static void ngx_http_upstream_free_couch_proxy_peer(ngx_peer_connection_t *pc, void *data, ngx_uint_t state);
static char *lounge_proxy(ngx_conf_t *cf, ngx_command_t *cmd, void *conf);
static char *lounge_proxy_again(ngx_conf_t *cf, ngx_command_t *cmd, void *conf);
static ngx_int_t ngx_http_upstream_init_hash(ngx_conf_t *cf, ngx_http_upstream_srv_conf_t *us);
static ngx_uint_t lounge_proxy_crc32(u_char *keydata, size_t keylen);
static void * lounge_proxy_create_main(ngx_conf_t *cf);
static ngx_int_t lounge_proxy_init_peer(ngx_http_request_t *r, ngx_http_upstream_srv_conf_t *us);
static ngx_int_t lounge_proxy_get_peer(ngx_peer_connection_t *pc, void *data);
static void lounge_proxy_free_peer(ngx_peer_connection_t *pc, void *data, ngx_uint_t state);

static ngx_command_t lounge_commands[] = {

	{   ngx_string("lounge-proxy"),
		NGX_HTTP_UPS_CONF | NGX_CONF_NOARGS,
		lounge_proxy,
		0,
		0,
		NULL },

	{   ngx_string("lounge-replication-map"),
		NGX_HTTP_MAIN_CONF | NGX_CONF_TAKE1,
		ngx_conf_set_str_slot,
		NGX_HTTP_MAIN_CONF_OFFSET,
		offsetof(lounge_main_conf_t, json_filename),
		NULL },

	{ 	ngx_string("lounge-shard-rewrite"),
		NGX_HTTP_LOC_CONF | NGX_CONF_TAKE1,
		ngx_conf_set_flag_slot,
		NGX_HTTP_LOC_CONF_OFFSET,
		offsetof(lounge_loc_conf_t, enabled),
		NULL },

	ngx_null_command
};


static ngx_http_module_t lounge_module_ctx = {
    NULL,                              		/* preconfiguration */
    lounge_init,                 			/* postconfiguration */
    lounge_proxy_create_main,  				/* create main configuration */
    NULL,                                  	/* init main configuration */
    NULL,                                	/* create server configuration */
    NULL,                                	/* merge server configuration */
    lounge_create_loc_conf,      			/* create location configration */
    lounge_merge_loc_conf        			/* merge location configration */
};


ngx_module_t lounge_module = {
    NGX_MODULE_V1,
    &lounge_module_ctx,          			/* module context */
    lounge_commands,             			/* module directives */
    NGX_HTTP_MODULE,                       	/* module type */
    NULL,                                  	/* init master */
    NULL,                                  	/* init module */
    NULL,                                  	/* init process */
    NULL,                                  	/* init thread */
    NULL,                                  	/* exit thread */
    NULL,                                  	/* exit process */
    NULL,                                  	/* exit master */
    NGX_MODULE_V1_PADDING
};


static ngx_int_t
lounge_handler(ngx_http_request_t *r)
{
	const size_t        buffer_size = 1024;
	lounge_main_conf_t *lmcf;
    lounge_loc_conf_t  *rlcf;
	lounge_req_ctx_t   *ctx;
	int                 shard_id, n;
	char                db[buffer_size], 
	                    key[buffer_size], 
	                    extra[buffer_size];
	u_char             *uri;

    rlcf = ngx_http_get_module_loc_conf(r, lounge_module);
	if (!rlcf->enabled) {
		return NGX_DECLINED;
	}

	ctx = ngx_http_get_module_ctx(r, lounge_module);
	if (!ctx) {
		ctx = ngx_pcalloc(r->pool, sizeof(lounge_req_ctx_t));
		if (!ctx) return NGX_ERROR;
		ngx_http_set_ctx(r, ctx, lounge_module);
	}

	if (ctx->uri_sharded) return NGX_DECLINED;

	/* copy the uri so we can have a null-terminated uri, letting us
	 * use sscanf with worrying about length*/
	uri = ngx_pcalloc(r->pool, r->unparsed_uri.len+1);
	ngx_memcpy(uri, r->unparsed_uri.data, r->unparsed_uri.len); 

	lmcf = ngx_http_get_module_main_conf(r, lounge_module);

	ngx_log_debug2(NGX_LOG_DEBUG_HTTP, r->connection->log, 0,
			"r->unparsed_uri.data: %s, r->unparsed_uri.len: %d", 
			r->unparsed_uri.data, r->unparsed_uri.len);
	
	/* We're expecting a URI that looks something like:
	 * /<DATABASE>/<KEY>[/<ATTACHMENT>][?<QUERYSTRING>]
	 * e.g. 	/targeting/some_key
	 *  		/targeting/some_key?vijay=hobbit
	 */
	n = sscanf((char*)uri, "/%1024[^/]/%1024[^?/]%1024[^\n]", db, key, extra);

	ngx_log_debug2(NGX_LOG_DEBUG_HTTP, r->connection->log, 0,
			"db: %s\nkey: %s\n", db, key);

	if (n < 2) {
		ngx_log_debug1(NGX_LOG_DEBUG_HTTP, r->connection->log, 0,
			"sscanf only matched %d (should have been 2-3) -- declining to rewrite this url.", n);
		return NGX_DECLINED;
	}

	/* allocate some space for the new uri */
	size_t new_uri_len = r->unparsed_uri.len + 5;  /* this gives room for up to 5 digits to mark the shard id */
	r->uri.data = ngx_pcalloc(r->pool, new_uri_len);
	if (!r->uri.data) {
		return NGX_ERROR;
	}

	/* hash the key to figure out which db shard it lives on */
	ngx_uint_t crc32 = (ngx_crc32_short((u_char *)key, 
				strlen(key)) >> 16) & 0x7fff;
	shard_id = crc32 % lmcf->num_shards;
	ctx->shard_id = shard_id;

	if (n == 2) {
		/* uri was of the form:
		 * /<DATABASE>/<KEY>
		 */
		r->uri.len = snprintf((char*)r->uri.data, 
				new_uri_len, "/%s%d/%s", db, shard_id, key);
	} else if (n == 3) {
		/* uri was either:
		 * /<DATABASE>/<KEY>/<ATTACHMENT>
		 * /<DATABASE>/<KEY><QUERYSTRING>
		 */
		r->uri.len = snprintf((char*)r->uri.data, 
				new_uri_len, "/%s%d/%s%s", db, shard_id, key, extra);
	}

	if (r->uri.len >= new_uri_len) {
		return NGX_ERROR;
	}

	/* Set a variety of flags to tell nginx that we've modified the uri */
	/* okay I was just setting shit because I didn't know which flag actually
	 * made nginx use the modified uri
	 */
	r->internal = 0;
	r->valid_unparsed_uri = 0;
	r->uri_changed = 1;
	r->quoted_uri = 0;

	ctx->uri_sharded = 1;

    return NGX_OK;
}


static void *
lounge_create_loc_conf(ngx_conf_t *cf)
{
    lounge_loc_conf_t  *conf;

    conf = ngx_pcalloc(cf->pool, sizeof(lounge_loc_conf_t));
    if (conf == NULL) {
        return NGX_CONF_ERROR;
    }

	conf->enabled = NGX_CONF_UNSET;
    return conf;
}


static char *
lounge_merge_loc_conf(ngx_conf_t *cf, void *parent, void *child)
{
    lounge_loc_conf_t *prev = parent;
    lounge_loc_conf_t *conf = child;

	ngx_conf_merge_value(conf->enabled, prev->enabled, 0);
    return NGX_CONF_OK;
}


static ngx_int_t
lounge_init(ngx_conf_t *cf)
{
    ngx_http_handler_pt        *h;
    ngx_http_core_main_conf_t  *cmcf;

    cmcf = ngx_http_conf_get_module_main_conf(cf, ngx_http_core_module);

    h = ngx_array_push(&cmcf->phases[NGX_HTTP_REWRITE_PHASE].handlers);
    if (h == NULL) {
        return NGX_ERROR;
    }

    *h = lounge_handler;

    return NGX_OK;
}


/**
 * Upstream proxying related functions
 */

static struct json_object*
lounge_proxy_parse_config(const char *filename)
{
	struct json_tokener *tok;
	struct json_object *obj;
	FILE *json_file = fopen(filename, "r");
	if (!json_file) {
		fprintf(stderr, "Couldn't open %s: %s\n", filename, strerror(errno));
		return NULL;
	}
	const size_t buf_size = 1024;
	char buffer[buf_size];
	tok = json_tokener_new();
	size_t n_read;
	do {
		n_read = fread(buffer, sizeof(char), buf_size, json_file);
		obj = json_tokener_parse_ex(tok, buffer, n_read);
	} while (!obj && (n_read == buf_size));
	json_tokener_free(tok);
	fclose(json_file);
	return obj;
}

static ngx_int_t
lounge_proxy_build_lookup_table(ngx_conf_t *cf, lounge_main_conf_t *lmcf,
		struct json_object *lounge_proxy_conf)
{
	/* Example json file looks like this: 	
	 * {
	 * 	"shard_map": [[0,1], [1,0]],
	 * 	"nodes": [ ["localhost", 5984], ["localhost", 5984] ]
	 * }
	 */

	/* TODO:  Write the lookup table to have everything in a contiguous block of memory,
	 * avoiding pointer lookups and maximizing cache hits.
	 */
	struct json_object 					*shard_map, *shard_host_array, *host;
	ngx_uint_t 							num_shards;	
	int 								host_id;
	unsigned int						i,j;
	lounge_shard_lookup_table_t 		*lounge_lookup_table;

	shard_map = json_object_object_get(lounge_proxy_conf, "shard_map");
	shard_map = json_object_object_get(lounge_proxy_conf, LOUNGE_PROXY_SHARDMAP_KEY);
	num_shards = json_object_array_length(shard_map);
	lmcf->num_shards = num_shards;

	lounge_lookup_table = ngx_pcalloc(cf->pool, sizeof(lounge_shard_lookup_table_t));
	if (!lounge_lookup_table) {
		return NGX_ERROR;
	}
	lounge_lookup_table->num_peers = ngx_pcalloc(cf->pool, sizeof(ngx_uint_t) * num_shards);
	lounge_lookup_table->shard_id_peers = ngx_pcalloc(cf->pool, 
			sizeof(lounge_peer_t **) * num_shards);

	for (i = 0; i < num_shards; i++) {
		unsigned int num_hosts;
		shard_host_array = json_object_array_get_idx(shard_map, i);
		if (!shard_host_array) {
			return NGX_ERROR;
		}
		num_hosts = json_object_array_length(shard_host_array);
		lounge_lookup_table->num_peers[i] = num_hosts;

		lounge_lookup_table->shard_id_peers[i] = ngx_pcalloc(cf->pool, 
			sizeof(lounge_peer_t *) * num_hosts);

		for (j = 0; j < num_hosts; j++) {
			host = json_object_array_get_idx(shard_host_array, j);
			if (!host) {
				return NGX_ERROR;
			}
			host_id = json_object_get_int(host);
			lounge_lookup_table->shard_id_peers[i][j] = &lmcf->couch_nodes[host_id];
		}
	}
	lmcf->lookup_table = *lounge_lookup_table;
	return NGX_OK;
}

static ngx_int_t
lounge_proxy_build_peer_list(ngx_conf_t *cf, lounge_main_conf_t *lmcf, struct json_object *couch_proxy_conf)
{
	struct json_object *couch_proxies;
	int num_proxies, proxy_index;

	couch_proxies = json_object_object_get(couch_proxy_conf, LOUNGE_PROXY_NODES_KEY);
	num_proxies = json_object_array_length(couch_proxies);

    lmcf->couch_nodes = ngx_pcalloc(cf->pool, sizeof(lounge_peer_t) * num_proxies);
    if (lmcf->couch_nodes == NULL) {
        return NGX_ERROR;
    }
	lmcf->num_couch_nodes = num_proxies;

	/* iterate through the available hosts, adding them to the peers array */
	for (proxy_index = 0; proxy_index < num_proxies; proxy_index++) {
		struct json_object *node, *_port;
		char *host;
		short port;
		ngx_url_t *u;

		u = ngx_pcalloc(cf->pool, sizeof(ngx_url_t));
		
		/* Extract the host and port from the json config */
		node = json_object_array_get_idx(couch_proxies, proxy_index);
		host = json_object_get_string(json_object_array_get_idx(node, LOUNGE_PROXY_HOST_INDEX));

		_port = json_object_array_get_idx(node, LOUNGE_PROXY_PORT_INDEX); 
		if (!_port) {
			return NGX_ERROR;
		}

		port = (short)json_object_get_int(_port);
		if (! (host && port) ) {
			return NGX_ERROR;
		}

		/* Use nginx's built in url parsing mechanisms */
		u->url.data = (u_char *)host;
		u->url.len = strlen(host);
		u->default_port = port;

		if (ngx_parse_url(cf->pool, u) != NGX_OK) {
			if (u->err) {
				ngx_conf_log_error(NGX_LOG_EMERG, cf, 0,
						"%s in upstream \"%V\"", u->err, &u->url);
			}
			return NGX_ERROR;
		}

		lmcf->couch_nodes[proxy_index].peer = *u->addrs;
	}
	return NGX_OK;
}

static ngx_int_t
lounge_proxy_init(ngx_conf_t *cf, ngx_http_upstream_srv_conf_t *us)
{
	lounge_main_conf_t *conf;
	struct json_object *lounge_proxy_conf;
	u_char *json_filename;

	conf = ngx_http_conf_get_module_main_conf(cf, lounge_module);
	
	json_filename = ngx_pcalloc(cf->pool, conf->json_filename.len+1);
	ngx_memcpy(json_filename, conf->json_filename.data, conf->json_filename.len);

	lounge_proxy_conf = lounge_proxy_parse_config((char*)json_filename);
	if (!lounge_proxy_conf) {
		return NGX_ERROR;
	}

	if (lounge_proxy_build_peer_list(cf, conf, lounge_proxy_conf) != NGX_OK) {
		json_object_put(lounge_proxy_conf);
		return NGX_ERROR;
	}

	if (!lounge_proxy_build_lookup_table(cf, conf, lounge_proxy_conf) == NGX_OK) {
		json_object_put(lounge_proxy_conf);
		return NGX_ERROR;
	}

	json_object_put(lounge_proxy_conf);

    us->peer.data = conf->couch_nodes;

	/* Callback for request initialization */
    us->peer.init = lounge_proxy_init_peer;
	return NGX_OK;
}


static ngx_int_t
lounge_proxy_init_peer(ngx_http_request_t *r,
    ngx_http_upstream_srv_conf_t *us)
{
    lounge_proxy_peer_data_t     	*lpd;
	lounge_main_conf_t 				*lmcf;	
	lounge_req_ctx_t 				*ctx;
	u_char *uri;

	ctx = ngx_http_get_module_ctx(r, lounge_module);
	if (!ctx) return NGX_ERROR;

	lmcf = ngx_http_get_module_main_conf(r, lounge_module);
	if (!lmcf) {
		return NGX_ERROR;
	}
    
    lpd = ngx_pcalloc(r->pool, sizeof(lounge_proxy_peer_data_t));
    if (lpd == NULL) {
        return NGX_ERROR;
    }

	lpd->shard_id = ctx->shard_id;
	lpd->current_host_id = 0;

	lpd->addrs = lmcf->lookup_table.shard_id_peers[lpd->shard_id];
	lpd->num_peers = lmcf->lookup_table.num_peers[lpd->shard_id];

	/* setup callbacks */
    r->upstream->peer.free = lounge_proxy_free_peer;
    r->upstream->peer.get = lounge_proxy_get_peer;

	/* set maximum number of retries */
	r->upstream->peer.tries = lmcf->lookup_table.num_peers[lpd->shard_id];

    r->upstream->peer.data = lpd;
    return NGX_OK;
}


static ngx_int_t
lounge_proxy_get_peer(ngx_peer_connection_t *pc, void *data)
{
    lounge_proxy_peer_data_t  *lpd = data;
	lounge_peer_t             *lp;
    ngx_peer_addr_t           *peer;

    ngx_log_debug1(NGX_LOG_DEBUG_HTTP, pc->log, 0,
                   "upstream_couch_proxy: get upstream request "
				   "hash peer try %ui",
				   pc->tries);
    pc->cached = 0;
    pc->connection = NULL;

	int id = lpd->current_host_id;
	lp = lpd->addrs[id];

	if (lp->fail_retry_time) {
		/* the node repeatedly failed some time previously, check if we're
		 * past the retry timeout
		 */
		if (time(NULL) > lp->fail_retry_time) {
			/* the failure was old -- try this host again */
			lp->fail_retry_time = 0;
		} else {
			/* the failure was recent, just skip this host */
			if (!--pc->tries) {
				/* we have no more hosts to try!  fail. */
				return NGX_ERROR;
			}
			lp = lpd->addrs[++lpd->current_host_id];
		}
	}

	peer = &lp->peer;
    pc->sockaddr = peer->sockaddr;
    pc->socklen = peer->socklen;
    pc->name = &peer->name;

    return NGX_OK;
}

static void
lounge_proxy_free_peer(ngx_peer_connection_t *pc, void *data,
    ngx_uint_t state)
{
    lounge_proxy_peer_data_t  *lpd = data;
	lounge_peer_t             *lp;

    ngx_log_debug1(NGX_LOG_DEBUG_HTTP, pc->log, 0, 
            "upstream_couch_proxy: free upstream hash peer try %u", pc->tries);

	ngx_uint_t id = lpd->current_host_id;
	lp = lpd->addrs[id];
    if (state & (NGX_PEER_FAILED | NGX_PEER_NEXT) 
			&& --pc->tries)
    {
		/* if this host has failed 10 times in a row, mark the time when we 
		 * should retry it
		 */
		if (++lp->fail_count > 10) {
			/* every time we fail, wait 30s more */
			int retry_timeout = (lp->fail_count - 10) * 30;
			retry_timeout = retry_timeout < FAILED_NODE_MAX_RETRY ?
				retry_timeout : FAILED_NODE_MAX_RETRY;
			lp->fail_retry_time = time(NULL) + retry_timeout;

			ngx_log_error(NGX_LOG_ALERT, pc->log, 0,
					"[%s] host %V failed %d times, removed for %d seconds",
					__FUNCTION__, &lp->peer.name, lp->fail_count,
					retry_timeout);
		}
		lpd->current_host_id++;
    } else {
		/* proxy attempt was successful -- if there was a fail count, 
		 * decrement it
		 */
		if (lp->fail_count) {
			lp->fail_count--;
		}
	}
}

static char *
lounge_proxy(ngx_conf_t *cf, ngx_command_t *cmd, void *conf)
{
    ngx_http_upstream_srv_conf_t *uscf;
    uscf = ngx_http_conf_get_module_srv_conf(cf, ngx_http_upstream_module);
	uscf->peer.init_upstream = lounge_proxy_init;
    return NGX_CONF_OK;
}

static void *
lounge_proxy_create_main(ngx_conf_t *cf)
{
	lounge_main_conf_t *conf;

	conf = ngx_pcalloc(cf->pool, sizeof(lounge_main_conf_t));
	if (conf == NULL) {
		return NGX_CONF_ERROR;
	}

	conf->json_filename = (ngx_str_t)ngx_string(NULL);
	return conf;
}


