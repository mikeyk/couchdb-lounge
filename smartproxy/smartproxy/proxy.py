#Copyright 2009 Meebo, Inc.
#
#Licensed under the Apache License, Version 2.0 (the "License");
#you may not use this file except in compliance with the License.
#You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
#Unless required by applicable law or agreed to in writing, software
#distributed under the License is distributed on an "AS IS" BASIS,
#WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#See the License for the specific language governing permissions and
#limitations under the License.

import atexit
import cjson
import cPickle
import copy
import itertools
import lounge
import os
import random
import re
import sys
import time
import urllib

from zope.interface import implements

from twisted.python import log
from twisted.internet import defer
from twisted.internet import protocol, reactor, defer, process, task, threads
from twisted.internet.interfaces import IConsumer
from twisted.protocols import basic
from twisted.web import server, resource, client
from twisted.python.failure import DefaultException

from fetcher import HttpFetcher, MapResultFetcher, DbFetcher, DbGetter, ViewFetcher, AllDbFetcher, ProxyFetcher, ChangesFetcher, UuidFetcher, getPageWithHeaders

from reducer import ReduceQueue, ReducerProcessProtocol, Reducer, AllDocsReducer, ChangesReducer, ChangesMerger

import streaming

def normalize_header(h):
	return '-'.join([word.capitalize() for word in h.split('-')])

qsre = re.compile('([^&])+=([^&]*)(?=&|$)')
def qsparse(qs):
	return dict((map(urllib.unquote, pair.groups()) for pair in qsre.finditer(qs)))

def should_regenerate(now, cached_at, cachetime):
	age = now - cached_at
	time_left = cachetime - age
	# probably is 0.0 at 5 minutes before expire, 1.0 at expire time, linear
	# in between
	if time_left > 5*60:
		return False
	if time_left <= 0:
		return True
	p = 1 - (time_left/float(5*60))
	return random.random() < p

class ClientQueue:
	def __init__(self, prefs):
		self.queue = []
		self.pool_size = prefs.get_pref("/client_pool_size")
		self.count = 0
	
	def enqueue(self, url, good_cb, bad_cb, headers=None):
		self.queue.append((url, good_cb, bad_cb, headers))
		self.next()
	
	def next(self):
		# if we have something in the queue, and an available reducer, take care of it
		if len(self.queue)>0 and self.count < self.pool_size:
			url, success, err, headers = self.queue.pop(0)

			def succeed(*args, **kwargs):
				log.debug("ClientQueue: success, queue size %d, reqs out %d" % (len(self.queue), self.count))
				self.count -= 1
				try:
					success(*args, **kwargs)
				except Exception, e:
					log.err("Exception '%s' in ClientQueue callback; moving on" % str(e))
				self.next()

			def fail(*args, **kwargs):
				log.debug("ClientQueue: failure, queue size %d, reqs out %d" % (len(self.queue), self.count))
				self.count -= 1
				try:
					err(*args, **kwargs)
				except:
					log.err("Exception in ClientQueue errback; moving on")
				self.next()

			self.count += 1
			defer = getPageWithHeaders(url, headers=headers).deferred
			defer.addCallback(succeed)
			defer.addErrback(fail)
		else:
			log.debug("ClientQueue: queue size %d, reqs out %d" % (len(self.queue), self.count))

def make_success_callback(request):
	def send_output(params):
		code, headers, doc = params
		for k in headers:
			if len(headers[k])>0:
				request.setHeader(normalize_header(k), headers[k][0])
		request.setHeader('Content-Length', str(len(doc)))
		request.setResponseCode(code)
		request.write(doc)
		request.finish()
	return send_output

def make_errback(request):
	def handle_error(s):
		# if we get back some non-http response type error, we should
		# return 500
		if hasattr(s.value, 'status'):
			status = int(s.value.status)
		else:
			status = 500
		if hasattr(s.value, 'response'):
			response = s.value.response
		else:
			response = '{}'
		request.setResponseCode(status)
		request.write(response+"\n") 
		request.finish()
	return handle_error

class HTTPProxy(resource.Resource):
	isLeaf = True

	def __init__(self, prefs):
		"""
		prefs is a lounge.prefs.Prefs instance
		persistCache is a boolean -- True means an atexit handler will be
		  registered to persist the cache to disk when the process 
			finishes.
		"""
		self.prefs = prefs

		self.reduce_queue = ReduceQueue(self.prefs)
		self.client_queue = ClientQueue(self.prefs)

		self.__last_load_time = 0
		self.__loadConfig()
		self.__loadConfigCallback = task.LoopingCall(self.__loadConfig)
		self.__loadConfigCallback.start(300)
		self._loadCache()

		self.in_progress = {}

	def _loadCache(self):
		"""
		Load the cache with any data we've persisted.  If we can't unpickle the
		cache file for any reason, this will create an empty cache.
		"""
		self.cache = {}

	def __loadConfig(self):
		conf_file = self.prefs.get_pref("/proxy_conf")
		mtime = os.stat(conf_file)[8]
		if mtime <= self.__last_load_time:
			return
		self.__last_load_time = mtime
		self.conf_data = lounge.ShardMap(conf_file)

		try:
			cache_conf = self.prefs.get_pref('/cacheable_file_path')
			cacheables = cjson.decode(file(cache_conf).read())
			self.cacheable = [(re.compile(pat), t) for pat, t in cacheables]
		except KeyError:
			self.cacheable = []

	def render_temp_view(self, request):
		"""Farm out a view query to all nodes and combine the results."""
		deferred = defer.Deferred()
		deferred.addCallback(make_success_callback(request))
		deferred.addErrback(make_errback(request))

		raw_body = request.content.read()
		body = cjson.decode(raw_body)
		reduce_fn = body.get("reduce", None)
		if reduce_fn is not None:
			reduce_fn = reduce_fn.replace("\n", " ") # TODO do we need this?

		uri = request.uri[1:]
		db, req = uri.split('/', 1)
		shards = self.conf_data.shards(db)
		reducer = Reducer(reduce_fn, len(shards), {}, deferred, self.reduce_queue)

		failed = False
		for shard in shards:
			def succeed(data):
				log.err("This should not get called?")
				pass

			def fail(data):
				if not failed:
					failed = True
					deferred.errback(data)

			shard_deferred = defer.Deferred()
			shard_deferred.addCallback(succeed)
			shard_deferred.addErrback(fail)

			nodes = self.conf_data.nodes(shard)
			urls = ['/'.join([node, req]) for node in nodes]
			fetcher = MapResultFetcher(shard, urls, reducer, deferred, self.client_queue)
			fetcher.fetch(request)

		return server.NOT_DONE_YET

	def render_view(self, request):
		"""Farm out a view query to all nodes and combine the results."""
		path = request.path[1:]
		parts = path.split('/')
		database = parts[0]
		if parts[1] == '_design':
			view = parts[4]
			design_doc = '/'.join([parts[1], parts[2]])
		else:
			view = parts[3]
			design_doc = "_design%2F" + parts[2]

		path_no_db = path[ path.find('/')+1:]
			
		#  old:  http://host:5984/db/_view/designname/viewname
		#  new:  http://host:5984/db/_design/designname/_view/viewname

		# build the query string to send to shard requests
		strip_params = ['skip'] # handled by smartproxy, do not pass to upstream nodes
		if 'skip' in request.args:
			strip_params.append('limit') # have to handle limit in smartproxy -- SLOW!!!
		args = dict([(k,v) for k,v in request.args.iteritems() if k.lower() not in strip_params])
		qs = urllib.urlencode([(k,v) for k in args for v in args[k]] or '')
		if qs: qs = '?' + qs

		# get the urls for the shard primary replicas
		primary_urls = [self._rewrite_url("/".join([host, design_doc])) for host in self.conf_data.primary_shards(database)]
		view_uri = request.path.split("/",2)[2] + qs

		#if we're already processing a request for this uri, just append this
		#request to the list -- it will be handled with the results from 
		#the previous request.
		if request.uri in self.in_progress:
			self.in_progress[request.uri].append(request)
			log.debug("Attaching request for %s to an earlier request for that uri" % request.uri)
			return server.NOT_DONE_YET
		else:
			self.in_progress[request.uri] = []

		# otherwise set up a deferred to return the result
		deferred = defer.Deferred()

		def send_output(s):
			if type(s) is tuple:
				code, headers, response = s
				for k in headers:
					request.setHeader(k, headers[k][-1])

			# write the response to all requests
			clients = itertools.chain([request], self.in_progress.pop(request.uri, []))

			for c in clients:
				c.write(response+"\n")
				c.finish()
			return s
		deferred.addCallback(send_output)

		def handle_error(s):
			# send the error to all requests
			clients = itertools.chain([request], self.in_progress.pop(request.uri, []))

			# if we get back some non-http response type error, we should
			# return 500
			if hasattr(s.value, 'status'):
				status = int(s.value.status)
			else:
				status = 500
			if hasattr(s.value, 'response'):
				response = s.value.response
			else:
				map(lambda r: r.finish(), clients)
				raise Exception(str(s))

			for c in clients:
				c.setResponseCode(status)
				c.write(response+"\n") 
				c.finish()
		deferred.addErrback(handle_error)

		# chain callback if the response should be cached
		def cache_output(s):
			self.cache[request.uri] = (time.time(), s)
			return s

		# predicate check for whether path matches a cache configuration pattern
		def cache_pred(x):
			pattern, cachetime = x
			return pattern.match(request.path)

		try:
			# don't look for cache pattern matches if stale data is not ok
			if 'ok' not in request.args.get('stale', []):
				raise StopIteration

			# otherwise check the cache predicate against known cacheable patterns
			match = itertools.dropwhile(lambda x: not cache_pred(x), self.cacheable)
			pattern, cachetime = match.next() # raises StopIteration

			if request.uri in self.cache:
				cached_at, response = self.cache[request.uri]
				# see if it is yet to expire
				now = time.time()
				cached_response = response
				# if the cache is stale and there are no other requests for this uri,
				# chain a callback to cache the response
				# otherwise
				if should_regenerate(now, cached_at, cachetime):
					log.debug("Cache of " + request.uri + " is stale. Regenerating.")
					deferred.addCallback(cache_output)
				else:
					log.debug("Using cached copy of " + request.uri)
					reactor.callLater(0, deferred.callback, cached_response)
					return server.NOT_DONE_YET
			else:
				deferred.addCallback(cache_output)
		except StopIteration:
			pass # time to make the request

		r = ReduceFunctionFetcher(self.conf_data, primary_urls, database, view_uri, view, deferred, self.client_queue, self.reduce_queue)
		r.fetch(request)
		return server.NOT_DONE_YET
	
	def render_continuous_changes(self, request, database):
		shards = self.conf_data.shards(database)

		if 'since' in request.args:
			since = cjson.decode(request.args['since'][-1])
		else:
			since = len(shards)*[0]
		
		consumer = ChangesMerger(request, since)

		shard_args = copy.copy(request.args)
		urls = []
		for i,shard in enumerate(shards):
			shard_args['since'] = [since[i]]
			qs = urllib.urlencode([(k,v) for k in shard_args for v in shard_args[k]])
			urls = [node + '/_changes?' + qs for node in self.conf_data.nodes(shard)]
			# TODO failover to the slaves
			url = urls[0]
			log.msg("connecting factory to " + url)
			factory = streaming.StreamingHTTPClientFactory(url, headers=request.getAllHeaders(), consumer=consumer, shard_idx=i)
			scheme, host, port, path = client._parse(url)
			reactor.connectTCP(host, port, factory)

		return server.NOT_DONE_YET
	
	def render_changes(self, request):
		database, changes = request.path[1:].split('/', 1)

		if 'continuous' in request.args.get('feed',['nofeed']):
			return self.render_continuous_changes(request, database)

		deferred = defer.Deferred()

		def send_output(params):
			code, headers, doc = params
			for k in headers:
				if len(headers[k])>0:
					request.setHeader(normalize_header(k), headers[k][0])
			request.setResponseCode(code)
			request.write(doc + '\n')
			request.finish()
		deferred.addCallback(send_output)

		def handle_error(s):
			# if we get back some non-http response type error, we should
			# return 500
			if request.uri in self.in_progress:
				del self.in_progress[request.uri]
			if hasattr(s.value, 'status'):
				status = int(s.value.status)
			else:
				status = 500
			if hasattr(s.value, 'response'):
				response = s.value.response
			else:
				response = '{}'
			request.setResponseCode(status)
			request.write(response+"\n") 
			request.finish()
		deferred.addErrback(handle_error)

		shards = self.conf_data.shards(database)
		seq = cjson.decode(request.args.get('since', [cjson.encode([0 for shard in shards])])[-1])
		reducer = ChangesReducer(seq, deferred)
		for shard,shard_seq in zip(shards, seq):
			nodes = self.conf_data.nodes(shard)
			shard_args = copy.copy(request.args)
			shard_args['since'] = [shard_seq]

			qs = urllib.urlencode([(k,v) for k in shard_args for v in shard_args[k]])
			urls = [node + "/_changes?" + qs for node in nodes]
			fetcher = ChangesFetcher(shard, urls, reducer, deferred, self.client_queue)
			fetcher.fetch(request)

		return server.NOT_DONE_YET
	
	def render_all_docs(self, request):
		# /database/_all_docs?stuff => database, _all_docs?stuff
		database, rest = request.path[1:].split('/', 1)
		deferred = defer.Deferred()
		deferred.addCallback(make_success_callback(request))
		deferred.addErrback(make_errback(request))

		# build the query string to send to shard requests
		strip_params = ['skip'] # handled by smartproxy, do not pass
		args = dict([(k,v) for k,v in request.args.iteritems() if k.lower() not in strip_params])
		qs = urllib.urlencode([(k,v) for k in args for v in args[k]] or '')
		if qs: qs = '?' + qs

		# this is exactly like a view with no reduce
		shards = self.conf_data.shards(database)
		reducer = AllDocsReducer(None, len(shards), request.args, deferred, self.reduce_queue)
		for shard in shards:
			nodes = self.conf_data.nodes(shard)
			urls = [self._rewrite_url("/".join([node, rest])) + qs for node in nodes]
			fetcher = MapResultFetcher(shard, urls, reducer, deferred, self.client_queue)
			fetcher.fetch(request)

		return server.NOT_DONE_YET
	
	def proxy_special(self, request):
		"""Proxy a special document to a single shard (any shard will do, but start with the first)"""
		deferred = defer.Deferred()

		def handle_success(params):
			code, headers, doc = params
			for k in headers:
				if len(headers[k])>0:
					request.setHeader(k, headers[k][0])
			request.setResponseCode(code)
			request.write(doc)
			request.finish()
		deferred.addCallback(handle_success)

		def handle_error(s):
			# if we get back some non-http response type error, we should
			# return 500
			if hasattr(s.value, 'status'):
				status = int(s.value.status)
			else:
				status = 500
			if hasattr(s.value, 'response'):
				response = s.value.response
			else:
				response = '{}'
			request.setResponseCode(status)
			request.write(response+"\n") 
			request.finish()
		deferred.addErrback(handle_error)

		database, rest = request.path[1:].split('/',1)
		_primary_urls = ['/'.join([host, rest]) for host in self.conf_data.primary_shards(database)]
		primary_urls = [self._rewrite_url(url) for url in _primary_urls]
		fetcher = ProxyFetcher("proxy", primary_urls, deferred, self.client_queue)
		fetcher.fetch(request)
		return server.NOT_DONE_YET

	def render_GET(self, request):
		if request.uri == "/ruok":
			request.setHeader('Content-Type', 'text/html')
			return "imok"

		if request.uri == "/_smartproxy_stats":
			request.setHeader('Content-Type', 'text/plain')
			status = """ClientQueue size: %d\nClient reqs out: %d\nReduceQueue size: %d\nReduce pool size: %d\n\nGETs in progress:\n""" % (len(self.client_queue.queue), self.client_queue.count, len(self.reduce_queue.queue), len(self.reduce_queue.pool))
			for uri in self.in_progress:
				status += "  * %s (%d)\n" % (uri, len(self.in_progress[uri]))
			return status + "\n"

		if request.uri == "/_all_dbs":
			db_urls = [self._rewrite_url(host + "_all_dbs") for host in self.conf_data.nodes()]
			deferred = defer.Deferred()
			deferred.addCallback(lambda s:
				(request.write(cjson.encode(s)+"\n"), request.finish()))
			all = AllDbFetcher(self.conf_data, db_urls, deferred, self.client_queue)
			all.fetch(request)
			return server.NOT_DONE_YET

		# GET /db
		if '/' not in request.uri.strip('/'):
			return self.get_db(request)

		# GET /db/_all_docs .. or ..
		# GET /db/_all_docs?options
		if request.path.endswith("/_all_docs"):
			return self.render_all_docs(request)

		# GET /db/_changes .. or ..
		# GET /db/_changes?options
		if request.path.endswith("/_changes"):
			return self.render_changes(request)

		if '/_view/' in request.uri:
			return self.render_view(request)

		# GET /db/_somethingspecial
		if re.match(r'/[^/]+/_.*', request.uri):
			return self.proxy_special(request)

		return cjson.encode({"error": "smartproxy is not smart enough for that request"})+"\n"
	
	def render_PUT(self, request):
		"""Handle a special PUT (design doc or database)"""
		# PUT /db/_somethingspecial
		if re.match(r'/[^/]+/_.*', request.uri):
			return self.proxy_special(request)
			#return cjson.encode({"stuff":"did not happen"})+"\n"

		# create all shards for a database
		if '/' not in request.uri.strip('/'):
			return self.do_db_op(request)

		return cjson.encode({"error": "smartproxy is not smart enough for that request"})+"\n"
	
	def render_POST(self, request):
		"""Create all the shards for a database."""
		db, rest = request.uri[1:], None
		if '/' in db:
			db, rest = request.uri[1:].split('/',1)
			if rest=='':
				rest = None

		# POST /db/_temp_view .. or ..
		# POST /db/_temp_view?options
		if request.uri.endswith("/_temp_view") or ("/_temp_view?" in request.uri):
			return self.render_temp_view(request)

		# POST /db/_ensure_full_commit
		if request.uri.endswith("/_ensure_full_commit"):
			return self.do_db_op(request)

		# PUT /db/_somethingspecial
		if re.match(r'/[^/]+/_.*', request.uri):
			return self.proxy_special(request)

		if rest is None:
			return self.create_doc(request)

		return cjson.encode({"error": "smartproxy is not smart enough for that request"})+"\n"
	
	def render_DELETE(self, request):
		"""Delete all the shards for a database."""
		return self.do_db_op(request)
	
	def create_doc(self, request):
		"""Create a document via POST.

		1. Ask any node for a UUID
		2. Hash the UUID to find a shard
		3. PUT the document to that shard
		"""
		db_name = request.uri[1:]
		nodes = self.conf_data.nodes()
		urls = [node + '_uuids' for node in nodes]

		body = request.content.read()
		deferred = defer.Deferred()
		deferred.addCallback(make_success_callback(request))
		deferred.addErrback(make_errback(request))
		fetcher = UuidFetcher(db_name, urls, deferred, body, self.conf_data)

		doc = cjson.decode(body)
		if '_id' in doc:
			fetcher.put_doc(doc['_id'])
		else:
			log.msg("create_doc " + request.uri + " fetching")
			fetcher.fetch(request)

		return server.NOT_DONE_YET
	
	def do_db_op(self, request):
		"""Do an operation on each shard in a database."""
		# chop off the leading /
		uri = request.path[1:]
		if '/' in uri:
			db_name, rest = uri.split('/', 1)
		else:
			db_name, rest = uri, None
		if rest=='':
			rest = None

		# make sure it's an operation we support
		if rest not in [None, '_ensure_full_commit']:
			request.setResponseCode(500)
			return cjson.encode({"error": "smartproxy got a " + request.method + " to " + request.uri + ". don't know how to handle it"})+"\n"

		# farm it out.  generate a list of resources to PUT
		shards = self.conf_data.shards(db_name)
		nodes = set() # ensures once-per-node when replicas cohabitate
		for shard in shards:
			if rest:
				nodes.update(['/'.join([db, rest]) for db in self.conf_data.nodes(shard)])
			else:
				nodes.update(self.conf_data.nodes(shard))

		deferred = defer.Deferred()
		deferred.addCallback(make_success_callback(request))
		deferred.addErrback(make_errback(request))

		f = DbFetcher(self.conf_data, nodes, deferred, request.method, self.client_queue)
		f.fetch(request)
		return server.NOT_DONE_YET
	
	def get_db(self, request):
		"""Get general information about a database."""

		# chop off the leading /
		db_name = request.uri.strip('/')
		# TODO fail over each primary shard to its replicants
		nodes = self.conf_data.primary_shards(db_name)

		deferred = defer.Deferred()
		deferred.addCallback(lambda s:
									(request.write(cjson.encode(s)+"\n"), request.finish()))
		def handle_error(s):
			# if we get back some non-http response type error, we should
			# return 500
			if hasattr(s.value, 'status'):
				status = int(s.value.status)
			else:
				status = 500
			if hasattr(s.value, 'response'):
				response = s.value.response
			else:
				response = '{}'
			request.setResponseCode(status)
			request.write(response+"\n") 
			request.finish()
		deferred.addErrback(handle_error)

		f = DbGetter(self.conf_data, nodes, deferred, db_name, self.client_queue)
		f.fetch(request)
		return server.NOT_DONE_YET

	def _rewrite_url(self, url):
		parts = url[7:].split('/')
		hostport = parts[0]
		host,port = hostport.split(':')

		new_url = url

		#  old:  http://host:5984/db/_view/designname/viewname
		#  new:  http://host:5984/db/_design/designname/_view/viewname
		if len(parts) > 2 and parts[2] == '_view':
			new_parts = [hostport, parts[1], '_design', parts[3], '_view', parts[4]]
			new_url = '/'.join(new_parts)	
			new_url = 'http://' + new_url

		if '_design%2F' in new_url:
			new_url = new_url.replace('_design%2F', '_design/')

		log.debug('_rewrite_url: "%s" => "%s"' % (url, new_url))
		return new_url

# vi: noexpandtab ts=2 sw=2 sts=2
