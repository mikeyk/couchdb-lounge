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
import cPickle
import logging
import lounge
import os
import random
import re
import sys
import time
import urllib

import cjson

from twisted.internet import defer
from twisted.internet import protocol, reactor, defer, process, task, threads
from twisted.protocols import basic
from twisted.web import server, resource, client
from twisted.python.failure import DefaultException

def to_reducelist(stuff):
	return [[[row["key"], ""], row["value"]] for row in stuff.get("rows",[])]

def split_by_key(rows):
	rv = []
	cur = []
	prev = None
	for row in rows:
		if row["key"]!=prev:
			if len(cur)>0:
				rv.append((prev, {"rows": cur}))
			cur = []
		cur.append(row)
		prev = row["key"]
	if len(cur)>0:
		rv.append((prev, {"rows": cur}))
	return rv

def parse_uri(uri):
	query = ''
	if uri.find('?')>=0:
		path, query = uri.split('?',1)
	else:
		path = uri
	if path[0]=='/':
		path = path[1:]
	database, _view, document, view = path.split('/', 3)
	assert _view=='_view'
	return database, _view, document, view

def dup_merge(rows1, rows2, compare=cmp):
	"""Merge the two results, preserving duplicates"""
	out = []
	i,j = 0,0
	while i<len(rows1) and j<len(rows2):
		if compare(rows1[i]["key"], rows2[j]["key"])<0:
			out.append(rows1[i])
			i += 1
		else:
			out.append(rows2[j])
			j += 1
	if i<len(rows1):
		out += rows1[i:]
	if j<len(rows2):
		out += rows2[j:]
	return out

def unique_merge(rows1, rows2, compare=cmp):
	"""Merge the results from r2 into r1, removing duplicates."""
	out = []
	i,j = 0,0
	while i<len(rows1) and j<len(rows2):
		if compare(rows1[i]["key"], rows2[j]["key"])<0:
			out.append(rows1[i])
		else:
			out.append(rows2[j])
		# advance both until we no longer match
		while i<len(rows1) and compare(rows1[i]["key"], out[-1]["key"])==0:
			i += 1
		while j<len(rows2) and compare(rows2[j]["key"], out[-1]["key"])==0:
			j += 1
	if i<len(rows1):
		out += rows1[i:]
	if j<len(rows2):
		out += rows2[j:]
	return out

def merge(r1, r2, compare=cmp, unique=False):
	"""Merge the results from r2 into r1."""
	rows1 = r1["rows"]
	rows2 = r2["rows"]
	merge_fn = unique and unique_merge or dup_merge
	r1["rows"] = merge_fn(rows1, rows2, compare)
	if "total_rows" in r2:
		if not ("total_rows" in r1):
			r1["total_rows"] = 0
		r1["total_rows"] += r2["total_rows"]
	if "offset" in r2:
		if not ("offset" in r1):
			r1["offset"] = 0
		r1["offset"] += r2["offset"]
	return r1

class ClientQueue:
	def __init__(self, pool_size):
		self.queue = []
		self.pool_size = pool_size
		self.count = 0
	
	def enqueue(self, url, good_cb, bad_cb):
		self.queue.append((url, good_cb, bad_cb))
		self.next()
	
	def next(self):
		# if we have something in the queue, and an available reducer, take care of it
		if len(self.queue)>0 and self.count < self.pool_size:
			url, success, err = self.queue.pop(0)

			def succeed(*args, **kwargs):
				logging.debug("ClientQueue: success, queue size %d, reqs out %d" % (len(self.queue), self.count))
				self.count -= 1
				try:
					success(*args, **kwargs)
				except:
					logging.exception("Exception in ClientQueue callback; moving on")
				self.next()

			def fail(*args, **kwargs):
				logging.debug("ClientQueue: failure, queue size %d, reqs out %d" % (len(self.queue), self.count))
				self.count -= 1
				try:
					err(*args, **kwargs)
				except:
					logging.exception("Exception in ClientQueue errback; moving on")
				self.next()

			self.count += 1
			defer = client.getPage(url)
			defer.addCallback(succeed)
			defer.addErrback(fail)
		else:
			logging.debug("ClientQueue: queue size %d, reqs out %d" % (len(self.queue), self.count))

class ReduceQueue:
	def __init__(self, pool_size):
		self.queue = []
		self.pool = []
		self.started = False
		self.pool_size = pool_size
		self.process = "/usr/bin/couchjs /usr/share/couchdb/server/main.js".split()
	
	def start_reducers(self):
		# we can't do this until after the reactor starts.
		for i in range(self.pool_size):
			rPP = ReducerProcessProtocol()
			reactor.spawnProcess(rPP, self.process[0], self.process)
		self.started = True

	def enqueue(self, keys, lines, cb):
		# Accept some data for the reducer pool.
		if not self.started:
			self.start_reducers()
		self.queue.append((keys, lines, cb))
		self.next()
	
	def return_to_pool(self, reducer):
		# A reducer calls this when it's finished.
		self.pool.append(reducer)
		self.next()
	
	def next(self):
		# if we have something in the queue, and an available reducer, take care of it
		if len(self.queue)>0 and len(self.pool)>0:
			keys, lines, cb = self.queue.pop(0)
			reducer = self.pool.pop(0)

			def reduce_finished(response):
				logging.debug("ReduceQueue: success, queue size %d, pool size %d" % (len(self.queue), len(self.pool)))
				self.return_to_pool(reducer)
				cb(response)

			def reduce_failed(*args, **kwargs):
				logging.debug("ReduceQueue: failure, queue size %d, pool size %d" % (len(self.queue), len(self.pool)))
				self.return_to_pool(reducer)

			reducer.feed(keys, lines, reduce_finished, reduce_failed)
		else:
			logging.debug("ReduceQueue: success, queue size %d, pool size %d" % (len(self.queue), len(self.pool)))

class ReducerProcessProtocol(protocol.ProcessProtocol):
	def feed(self, keys, lines, fn, err_fn):
		self._deferred = defer.Deferred()
		self._deferred.addCallback(fn)
		self._deferred.addErrback(err_fn)

		self.keys = keys
		self.lines = lines
		self.response = ""

		for line in self.lines:
			logging.debug("Sending line to reducer %s" % line)
			self.transport.writeToChild(0, line + "\r\n")
		logging.debug("done sending data")

	def connectionMade(self):
		# tell the reduce queue we are ready for action
		reduce_queue.return_to_pool(self)

	def childDataReceived(self, childFD, response):
		logging.debug("Received data from reducer %s" % response)
		if childFD == 1:
			self.response += response
			# should get one line back for each line we sent (plus one for trailing newline)
			response_lines = len(self.response.split("\n"))
			if response_lines>len(self.lines):
				self._deferred.callback( (self.keys, self.response) )

class Reducer:
	def __init__(self, reduce_func, num_entries, args, deferred):
		self.reduce_func = reduce_func
		self.num_entries_remaining = num_entries
		self.process = "/usr/bin/couchjs /usr/share/couchdb/server/main.js".split()
		self.queue = []
		self.reduce_deferred = deferred
		self.reduces_out = 0
		self.count = None
		if 'count' in args:
			self.count = int(args['count'][0])

	def process_map(self, data):
		#TODO: check to make sure this doesn't go less than 0
		self.num_entries_remaining = self.num_entries_remaining - 1
		try:
			results = cjson.decode(data)
		except:
			logging.exception('Could not json decode: %s' % data)
			results = {'rows': []}
		#result => {'rows' : [ {key: key1, value:value1}, {key:key2, value:value2}]}
		self.queue_data(results)

	def process_reduce(self, args):
		self.reduces_out -= 1
		keys, data = args
		entries = data.split("\n")
		logging.debug("in process reduce: %s %s" % (keys, entries))
		results = [cjson.decode(entry) for entry in entries if len(entry) > 0]
		#keys = [key1, key2]
		#results = [ [success_for_key1, [value_from_fn1, value_from_fn2]], [success_for_key2, [value_from_fn1, value_from_fn2]]]
		r = []
		for k, v in zip(keys, [val[0] for s,val in results]):
			r.append( dict(key=k, value=v) )
		self.queue_data(dict(rows=r))

	def queue_data(self, data):
		self.queue.append(data)
		self.__reduce()
	
	def _do_reduce(self, a, b):
		"""Actually combine two documents into one.

		Override this to get different reduce behaviour.
		"""
		inp = merge(a, b) #merge two sorted lists together

		if self.reduce_func:
			args = [ (key, ["reduce", [self.reduce_func], to_reducelist(chunk)]) for key,chunk in split_by_key(inp["rows"])]
			lines = [cjson.encode(chunk) for key, chunk in args]
			keys = [key for key,chunk in args]
			#TODO: maybe this could be lines,keys = zip(*(key, simplejson.dumps(chunk) for key, chunk in args))
			self.reduces_out += 1
			reduce_queue.enqueue(keys, lines, self.process_reduce)
		else:
			# no reduce function; just merge
			self.queue_data(inp)

	def __reduce(self):
		"""Pull stuff off the queue."""
		#only need to reduce if there is more than one item in the queue
		if len(self.queue) == 1:
			#if we've received all the results from all the shards
			#and the queue only has 1 element in it, then we're done reducing
			if self.num_entries_remaining == 0 and self.reduces_out == 0:
				# if this was a count query, slice stuff off
				if self.count is not None:
					self.queue[0]['rows'] = self.queue[0]['rows'][0:self.count]
				self.reduce_deferred.callback(cjson.encode(self.queue[0]))
			return

		a,b = self.queue[:2]
		self.queue = self.queue[2:]
		# hand the work off to _do_reduce, which we can override
		self._do_reduce(a,b)

	def get_deferred(self):
		return self.reduce_deferred

class AllDocsReducer(Reducer):
	def _do_reduce(self, a, b):
		# merge and unique.  no reduce
		self.queue_data(merge(a, b, unique=True))

class HttpFetcher:
	def __init__(self, name, nodes, deferred):
		self._name = name
		self._remaining_nodes = nodes
		self._deferred = deferred

	def fetch(self):
		url = self._remaining_nodes[0]
		self._remaining_nodes = self._remaining_nodes[1:]
		client_queue.enqueue(url, self._onsuccess, self._onerror)

	def _onsuccess(self, data):
		pass

	def _onerror(self, data):
		logging.warning("Unable to fetch data from node %s" % data)
		if len(self._remaining_nodes) == 0:
			logging.warning("unable to fetch data from shard %s.  Failing" % self._name)
			self._deferred.errback(data)
		else:
			self.fetch()

class MapResultFetcher(HttpFetcher):

	def __init__(self, shard, nodes, reducer, deferred):
		HttpFetcher.__init__(self, shard, nodes, deferred)
		self._reducer = reducer

	def _onsuccess(self, page):
		self._reducer.process_map(page)

class DbFetcher(HttpFetcher):
	"""Perform an HTTP request on all shards in a database."""
	def __init__(self, config, nodes, deferred, method):
		self._method = method
		HttpFetcher.__init__(self, config, nodes, deferred)

	def fetch(self):
		self._remaining = len(self._remaining_nodes)
		self._failed = False
		for url in self._remaining_nodes:
			deferred = client.getPage(url = url, method=self._method)
			deferred.addCallback(self._onsuccess)
			deferred.addErrback(self._onerror)
	
	def _onsuccess(self, data):
		self._remaining -= 1
		if self._remaining < 1:
			# can't call the deferred twice
			if not self._failed:
				self._deferred.callback(data)

	def _onerror(self, data):
		# don't retry on our all-database operations
		if not self._failed:
			# prevent from calling the errback twice
			logging.warning("unable to fetch from node %s; db operation %s failed" % (data, self._name))
			self._failed = True
			self._deferred.errback(data)

class DbGetter(DbFetcher):
	"""Get info about every shard of a database and accumulate the results."""
	def __init__(self, config, nodes, deferred, name):
		DbFetcher.__init__(self, config, nodes, deferred, 'GET')
		self._acc = {"db_name": name, "doc_count": 0, "doc_del_count": 0, "update_seq": 0, "purge_seq": 0, "compact_running": False, "disk_size": 0,
			"compact_running_shards": [], # if compact is running, which shards?
			"update_seq_shards": {},      # aggregate update_seq isn't really relevant
			"purge_seq_shards": {},       # ditto purge_seq
			}
	
	def _onsuccess(self, data):
		# accumulate results
		res = cjson.decode(data)
		self._acc["doc_count"] += res.get("doc_count",0)
		self._acc["doc_del_count"] += res.get("doc_del_count",0)
		self._acc["disk_size"] += res.get("disk_size",0)
		self._acc["compact_running"] = self._acc["compact_running"] or res.get("compact_running", False)
		if res.get("compact_running", False):
			self._acc["compact_running_shards"].append(res["db_name"])

		# these will be kinda meaningless...
		if "update_seq" in res:
			# so we aggregate per-shard update/purge sequences
			self._acc["update_seq_shards"][res["db_name"]] = res["update_seq"]
			if res["update_seq"] > self._acc["update_seq"]:
				self._acc["update_seq"] = res["update_seq"]
		if "purge_seq" in res:
			self._acc["purge_seq_shards"][res["db_name"]] = res["purge_seq"]
			if res["purge_seq"] > self._acc["purge_seq"]:
				self._acc["purge_seq"] = res["purge_seq"]

		self._remaining -= 1
		if self._remaining < 1:
			self._deferred.callback(self._acc)

class ReduceFunctionFetcher(HttpFetcher):
	def __init__(self, config, nodes, database, uri, view, args, deferred):
		HttpFetcher.__init__(self, "reduce_func", nodes, deferred)
		self._config = config
		self._view = view
		self._database = database
		self._uri = uri
		self._args = args

	def _onsuccess(self, page):
		design_doc = cjson.decode(page)
		reduce_func = design_doc.get("views",{}).get(self._view, {}).get("reduce", None)
		if reduce_func is not None:
			reduce_func = reduce_func.replace("\n","")
		shards = self._config.shards(self._database)
		reducer = Reducer(reduce_func, len(shards), self._args, self._deferred)

		for shard in shards:
			nodes = self._config.nodes(shard)
			urls = ["/".join([node, self._uri]) for node in nodes]
			fetcher = MapResultFetcher(shard, urls, reducer, self._deferred)
			fetcher.fetch()

class AllDbFetcher(HttpFetcher):
	def __init__(self, config, nodes, deferred):
		HttpFetcher.__init__(self, "_all_dbs", nodes, deferred)
		self._config = config
	
	def _onsuccess(self, page):
		# in is a list like ["test71", "test22", "funstuff102", ...]
		# out is a list like ["test", "funstuff", ...]
		shards = cjson.decode(page)
		dbs = dict([(self._config.get_db_from_shard(shard), 1) for shard in shards])
		self._deferred.callback(dbs.keys())

class ProxyFetcher(HttpFetcher):
	"""Pass along a GET, POST, or PUT."""
	def __init__(self, name, nodes, method, body, deferred):
		HttpFetcher.__init__(self, name, nodes, deferred)
		self._method = method
		self._body = body

	def fetch(self):
		url = self._remaining_nodes[0]
		self._remaining_nodes = self._remaining_nodes[1:]
		self._remaining_nodes = []
		deferred = client.getPage(url = url, method=self._method, postdata=self._body)
		deferred.addCallback(self._onsuccess)
		deferred.addErrback(self._onerror)

	def _onsuccess(self, page):
		self._deferred.callback(page)

	def _onerror(self, data):
		logging.warning("unable to fetch from node %s" % self._name)
		self._deferred.errback(data)

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

class HTTPProxy(resource.Resource):
	isLeaf = True

	def __init__(self, prefs, persistCache=False):
		"""
		prefs is a lounge.prefs.Prefs instance
		persistCache is a boolean -- True means an atexit handler will be
		  registered to persist the cache to disk when the process 
			finishes.
		"""
		self.prefs = prefs

		global reduce_queue
		global client_queue
		reduce_queue = ReduceQueue(self.prefs.get_pref("/reduce_pool_size"))
		client_queue = ClientQueue(self.prefs.get_pref("/client_pool_size"))

		self.__last_load_time = 0
		self.__loadConfig
		self.__loadConfigCallback = task.LoopingCall(self.__loadConfig)
		self.__loadConfigCallback.start(300)
		# list of cacheable URI patterns
		self.cacheable = []
		# TODO load this from a config file
		# cache poll responses for 15 minutes
		self.cacheable.append((re.compile(r'^/facts/_view/polls/responses.*$'), 20*60))
		# cache all (active, inactive) polls for 5 minutes
		self.cacheable.append((re.compile(r'^/facts/_view/polls/all.*$'), 10*60))
		# cache all (active, inactive) polls for 30 minutes
		self.cacheable.append((re.compile(r'^/facts/_view/polls/active_by_category.*$'), 30*60))

		self.cache_file_path = self.prefs.get_pref("/cache_file_path")
		self._loadCache()
		if persistCache:
			atexit.register(self._persistCache)

		self.in_progress = {}

	def _loadCache(self):
		"""
		Load the cache with any data we've persisted.  If we can't unpickle the
		cache file for any reason, this will create an empty cache.
		"""
		try:
			self.cache = cPickle.load(file(self.cache_file_path))
			logging.info ("Loaded cache from %s" % self.cache_file_path)
		except:
			logging.info ("No cache file found -- starting with an empty cache")
			self.cache = {}

	def _persistCache(self):
		"""
		Pickle the cache to disk.  If we're restarting the daemon, we don't want to
		"""
		try:
			cPickle.dump(self.cache, open(self.cache_file_path, 'w'))
			logging.info ("Persisted the view cache to %s" % self.cache_file_path)
		except:
			logging.exception("Failed to persist the view cache to %s" % self.cache_file_path)

	def __loadConfig(self):
		conf_file = self.prefs.get_pref("/proxy_conf")		
		mtime = os.stat(conf_file)[8]
		if mtime <= self.__last_load_time:
			return
		self.__last_load_time = mtime
		json_data = cjson.decode(open(conf_file).read())
		self.conf_data = lounge.ShardMap()

	def render_view(self, request):
		"""Farm out a view query to all nodes and combine the results."""
		request.setHeader('Content-Type', 'application/json')
		database, _view, document, view = parse_uri(request.uri)

		primary_urls = ["/".join([host, "_design%2F" + document]) for host in self.conf_data.primary_shards(database)]

		uri = request.uri.split("/",2)[2]

		# check if this uri is cacheable
		request.cache = None
		request.cache_only = False
		cached_result = None
		for pattern, cachetime in self.cacheable:
			if pattern.match(request.uri):
				request.cache = self.cache
				# if so see if it matches something in the cache
				if request.uri in self.cache:
					cached_at, response = self.cache[request.uri]
					# if so see if it is yet to expire
					now = time.time()
					logging.debug("Using cached copy of " + request.uri)
					request.setHeader('Content-Type', 'application/json')
					cached_result = response
					# if the cache has expired and we don't already have a request for this uri running,
					# we set the cache_only flag, otherwise we just return what's in the cache.
					if should_regenerate(now, cached_at, cachetime) and not request.uri in self.in_progress:
						request.cache_only = True
					else:
						return cached_result

		#if we're already processing a request for this uri, just append this
		#request to the list -- it will be handled with the results from 
		#the previous request.
		if request.uri in self.in_progress:
			self.in_progress[request.uri].append(request)
			logging.debug("Attaching request for %s to an earlier request for that uri" % request.uri)
			return server.NOT_DONE_YET
		else:
			self.in_progress[request.uri] = []

		deferred = defer.Deferred()
		# if the request is cacheable, save it
		def send_output(s):
			if request.cache is not None:
				request.cache[request.uri] = (time.time(), s+"\n")
			if not request.cache_only:
				request.write(s+"\n")
				request.finish()

			#check if any other requests came in for this uri while this request
			#was being processed
			if request.uri in self.in_progress:
				for r in self.in_progress[request.uri]:
					r.write(s+"\n")
					r.finish()
				del self.in_progress[request.uri]
		deferred.addCallback(send_output)

		def handle_error(s):
			# if we get back some non-http response type error, we should
			# return 500
			if hasattr(s.value, 'status'):
				status = s.value.status
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

		r = ReduceFunctionFetcher(self.conf_data, primary_urls, database, uri, view, request.args, deferred)
		r.fetch()
		# we have a cached copy; we're just processing the request to replace it
		if cached_result:
			return cached_result
		return server.NOT_DONE_YET
	
	def render_all_docs(self, request):
		# /database/_all_docs?stuff => database, _all_docs?stuff
		database, rest = request.uri[1:].split('/', 1)
		deferred = defer.Deferred()
		deferred.addCallback(lambda s:
									(request.write(s+"\n"), request.finish()))

		deferred.addErrback(lambda s:
								   (request.write(str(s)), request.finish()))

		# this is exactly like a view with no reduce
		shards = self.conf_data.shards(database)
		reducer = AllDocsReducer(None, len(shards), request.args, deferred)
		for shard in shards:
			nodes = self.conf_data.nodes(shard)
			urls = ["/".join([node, rest]) for node in nodes]
			fetcher = MapResultFetcher(shard, urls, reducer, deferred)
			fetcher.fetch()

		return server.NOT_DONE_YET
	
	def proxy_special(self, request, method):
		"""Proxy a special document to a single shard (any shard will do, but start with the first)"""
		deferred = defer.Deferred()
		deferred.addCallback(lambda s:
									(request.write(s+"\n"), request.finish()))

		deferred.addErrback(lambda s:
								   (request.setResponseCode(int(s.value.status)), 
									 request.write(s.value.response+"\n"), 
									 request.finish()))

		database, rest = request.uri[1:].split('/',1)
		primary_urls = ['/'.join([host, rest]) for host in self.conf_data.primary_shards(database)]
		body = ''
		if method=='PUT' or method=='POST':
			body = request.content.read()
		fetcher = ProxyFetcher("proxy", primary_urls, method, body, deferred)
		fetcher.fetch()
		return server.NOT_DONE_YET

	def render_GET(self, request):
		if request.uri == "/ruok":
			request.setHeader('Content-Type', 'text/html')
			return "imok"

		if request.uri == "/_all_dbs":
			db_urls = [host + "_all_dbs" for host in self.conf_data.nodes()]
			deferred = defer.Deferred()
			deferred.addCallback(lambda s:
				(request.write(cjson.encode(s)+"\n"), request.finish()))
			all = AllDbFetcher(self.conf_data, db_urls, deferred)
			all.fetch()
			return server.NOT_DONE_YET

		# GET /db
		if '/' not in request.uri.strip('/'):
			return self.get_db(request)

		# GET /db/_all_docs .. or ..
		# GET /db/_all_docs?options
		if request.uri.endswith("/_all_docs") or ("/_all_docs?" in request.uri):
			return self.render_all_docs(request)

		if '/_view/' in request.uri:
			return self.render_view(request)

		# GET /db/_somethingspecial
		if re.match(r'/[^/]+/_.*', request.uri):
			return self.proxy_special(request, 'GET')

		return cjson.encode({"error": "smartproxy is not smart enough for that request"})+"\n"
	
	def render_PUT(self, request):
		"""Handle a special PUT (design doc or database)"""
		# PUT /db/_somethingspecial
		if re.match(r'/[^/]+/_.*', request.uri):
			return self.proxy_special(request, 'PUT')
			#return cjson.encode({"stuff":"did not happen"})+"\n"

		# create all shards for a database
		if '/' not in request.uri.strip('/'):
			return self.do_db_op(request, "PUT")

		return cjson.encode({"error": "smartproxy is not smart enough for that request"})+"\n"
	
	def render_POST(self, request):
		"""Create all the shards for a database."""
		# PUT /db/_somethingspecial
		if re.match(r'/[^/]+/_.*', request.uri):
			return self.proxy_special(request, 'POST')

		return cjson.encode({"error": "smartproxy is not smart enough for that request"})+"\n"
	
	def render_DELETE(self, request):
		"""Delete all the shards for a database."""
		return self.do_db_op(request, "DELETE")
	
	def do_db_op(self, request, method):
		"""Do an operation on each shard in a database."""
		# chop off the leading /
		db_name = request.uri.strip('/')

		# if there's another /, we are trying to put a document. very bad!
		if '/' in db_name:
			return cjson.encode({"error": "smartproxy got a " + method + " that's not a database.  whoops."})+"\n"

		# farm it out.  generate a list of resources to PUT
		shards = self.conf_data.shards(db_name)
		nodes = []
		for shard in shards:
			nodes += self.conf_data.nodes(shard)

		deferred = defer.Deferred()
		deferred.addCallback(lambda s:
									(request.write("{\"ok\":true}\n"), request.finish()))

		deferred.addErrback(lambda s:
								   (request.setResponseCode(int(s.value.status)), 
									 request.write(s.value.response+"\n"), 
									 request.finish()))

		f = DbFetcher(self.conf_data, nodes, deferred, method)
		f.fetch()
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
		deferred.addErrback(lambda s:
								   (request.setResponseCode(int(s.value.status)), 
									 request.write(s.value.response+"\n"), 
									 request.finish()))

		f = DbGetter(self.conf_data, nodes, deferred, db_name)
		f.fetch()
		return server.NOT_DONE_YET


if __name__ == "__main__":
	from copy import deepcopy
	import unittest
	import os
	from lounge.prefs import Prefs
	logging.basicConfig(level=logging.DEBUG)

	class HTTPProxyTestCase(unittest.TestCase):

		def setUp(self):
			self.prefs = Prefs(os.environ.get("PREFS", '/var/lounge/etc/smartproxy/smartproxy.xml'))

		def testCaching(self):
			cache_file = self.prefs.get_pref("/cache_file_path")
			try:
				os.unlink(cache_file)
				logging.info("deleted old cache file (%s)" % cache_file)
			except:
				pass

			test_cache = {'key1':'value1', 'key2':'value2'}
			hp = HTTPProxy(self.prefs)
			assert (hp.cache == {})
			hp.cache = deepcopy(test_cache)
			hp._persistCache()
			hp = HTTPProxy(self.prefs)
			assert (hp.cache == test_cache)

	unittest.main()

# vi: noexpandtab ts=2 sw=2
