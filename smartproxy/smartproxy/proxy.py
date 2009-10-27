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
import copy
import lounge
import os
import random
import re
import sys
import time
import urllib

import cjson

from twisted.python import log
from twisted.internet import defer
from twisted.internet import protocol, reactor, defer, process, task, threads
from twisted.protocols import basic
from twisted.web import server, resource, client
from twisted.python.failure import DefaultException

from fetcher import HttpFetcher, MapResultFetcher, DbFetcher, DbGetter, ReduceFunctionFetcher, AllDbFetcher, ProxyFetcher, ChangesFetcher, UuidFetcher

from reducer import ReduceQueue, ReducerProcessProtocol, Reducer, AllDocsReducer, ChangesReducer

def normalize_header(h):
	return '-'.join([word.capitalize() for word in h.split('-')])

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

def qsparse(qs):
	# shouldn't this be a python standard library?
	rv = []
	# ''.split('&') does not return an empty list. jerk
	if qs:
		for pair in qs.split('&'):
			k,v = pair.split('=')
			k = urllib.unquote(k)
			v = urllib.unquote(v)
			rv.append((k,v))
	return dict(rv)

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
			defer = client.getPage(url)
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

	def __init__(self, prefs, persistCache=False):
		"""
		prefs is a lounge.prefs.Prefs instance
		persistCache is a boolean -- True means an atexit handler will be
		  registered to persist the cache to disk when the process 
			finishes.
		"""
		self.prefs = prefs

		self.reduce_queue = ReduceQueue(self.prefs.get_pref("/reduce_pool_size"))
		self.client_queue = ClientQueue(self.prefs.get_pref("/client_pool_size"))

		self.__last_load_time = 0
		self.__loadConfig()
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
			log.msg ("Loaded cache from %s" % self.cache_file_path)
		except:
			log.msg ("No cache file found -- starting with an empty cache")
			self.cache = {}

	def _persistCache(self):
		"""
		Pickle the cache to disk.  If we're restarting the daemon, we don't want to
		"""
		try:
			cPickle.dump(self.cache, open(self.cache_file_path, 'w'))
			log.msg ("Persisted the view cache to %s" % self.cache_file_path)
		except:
			log.err("Failed to persist the view cache to %s" % self.cache_file_path)

	def __loadConfig(self):
		conf_file = self.prefs.get_pref("/proxy_conf")		
		mtime = os.stat(conf_file)[8]
		if mtime <= self.__last_load_time:
			return
		self.__last_load_time = mtime
		self.conf_data = lounge.ShardMap(conf_file)

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
			fetcher = MapResultFetcher(shard, urls, reducer, deferred, self.client_queue, body=raw_body, method='POST')
			fetcher.fetch()

		return server.NOT_DONE_YET

	def render_view(self, request):
		"""Farm out a view query to all nodes and combine the results."""
		request.setHeader('Content-Type', 'application/json')
		#database, _view, document, view = parse_uri(request.uri)
		uri = request.uri[1:]
		parts = uri.split('/')
		database = parts[0]
		if parts[1] == '_design':
			view = parts[4]
			design_doc = '/'.join([parts[1], parts[2]])
		else:
			view = parts[3]
			design_doc = "_design%2F" + parts[2]
		# strip query string from view name
		qs = ""
		options = {}
		if '?' in view:
			# TODO why aren't we using qsparse or something?
			view, qs = view.split("?")
			options = dict([kv.split("=") for kv in qs.split("&")])

		uri_no_db = uri[ uri.find('/')+1:]

			
		#  old:  http://host:5984/db/_view/designname/viewname
		#  new:  http://host:5984/db/_design/designname/_view/viewname

		primary_urls = [self._rewrite_url("/".join([host, design_doc])) for host in self.conf_data.primary_shards(database)]

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
					log.debug("Using cached copy of " + request.uri)
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
			log.debug("Attaching request for %s to an earlier request for that uri" % request.uri)
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

		r = ReduceFunctionFetcher(self.conf_data, primary_urls, database, uri, view, request.args, deferred, self.client_queue, self.reduce_queue, options)
		r.fetch()
		# we have a cached copy; we're just processing the request to replace it
		if cached_result:
			return cached_result
		return server.NOT_DONE_YET
	
	def render_changes(self, request):
		database, changes = request.uri[1:].split('/', 1)
		qs = ''
		if '?' in changes:
			changes, qs = changes.split('?', 1)

		deferred = defer.Deferred()

		def send_output(params):
			code, headers, doc = params
			for k in headers:
				if len(headers[k])>0:
					request.setHeader(normalize_header(k), headers[k][0])
			request.setResponseCode(code)
			request.write(doc)
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
		args = qsparse(qs)
		seq = cjson.decode(args.get('since', cjson.encode([0 for shard in shards])))
		reducer = ChangesReducer(seq, deferred)
		for shard,shard_seq in zip(shards, seq):
			nodes = self.conf_data.nodes(shard)
			shard_args = copy.copy(args)
			shard_args['since'] = shard_seq

			urls = [node + "/_changes?" + urllib.urlencode(shard_args) for node in nodes]
			fetcher = ChangesFetcher(shard, urls, reducer, deferred, self.client_queue)
			fetcher.fetch()

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
		reducer = AllDocsReducer(None, len(shards), request.args, deferred, self.reduce_queue)
		for shard in shards:
			nodes = self.conf_data.nodes(shard)
			urls = [self._rewrite_url("/".join([node, rest])) for node in nodes]
			fetcher = MapResultFetcher(shard, urls, reducer, deferred, self.client_queue)
			fetcher.fetch()

		return server.NOT_DONE_YET
	
	def proxy_special(self, request, method):
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

		database, rest = request.uri[1:].split('/',1)
		_primary_urls = ['/'.join([host, rest]) for host in self.conf_data.primary_shards(database)]
		primary_urls = [self._rewrite_url(url) for url in _primary_urls]
		body = ''
		if method=='PUT' or method=='POST':
			body = request.content.read()
		fetcher = ProxyFetcher("proxy", primary_urls, method, request.getAllHeaders(), body, deferred, self.client_queue)
		fetcher.fetch()
		return server.NOT_DONE_YET

	def render_GET(self, request):
		if request.uri == "/ruok":
			request.setHeader('Content-Type', 'text/html')
			return "imok"

		if request.uri == "/_all_dbs":
			db_urls = [self._rewrite_url(host + "_all_dbs") for host in self.conf_data.nodes()]
			deferred = defer.Deferred()
			deferred.addCallback(lambda s:
				(request.write(cjson.encode(s)+"\n"), request.finish()))
			all = AllDbFetcher(self.conf_data, db_urls, deferred, self.client_queue)
			all.fetch()
			return server.NOT_DONE_YET

		# GET /db
		if '/' not in request.uri.strip('/'):
			return self.get_db(request)

		# GET /db/_all_docs .. or ..
		# GET /db/_all_docs?options
		if request.uri.endswith("/_all_docs") or ("/_all_docs?" in request.uri):
			return self.render_all_docs(request)

		# GET /db/_changes .. or ..
		# GET /db/_changes?options
		if request.uri.endswith("/_changes") or ("/_changes?" in request.uri):
			return self.render_changes(request)

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
		# POST /db/_temp_view .. or ..
		# POST /db/_temp_view?options
		if request.uri.endswith("/_temp_view") or ("/_temp_view?" in request.uri):
			return self.render_temp_view(request)

		# POST /db/_ensure_full_commit
		if request.uri.endswith("/_ensure_full_commit"):
			return self.do_db_op(request, "POST")

		# PUT /db/_somethingspecial
		if re.match(r'/[^/]+/_.*', request.uri):
			return self.proxy_special(request, 'POST')

		if request.method=='POST' and (not '/' in request.uri[1:]):
			return self.create_doc(request)

		return cjson.encode({"error": "smartproxy is not smart enough for that request"})+"\n"
	
	def render_DELETE(self, request):
		"""Delete all the shards for a database."""
		return self.do_db_op(request, "DELETE")
	
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
		log.msg("fetching uuids from " + str(urls))
		fetcher = UuidFetcher(db_name, urls, deferred, body, self.conf_data)
		fetcher.fetch()

		return server.NOT_DONE_YET
	
	def do_db_op(self, request, method):
		"""Do an operation on each shard in a database."""
		# chop off the leading /
		uri = request.uri[1:]
		if '/' in uri:
			db_name, rest = uri.split('/', 1)
		else:
			db_name, rest = uri, None

		# make sure it's an operation we support
		if rest not in [None, '_ensure_full_commit']:
			return cjson.encode({"error": "smartproxy got a " + method + " to %s.  don't know how to handle it"})+"\n"

		# farm it out.  generate a list of resources to PUT
		shards = self.conf_data.shards(db_name)
		nodes = []
		for shard in shards:
			if rest:
				nodes += ['/'.join([db, rest]) for db in self.conf_data.nodes(shard)]
			else:
				nodes += self.conf_data.nodes(shard)

		deferred = defer.Deferred()
		deferred.addCallback(make_success_callback(request))
		deferred.addErrback(make_errback(request))

		f = DbFetcher(self.conf_data, nodes, deferred, method, self.client_queue)
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
		f.fetch()
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

		new_url = new_url.replace('?count', '?limit')
		new_url = new_url.replace('&count', '&limit')

		log.debug('_rewrite_url: "%s" => "%s"' % (url, new_url))
		return new_url


if __name__ == "__main__":
	from copy import deepcopy
	import unittest
	import os
	from lounge.prefs import Prefs

	class HTTPProxyTestCase(unittest.TestCase):

		def setUp(self):
			self.prefs = Prefs(os.environ.get("PREFS", '/var/lounge/etc/smartproxy/smartproxy.xml'))

		def testCaching(self):
			cache_file = self.prefs.get_pref("/cache_file_path")
			try:
				os.unlink(cache_file)
				log.msg("deleted old cache file (%s)" % cache_file)
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

# vi: noexpandtab ts=2 sw=2 sts=2
