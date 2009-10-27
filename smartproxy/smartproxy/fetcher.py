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
import lounge
import os
import random
import re
import sys
import time
import urllib
import urllib2
import zlib

import cjson

from twisted.python import log
from twisted.internet import defer
from twisted.internet import protocol, reactor, defer, process, task, threads
from twisted.protocols import basic
from twisted.web import server, resource, client
from twisted.python.failure import DefaultException

from reducer import Reducer

def getPageWithHeaders(url, *args, **kwargs):
	# basically a clone of client.getPage, but with a handle on the factory
	# so we can pull the headers later
	scheme, host, port, path = client._parse(url)
	factory = client.HTTPClientFactory(url, *args, **kwargs)
	reactor.connectTCP(host, port, factory)
	return factory

class HttpFetcher:
	def __init__(self, name, nodes, deferred, client_queue):
		self._name = name
		self._remaining_nodes = nodes
		self._deferred = deferred
		self.client_queue = client_queue

	def fetch(self):
		url = self._remaining_nodes[0]
		self._remaining_nodes = self._remaining_nodes[1:]
		self.client_queue.enqueue(url, self._onsuccess, self._onerror)

	def _onsuccess(self, data):
		pass

	def _onerror(self, data):
		log.msg("Unable to fetch data from node %s" % data)
		if len(self._remaining_nodes) == 0:
			log.msg("unable to fetch data from shard %s.  Failing" % self._name)
			self._deferred.errback(data)
		else:
			self.fetch()

class UuidFetcher(HttpFetcher):
	def __init__(self, db, urls, deferred, body, conf):
		HttpFetcher.__init__(self, "uuids", urls, deferred, None)
		self._db = db
		self._body = body
		self._conf = conf

	def fetch(self):
		url = self._remaining_nodes[0]
		self._remaining_nodes = self._remaining_nodes[1:]
		d = client.getPage(url)
		d.addCallback(self._onsuccess)
		d.addErrback(self._onerror)
	
	def _onsuccess(self, page):
		uuid = cjson.decode(page)["uuids"][0]
		# identify the shard for this uui
		shards = self._conf.primary_shards(self._db)
		idx = zlib.crc32(uuid, 0) % len(shards)
		shard = shards[idx]

		def succeed(data):
			self._deferred.callback((int(self.factory.status), self.factory.response_headers, data))
		def fail(data):
			self._deferred.errback(data)
		self.factory = getPageWithHeaders('/'.join([shard, uuid]), method='PUT', postdata=self._body)
		self.factory.deferred.addCallback(succeed)
		self.factory.deferred.addErrback(fail)

def getPageWithHeaders(url, *args, **kwargs):
	# basically a clone of client.getPage, but with a handle on the factory
	# so we can pull the headers later
	scheme, host, port, path = client._parse(url)
	factory = client.HTTPClientFactory(url, *args, **kwargs)
	reactor.connectTCP(host, port, factory)
	return factory

class MapResultFetcher(HttpFetcher):
	def __init__(self, shard, nodes, reducer, deferred, client_queue, body='', method='GET'):
		HttpFetcher.__init__(self, shard, nodes, deferred, client_queue)
		self._method = method
		self._reducer = reducer
		self._body = body

	def _onsuccess(self, page):
		self._reducer.process_map(page, int(self.factory.status), self.factory.response_headers)

	def fetch(self):
		url = self._remaining_nodes[0]
		self._remaining_nodes = self._remaining_nodes[1:]
		if self._method=='POST':
			self.factory = getPageWithHeaders(url=url, postdata=self._body, method=self._method)
		else:
			self.factory = getPageWithHeaders(url=url, method=self._method)
		self.factory.deferred.addCallback(self._onsuccess)
		self.factory.deferred.addErrback(self._onerror)

class DbFetcher(HttpFetcher):
	"""Perform an HTTP request on all shards in a database."""
	def __init__(self, config, nodes, deferred, method, client_queue):
		self._method = method
		self._config = config
		HttpFetcher.__init__(self, config, nodes, deferred, client_queue)

	def fetch(self):
		self._remaining = len(self._remaining_nodes)
		self._failed = False
		for url in self._remaining_nodes:
			self.factory = getPageWithHeaders(url=url, method=self._method)
			self.factory.deferred.addCallback(self._onsuccess)
			self.factory.deferred.addErrback(self._onerror)
	
	def _onsuccess(self, data):
		self._remaining -= 1
		if self._remaining < 1:
			# can't call the deferred twice
			if not self._failed:
				# rewrite the Location to be a proxied url
				for k in self.factory.response_headers:
					if k.lower()=='location':
						# http://localhost/db5 -> http://localhost/db
						url = self.factory.response_headers[k][0]
						scheme, netloc, path, params, query, fragment = urllib2.urlparse.urlparse(url)
						path = self._config.get_db_from_shard(path)
						self.factory.response_headers[k] = [urllib2.urlparse.urlunparse((scheme, netloc, path, params, query, fragment))]

				self._deferred.callback((int(self.factory.status), self.factory.response_headers, data))

	def _onerror(self, data):
		# don't retry on our all-database operations
		if not self._failed:
			# prevent from calling the errback twice
			log.msg("unable to fetch from node %s; db operation %s failed" % (data, self._name))
			self._failed = True
			self._deferred.errback(data)

class ChangesFetcher(HttpFetcher):
	def __init__(self, shard, nodes, reducer, deferred, client_queue):
		HttpFetcher.__init__(self, shard, nodes, deferred, client_queue)
		self._reducer = reducer
		self._shard = shard

	def _onsuccess(self, page):
		self._reducer.process_map(self._shard, page, self.factory.response_headers)

	def fetch(self):
		url = self._remaining_nodes[0]
		self._remaining_nodes = self._remaining_nodes[1:]
		self.factory = getPageWithHeaders(url=url, method='GET')
		self.factory.deferred.addCallback(self._onsuccess)
		self.factory.deferred.addErrback(self._onerror)

class DbGetter(DbFetcher):
	"""Get info about every shard of a database and accumulate the results."""
	def __init__(self, config, nodes, deferred, name, client_queue):
		DbFetcher.__init__(self, config, nodes, deferred, 'GET', client_queue)
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
	def __init__(self, config, nodes, database, uri, view, args, deferred, client_queue, reduce_queue, options={}):
		HttpFetcher.__init__(self, "reduce_func", nodes, deferred, client_queue)
		self._config = config
		self._view = view
		self._database = database
		self._uri = uri
		self._args = args
		self._reduce_queue = reduce_queue
		self._client_queue = client_queue
		self._failed = False

		self._do_reduce = (options.get("reduce","true")=="true")
	
	def fetch(self):
		if self._do_reduce:
			return HttpFetcher.fetch(self)
		# if reduce=false, then we don't have to pull the reduce func out
		# of the design doc.  Just go straight to the view
		return self._onsuccess("{}")

	def _onsuccess(self, page):
		design_doc = cjson.decode(page)
		reduce_func = design_doc.get("views",{}).get(self._view, {}).get("reduce", None)
		if reduce_func is not None:
			reduce_func = reduce_func.replace("\n","")
		shards = self._config.shards(self._database)
		reducer = Reducer(reduce_func, len(shards), self._args, self._deferred, self._reduce_queue)

		# make sure we don't call this deferred twice
		def handle_success(data):
			if not self._failed:
				self._deferred.callback(data)

		def handle_error(data):
			if not self._failed:
				self._failed = True
				self._deferred.errback(data)

		for shard in shards:
			shard_deferred = defer.Deferred()
			shard_deferred.addCallback(handle_success)
			shard_deferred.addErrback(handle_error)

			nodes = self._config.nodes(shard)
			if "stale" not in self._uri:
				if "?" not in self._uri:
					self._uri += "?stale=ok"
				else:
					self._uri += "&stale=ok"
			urls = ["/".join([node, self._uri]) for node in nodes]
			fetcher = MapResultFetcher(shard, urls, reducer, shard_deferred, self._client_queue)
			fetcher.fetch()

class AllDbFetcher(HttpFetcher):
	def __init__(self, config, nodes, deferred, client_queue):
		HttpFetcher.__init__(self, "_all_dbs", nodes, deferred, client_queue)
		self._config = config
	
	def _onsuccess(self, page):
		# in is a list like ["test71", "test22", "funstuff102", ...]
		# out is a list like ["test", "funstuff", ...]
		shards = cjson.decode(page)
		dbs = dict([(self._config.get_db_from_shard(shard), 1) for shard in shards])
		self._deferred.callback(dbs.keys())

class ProxyFetcher(HttpFetcher):
	"""Pass along a GET, POST, or PUT."""
	def __init__(self, name, nodes, method, headers, body, deferred, client_queue):
		HttpFetcher.__init__(self, name, nodes, deferred, client_queue)
		log.msg ('ProxyFetcher, nodes: %s' % nodes)
		self._method = method
		self._headers = headers
		self._body = body

	def fetch(self):
		url = self._remaining_nodes[0]
		self._remaining_nodes = self._remaining_nodes[1:]
		self._remaining_nodes = []
		self.factory = getPageWithHeaders(url, method=self._method, postdata=self._body, headers=self._headers)
		self.factory.deferred.addCallback(self._onsuccess)
		self.factory.deferred.addErrback(self._onerror)

	def _onsuccess(self, page):
		self._deferred.callback((int(self.factory.status), self.factory.response_headers, page))

	def _onerror(self, data):
		log.msg("unable to fetch from node %s" % self._name)
		data.printTraceback()
		log.msg("traceback? : %s" % data.getTraceback())
		log.msg("data: %s" % data)
		log.msg("dir(data): %s" % dir(data))
		self._deferred.errback(data)
# vi: noexpandtab ts=2 sts=2 sw=2
