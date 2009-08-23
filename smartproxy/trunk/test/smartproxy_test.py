#!/usr/bin/python

import logging
import os
import pycurl
import simplejson
import StringIO
import urllib2

from unittest import TestCase, main

from lounge import client

from couchstub import CouchStub
import process

class Response:
	def __init__(self, code, body, headers):
		self.code = code
		self.body = simplejson.loads(body)
		self.headers = headers

def req(url, method, body=None, headers=None):
	curl = pycurl.Curl()
	curl.setopt(pycurl.URL, url)
	outbuf = StringIO.StringIO()
	curl.setopt(pycurl.WRITEFUNCTION, outbuf.write)

	if method=='PUT':
		curl.setopt(pycurl.UPLOAD, 1)
	elif method=='POST':
		curl.setopt(pycurl.POST, 1)
	elif method=='DELETE':
		curl.setopt(pycurl.CUSTOMREQUEST, 'DELETE')
	
	headers = {}
	def parse_header(txt):
		if ': ' in txt:
			k,v = txt.strip().split(': ',1)
			headers[k] = v
	curl.setopt(pycurl.HEADERFUNCTION, parse_header)

	curl.perform()
	rv = outbuf.getvalue()
	return Response(curl.getinfo(pycurl.HTTP_CODE), outbuf.getvalue(), headers)

def get(url, body=None, headers=None):
	return req(url, "GET", body, headers)

def assert_raises(exception, function, *args, **kwargs):
	"""Make sure that when you call function, it raises exception"""
	try:
		function(*args, **kwargs)
		assert False, "Should have raised %s" % exception
	except exception:
		pass

class ProxyTest(TestCase):
	def setUp(self):
		self.smartproxy_pid = process.background("/usr/bin/twistd -l log/smartproxy.log -n -y fixtures/smartproxy.tac")
		assert process.wait_for_connect("localhost", 22008), "Smartproxy didn't start"

	def tearDown(self):
		process.stop_pid(self.smartproxy_pid)
		process.wait_for_process_exit(self.smartproxy_pid)
		try:
			os.unlink("twistd.pid")
		except OSError:
			pass

	def testNothing(self):
		"""Trivial smartproxy health check"""
		assert urllib2.urlopen("http://localhost:22008/ruok").read()=="imok"

	def testGetDB(self):
		"""Try to GET information on a database.

		smartproxy should get info on each shard and merge them together
		"""
		be1 = CouchStub()
		be1.expect_GET("/test0").reply(200, dict(
			db_name="test0", 
			doc_count=5, 
			doc_del_count=1,
			update_seq=0,
			purge_seq=0,
			compact_running=False,
			disk_size=16384,
			instance_start_time="1250979728236424"))
		be1.listen("localhost", 23456)

		be2 = CouchStub()
		be2.expect_GET("/test1").reply(200, dict(
			db_name="test1", 
			doc_count=10, 
			doc_del_count=2,
			update_seq=0,
			purge_seq=0,
			compact_running=False,
			disk_size=16384,
			instance_start_time="1250979728236424"))
		be2.listen("localhost", 34567)

		resp = get("http://localhost:22008/test")

		be1.verify()
		be2.verify()

		self.assertEqual(resp.code, 200)
		self.assertEqual(resp.body['db_name'], 'test')
		self.assertEqual(resp.body['doc_count'], 15)
		self.assertEqual(resp.body['doc_del_count'], 3)
		self.assertEqual(resp.body['disk_size'], 32768)

	def testGetMissingDB(self):
		"""Try to GET information on a missing database."""
		be1 = CouchStub()
		be1.expect_GET("/test0").reply(404, dict(error="not_found",reason="missing"))
		be1.listen("localhost", 23456)

		be2 = CouchStub()
		be2.expect_GET("/test1").reply(404, dict(error="not_found",reason="missing"))
		be2.listen("localhost", 34567)

		resp = get("http://localhost:22008/test", "GET")

		be1.verify()
		be2.verify()

		self.assertEqual(resp.code, 404)
		self.assertEqual(resp.body['error'], 'not_found')
		self.assertEqual(resp.body['reason'], 'missing')

if __name__=="__main__":
	if os.environ.get("DEBUG",False):
		console = logging.StreamHandler()
		console.setLevel(logging.DEBUG)
		logging.basicConfig(level=logging.DEBUG, handler=console)
	main()
# vi: noexpandtab ts=2 sts=2 sw=2
