#!/usr/bin/python

import os
import simplejson
import urllib2

from unittest import TestCase, main

from couchstub import CouchStub
import process

def get_json(url):
  return simplejson.loads(urllib2.urlopen(url).read())

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

    doc = get_json("http://localhost:22008/test")

    be1.verify()
    be2.verify()

    self.assertEqual(doc['db_name'], 'test')
    self.assertEqual(doc['doc_count'], 15)
    self.assertEqual(doc['doc_del_count'], 3)
    self.assertEqual(doc['disk_size'], 32768)

if __name__=="__main__":
	main()
