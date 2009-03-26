#!/usr/bin/python

import logging
import os
import sys
import time

# prepend the location of the local python-lounge
# the tests will find the local copy first, so we don't
# have to install system-wide before running tests.
sys.path = ['..'] + sys.path

from unittest import TestCase, main
from test_helpers import *

class LoungeTestCase(TestCase):
	"""Guarantees a clean database for each test."""
	def setUp(self):
		create_test_db("pytest")

	def tearDown(self):
		db = Database.find("pytest")
		db.destroy()

	def testBasics(self):
		"""Test some basic read/write operations."""
		a = TestDoc.create("a", x=1, y=1)
		b = TestDoc.create("b", x=2, y=4)
		c = TestDoc.create("c", x=3, y=9)
		d = TestDoc.create("d", x=4, y=16)

		db = Database.find("pytest")
		assert db.doc_count==4, "pytest should have 4 records but has %d" % db.doc_count

		# delete some records
		b.destroy()
		d.destroy()

		assert_raises(NotFound, TestDoc.find, "b")

		db.reload()
		assert db.doc_count==2, "pytest should have 2 records but has %d" % db.doc_count

		# test _all_docs
		view = AllDocView.execute("pytest")
		assert view.total_rows==2
		assert len(view.rows)==2
		assert view.rows[0][0]=='a'
		assert view.rows[1][0]=='c'

	def testAllShards(self):
		"""Write to and read from each shard."""
		# put something in each shard
		for i,key in enumerate(shard_keys):
			x = TestDoc.create(key, index=i)

		for i,key in enumerate(shard_keys):
			y = TestDoc.find(key)
			assert y.index==i, "Shard %d: expected %d, got %d" % (i, i, TestDoc.index)

		for i,key in enumerate(shard_keys):
			TestDoc.find(key).destroy()
	
	def testMultiKey(self):
		# test PUT
		a = MultiKey.create("one", "two", x=2, y=3)
		# GET
		x = MultiKey.find("one", "two")
		assert x._id=="one:two"
		assert x.x==2
		assert x.y==3

		# PUT (update)
		x.z = 4
		x.save()
		assert MultiKey.find("one", "two").z==4

		# and DELETE
		MultiKey.find("one", "two").destroy()
		assert_raises(NotFound, MultiKey.find, "one", "two")

	def testUpdate(self):
		"""Try to update a record ..."""
		a = TestDoc.create("a11", x=1, y=2)
		a.save()

		assert a.x == 1
		a.update(dict(x=3,z=3))

		assert a.x == 3
		assert a.z == 3

	def testContains(self):
		"""Check if the 'in' operator works """
		a = TestDoc.create("a22", x=1)
		assert 'x' in a
		assert 'y' not in a

	def testSetGetItem(self):
		"""Test the implementation of self[key]"""
		a = TestDoc.create("a")
		a['x'] = "hi"
		assert 'x' in a
		assert a['x'] == "hi"

	def testSaveAgain(self):
		"""Create a document, then try saving again.

		If our method of getting revision numbers is wrong, the second save
		will raise an exception.
		"""
		a = TestDoc.create("a", x=1, y=2)
		a.z = 3
		a.save()
		a.w = 4
		a.save()
		aa = TestDoc.find("a")
		assert aa.z==3
		assert aa.w==4

		# clean up
		TestDoc.find("a").destroy()
	
	def testConflict(self):
		"""Try to save conflicting revisions of a document."""
		TestDoc.create("a1", x=1, y=2)
		a1, a2 = TestDoc.find("a1"), TestDoc.find("a1")
		a1.y = 3
		a1.save()
		a2.x = 1
		# This save is out of date.  We should get a RevisionConflict
		assert_raises(RevisionConflict, a2.save)

		# clean up
		TestDoc.find("a1").destroy()
	
	def testTempView(self):
		"""Run a temporary view."""
		k1, k2, k3, k4, k5 = shard0_keys[0:5]

		TestDoc.create(k1, x=1, y=2)
		TestDoc.create(k2, x=1, y=4)
		TestDoc.create(k3, x=2, y=6)
		TestDoc.create(k4, x=3, y=8)
		TestDoc.create(k5, x=4, y=10)

		view = TempView.execute('pytest', language="javascript", map="(function (doc) {if (doc.x == 1) {emit(doc._id, doc.y);}})")
		assert view.total_rows==2
		assert len(view.rows)==2
		assert view.rows[0]==(k1, 2)
		assert view.rows[1]==(k2, 4)

		# add another doc and make sure our results update appropriately
		doc3 = TestDoc.find(k3)
		doc3.x = 1
		doc3.save()

		view = TempView.execute('pytest', language="javascript", map="(function (doc) {if (doc.x == 1) {emit(doc._id, doc.y);}})")
		assert view.total_rows==3
		assert view.rows[2]==(k3, 6)

		# clean up for the next test
		for k in [k1,k2,k3,k4,k5]:
			TestDoc.find(k).destroy()
	
	def testView(self):
		"""Create and run a permanent view."""
		DesignDoc.create("pytest", "test", language="javascript", views={"test1": {"map": "(function (doc) {if (doc.x == 1) {emit(doc._id, doc.y);}})"}})

		# while waiting for the design doc to sync, insert some records.
		TestDoc.create("a", x=1, y=1)
		TestDoc.create("b", x=2, y=2)
		TestDoc.create("c", x=1, y=3)
		TestDoc.create("d", x=2, y=4)
		TestDoc.create("e", x=1, y=5)

		# wait up to 30 seconds for sync.  if it goes 30 seconds, there is a 
		# problem with replication
		tries = 0
		view = None
		while view is None:
			tries += 1
			assert tries<30, "Design document never replicated after 30+ seconds."
			try:
				view = View.execute('pytest', 'test/test1')
			except NotFound:
				view = None
				time.sleep(1)

		assert view.total_rows==3
		assert view.rows[0]==("a", 1)
		assert view.rows[1]==("c", 3)
		assert view.rows[2]==("e", 5)

		# test slicing a view
		view = View.execute('pytest', 'test/test1', args={'startkey': 'c', 'endkey': 'e'})
		assert view.total_rows==3
		assert len(view.rows)==2
		assert view.rows[0]==("c", 3)
		assert view.rows[1]==("e", 5)

		# test fetching a single key
		view = View.execute('pytest', 'test/test1', args={'key': 'a'})
		assert view.total_rows==3
		assert len(view.rows)==1
		assert view.rows[0]==("a", 1)

		# update a doc to add a new row to the
		doc_d = TestDoc.find("d")
		doc_d.x = 1
		doc_d.save()

		view = View.execute('pytest', 'test/test1', args={'startkey': 'c', 'endkey': 'e'})
		assert view.total_rows==4
		assert len(view.rows)==3
		assert view.rows[0]==("c", 3)
		assert view.rows[1]==("d", 4)
		assert view.rows[2]==("e", 5)

		for k in ['a','b','c','d','e']:
			TestDoc.find(k).destroy()
	
	def testFindOrNew(self):
		x = TestDoc.find_or_new('xyzzy')
		# new records have no _rev
		assert '_rev' not in x._rec
		x.cheese = 'good'
		x.save()

		x = TestDoc.find_or_new('xyzzy')
		# existing records do have a _rev
		assert '_rev' in x._rec
		assert x.cheese=='good'
	
	def testDefaults(self):
		class DefDoc(Document):
			defaults = { "cheeses": [] }
			db_name = "pytest"

		one = DefDoc.new("first")
		one.cheeses.append("gouda")
		one.cheeses.append("cheddar")
		one.save()

		two = DefDoc.new("second")
		assert two.cheeses==[]
	
	def testSpacesInKey(self):
		a = MultiKey.new("one", " a b c ", x=1, y=2, z=3)
		a.save()
		b = MultiKey.find("one", " a b c ")
		assert b.x==1
		assert b.y==2
		assert b.z==3
	
	def testUnicodeKey(self):
		name = u'm\xf4t\xf4rhead'
		a = MultiKey.new("one", name, x=1, y=2)
		a.save()
		b = MultiKey.find("one", name)
		assert b.x==1
		assert b.y==2
	
	def testZeroResponseCode(self):
		# nginx will give us a zero response code in some weird circumstances.
		# we should treat it like a 400
		assert_raises(LoungeError, Resource.find, db_connectinfo + "pytest/a b c")

if __name__=="__main__":
	# log all REST calls if the DEBUG env var is set
	if os.environ.get("DEBUG",False):
		console = logging.StreamHandler()
		console.setLevel(logging.DEBUG)
		logging.basicConfig(level=logging.DEBUG, handler=console)
	
	use_config('dev', testing=True)
	try:
		main()
	finally:
		# final cleanup
		try:
			Database.find("pytest").destroy()
			pass
		except:
			pass

# vi: noexpandtab ts=2 sw=2
