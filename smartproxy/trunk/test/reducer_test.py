#!/usr/bin/python

import copy
import sys

# prepend the location of the local python-lounge
# the tests will find the local copy first, so we don't
# have to install system-wide before running tests.
sys.path = ['..'] + sys.path

from unittest import TestCase, main

from smartproxy import reducer

class ReducerTest(TestCase):
	def testMergeCollation(self):
		r1 = {'rows': [{'value': 1, 'key': 'a'}]}
		r2 = {'rows': [{'value': 6, 'key': 'a'}, {'value': 1, 'key': 'A'}]}

		# merge mangles its argument, so make a copy
		a = copy.deepcopy(r1)
		b = copy.deepcopy(r2)
		x = reducer.merge(b, a)
		self.assertEqual(len(x['rows']), 3)
		self.assertEqual(x['rows'][0]['key'], 'a')
		self.assertEqual(x['rows'][1]['key'], 'a')
		self.assertEqual(x['rows'][2]['key'], 'A')

		a = copy.deepcopy(r1)
		b = copy.deepcopy(r2)
		x = reducer.merge(a, b)
		self.assertEqual(len(x['rows']), 3)
		self.assertEqual(x['rows'][0]['key'], 'a')
		self.assertEqual(x['rows'][1]['key'], 'a')
		self.assertEqual(x['rows'][2]['key'], 'A')

if __name__=="__main__":
	main()
