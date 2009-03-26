#!/usr/bin/python
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

from urllib import urlopen, urlencode
import simplejson
import logging
import time
import lounge
from lounge.cronguard import CronGuard

COUCH_URL = "http://localhost:5984/"
LOG_LEVEL = logging.INFO
LOG_LOCATION = '/var/lounge/log/view_updater.log'

def get_all_dbs():
	x = urlopen(COUCH_URL + "_all_dbs").read()
	db_json = simplejson.loads(x)
	return db_json


def get_all_design_docs(db):
	url = COUCH_URL + "%s/_all_docs?" % db
	url = url + urlencode( [ ("startkey", '"_design/"'), ("endkey", '"_design/ZZZ"')])
	x = urlopen(url).read()
	design_doc_json = simplejson.loads(x)
	design_docs = []
	for row in design_doc_json['rows']:
		dd = row['key']
		dd = dd[dd.rfind('/')+1:]
		design_docs.append(dd)
	return design_docs


def get_views(db, design_doc):
	url = COUCH_URL + "%s/_design%%2F%s" % (db,design_doc)
	x = urlopen(url).read()
	design_doc_json = simplejson.loads(x)
	if "views" in design_doc_json:
		return design_doc_json['views'].keys()
	else:
		logging.warning("get_views(%s) returned:\n%s" % (url, design_doc_json))
		return []


def run_view(db, design_doc, view):
	url = COUCH_URL + "%s/_view/%s/%s" % (db, design_doc, view)
	try:
		start_time = time.time()
		x = urlopen(url).read()
		end_time = time.time()
		logging.info ("%-80s took %d seconds" % (url, end_time - start_time))
	except:
		logging.exception("run_view(%s,%s,%s)" % (db, design_doc, view))


if __name__ == "__main__":

	logging.basicConfig(level=LOG_LEVEL, filename=LOG_LOCATION)
	do_view_update = True

	try:
		cg = CronGuard(pidfile_name="view_updater.pid")
	except lounge.cronguard.ProcessStillRunning:
		logging.info("Previous views still updating, not starting another process")
		do_view_update = False

	if do_view_update:
		for db in get_all_dbs():
			for design_doc in get_all_design_docs(db):
				for view in get_views(db, design_doc):
					run_view(db, design_doc, view)
		
