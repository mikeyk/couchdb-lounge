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

if 'DEBUG' in os.environ:
	COUCH_URL = "http://bfp4.dev.meebo.com:5984/"
else:
	COUCH_URL = "http://localhost:5984/"

LOG_FORMAT = '%(asctime)s %(levelname)s %(message)s'
LOG_LEVEL = logging.INFO
LOG_LOCATION = '/var/meebo/log/view_updater.log'

DESIGN_DOCS_TO_SKIP = {
		'analyze':1,
}

def get_all_dbs():
	try:
		x = urlopen(COUCH_URL + "_all_dbs").read()
	except:
		logging.error("Failed to retrieve the database list from the local couch node")
		raise
	db_json = simplejson.loads(x)
	return db_json


def get_all_design_docs(db):
	url = COUCH_URL + "%s/_all_docs?" % db
	#note about couch 0.9.0 collation:
	#collation on documents is case insensitive and unpredictable.  For instance,
	#the following query requires '_designZZZZZZ' instead of '_design/ZZZZZZ'.
	#Evidently the slash throws off the collation.  Very strange...
	url = url + urlencode( [ ("startkey", '"_design/"'), ("endkey", '"_designZZZZZZ"')])
	try:
		x = urlopen(url).read()
	except IOError:
		logging.exception("Failed trying to fetch %s" % url)
		return []
	design_doc_json = simplejson.loads(x)
	design_docs = []
	if 'rows' not in design_doc_json:
		logging.info ("No design docs in %s" % db)
		return []
	for row in design_doc_json['rows']:
		dd = row['key']
		dd = dd[dd.rfind('/')+1:]
		logging.debug ("checking if %s is in the dict of design docs to skip..." % dd)
		if dd in DESIGN_DOCS_TO_SKIP:
			logging.debug("skipping %s" % dd)
			continue
		logging.debug("adding %s to the design doc list" % dd)
		design_docs.append(dd)
	return design_docs


def get_views(db, design_doc):
	url = COUCH_URL + "%s/_design/%s" % (db,design_doc)
	try:
		x = urlopen(url).read()
	except IOError:
		logging.exception("Failed trying to fetch %s" % url)
		return []
	design_doc_json = simplejson.loads(x)
	if "views" in design_doc_json:
		return design_doc_json['views'].keys()
	else:
		logging.warning("get_views(%s) returned:\n%s" % (url, design_doc_json))
		return []


def run_view(db, design_doc, view):
	url = COUCH_URL + "%s/_design/%s/_view/%s" % (db, design_doc, view)
	try:
		start_time = time.time()
		x = urlopen(url).read()
		end_time = time.time()
		logging.info ("%-80s took %d seconds" % (url, end_time - start_time))
	except:
		logging.exception("run_view(%s,%s,%s)" % (db, design_doc, view))


if __name__ == "__main__":

	if 'DEBUG' in os.environ:
		logging.basicConfig(level=logging.DEBUG, format=LOG_FORMAT)
	else:
		logging.basicConfig(level=LOG_LEVEL, filename=LOG_LOCATION, format=LOG_FORMAT)
	do_view_update = True

	try:
		cg = CronGuard(pidfile_name="view_updater.pid")
	except lounge.cronguard.ProcessStillRunning:
		logging.info("Previous views still updating, not starting another process")
		do_view_update = False

	if do_view_update:
		for db in get_all_dbs():
			logging.info("starting database: %s" % db)
			for design_doc in get_all_design_docs(db):
				logging.info("starting design doc: %s" % design_doc)
				for view in get_views(db, design_doc):
					logging.info("fetching view: %s" % view)
					start_time = time.time()
					run_view(db, design_doc, view)
					end_time = time.time()
					elapsed = end_time - start_time
					logging.info("%s took %d seconds" % (view,elapsed))
		
