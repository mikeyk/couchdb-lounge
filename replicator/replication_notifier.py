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

# install me to /var/lounge/lib/replication_notifier.py

LOG_PATH = '/var/log/lounge/replicator/replication_notifier.log'

import logging
import os
import pycurl
import Queue
import re
import sys
import signal
import simplejson
import socket
socket.setdefaulttimeout(900)
from stat import *
import StringIO
import threading
import time
import urllib2

import lounge

# TODO some other time use couch.ini for the port
me = 'http://' + socket.gethostname() + ':5984/'

# when this many documents have been updated, perform the replication
UPDATES_PER_REPLICATION = 10

shard_map = None

repq = Queue.Queue()
last_update = {}
update_count = {}

def i_dont_host(node):
	return not node.startswith(me)

class BgReplicator(threading.Thread):
	def __init__(self):
		threading.Thread.__init__(self)

	def run(self):
		while True:
			source, target, opts, tm = repq.get()
			do = opts.get("designonly", False)
			last = last_update.get((source, target, do), None)
			# if we have performed this replication since the record was enqueued,
			# we can skip it.
			if last is None or last < tm:
				last_update[(source, target, do)] = time.time()
				try:
					target_host, target_db = target.rsplit('/', 1)
					post_data = simplejson.dumps({"source": source, "target": target, "designonly": do})
					urllib2.urlopen(target_host + "/_replicate", post_data)
				except:
					# don't panic!  keep going to the next record in the queue.
					pass

def do_background_replication(source, target, **opts):
	# enqueue the request and let the consumer worry about it
	repq.put((source, target, opts, time.time()))

def replicate(shard):
	global update_count
	update_count[shard] = update_count.get(shard, 0) + 1

	# don't replicate until we've accumulated 10 updates
	if update_count[shard] < UPDATES_PER_REPLICATION:
		return

	update_count[shard] = 0

	# first do full replication
	source = shard
	local = me + source
	for target in shard_map.nodes(source):
		# for full replication, we don't want to replicate to our self.	how silly
		if i_dont_host(target):
			do_background_replication(source, target)
	
	# then design replications: from shard 0 to the rest
	shard_index = shard_map.get_index_from_shard(source)
	if shard_index==0:
		for target in shard_map.primary_shards(shard_map.get_db_from_shard(source)):
			if target != local:
				do_background_replication(source, target, designonly=True)

def load_config(fname):
	global shard_map
	old = shard_map

	shard_map = lounge.ShardMap()
	try:
		pass
	except:
		# config invalid; use old conf
		shard_map = old

def read_config_if_changed(last_read):
	fname = "/etc/lounge/shards.conf"
	mtime = os.stat(fname)[ST_MTIME]
	if last_read is None or mtime > last_read:
		load_config(fname)
		last_read = mtime
	return last_read

def main():
	logging.basicConfig(filename=LOG_PATH, level=logging.DEBUG)
	read_conf_at = None
	BgReplicator().start()

	logging.info("Starting up")

	while True:
			try:
				# wait for a line from the database
				stuff = sys.stdin.readline()
				if not stuff:
					return
				# format: {"type": "updated", "db": "nameofdb"}
				notification = simplejson.loads(stuff)

				# check for updated config
				read_conf_at = read_config_if_changed(read_conf_at)
				
				# extract the database name
				if notification['type']=='updated':
					db = notification['db'] 
					replicate(db)
			except:
				logging.exception("error in main loop")
				pid = os.getpid()
				os.kill(pid, signal.SIGTERM)
				sys.exit(0)
	
if __name__=='__main__':
	main()

# vi: noexpandtab ts=2 sw=2
