#
# Regular cron jobs for the couchdb-lounge package
#
0 4	* * *	root	[ -x /usr/bin/couchdb-lounge_maintenance ] && /usr/bin/couchdb-lounge_maintenance
