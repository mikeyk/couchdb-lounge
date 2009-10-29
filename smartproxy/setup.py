#!/usr/bin/env python
try:
	from setuptools import setup
except ImportError:
	from distutils.core import setup

init_files = ('/etc/init.d/', ['smartproxyd'])
conf_files = ('/etc/lounge/', ['smartproxy.xml', 'smartproxy.tac'])
check_files = ('/root/bin/', ['check-smartproxy.py'])
cron_files = ('/etc/cron.d/', ['check-smartproxy'])
cache_files = ('/usr/lounge/lib/smartproxy', ['cache.dat'])

data_files = [init_files, conf_files, check_files, cron_files, cache_files]

py_modules = ["smartproxy.proxy", "smartproxy.fetcher", "smartproxy.reducer", "smartproxy.streaming"]

setup( version = '1.1',
	   name = 'lounge-smartproxy',
	   author='meebo',
	   author_email='shaun@meebo-inc.com',
	   url='http://code.google.com/p/couchdb-lounge',
	   data_files = data_files,
	   py_modules = py_modules)
