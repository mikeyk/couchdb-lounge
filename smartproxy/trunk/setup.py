from distutils.core import setup
import os

tac_files = ('/var/lounge/etc', ['smartproxy.tac'])
init_files = ('/etc/init.d/', ['smartproxyd'])
conf_files = ('/var/lounge/etc/smartproxy/', ['smartproxy.xml'])
check_files = ('/root/bin/', ['check-smartproxy.py'])
cron_files = ('/etc/cron.d/', ['check-smartproxy'])
cache_files = ('/var/lounge/lib/smartproxy', ['cache.dat'])

data_files = [tac_files, init_files, conf_files, check_files, cron_files, cache_files]

py_modules = ["smartproxy.proxy"]

setup( version = '1.0',
	   name = 'lounge-smartproxy',
	   author='meebo',
	   author_email='shaun@meebo.com',
	   url='http://code.google.com/p/couchdb-lounge',
	   data_files = data_files,
	   py_modules = py_modules)
