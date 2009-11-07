#!/usr/bin/env python

from distutils.core import setup

py_packages = ['lounge', 'lounge.client']

setup( version = curr_version,
	   description = description,
	   long_description = long_description,
	   name = 'python-lounge',
	   author='meebo',
	   author_email='shaun@meebo.com',
	   url='http://tilgovi.github.com/couchdb-lounge/',
	   packages = py_packages)
