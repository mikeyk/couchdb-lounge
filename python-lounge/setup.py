#!/usr/bin/env python

from distutils.core import setup
import os

py_modules = ['lounge.prefs', 'lounge.cronguard']

setup( version = '1.0',
	   name = 'python-lounge',
	   author='meebo',
	   author_email='shaun@meebo.com',
	   url='http://couchdb-lounge.code.google.com',
	   py_modules = py_modules)
