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

import cjson
import copy
import logging
import os
import pycurl
import random
import StringIO
import urllib

from cjson import DecodeError

db_config = {
	'prod': 'http://lounge:6984/',
	'dev': 'http://lounge.dev.meebo.com:6984/',
	'local': 'http://localhost:5984/',
	}
db_connectinfo = None
db_prefix = ''
db_timeout = None

def random_junk():
	return ''.join(random.sample("abcdefghijklmnopqrstuvwxyz", 6))

def use_config(cfg, testing=False):
	global db_connectinfo
	db_connectinfo = db_config[cfg]

	global db_prefix 
	if testing: 
		# for testing: prefix every database with our username
		# achieves two goals:
		# 1) don't screw up existing databases
		# 2) let two users run tests on (for example) dev at the same time
		db_prefix = 'test' + os.environ['USER'] + random_junk() + "_"
	else:
		db_prefix = ''

# default to production config
use_config('local')

class LoungeError(Exception):
	def __init__(self, code, key=''):
		self.key = key
		self.code = code

	def __str__(self):
		return "Resource %s returned %d" % (self.key, self.code)
		
	def __repr__(self):
		return "%s(%d, %s)" % (self.__class__.__name__, self.code, self.key)

	@classmethod
	def make(cls, code, key=''):
		"""Make an exception from an HTTP error code."""
		if code==404:
			return NotFound(code, key)
		elif code==409:
			return RevisionConflict(code, key)
		return cls(code, key)

class NotFound(LoungeError):
	"""Exception for when you read or update a missing record."""
	pass

class AlreadyExists(LoungeError):
	"""Exception for when you write a record that already exists."""
	pass

class RevisionConflict(LoungeError):
	"""Exception for updating a record with an out-of-date revision."""
	pass

class ValidationFailed(Exception):
	"""Exception for when an object fails validation."""
	pass

class Resource(object):
	"""A generic REST resource.
	
	You can override url() and make_key() to specify how to
	access the resource.
	"""
	# you can set default values for attributes here
	# e.g., defaults = {"interests": []}
	# that way when you create a new record, you can do:
	#
	# me = Person.new("kevin")
	# me.interests.append("books")
	# 
	# and you will guarantee that it will be set to 
	# some kind of list.  Saves a lot of edge-case handling!
	defaults = {}

	def __init__(self):
		"""Private!  Use find or new."""
		self._responsecode = 0
		# set ._rec last!
		# TODO can we make this private?
	
	def url(self):
		"""Get the URL of this resource.

		For generic resources, the key *is* the URI.  For other resources,
		override it.
		"""
		return self._key
	
	@classmethod
	def make_key(cls, strkey):
		"""Turn some arguments into a string key.
		
		By default, just take one argument and don't touch it.

		If a subclass wants to have a special key, you can override the make_key
		method with whatever arguments you want.  For example,

		@classmethod
		def make_key(cls, protocol, username):
		  return ':'.join(protocol, username)

		Then you can do

		Whatever.find("aim", "meebokevin")

		and it will look up the record with the key aim:meebokevin
		"""
		return strkey
	
	def _encode(self, payload):
		"""Encode an object for writing.

		For typical Couch stuff, we encode as JSON.  Override as needed.
		
		Returns content-type, body pair.
		"""
		return "application/json", cjson.encode(payload)
	
	def _decode(self, payload, headers):
		"""Decode a response.

		For typical Couch stuff, we parse as JSON.  Override as needed.
		"""
		try:
			return cjson.decode(payload)
		except DecodeError:
			raise DecodeError(payload)
	
	### REST helpers
	def _request(self, method, url, args=None, body=None):
		"""Make a REST request."""

		# set up debuggin
		def dbg(dtyp, dmsg):
			# curl's debug messages add extra newlines
			logging.debug(dmsg.strip())
		curl = pycurl.Curl()
		if db_timeout is not None:
			curl.setopt(pycurl.TIMEOUT, db_timeout)
		curl.setopt(pycurl.VERBOSE, 1)
		curl.setopt(pycurl.DEBUGFUNCTION, dbg)

		if args is not None:
			url += '?' + urllib.urlencode(args)
		curl.setopt(pycurl.URL, str(url))

		# capture the result in a buffer
		outbuf = StringIO.StringIO()
		curl.setopt(pycurl.WRITEFUNCTION, outbuf.write)

		# POST/PUT body
		if body is not None:
			content_type, body = self._encode(body)
			curl.setopt(pycurl.HTTPHEADER, ['Content-Type: ' + content_type])
			if method=='POST':
				# CURL handles POST differently.  We must set POSTFIELDS
				curl.setopt(pycurl.POSTFIELDS, body)
			else:
				inbuf = StringIO.StringIO(body)
				curl.setopt(pycurl.INFILESIZE, len(body))
				curl.setopt(pycurl.READFUNCTION, inbuf.read)

		if method=='PUT':
			curl.setopt(pycurl.UPLOAD, 1)
		elif method=='POST':
			curl.setopt(pycurl.POST, 1)
		elif method=='DELETE':
			curl.setopt(pycurl.CUSTOMREQUEST, 'DELETE')

		headers = {}
		def add_header(ln):
			stripped = ln.strip()
			if stripped and stripped.find(': ') >= 0:
				key,val = stripped.split(": ")
				# From http://bugs.python.org/issue2275
				key = '-'.join((ck.capitalize() for ck in key.split('-')))
				headers[key] = val

		curl.setopt(pycurl.HEADERFUNCTION, add_header)

		curl.perform()
		rv = outbuf.getvalue()
		self._responsecode = curl.getinfo(pycurl.HTTP_CODE)

		# if nginx has a bad request, it will return a 400-like error page
		# without setting the correct header.
		if self._responsecode==0:
			self._responsecode = 400

		if self._responsecode>=400:
			raise LoungeError.make(self._responsecode, self._key)
		return self._decode(rv, headers)
	
	### basic REST operations
	def get(self, args=None):
		return self._request('GET', self.url(), args=args)
	
	def put(self):
		result = self._request('PUT', self.url(), body=self._rec)
		return result
	
	def delete(self, args=None):
		return self._request('DELETE', self.url(), args=args)

	@classmethod
	def generate_uuid(cls):
		"""Implement in subclasses where it's OK to have a UUID as a key"""
		raise NotImplementedError

	@classmethod	
	def new(cls, *key, **attrs):
		if not key:
			key = (cls.generate_uuid(),)
		"""Make a new record."""
		inst = cls()
		inst._key = cls.make_key(*key)
		inst._rec = copy.deepcopy(inst.defaults)
		# fill in from kwargs
		for k,v in attrs.items():
			inst._rec[k] = v
		inst._rec["_id"] = inst._key
		return inst
	
	@classmethod
	def create(cls, *key, **attrs):
		"""Make a new record and save it."""
		inst = cls.new(*key, **attrs)
		inst.save()
		return inst

	@classmethod
	def find(cls, *key):
		"""Load a record from the database.

		Ex.
		me = UserProfile.find("kevin")

		raises ResourceNotFound if there is no match
		"""
		inst = cls()
		inst._key = cls.make_key(*key)
		inst._rec = inst.get()

		return inst

	@classmethod
	def find_or_new(cls, *key):
		"""Load a record from the database, or return a new one if it does not exist."""
		try:
			return cls.find(*key)
		except NotFound:
			return cls.new(*key)
	
	def save(self):
		"""Create or update an existing record."""
		result = self.put()
		if result.get("ok",False):
			if "id" in result:
				self._rec["_id"] = result["id"]
			if "rev" in result:
				self._rec["_rev"] = result["rev"]
	
	def reload(self):
		"""Update a record from the database."""
		self._rec = self.get()
	
	def destroy(self):
		"""Remove a record from the database."""
		rev = None
		if '_rev' in self._rec:
			rev = {'rev': self._rec['_rev']}
		response = self.delete(rev)

	def update(self, args):
		"""Update the element in the record w/ the elements in args"""
		self._rec.update(args)

	def __contains__(self, arg):
		return arg in self._rec

	def __setitem__(self, key, value):
		self._rec[key] = value

	def __getitem__(self, key):
		return self._rec[key]

	def __getattr__(self, attr):
		"""Allow apps to access document attributes directly.

		Python will call __getattr__ if an attribute is not in an object's
		dictionary.  We fall back on checking the record.  So for example 
		if our document is {"monkeys": "great"}, then inst.monkeys == "great".
		"""
		try:
			return self._rec[attr]
		except KeyError:
			# or we could
			raise AttributeError("%s has no attribute '%s'" % (str(self), attr))
	
	def __setattr__(self, attr, v):
		"""Allow apps to set document attributes directly.

		If an attribute is not in the dictionary, we set it on the record
		that will be stored upon save, not on the object.  So if you do
		inst.monkeys = "great", then the document will be 
		{"monkeys": "great"}

		We need to be able to set attributes in the constructor, however.
		So we check if '_rec' has been set before doing our override.

		Instead of that special case, we could use object.__setattr__
		in the constructor.
		"""
		# override default setattr only after construction
		if ("_rec" in self.__dict__) and (not attr in self.__dict__) and attr != "_rec":
			self._rec[attr] = v
		else: 
			return object.__setattr__(self, attr, v)

class Database(Resource):
	@classmethod
	def make_key(cls, key):
		return db_prefix + key

	def url(self):
		# key is database new; url is couch url/database
		return db_connectinfo + self._key

class Document(Resource):
	"""Base class for a lounge record.

	Example:

	class Person(Rec):
		db_name = "people"
	
	# set attributes by kwargs
	me = Person.new("kevin", age=25, gender='m')
	# set them directly
	me.interests = ["soccer","cheese"]
	me.save()
	"""

	# set this to the name of your database
	db_name = None

	# use _db_name internally-- it will add the test prefix if needed.
	# external applications can set db_name
	def get_db_name(self):
		return db_prefix + self.db_name
	_db_name = property(get_db_name)

	def __init__(self):
		# do it here, before ._rec is created, so this does not
		# become an attribute passed on to the database
		self._errors = {}
		Resource.__init__(self)

	@classmethod
	def generate_uuid(cls):
		url = db_connectinfo + "_uuids?count=1";
		uuids = Resource.find(url).uuids
		return uuids[0]

	def save(self):
		 is_valid = self.validate()
		 if not is_valid:
			 raise ValidationFailed("Validation failed for object of type %s: %s.  Errors: %s" % (self.__class__, str(self._rec), str(self._errors)))
		 super(Document, self).save()

	def url(self):
		# It should be OK to create a Document instance with no db-- the only
		# issue will come when you try to save it
		if self.db_name is None:
			raise NotImplementedError("Database not provided")
		return db_connectinfo + self._db_name + '/' + urllib.quote(self._key.encode('utf8', 'xmlcharrefreplace'), safe=':/,~@!')
	
	def set_error(self, attr, msg):
		"""Add an error message to the object's errors dict.

		Call this in your validation functions to explain why validation failed.
		"""
		if attr not in self._errors:
			self._errors[attr] = []
		self._errors[attr].append(msg)

	def errors_for(self, attr):
		if attr in self._errors:
			return self._errors[attr]
		return []

	def validate(self):
		"""Used to validate our document before we save it.  Returns True if
		the document is valid, False if it isn't.

		Implement methods starting with validate_ to add your validations.
		"""
		status = True
		self._errors = {}
		# find all method named validate_
		for attr in dir(self):
			if attr.startswith('validate_'):
				f = getattr(self, attr)
				# make sure it's actually callable
				if hasattr(f, '__call__'):
					status = f() and status
		return status

	def get_attachment(self, name):
		"""
		Retrieves an attachment from this Document, raising NotFound if
		it's not found.
		"""
		return Attachment.find(self.url() + "/" + urllib.quote_plus(name))
	
	def new_attachment(self, name):
		"""Set up for saving an attachment to this document.

		When creating or updating an attachment, CouchDB requires the MVCC token
		from the owning document.  This helper sets that token and generates the
		resource URI.
		"""
		return Attachment.new(self.url() + "/" + urllib.quote_plus(name), _rev=self._rev)
	
	def remove_attachment(self, name):
		"""
		Remove the attachment from the attachments dict.  Throws a
		KeyError if the attachment isn't found in the _attachments
		dict.
		"""
		self._attachments.pop(name)

class Changes(Resource):
	""" Shortcut for accessing a database's _changes API
		Use: (given a database called 'fruits')
		changed_docs = client.Changes.find("fruits", since=[15,151,16])
		('since' is a vector rather than a single revision when using the 
		lounge)
		"""

	@classmethod
	def make_key(cls, dbname, since=None):
		cls._db_name = dbname
		cls._since = since
		return "_changes"

	def get(self):
		args = {}
		if self._since: args = {'since': self._since}
		return Resource.get(self, args)

	@classmethod
	def find(cls, dbname, since=None):
		inst = cls()
		inst._key = cls.make_key(dbname, since)
		inst._rec = inst.get()

		return inst
	
	def url(self):
		return db_connectinfo + self._db_name + '/' + self._key

	

class DesignDoc(Document):
	def __init__(self):
		try:
			Document.__init__(self)
		except NotImplementedError:
			# trap the error for db_name not overridden.  that's ok
			pass

	@classmethod
	def make_key(cls, dbname, docname):
		cls.db_name = dbname
		return "_design/" + docname

	# we override the url method here because we have different quoting behavior from a regular document
	# if they do ever fix this in couchdb, we can revert this :)
	def url(self):
		return db_connectinfo + self._db_name + '/' + self._key

class TuplyDict(object):

	def __init__(self, row_dict):
		self._dict = row_dict
		
	def __contains__(self, item):
		return (item == 0) or (item == 1) or item in self._dict
	
	def __getitem__(self, key):
		if key == 0 or key == 1:
			return self._keyvalue[key]
		else:
			return self._dict[key]
			
	def __cmp__(self, obj):
		if isinstance(obj, tuple):
			return cmp(self._keyvalue, obj)
		else:
			return cmp(self._dict, obj._dict)
				
	def __iter__(self):
		""" We only iterate over the fake key,value tuple,
		 	for backwards compatibility
		"""
		return self._keyvalue.__iter__()
		
	@property
	def _keyvalue(self):
		return (self._dict['key'], self._dict['value'])

	def __repr__(self):
		return "TuplyDict(%s)" % repr(self._dict)

	def __str__(self):
		return str(self._dict)

class View(Resource):
	def __init__(self, db_name):
		Resource.__init__(self)
		self._db_name = db_prefix + db_name

	def url(self):
		return db_connectinfo + self._db_name + '/' + self._key

	@classmethod
	def make_key(cls, name):
		doc, view = name.split('/')
		return '_design/' + doc + '/_view/' + view

	@classmethod
	def execute(cls, db_name, *key, **kwargs):
		inst = cls(db_name)
		inst._key = cls.make_key(*key)
		args = None
		if 'args' in kwargs:
			args = kwargs['args']
			del kwargs['args']
			for k,v in args.items():
				# stale=ok is not json-encoded, but stuff like
				#	startkey=["one", "two"] is json-encoded.
				if k!='stale':
					# json-encode the args
					args[k] = cjson.encode(v)
		#this sets the post-body to the arguments of the view (so it's actually not a no-op)
		#this behaviour is used in TempView below
		inst._rec = kwargs
		inst._rec = inst.get_results(args)
		try:
			inst._rec['rows'] = [TuplyDict(row) for row in inst._rec['rows']]
		except TypeError:
			raise TypeError("Expected a JSON object with 'rows' attribute, got %s" % str(inst._rec))
		return inst

	def get_results(self, args):
		return self._request('GET', self.url(), args=args)
	
	def save(self):
		raise NotImplementedError

class TempView(View):
	@classmethod
	def make_key(cls):
		return '_temp_view'
	
	def get_results(self, args):
		return self._request('POST', self.url(), args=args, body=self._rec)

class AllDocView(View):
	@classmethod
	def make_key(cls):
		return '_all_docs'

class Attachment(Resource):
	"""A Resource with special encoding.

	needs:
	`content_type` -- mime type to use when storing the attachment
	and either of:
	`data` -- raw data to store
	`stream` -- file-type object with data

	When retrieving an attachment, you'll always get a stream.
	"""
	def _encode(self, payload):
		content_type = payload['content_type']
		if 'data' in payload:
			data = payload['data']
		else:
			data = payload['stream'].read()
		return content_type, data
	
	def _decode(self, data, headers):
		content_type = headers.get('Content-Type', 'application/octet-stream')
		return {
			"content_type": content_type,
			"stream": StringIO.StringIO(data)
		}

	def put(self):
		result = self._request('PUT', self.url(), args={"rev": self._rec["_rev"]}, body=self._rec)
		return result
