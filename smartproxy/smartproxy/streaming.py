#Copyright 2009 Meebo, Inc.
#
#Licensed under the Apache License, Version 2.0 (the "License");
#you may not use this file except in compliance with the License.
#You may obtain a copy of the License at
#
#	http://www.apache.org/licenses/LICENSE-2.0
#
#Unless required by applicable law or agreed to in writing, software
#distributed under the License is distributed on an "AS IS" BASIS,
#WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#See the License for the specific language governing permissions and
#limitations under the License.

from twisted.internet.interfaces import IPushProducer, IConsumer
from twisted.python import log
from twisted.web.client import HTTPClientFactory, HTTPPageGetter

from zope.interface import implements

class StreamingHTTPClient(HTTPPageGetter):
	# methods implemented in _PausableMixin
	# inherited through _PausableMixin -> LineReceiver -> HTTPClient
	implements(IPushProducer)
	body = False
	
	def handleEndHeaders(self):
		HTTPPageGetter.handleEndHeaders(self)
		self.body = True
		self.delimiter = "\n"

	def handleHeader(self, key, value):
		HTTPPageGetter.handleHeader(self, key, value)
		if self.factory.request and key != 'content-length':
			self.factory.request.setHeader(key, value)

	def lineReceived(self, line):
		if self.body:
			try:
				self.factory.consumer.write((self.factory.shard_idx, line + '\n'))
			except:
				# no consumer is listening, abort
				self.transport.loseConnection()
		else:
			HTTPPageGetter.lineReceived(self, line)
	
	def rawDataReceived(self, data):
		# since we stream lines switch back to line mode
		self.setLineMode(data)

	def connectionMade(self):
		self.factory.consumer.registerProducer(self, True)
		HTTPPageGetter.connectionMade(self)

	def connectionLost(self, reason):
		self.factory.consumer.unregisterProducer()
		HTTPPageGetter.connectionLost(self, reason)

	def handleStatus(self, version, status, message):
		if self.factory.request:
			self.factory.request.setResponseCode(int(status), message)

class StreamingHTTPClientFactory(HTTPClientFactory):
	protocol = StreamingHTTPClient 
	
	def __init__(self, url, method='GET', request=None, postdata=None, headers=None, agent="Lounge Streaming Client", timeout=0, cookies=None, followRedirect=1, consumer=None, shard_idx=None):
		HTTPClientFactory.__init__(self, url, method, postdata, headers, agent, timeout, cookies, followRedirect)
		self.request = request
		self.consumer = consumer
		self.shard_idx = shard_idx
# vi: noexpandtab ts=4 sts=4 sw=4
