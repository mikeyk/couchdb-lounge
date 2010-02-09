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

import sys

import cjson

from cStringIO import StringIO

from twisted.python import log
from twisted.internet.interfaces import IFinishableConsumer
from twisted.web import error, http, client, http_headers
from twisted.internet import defer
from twisted.protocols import pcp
from twisted.internet.error import ConnectionDone

from zope.interface import implements

def getPageFromAny(upstreams, factory=client.HTTPClientFactory, context_factory=None, *args, **kwargs):
	if not upstreams:
		raise error.Error(http.NOT_FOUND)

	def subgen():
		lastError = error.Error(http.NOT_FOUND)
		for (identifier, url) in upstreams:
			subfactory = client._makeGetterFactory(url, factory, context_factory, *args, **kwargs)
			wait = defer.waitForDeferred(subfactory.deferred)
			yield wait
			try:
				yield (identifier, subfactory, wait.getResult())
				return
			except:
				lastError = sys.exc_info()[1]
		raise lastError
	return defer.deferredGenerator(subgen)()

def getPageFromAll(upstreams, factory=client.HTTPClientFactory, context_factory=None, *args, **kwargs):
	def makeUpstreamGetter(u):
		identifier, url = u
		subfactory = client._makeGetterFactory(url, factory, context_factory, *args, **kwargs)
		subfactory.deferred.addBoth(lambda x: (identifier, subfactory, x))
		return subfactory.deferred

	return map(makeUpstreamGetter, upstreams)

class HTTPStreamer(client.HTTPClientFactory):
	protocol = client.HTTPPageDownloader

	def __init__(self, url, consumer, *args, **kwargs):
		client.HTTPClientFactory.__init__(self, url, *args, **kwargs)
		self.consumer = consumer
		self.deferred.addErrback(self.trapCleanClosure)

	def buildProtocol(self, addr):
		p = client.HTTPClientFactory.buildProtocol(self, addr)
		self.consumer.registerProducer(p, True)
		return p

	def pageStart(self, partialContent):
		pass

	def pagePart(self, data):
		self.consumer.write(data)

	def pageEnd(self):
		self.consumer.unregisterProducer()

	def trapCleanClosure(self, reason):
		reason.trap(ConnectionDone)

class JSONClientFactory(client.HTTPClientFactory):
	def __init__(self, *args, **kwargs):
		client.HTTPClientFactory.__init__(self, args, kwargs)
		self.deferred.addCallback(self,decode)

	def decode(self, page):
		return cjson.decode(page)

class HTTPLineStreamer(HTTPStreamer):
	def __init__(self, *args, **kwargs):
		HTTPStreamer.__init__(self, *args, **kwargs)
		self.oldLineReceived = None

	def buildProtocol(self, addr):
		p = HTTPStreamer.buildProtocol(self, addr)
		p.setRawMode = lambda: self.setRawModeWrapper(p)
		p.setLineMode = lambda r='': self.setLineModeWrapper(p, r)
		return p

	def setRawModeWrapper(self, protocol):
		if self.oldLineReceived:
			protocol.setRawMode()
			return
		self.oldLineReceived = protocol.lineReceived
		self.oldDelimiter = protocol.delimiter
		protocol.lineReceived = self.gotLine
		protocol.delimiter = '\n'

	def setLineModeWrapper(self, protocol, rest=''):
		if not self.oldLineReceived:
			self.protocol.setLineMode(rest)
			return
		protocol.lineReceived = self.oldLineReceived
		protocol.delimiter = self.oldDelimiter
		protocol.dataReceived(rest)
	
	def gotLine(self, data):
		if data:
			self.pagePart(data + '\n')

class MultiPCP(pcp.BasicProducerConsumerProxy):
	class MultiPCPChannel(pcp.BasicProducerConsumerProxy):
		def __init__(self, name, sink):
			pcp.BasicProducerConsumerProxy.__init__(self, sink)
			self.name = name

		def write(self, data):
			pcp.BasicProducerConsumerProxy.write(self, (self.name, data))

		def finish(self):
			#trap this so we don't stop the whole multi
			pass

	def __init__(self, consumer):
		pcp.BasicProducerConsumerProxy.__init__(self, consumer)
		self.channels = {}

	def createChannel(self, channel):
		log.msg("Creating channel %s" % channel)
		if channel in self.channels:
			raise ValueError, "channel already open"
		self.channels[channel] = self.MultiPCPChannel(channel, self)
		return self.channels[channel]

	def pauseProducing():
		for channel in self.channels.itervalues():
			channel.pauseProducing()

	def resumeProducing():
		for channel in self.channels.itervalues():
			channel.resumeProducing()

	def write(self, channelData):
		## Override to specify how this proxy merges channels ##
		channel, data = channelData
		pcp.BasicProducerConsumerProxy.write(self, data)

class JSONLinePCP(pcp.BasicProducerConsumerProxy):
	def __init__(self, consumer, encode=True):
		pcp.BasicProducerConsumerProxy.__init__(self, *args)
		self.encode = encode

	def write(self, data):
		if self.encode:
			pcp.BasicProducerConsumerProxy.write(self, cjson.encode(data) + '\n')
		else:
			pcp.BasicProducerConsumerProxy.write(self, cjson.decode(data))

# vi: noexpandtab ts=4 sts=4 sw=4
