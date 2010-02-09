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

import warnings

import cjson

from twisted.python import log
from twisted.web import error, http, client
from twisted.internet import interfaces
from twisted.protocols import pcp
from twisted.internet.error import ConnectionDone

from zope.interface import implements


class HTTPProducer(client.HTTPClientFactory):
	"""
	Like twisted.web.client.HTTPDownloader except instead of streaming
	to a file I stream to a consumer.
	"""
	
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
		self.consumer.finish()
		self.deferred.callback(None)

	def trapCleanClosure(self, reason):
		reason.trap(ConnectionDone)

class JSONLineProducer(HTTPProducer):
	def __init__(self, *args, **kwargs):
		HTTPProducer.__init__(self, *args, **kwargs)
		self.oldLineReceived = None

	def buildProtocol(self, addr):
		p = HTTPProducer.buildProtocol(self, addr)
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
	class MultiPCPChannel():
		implements(interfaces.IProducer, interfaces.IConsumer)

		def __init__(self, name, sink):
			pcp.BasicProducerConsumerProxy.__init__(self, None)
			self.name = name
			self.sink = sink

		# Producer methods

		def pauseProducing(self):
			self.producer.pauseProducing()

		def resumeProducing(self):
			self.producer.resumeProducing()

		def stopProducing(self):
			if self.producer is not None:
				self.producer.stopProducing()

		# Consumer methods

		def write(self, data):
			self.sink.write((self.name, data))

		def finish(self):
			self.sink.deleteChannel(self)

		def registerProducer(self, producer, streaming):
			self.producer = producer

		def unregisterProducer(self):
			if self.producer is not None:
				del self.producer

	def __init__(self, consumer):
		pcp.BasicProducerConsumerProxy.__init__(self, consumer)
		self.channels = {}

	def createChannel(self, name):
		log.msg("Creating channel %s" % channel)
		if channel in self.channels:
			raise ValueError, "channel already open"
		
		self.channels[channel] = self.MultiPCPChannel(channel, self)
		return self.channels[channel]

	def deleteChannel(self, channel):
		del self.channels[channel]
		if not self.channels:
			self.finish()

	def registerProducer(self, producer, streaming):
		warnings.warn("directly registering producers with MultiPCP objects is not supported, use createChannel() instead", category=RuntimeWarning)

	def unregisterProducer(self):
		warnings.warn("directly unregistering producers with MultiPCP objects is not supported, use createChannel() instead", category=RuntimeWarning)

	def pauseProducing():
		for channel in self.channels.itervalues():
			channel.pauseProducing()

	def resumeProducing():
		for channel in self.channels.itervalues():
			channel.resumeProducing()

	def stopProducing():
		for channel in self.channels.itervalues():
			channel.stopProducing()

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
