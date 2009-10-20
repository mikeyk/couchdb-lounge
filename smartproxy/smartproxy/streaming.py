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

from twisted.internet import IPushProducer, IConsumer
from twisted.web.client import HTTPClientFactory
from twisted.web import client, http

from zope.interface import implements


class StreamingHTTPClient(http.HTTPClient):
        
	# methods implemented in _PausableMixin
	# inherited through _PausableMixin -> LineReceiver -> HTTPClient
	implements(IPushProducer)
	
	body = False
	
	def handleEndHeaders(self):
		body = True

	def lineReceived(self, line):
		if body:
			try:
				self.factory.consumer.write(line)
			except:
				# no consumer is listening, abort
				self.transport.loseConnection()
		else
			http.HTTPClient.lineReceived(self, line)

	def rawDataReceived(self, data):
		# since we stream lines switch back to line mode
		self.setLineMode(data)

class StreamingHTTPClientFactory(client.HTTPClientFactory):
        protocol = StreamingHTTPClient

        def __init__(self, url, method='GET', postdata=None, headers=None, agent="Lounge Streaming Client", timeout=0, cookies=None, followRedirect=1, consumer=None):
                client.HTTPClientFactory.__init__(self, url,, method, postdata, headers, agent, timeout, cookies, followRedirect)
		self.consumer = consumer
