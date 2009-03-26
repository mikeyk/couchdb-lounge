import logging
import os

#import sys 
#sys.path.insert(0, '')

from twisted.application import internet, service
from twisted.web import static, server, script
from twisted.python import log

from lounge.prefs import Prefs
from smartproxy.proxy import HTTPProxy

prefs = Prefs(os.environ.get("PREFS",'/var/lounge/etc/smartproxy/smartproxy.xml'))

#log.startLogging(open('/var/lounge/log/smartproxyd.log', 'a'))

http_port = prefs.get_pref("/http_port")

loglevel = {'DEBUG' : logging.DEBUG,
			'INFO'  : logging.INFO,
			'WARN'  : logging.WARN }[prefs.get_pref('/log_level')]

logging.basicConfig(level=loglevel)

application = service.Application('smartproxy')

site = server.Site(HTTPProxy(prefs, True))
sc = service.IServiceCollection(application)
i = internet.TCPServer(http_port, site, interface="0.0.0.0")
i.setServiceParent(sc)
