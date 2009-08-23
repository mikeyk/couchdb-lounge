import simplejson
import threading
import time

from BaseHTTPServer import HTTPServer, BaseHTTPRequestHandler

class FakeCouchHandler(BaseHTTPRequestHandler):
  def log_message(self, format, *args):
    open("log/couchstub.log","a").write("%s - - [%s] %s\n" %
      (self.address_string(),
      self.log_date_time_string(),
      format%args))

  def do_any(self):
    # find the request we expect
    ex = None
    if self.server.stub.expected:
      ex = self.server.stub.expected.pop(0)

    if ex is None:
      self.server.failures.append("Didn't expect any more requests, but got %s %s" % (self.command, self.path))
      return

    if self.command != ex.method or self.path != ex.path:
      self.server.failures.append("Wanted %s %s but got %s %s" % (ex.method, ex.path, self.command, self.path))

    # send the mocked request
    self.send_response(ex.responsecode)
    for k in ex.responseheaders:
      self.send_header(k, ex.responseheaders[k])
    self.end_headers()
    self.wfile.write(ex.responsebody)

  do_GET = do_any
  do_PUT = do_any
  do_POST = do_any

class FakeCouch(threading.Thread):
  def listen(self, stub, addr, port):
    self.stub = stub
    self.failures = []
    self.server = HTTPServer((addr, port), FakeCouchHandler)
    self.server.timeout = 0.05
    self.server.failures = self.failures
    self.server.stub = self.stub

  def run(self):
    while True:
      if self.stub.stop.is_set():
        return
      self.server.handle_request()

class Request:
  def __init__(self, method, path, body, headers):
    self.method = method
    self.path = path
    self.body = body
    self.headers = headers

  def reply(self, code, body, headers={}):
    self.responsecode = code
    self.responsebody = simplejson.dumps(body)
    self.responseheaders = {}
    if 'Content-type' not in self.responseheaders:
      self.responseheaders['Content-type'] = 'application/json'

  def __str__(self):
    return "%s %s" % (self.method, self.path)

class CouchStub:
  def __init__(self):
    self.expected = []
    self.stop = threading.Event()

  def expect(self, method, path, body='', headers={}):
    req = Request(method, path, body, headers)
    self.expected.append(req)
    return req

  def make_expecter(method):
    def f(self, path, body='', headers={}):
      return self.expect(method, path, body, headers)
    return f
  expect_GET = make_expecter("GET")
  expect_PUT = make_expecter("PUT")
  expect_POST = make_expecter("POST")

  def listen(self, addr, port):
    """Spawn a background thread that will actually create an HTTP server."""
    self._thread = FakeCouch()
    self._thread.listen(self, addr, port)
    self._thread.start()

  def verify(self):
    # stop the background thread
    self.stop.set()
    self._thread.join()
    assert (not self._thread.failures), " and ".join(self._thread.failures)
    assert len(self.expected)==0, "Expected more requests: " + ', '.join([str(x) for x in self.expected])
