import httplib
import mechanize
import os
import random
import socket
import ssl
import time
import uuid
import urllib2

from urllib import urlencode

SITE_URL = 'https://127.0.0.1:8000/crowds/internal/'

class Transaction(object):
    HOMEPAGE_URL = SITE_URL + ''
    TASK_URL = SITE_URL + 'assignments/?task_type=%s&worker_id=%s'
    RESPONSE_URL = SITE_URL + 'responses/'
    DUMMY_SUBMIT_URL = SITE_URL + 'dummy_submit'
    ANSWER_MAP = {
        'sa': '{"t2":"1.0","t3":"4.0","t1":"3.0"}',
        'er': '{"pair1":"0.0","pair2":"1.0"}',
        'ft': '{"ft1":"1.0","ft2":"0.0"}'
    }

    def __init__(self):
        self.custom_timers = {}
        self.browser = mechanize.Browser()
        self.browser.set_handle_robots(False)

        # install custom opener for urllib and for mechanize
        handler = HTTPSHandlerV3()
        urllib2.install_opener(urllib2.build_opener(handler))
        self.browser.add_handler(handler)
        self.browser.handlers = [handler,]

    def run(self):

        # Get the homepage
        home_page_load_start = time.time()
        resp = self.browser.open(self.HOMEPAGE_URL)
        resp.read()
        assert resp.code == 200, "Bad Response: HTTP %s" % resp.code
        self.custom_timers["Homepage Load"] = time.time() - home_page_load_start

        # generate a new worker id
        worker_id = str(uuid.uuid4())

        # Pick a task type to work on
        task_type = random.choice(self.ANSWER_MAP.keys())

        # "Think" about which task to do
        time.sleep(random.uniform(1,2))

        # Click on the sa task type
        assignment_start = time.time()
        resp = self.browser.open(self.TASK_URL % (task_type, worker_id))
        resp.read()
        assert resp.code == 200, "Bad Response: HTTP %s" % resp.code
        self.custom_timers["Assignment Page Load"] = time.time() - assignment_start

        # Get the assignment/task ids out of the form
        self.browser.select_form(nr=0)
        assignment_id = self.browser.form["assignment_id"]
        task_id = self.browser.form["task_id"]
        form_worker_id = self.browser.form["worker_id"]
        assert form_worker_id == worker_id, "Worker ID mysteriously changed."

        # "Think" while doing the task
        time.sleep(random.uniform(5,15))

        # post a response to the backend
        real_response_start = time.time()
        resp = urllib2.urlopen(self.RESPONSE_URL, data=urlencode({
            'answers': self.ANSWER_MAP[task_type],
            'assignment_id': assignment_id,
            'task_id': task_id,
            'worker_id': worker_id
        }))
        resp.read()
        assert resp.code == 200, "Bad Response: HTTP %s" % resp.code
        self.custom_timers["Post Response"] = time.time() - real_response_start


        # post a fake response to the dummy_submit
        dummy_response_start = time.time()
        resp = urllib2.urlopen(self.DUMMY_SUBMIT_URL, data=urlencode({
            'assignment_id': assignment_id
        }))
        resp.read()
        assert resp.code == 200, "Bad Response: HTTP %s" % resp.code
        self.custom_timers["Submit Form"] = time.time() - dummy_response_start

        self.custom_timers["Complete Response"] = time.time() - real_response_start

# custom HTTPS opener, django-sslserver supports SSLv3 only
class HTTPSConnectionV3(httplib.HTTPSConnection):
    def __init__(self, *args, **kwargs):
        httplib.HTTPSConnection.__init__(self, *args, **kwargs)

    def connect(self):
        sock = socket.create_connection((self.host, self.port), self.timeout)
        if self._tunnel_host:
            self.sock = sock
            self._tunnel()
        try:
            self.sock = ssl.wrap_socket(sock, self.key_file, self.cert_file, ssl_version=ssl.PROTOCOL_TLSv1)
        except ssl.SSLError, e:
            print("Trying SSLv3.")
            self.sock = ssl.wrap_socket(sock, self.key_file, self.cert_file, ssl_version=ssl.PROTOCOL_TLSv1)

class HTTPSHandlerV3(urllib2.HTTPSHandler):
    def https_open(self, req):
        return self.do_open(HTTPSConnectionV3, req)

if __name__ == '__main__':
    trans = Transaction()
    trans.run()
    print trans.custom_timers
