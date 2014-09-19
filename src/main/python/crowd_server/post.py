import httplib
import socket
import ssl
import json
import urllib
import urllib2
import operator

from argparse import ArgumentParser

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

def send_request(data, crowds) :

    # Send request
    params = {'data' : json.dumps(data)}
    url = 'https://127.0.0.1:8000/crowds/%s/tasks/'
    for crowd in crowds:
        response = urllib2.urlopen(url%crowd,
                                   urllib.urlencode(params))
        res = json.loads(response.read())
        if res['status'] != 'ok' :
            print 'Got something wrong!!!!!!!!!!!!!!!!!!!!!!!!!!!!'

# Create batches of HIT
def create_hit(crowds, task_types):
    if not task_types or not crowds:
        print ("No crowds or no task types passed... not posting any data. Run "
               "`python post.py --help` for options.")
        return

    # install custom opener
    urllib2.install_opener(urllib2.build_opener(HTTPSHandlerV3()))

    if 'sa' in task_types:
        # Create a sentiment analysis HIT
        data = {}
        data['configuration'] = {}
        data['configuration']['task_type'] = 'sa'
        data['configuration']['task_batch_size'] = 2
        data['configuration']['num_assignments'] = 1
        data['configuration']['callback_url'] = 'www.google.com'

        data['group_id'] = 'test1'
        data['group_context'] = {}   # Empty group contest for sentiment analysis

        # This configuration generates two HITs, one with two tweets, the other with one tweet.
        data['content'] = {'t1' : 'This is tweet No.1',
                           't2' : 'This is tweet No.2',
                           't3' : 'This is tweet No.3'}
        # Send request
        send_request(data, crowds)

    if 'er' in task_types:
        # Create a Deduplication HIT
        data = {}
        data['configuration'] = {}
        data['configuration']['task_type'] = 'er'
        data['configuration']['task_batch_size'] = 1
        data['configuration']['num_assignments'] = 1
        data['configuration']['callback_url'] = 'www.google.com'

        data['group_id'] = 'test2'
        data['group_context'] = {'fields' : ['price', 'location']}

        # This configuration generates two HITs, each with one pair of records to be compared.
        data['content'] = {'pair1' : [['5', 'LA'], ['6', 'Berkeley']],
                           'pair2' : [['80', 'London'], ['80.0', 'Londyn']]}
        send_request(data, crowds)

    if 'ft' in task_types:
        # Create a Filtering HIT
        data = {}
        data['configuration'] = {}
        data['configuration']['task_type'] = 'ft'
        data['configuration']['task_batch_size'] = 1
        data['configuration']['num_assignments'] = 1
        data['configuration']['callback_url'] = 'www.google.com'

        data['group_id'] = 'test3'
        data['group_context'] = {'fields' : ['city', 'cuisine']}

        # This configuration generates two HITs, each with one pair of records to be compared.
        data['content'] = {'ft1': {'title' : 'Is this a mexican restaurant in California?',
                                   'record': ['San Francisco', 'Mexican']},
                           'ft2': {'title': 'Is this a French restaurant in Texas?',
                                   'record': ['El paso', 'Mediterranean']}}
        send_request(data, crowds)

def parse_args():
    parser = ArgumentParser()
    parser.add_argument('--task-types', '-t', nargs="+", metavar="TASK_TYPE",
                        choices=['sa', 'er', 'ft'],
                        help='task types for which to create HITs.')
    parser.add_argument('--crowds', '-c', nargs="+", metavar="CROWD_NAME",
                        choices=['amt', 'internal'],
                        help='crowds on which to create HITs.')
    args = parser.parse_args()
    return args

if __name__ == "__main__":
    args = parse_args()
    create_hit(args.crowds, args.task_types)
