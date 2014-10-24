import httplib
import socket
import ssl
import sys
import json
import urllib
import urllib2
import uuid
import random

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

def send_request(data):
    url = 'https://127.0.0.1:8000/dashboard/results/'
    response = urllib2.urlopen(url, urllib.urlencode(data))
    res = json.loads(response.read())
    if res['status'] != 'ok':
        print 'Got something wrong!!!!!!!!!!!!!!!!!!!!!!!!!!!!'

# Create batches of task
def create_query_results(query_ids):

    # install custom opener
    urllib2.install_opener(urllib2.build_opener(HTTPSHandlerV3()))

    for query_id, num_groups in query_ids.iteritems():
        is_grouped = num_groups > 1
        if is_grouped:
            results = { 'group%d' % (i+1) : random.uniform(10000,20000)
                        for i in range(num_groups) }
            query_string = "SELECT count(*) FROM the_best_table GROUP BY awesomeness;"
        else:
            results = random.uniform(10000, 20000)
            query_string = "SELECT count(*) FROM the_best_table;"
        data = {
            'querystring': query_string,
            'query_id': query_id,
            'pipeline_id': str(uuid.uuid4()),
            'result_col_name': 'num_rows',
            'grouped': "true" if is_grouped else "false",
            'results': json.dumps(results)
        }
        send_request(data)

def parse_args():
    parser = ArgumentParser(
        description="Post sample query results to the result dashboard")
    parser.add_argument('--query-ids', '-q', nargs="+", metavar="QUERY_ID",
                        help=('Query ids to create new results for. Defaults '
                              'to a single random query id'))
    parser.add_argument('--num-groups', '-g', nargs="+", type=int,
                        metavar="NUM_GROUPS",
                        help=('The number of groups for each query id '
                              'specified by -q. If 1, the query will be '
                              'ungrouped. Defaults to 1 each query.'))
    args = parser.parse_args()

    if not args.query_ids:
        args.query_ids = [str(uuid.uuid4())]

    if not args.num_groups:
        args.num_groups = [1 for query_id in args.query_ids]

    for num_group in args.num_groups:
        if num_group < 1:
            raise ValueError("A query can't have less than 1 group!")

    if len(args.num_groups) != len(args.query_ids):
        print ("Length of --num-groups option must match the length of the "
               "--query-ids option!")
        print ""
        parser.print_usage()
        sys.exit()

    args.query_id_map = { args.query_ids[i] : args.num_groups[i]
                       for i in range(len(args.query_ids)) }
    return args

if __name__ == "__main__":
    args = parse_args()
    create_query_results(args.query_id_map)
