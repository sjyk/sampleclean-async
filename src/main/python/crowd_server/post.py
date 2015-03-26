import httplib
import socket
import ssl
import sys
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
        #try:
        #    self.sock = ssl.wrap_socket(sock, self.key_file, self.cert_file, ssl_version=ssl.PROTOCOL_TLSv1)
        #except ssl.SSLError, e:
        #    print("Trying SSLv3.")
        #    self.sock = ssl.wrap_socket(sock, self.key_file, self.cert_file, ssl_version=ssl.PROTOCOL_TLSv1)

class HTTPSHandlerV3(urllib2.HTTPSHandler):
    def https_open(self, req):
        return self.do_open(HTTPSConnectionV3, req)

def send_request(data, crowds, num_requests) :

    # Send request
    params = {'data' : json.dumps(data)}
    url = 'http://127.0.0.1:8000/crowds/%s/tasks/'
    for crowd in crowds:
        for i in range(num_requests):
            response = urllib2.urlopen(url%crowd,
                                       urllib.urlencode(params))
            res = json.loads(response.read())
            if res['status'] != 'ok' :
                print 'Got something wrong!!!!!!!!!!!!!!!!!!!!!!!!!!!!'
            if num_requests >= 100 and i % (num_requests / 10) == 0:
                print '.',
                sys.stdout.flush()

# Create batches of task
def create_tasks(crowds, task_types):

    # install custom opener
    urllib2.install_opener(urllib2.build_opener(HTTPSHandlerV3()))

    if 'sa' in task_types:
        num_tasks, num_assignments = task_types['sa']
        print ("Creating %d sentiment analysis tasks with %d assignments "
               "each..." % (num_tasks, num_assignments)),

        # Create a sentiment analysis task
        data = {}
        data['configuration'] = {}
        data['configuration']['task_type'] = 'sa'
        data['configuration']['task_batch_size'] = 3
        data['configuration']['num_assignments'] = num_assignments
        data['configuration']['callback_url'] = 'www.google.com'
        data['configuration']['amt'] = {'sandbox' : True}

        data['group_id'] = 'test1'
        data['group_context'] = {}  # Empty group contest for sentiment analysis

        # This configuration generates one task, with three tweets.
        data['content'] = {'t1' : 'This is tweet No.1',
                           't2' : 'This is tweet No.2',
                           't3' : 'This is tweet No.3'}
        # Send request
        send_request(data, crowds, num_tasks)
        print "Done!"

    if 'er' in task_types:
        num_tasks, num_assignments = task_types['er']
        print ("Creating %d entity resolution tasks with %d assignments "
               "each..." % (num_tasks, num_assignments)),

        # Create a Deduplication task
        data = {}
        data['configuration'] = {}
        data['configuration']['task_type'] = 'er'
        data['configuration']['task_batch_size'] = 2
        data['configuration']['num_assignments'] = num_assignments
        data['configuration']['callback_url'] = 'www.google.com'
        data['configuration']['amt'] = {'sandbox' : True}

        data['group_id'] = 'test2'
        data['group_context'] = {'fields' : ['price', 'location']}

        # This configuration generates one task with two pairs of records.
        data['content'] = {'pair1' : [['5', 'LA'], ['6', 'Berkeley']],
                           'pair2' : [['80', 'London'], ['80.0', 'Londyn']]}
        send_request(data, crowds, num_tasks)
        print "Done!"

    if 'ft' in task_types:
        num_tasks, num_assignments = task_types['ft']
        print ("Creating %d filtering tasks with %d assignments each..." %
               (num_tasks, num_assignments)),

        # Create a Filtering task
        data = {}
        data['configuration'] = {}
        data['configuration']['task_type'] = 'ft'
        data['configuration']['task_batch_size'] = 2
        data['configuration']['num_assignments'] = num_assignments
        data['configuration']['callback_url'] = 'www.google.com'
        data['configuration']['amt'] = {'sandbox' : True}

        data['group_id'] = 'test3'
        data['group_context'] = {'fields' : ['city', 'cuisine']}

        # This configuration generates one tasks with two pairs of records.
        data['content'] = {'ft1': {'title' : 'Is this a mexican restaurant in California?',
                                   'record': ['San Francisco', 'Mexican']},
                           'ft2': {'title': 'Is this a French restaurant in Texas?',
                                   'record': ['El paso', 'Mediterranean']}}
        send_request(data, crowds, num_tasks)
        print "Done!"

def parse_args():
    parser = ArgumentParser(description="Post sample tasks to the crowd server")
    parser.add_argument('--task-types', '-t', nargs="+", metavar="TASK_TYPE",
                        choices=['sa', 'er', 'ft'], default=['sa'],
                        help=('task types for which to create tasks. (defaults '
                              'to just \'sa\''))
    parser.add_argument('--crowds', '-c', nargs="+", metavar="CROWD_NAME",
                        choices=['amt', 'internal'], default=['internal'],
                        help=('crowds on which to create tasks. (defaults to '
                              'just \'internal\''))
    parser.add_argument('--num-tasks', '-n', nargs="+", type=int,
                        metavar="NUM_TASKS",
                        help=('Number of tasks to create (one number for each '
                              'task type given with -t). Defaults to one task '
                              'for each task type.'))
    parser.add_argument('--num-assignments', '-a', nargs="+", type=int,
                        metavar="NUM_ASSIGNMENTS",
                        help=('Number of assignments to require (one number '
                              'for each task type given with -t). Defaults to '
                              'one assignment for each task type.'))
    args = parser.parse_args()

    if not args.num_tasks:
        args.num_tasks = [1 for task_type in args.task_types]

    if not args.num_assignments:
        args.num_assignments = [1 for task_type in args.task_types]

    if (len(args.num_tasks) != len(args.task_types)
        or len(args.num_assignments) != len(args.task_types)):
        print ("Length of --num-tasks and --num-assignments options must match "
               "the length of the --task-types option!")
        print ""
        parser.print_usage()
        sys.exit()

    args.types_map = { args.task_types[i] : (args.num_tasks[i],
                                             args.num_assignments[i])
                       for i in range(len(args.num_tasks)) }
    return args

if __name__ == "__main__":
    args = parse_args()
    create_tasks(args.crowds, args.types_map)
