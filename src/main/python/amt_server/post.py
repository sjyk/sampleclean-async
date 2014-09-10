import httplib
import socket
import ssl
import json
import urllib
import urllib2
import operator

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

def send_request(data) :

    # Send request
    params = {'data' : json.dumps(data)}

    response = urllib2.urlopen('https://127.0.0.1:8000/amt/hitsgen/',
                                urllib.urlencode(params))
    
    res = json.loads(response.read())
    if res['status'] != 'ok' :
        print 'Got something wrong!!!!!!!!!!!!!!!!!!!!!!!!!!!!'

# Create batches of HIT        
def create_hit() :

    # install custom opener
    urllib2.install_opener(urllib2.build_opener(HTTPSHandlerV3()))


    # Create a sentiment analysis HIT
    data = {}
    data['configuration'] = {}
    data['configuration']['type'] = 'sa'
    data['configuration']['hit_batch_size'] = 2
    data['configuration']['num_assignments'] = 1
    data['configuration']['callback_url'] = 'www.google.com'
    
    data['group_id'] = 'test1'
    data['group_context'] = {}   # Empty group contest for sentiment analysis
    
    # This configuration generates two HITs, one with two tweets, the other with one tweet.
    data['content'] = {'t1' : 'This is tweet No.1', 
                       't2' : 'This is tweet No.2',
                       't3' : 'This is tweet No.3'}    
    # Send request
    send_request(data)
    
    # Create a Deduplication HIT
    data = {}
    data['configuration'] = {}
    data['configuration']['type'] = 'er'
    data['configuration']['hit_batch_size'] = 1
    data['configuration']['num_assignments'] = 1
    data['configuration']['callback_url'] = 'www.google.com'
    
    data['group_id'] = 'test2'
    data['group_context'] = {'fields' : ['price', 'location']}
    
    # This configuration generates two HITs, each with one pair of records to be compared.
    data['content'] = {'pair1' : [['5', 'LA'], ['6', 'Berkeley']], 
                       'pair2' : [['80', 'London'], ['80.0', 'Londyn']]}
    
    send_request(data)    

if __name__ == "__main__":

    create_hit()

