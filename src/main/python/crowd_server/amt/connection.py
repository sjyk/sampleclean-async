''' Connection.py

    Utilities for connecting to Amazon's Mechanical Turk.
    Requires amazon's boto package (http://boto.readthedocs.org/en/latest/)

'''

from boto.mturk.connection import MTurkConnection, MTurkRequestError
from boto.mturk.question import ExternalQuestion
from boto.mturk.price import Price
from datetime import timedelta
from django.conf import settings
from django.contrib.sites.models import Site
from urllib2 import urlopen
import json

AMT_NO_ASSIGNMENT_ID = 'ASSIGNMENT_ID_NOT_AVAILABLE'

def get_amt_connection(sandbox):
    ''' Get a connection object to communicate with the AMT API. '''
    host = (settings.AMT_SANDBOX_HOST
            if sandbox else settings.AMT_HOST)
    return MTurkConnection(aws_access_key_id=settings.AMT_ACCESS_KEY,
                           aws_secret_access_key=settings.AMT_SECRET_KEY,
                           host=host)

def create_hit(hit_options):
    ''' Create a new HIT on AMT.

        `hit_options` is a dictionary that can contain:

        * `title`: The title that will show up in AMT's HIT listings
        * `description`: The description that will show up in AMT's HIT listings
        * `reward`: A float containing the number of cents to pay for each
          assignment
        * `duration`: The expected amount of time a worker should spend on each
          assignment, in minutes
        * `num_responses`: The number of responses to get for the HIT
        * `frame_height`: The height of the iframe in which workers will see the 
          assignment
        * `use_https`: whether or not to load assignment in AMT's iframe using
          HTTPS. Strongly recommended to be True

        By default, options are loaded from `settings.AMT_DEFAULT_HIT_OPTIONS`.
    '''
    options = settings.AMT_DEFAULT_HIT_OPTIONS
    options.update(hit_options)

    scheme = 'https' if options['use_https'] else 'http'

    from interface import AMT_INTERFACE
    path = AMT_INTERFACE.get_assignment_url()

    url = (scheme + '://' + json.loads(urlopen('http://jsonip.com').read())['ip'] + ':8000' +  path
           if settings.HAVE_PUBLIC_IP else scheme + '://' + Site.objects.get_current().domain + path)

    question = ExternalQuestion(
        external_url=url,
        frame_height=options['frame_height'])
    conn = get_amt_connection(options['sandbox'])

    create_response = conn.create_hit(
        question=question,
        title=options['title'],
        description=options['description'],
        reward=Price(amount=options['reward']),
        duration=timedelta(minutes=options['duration']),
        max_assignments=options['num_responses'],
        approval_delay=0)

    return create_response[0].HITId

def disable_hit(task) :
    crowd_config = json.loads(task.group.crowd_config)
    conn = get_amt_connection(crowd_config['sandbox'])
    try:
        conn.disable_hit(task.task_id)
    except MTurkRequestError, e:
        raise ValueError("Couldn't delete HIT " + task.task_id + ": " + str(e))
