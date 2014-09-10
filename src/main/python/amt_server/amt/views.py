from django.shortcuts import render
from django.views.decorators.clickjacking import xframe_options_exempt
from django.views.decorators.csrf import csrf_exempt
from django.views.decorators.http import require_GET, require_POST
from connection import create_hit, disable_hit, AMT_NO_ASSIGNMENT_ID
from django.http import HttpResponse
from django.conf import settings
from datetime import datetime
import pytz
import json
import os


from models import *
from blend import *
from store import *
from amt import tasks
        
# A separate view for generating HITs
@require_POST
@csrf_exempt
def hits_gen(request):
    '''
        See README.md        
    '''
    # Response dictionaries
    correct_response = {'status' : 'ok'}
    wrong_response = {'status' : 'wrong'}
    
    # Parse information contained in the URL
    json_dict = request.POST.get('data')
    # Check if it has correct format
    if not check_format(json_dict) :
        return HttpResponse(json.dumps(wrong_response))

    # Loads the JSON string to a dictionary
    json_dict = json.loads(json_dict)
    configuration = json_dict['configuration']
    
    # Retrieve configuration
    hit_type = configuration['type']
    hit_batch_size = configuration['hit_batch_size']
    num_assignment = configuration['num_assignments']
    callback_url = configuration['callback_url']
    
    # Retrieve other important fields
    group_id = json_dict['group_id']
    content = json_dict['content']
    group_context = json.dumps(json_dict['group_context'])
    
    # Store the current group into the database
    store_group(group_id, 0, callback_url, group_context)
    current_group = Group.objects.filter(group_id = group_id)[0]

    point_identifiers = content.keys()

    for i in range(0, len(point_identifiers), hit_batch_size) :
                
        # Using boto API to create an AMT HIT
        additional_options = {'num_responses' : num_assignment}
        current_hit_id = create_hit(additional_options)

        current_content = {}
        for j in range(i, i + hit_batch_size) :
            
            if j >= len(point_identifiers):
                break
            current_content[point_identifiers[j]] = content[point_identifiers[j]]
            
        # Deal with delimiters
        current_content = json.dumps(current_content)
                
        # Save this HIT to the database
        store_hit(hit_type,
                  current_content,
                  pytz.utc.localize(datetime.now()),
                  current_hit_id,
                  current_group,
                  num_assignment)
                
    return HttpResponse(json.dumps(correct_response))

def hits_del(request) :

    hit_set = HIT.objects.all()
    for hit in hit_set :
        try:
            disable_hit(hit.HITId)
        except:
            pass
    HIT.objects.all().delete()
    return HttpResponse('ok')


# we need this view to load in AMT's iframe, so disable Django's built-in
# clickjacking protection.
@xframe_options_exempt
@require_GET
def get_assignment(request):

    # parse information from AMT in the URL
    hit_id = request.GET.get('hitId')
    worker_id = request.GET.get('workerId')
    submit_url = request.GET.get('turkSubmitTo')
    assignment_id = request.GET.get('assignmentId')

    # this request is for a preview of the task: we shouldn't allow submission.
    if assignment_id == AMT_NO_ASSIGNMENT_ID:
        assignment_id = None
        allow_submission = False
    else:
        allow_submission = True

    # Retrieve the tweet based on hit_id from the database
    
    current_hit = HIT.objects.filter(HITId = hit_id)
    if len(current_hit) != 0:
        current_hit = current_hit[0]
        content = current_hit.content
    else:
        return HttpResponse('No task available at the moment')

    content = json.loads(content)
    group_context = json.loads(current_hit.group.group_context)
    
    # Save the information of this worker
    if worker_id != None:
        store_worker(worker_id)
        current_worker = Worker.objects.filter(worker_id = worker_id)[0]
    else:
        current_worker = None
    
    # Save the information of this request(only when it is an acceptance)
    if assignment_id != None:
        store_request(request)
    
    # Build relationships between workers and HITs (when a worker accepts this hit)
    if current_worker != None and assignment_id != None and current_hit != None:
        current_worker.hits.add(current_hit)

    # Render the template    
    context = {'assignment_id' : assignment_id,
               'group_context' : group_context,
               'content' : content,
               'allow_submission' : allow_submission
                }
    return render(request, 'amt/' + current_hit.type + '.html', context)
        
# When workers submit assignments, we should send data to this view via AJAX
# before submitting to AMT.
@require_POST
@csrf_exempt
def post_response(request):

    # Extract data from the request 
    answers = request.POST['answers']
    hit_id = request.POST['HITId']
    worker_id = request.POST['workerId']
    assignment_id = request.POST['assignmentId']

    # Check if this is a duplicate response
    if Response.objects.filter(assignment_id = assignment_id).count() > 0 :
        return HttpResponse('Duplicate!')
    
    # Retrieve the corresponding HIT from the database based on the HITId
    current_hit = HIT.objects.filter(HITId = hit_id)[0]
    
    # Retrieve the worker from the database based on the workerId
    current_worker = Worker.objects.filter(worker_id = worker_id)[0]

    # Store this response into the database
    store_response(current_hit, current_worker, answers, assignment_id)

    # Check if this HIT has been finished 
    if current_hit.response_set.count() == current_hit.num_assignment:

        tasks.gather_answer.delay(current_hit)
        
    return HttpResponse('ok') # AJAX call succeded.
