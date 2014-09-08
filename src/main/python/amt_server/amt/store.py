from datetime import datetime
from models import *
import json


# Store a group into the database
def store_group(_group_id, _HIT_finished, _callback_url, _group_context):

    current_group = Group(group_id = _group_id,
                          HIT_finished = _HIT_finished,
                          callback_url = _callback_url,
                          group_context = _group_context)
    current_group.save()
    
# Store a hit into the database
def store_hit(_type, _content, _create_time, _HITId, _group, _num_assignment):

    current_hit = HIT(type = _type,
                      content = _content,
                      create_time = _create_time,
                      HITId = _HITId,
                      group = _group,
                      num_assignment = _num_assignment)
    current_hit.save()

# Store the information of a worker into the database
def store_worker(_worker_id):

    current_worker = Worker(worker_id = _worker_id)
    current_worker.save()

# Store the information of a response
def store_response(_hit, _worker, _content, _assignment_id):
    
    current_response = Response(hit = _hit,
                                HITId = _hit.HITId,
                                worker = _worker,
                                workerId = _worker.worker_id,
                                content = _content,
                                assignment_id = _assignment_id)
    current_response.save()
    
# Store the information of an acceptance
def store_request(request):

    # Extract the POST, GET and META parameters
    request_dict = {}
    
    for key, value in request.GET.iteritems():
        request_dict[key] = value
    
    for key, value in request.POST.iteritems():
        request_dict[key] = value
      
    current_request = Request(path = request.get_full_path(),
                              post_json = json.dumps(request_dict),
                              recv_time = datetime.now())
    current_request.save()
