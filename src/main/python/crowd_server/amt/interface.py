import json
import pytz
from datetime import datetime

from django.conf import settings

from basecrowd.interface import CrowdInterface
from connection import create_hit, disable_hit, AMT_NO_ASSIGNMENT_ID
from models import Request

class AMTCrowdInterface(CrowdInterface):
    @staticmethod
    def create_task(configuration, content):
        # Use the boto API to create an AMT HIT
        additional_options = {'num_responses' : configuration['num_assignments']}
        return create_hit(additional_options)

    @staticmethod
    def delete_tasks(task_objects):
        # Use the boto API to delete the HITs
        for task in task_objects:
            try:
                disable_hit(task.task_id)
            except ValueError:
                pass

    @staticmethod
    def get_assignment_context(request):
        # parse information from AMT in the URL
        context = {
            'task_id': request.GET.get('hitId'),
            'worker_id': request.GET.get('workerId'),
            'submit_url': request.GET.get('turkSubmitTo'),
        }

        # check for requests for a preview of the task
        assignment_id = request.GET.get('assignmentId')
        if assignment_id == AMT_NO_ASSIGNMENT_ID:
            assignment_id = None
            is_accepted = False
        else:
            is_accepted = True
        context['assignment_id'] = assignment_id
        context['is_accepted'] = is_accepted

        # store the request if it has been accepted
        if is_accepted:
            Request.objects.create(
                path=request.get_full_path(),
                post_json=json.dumps(dict(request.GET.items() + request.POST.items())),
                recv_time=pytz.utc.localize(datetime.now()))

        return context

    @staticmethod
    def get_response_context(request):
        # Extract data from the request
        return {'answers': request.POST.get('answers'),
                'task_id': request.POST.get('HITId'),
                'worker_id': request.POST.get('workerId'),
                'assignment_id': request.POST.get('assignmentId')
            }

    @staticmethod
    def get_frontend_submit_url():
        return settings.AMT_SANDBOX_WORKER_SUBMIT

AMT_INTERFACE = AMTCrowdInterface('amt')
