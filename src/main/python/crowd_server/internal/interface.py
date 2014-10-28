import uuid

from django.core.urlresolvers import reverse
from django.db.models import Q

from basecrowd.interface import CrowdInterface
from models import CrowdTask

class InternalCrowdInterface(CrowdInterface):

    @staticmethod
    def get_assignment_context(request):
        """ Get a random task of the specified type."""
        worker_id = request.GET.get('worker_id')
        task_type = request.GET.get('task_type')
        task = (CrowdTask.objects
                # Only tasks still in progress.
                # NOTE: this will allow over-assigning tasks.
                .filter(is_complete=False,
                        task_type=task_type)

                # No tasks the worker has already worked on.
                .filter(~Q(responses__worker__worker_id=worker_id))

                # Pick a random one
                .order_by('?')[0])

        # generate a random assignment id for this assignment.
        assignment_id = uuid.uuid4()

        return {
            'task_id': task.task_id,
            'worker_id': worker_id,
            'is_accepted': True,
            'assignment_id': assignment_id,
        }


    def get_frontend_submit_url(self, crowd_config):
        return reverse('internal:fake_submit_endpoint')

INTERNAL_CROWD_INTERFACE = InternalCrowdInterface('internal')
