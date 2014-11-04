import uuid
from numpy.random import geometric

from django.core.urlresolvers import reverse
from django.db.models import Count, F, Q

from basecrowd.interface import CrowdInterface
from models import CrowdTask

SLACK = 2
ORDER_WEIGHT = 0.3
class InternalCrowdInterface(CrowdInterface):

    @staticmethod
    def get_assignment_context(request):
        """ Get a random task of the specified type."""
        worker_id = request.GET.get('worker_id')
        task_type = request.GET.get('task_type')
        eligible_tasks = (InternalCrowdInterface.get_eligible_tasks(worker_id)
                          .filter(task_type=task_type)
                          .order_by('create_time'))

        # Pick a random task, biased towards older tasks.
        task_index = min(geometric(ORDER_WEIGHT) - 1,
                         eligible_tasks.count() - 1)

        # generate a random assignment id for this assignment.
        assignment_id = uuid.uuid4()

        return {
            'task_id': (eligible_tasks[task_index].task_id
                        if task_index >= 0 else None),
            'worker_id': worker_id,
            'is_accepted': True,
            'assignment_id': assignment_id,
        }


    def get_frontend_submit_url(self, crowd_config):
        return reverse('internal:fake_submit_endpoint')

    @staticmethod
    def get_eligible_tasks(worker_id):
        return (
            CrowdTask.objects

            # Only tasks still in progress.
            .filter(is_complete=False)

            # No tasks the worker has already worked on.
            .exclude(responses__worker__worker_id=worker_id)

            # No tasks that already have enough workers assigned.
            # "Enough" is the number of assignments for the task plus some SLACK
            # for workers who abandon their tasks.
            .annotate(num_workers=Count('workers'))
            .filter(Q(num_workers__lt=F('num_assignments') + SLACK)

                     # always let worker work on a task they've already been
                     # assigned but haven't completed.
                     | Q(workers__worker_id=worker_id))

            # No duplicates
            .distinct())

INTERNAL_CROWD_INTERFACE = InternalCrowdInterface('internal')
