import uuid
from urllib import urlencode

from django.db.models import Count, Sum
from django.shortcuts import render, redirect
from django.views.decorators.http import require_GET, require_POST
from django.views.decorators.csrf import csrf_exempt

from models import CrowdTask, CrowdWorker
from interface import INTERNAL_CROWD_INTERFACE as interface

@require_POST
@csrf_exempt
def fake_submit_endpoint(request):
    return redirect('internal:index')

@require_GET
def index(request):
    # Get worker id from session, or create one if this is a first-time user.
    worker_id = request.session.get('worker_id')
    if not worker_id:
        worker_id = str(uuid.uuid4())
        request.session['worker_id'] = worker_id

    # Map from task type to total assignments in the system
    assignments_by_type = dict(
        CrowdTask.objects.values_list('task_type')
        .annotate(num_assignments=Sum('num_assignments')))

    # Incomplete task types with the number of available assignments for each.
    task_types_incomplete = (CrowdTask.objects.values('task_type')
                             .filter(is_complete=False)
                             .annotate(num_assignments=Sum('num_assignments'))
                             .annotate(num_responses=Count('responses')))

    task_type_map = {
        'sa': 'Sentiment Analysis',
        'er': 'Entity Resolution',
        'ft': 'Filtering',
    }

    task_types = { t['task_type'] :
                   build_context(task_type_map, assignments_by_type,
                                 worker_id, task_type_obj=t)
                   for t in task_types_incomplete }

    for t_shortname, t_fullname in task_type_map.iteritems():
        if t_shortname not in task_types:
            task_types[t_shortname] = build_context(
                task_type_map, assignments_by_type, worker_id,
                task_type=t_shortname)

    # Render index template
    return render(request, 'internal/index.html', {'task_types': task_types,
                                                   'task_map': task_type_map})

def build_context(task_type_map, assignments_by_type, worker_id, task_type=None,
                  task_type_obj=None):
    task_type = task_type or task_type_obj['task_type']
    remaining_tasks = (task_type_obj['num_assignments']
                       - task_type_obj['num_responses']
                       if task_type_obj else 0)
    percent_complete = (100 * (1.0 - (float(remaining_tasks)
                                      / assignments_by_type[task_type]))
                        if task_type_obj else 100)
    assignment_url = (interface.get_assignment_url() + '?'
                      + urlencode({'task_type': task_type,
                                   'worker_id': worker_id}))

    return {'full_name': task_type_map[task_type],
            'remaining_tasks': remaining_tasks,
            'percent_complete': percent_complete,
            'assignment_url': assignment_url}
