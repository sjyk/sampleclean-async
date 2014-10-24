from django.template import RequestContext, TemplateDoesNotExist
from django.template.loader import get_template, select_template
from django.views.decorators.clickjacking import xframe_options_exempt
from django.views.decorators.csrf import csrf_exempt
from django.views.decorators.http import require_GET, require_POST
from django.http import HttpResponse
from datetime import datetime
import pytz
import json
import os

from basecrowd.interface import CrowdRegistry
from basecrowd.tasks import gather_answer

# Create new tasks
@require_POST
@csrf_exempt
def create_task_group(request, crowd_name):
    ''' See README.md for API. '''

    # get the interface implementation from the crowd name.
    interface, model_spec = CrowdRegistry.get_registry_entry(crowd_name)

    # Response dictionaries
    correct_response = {'status' : 'ok'}
    wrong_response = {'status' : 'wrong'}

    # Parse information contained in the URL
    json_dict = request.POST.get('data')

    # Validate the format.
    if not interface.validate_create_request(json_dict):
        return HttpResponse(json.dumps(wrong_response))

    # Pull out important data fields
    json_dict = json.loads(json_dict)
    configuration = json_dict['configuration']
    group_id = json_dict['group_id']
    group_context = json.dumps(json_dict['group_context'])
    content = json_dict['content']
    point_identifiers = content.keys()

    # Create a new group for the tasks.
    current_group = model_spec.group_model(
        group_id=group_id,
        tasks_finished=0,
        callback_url=configuration['callback_url'],
        group_context=group_context,
        crowd_config = json.dumps(configuration[crowd_name]))

    # Call the group hook function, then save the new group to the database.
    interface.group_pre_save(current_group)
    current_group.save()

    # Create a task for each batch of points.
    for i in range(0, len(point_identifiers), configuration['task_batch_size']):

        # build the batch
        current_content = {}
        for j in range(i, i + configuration['task_batch_size']):

            if j >= len(point_identifiers):
                break
            current_content[point_identifiers[j]] = content[point_identifiers[j]]
        current_content = json.dumps(current_content)


        # Call the create task hook
        current_task_id = interface.create_task(configuration, current_content)

        # Build the task object
        current_task = model_spec.task_model(
            task_type=configuration['task_type'],
            data=current_content,
            create_time=pytz.utc.localize(datetime.now()),
            task_id=current_task_id,
            group=current_group,
            num_assignments=configuration['num_assignments'])

        # Call the pre-save hook, then save the task to the database.
        interface.task_pre_save(current_task)
        current_task.save()

    return HttpResponse(json.dumps(correct_response))

# Delete all tasks from the system.
def purge_tasks(request, crowd_name):
    interface, model_spec = CrowdRegistry.get_registry_entry(crowd_name)
    tasks = model_spec.task_model.objects.all()

    # Call the delete hook, then delete the tasks from our database.
    interface.delete_tasks(tasks)
    tasks.delete()
    return HttpResponse('ok')


# we need this view to load in AMT's iframe, so disable Django's built-in
# clickjacking protection.
@xframe_options_exempt
@require_GET
def get_assignment(request, crowd_name):

    # get the interface implementation from the crowd name.
    interface, model_spec = CrowdRegistry.get_registry_entry(crowd_name)

    # get assignment context
    context = interface.get_assignment_context(request)
    interface.require_context(
        context, ['task_id', 'is_accepted'],
        ValueError('Task id unavailable in assignment request context.'))

    # Retrieve the tweet based on task_id from the database
    try:
        current_task = model_spec.task_model.objects.get(
            task_id=context['task_id'])
    except model_spec.task_model.DoesNotExist:
        raise ValueError('Invalid task id: ' + context['task_id'])

    content = json.loads(current_task.data)
    group_context = json.loads(current_task.group.group_context)

    # Save the information of this worker
    worker_id = context.get('worker_id')
    if worker_id:
        try:
            current_worker = model_spec.worker_model.objects.get(
                worker_id=worker_id)
        except model_spec.worker_model.DoesNotExist:
            current_worker = model_spec.worker_model(
                worker_id=context['worker_id'])

            # Call the pre-save hook, the save to the database
            interface.worker_pre_save(current_worker)
            current_worker.save()
    else:
        current_worker = None

    # Relate workers and tasks (after a worker accepts the task).
    if context.get('is_accepted', False):
        if not current_worker:
            raise ValueError("Accepted tasks must have an associated worker.")
        if not current_worker.tasks.filter(task_id=current_task.task_id).exists():
            current_worker.tasks.add(current_task)

    # Add task data to the context.
    crowd_config = json.loads(current_task.group.crowd_config)
    context.update(group_context=group_context,
                   content=content,
                   backend_submit_url=interface.get_backend_submit_url(),
                   frontend_submit_url=interface.get_frontend_submit_url(crowd_config))

    # Get the base template name, preferring a crowd-specific base template.
    try:
        base_template_name = os.path.join(crowd_name, 'base.html')
        t = get_template(base_template_name)
    except TemplateDoesNotExist:
        base_template_name = 'basecrowd/base.html'
    context['base_template_name'] = base_template_name

    # Load the child template, preferring a crowd-specific implementation
    template = select_template([
        os.path.join(crowd_name, current_task.task_type + '.html'),
        os.path.join('basecrowd', current_task.task_type + '.html')])

    return HttpResponse(template.render(RequestContext(request, context)))

# When workers submit assignments, we should send data to this view via AJAX
# before submitting to AMT.
@require_POST
@csrf_exempt
def post_response(request, crowd_name):

    # get the interface implementation from the crowd name.
    interface, model_spec = CrowdRegistry.get_registry_entry(crowd_name)

    # get context from the request
    context = interface.get_response_context(request)

    # validate context
    interface.require_context(
        context, ['assignment_id', 'task_id', 'worker_id', 'answers'],
        ValueError("Response context missing required keys."))

    # Check if this is a duplicate response
    if model_spec.response_model.objects.filter(
            assignment_id=context['assignment_id']).exists():
        return HttpResponse('Duplicate!')

    # Retrieve the task and worker from the database based on ids.
    current_task = model_spec.task_model.objects.get(task_id=context['task_id'])
    current_worker = model_spec.worker_model.objects.get(
        worker_id=context['worker_id'])

    # Store this response into the database
    current_response = model_spec.response_model(
        task=current_task,
        worker=current_worker,
        content=context['answers'],
        assignment_id=context['assignment_id'])
    interface.response_pre_save(current_response)
    current_response.save()

    # Check if this task has been finished
    # If we've gotten too many responses, ignore.
    if (not current_task.is_complete
        and current_task.responses.count() >= current_task.num_assignments):
        current_task.is_complete = True
        current_task.save()
        gather_answer.delay(current_task.task_id, model_spec)

    return HttpResponse('ok') # AJAX call succeded.
