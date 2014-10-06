import json
import urllib
import urllib2
from djcelery import celery
from quality_control.em import make_em_answer

# Function for gathering results after a task gets enough votes from the crowd
@celery.task
def gather_answer(current_task, model_spec):
    current_task.em_answer = make_em_answer(current_task, model_spec)
    current_task.save()
    current_task.group.tasks_finished += 1
    current_task.group.save()
    submit_callback_answer(current_task)

# Submit the answers to the callback URL
def submit_callback_answer(current_task):
    url = current_task.group.callback_url
    json_answer = {'group_id' : current_task.group.group_id}
    current_em_answer = json.loads(current_task.em_answer)

    json_answer['answers'] = []
    for key in current_em_answer.keys() :
        json_answer['answers'].append({'identifier' : key, 'value' : current_em_answer[key]})

    # Send back data using urllib2
    params = {'data' : json.dumps(json_answer)}
    urllib2.urlopen(url, urllib.urlencode(params))
