from djcelery import celery
from callback import *

# Function for gathering results after a HIT gets enough votes from the crowd
@celery.task
def gather_answer(current_hit) :


    make_em_answer(current_hit)
    current_hit.group.HIT_finished += 1
    current_hit.group.save()    
    submit_callback_answer(current_hit)

