from interface import AMT_INTERFACE
from models import *
from basecrowd.interface import CrowdRegistry

# Register the AMT crowd with our basecrowd
CrowdRegistry.register_crowd(
    AMT_INTERFACE,
    task_model=CrowdTask,
    group_model=CrowdTaskGroup,
    worker_model=CrowdWorker,
    response_model=CrowdWorkerResponse,
    metric_model=CrowdMetric)
