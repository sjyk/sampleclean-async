from django.db import models
from basecrowd.models import AbstractCrowdTaskGroup
from basecrowd.models import AbstractCrowdTask
from basecrowd.models import AbstractCrowdWorker
from basecrowd.models import AbstractCrowdWorkerResponse

# Inherited crowd models for the interface.
# No need for special subclasses, we use the base implementations.
class CrowdTaskGroup(AbstractCrowdTaskGroup): pass
class CrowdTask(AbstractCrowdTask): pass
class CrowdWorker(AbstractCrowdWorker): pass
class CrowdWorkerResponse(AbstractCrowdWorkerResponse): pass
