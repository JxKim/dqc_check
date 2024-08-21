class SchedulerAlreadyRunningError(Exception):
    """Raised when attempting to start or configure the scheduler when it's already running."""

    def __str__(self):
        return 'Scheduler is already running'


class SchedulerNotRunningError(Exception):
    """Raised when attempting to shutdown the scheduler when it's not running."""

    def __str__(self):
        return 'Scheduler is not running'

class MaxInstancesReachedError(Exception):
    def __init__(self, job):
        super(MaxInstancesReachedError, self).__init__(
            'Job "%s" has already reached its maximum number of instances (%d)' %
            (job.id, job.max_instances))