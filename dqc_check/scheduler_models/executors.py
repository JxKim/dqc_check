from abc import ABCMeta, abstractmethod
from collections import defaultdict
from datetime import datetime, timedelta
from traceback import format_tb
import logging
import sys
from dqc_check.scheduler_models import MaxInstancesReachedError
import six
import concurrent.futures


try:
    from concurrent.futures.process import BrokenProcessPool
except ImportError:
    BrokenProcessPool = None

class BaseExecutor(six.with_metaclass(ABCMeta, object)):
    """Abstract base class that defines the interface that every executor must implement."""

    _scheduler = None
    _lock = None
    _logger = logging.getLogger('executors')

    def __init__(self):
        super(BaseExecutor, self).__init__()
        self._instances = defaultdict(lambda: 0)

    def start(self, scheduler, alias):
        """
        Called by the scheduler when the scheduler is being started or when the executor is being
        added to an already running scheduler.

        :param apscheduler.schedulers.base.BaseScheduler scheduler: the scheduler that is starting
            this executor
        :param str|unicode alias: alias of this executor as it was assigned to the scheduler

        """
        self._scheduler = scheduler
        self._lock = scheduler._create_lock()
        self._logger = logging.getLogger('executors.%s' % alias)

    def shutdown(self, wait=True):
        """
        Shuts down this executor.

        :param bool wait: ``True`` to wait until all submitted jobs
            have been executed
        """

    def submit_job(self, job, run_times):
        """
        Submits job for execution.

        :param Job job: job to execute
        :param list[datetime] run_times: list of datetimes specifying
            when the job should have been run
        :raises MaxInstancesReachedError: if the maximum number of
            allowed instances for this job has been reached

        """
        assert self._lock is not None, 'This executor has not been started yet'
        with self._lock:
            if self._instances[job.id] >= job.max_instances:
                raise MaxInstancesReachedError(job)

            self._do_submit_job(job, run_times)
            self._instances[job.id] += 1

    @abstractmethod
    def _do_submit_job(self, job, run_times):
        """Performs the actual task of scheduling `run_job` to be called."""

    def _run_job_success(self, job_id, events):
        """
        Called by the executor with the list of generated events when :func:`run_job` has been
        successfully called.

        """
        with self._lock:
            self._instances[job_id] -= 1
            if self._instances[job_id] == 0:
                del self._instances[job_id]

        for event in events:
            self._scheduler._dispatch_event(event)

    def _run_job_error(self, job_id, exc, traceback=None):
        """Called by the executor with the exception if there is an error  calling `run_job`."""
        with self._lock:
            self._instances[job_id] -= 1
            if self._instances[job_id] == 0:
                del self._instances[job_id]

        exc_info = (exc.__class__, exc, traceback)
        self._logger.error('Error running job %s', job_id, exc_info=exc_info)

def run_job(job, run_times, logger_name):
    """
    真正执行相关任务
    Called by executors to run the job. Returns a list of scheduler events to be dispatched by the
    scheduler.

    """
    events = []
    logger = logging.getLogger(logger_name)
    for run_time in run_times:
        # See if the job missed its run time window, and handle
        # possible misfires accordingly
        if job.misfire_grace_time is not None:
            difference = datetime.now() - run_time
            grace_time = timedelta(seconds=job.misfire_grace_time)
            if difference > grace_time:
                logger.warning('Run time of job "%s" was missed by %s', job, difference)
                continue

        logger.info('Running job "%s" (scheduled at %s)', job, run_time)
        try:
            retval = job.func(*job.args, **job.kwargs)
        except BaseException:
            exc, tb = sys.exc_info()[1:]
            formatted_tb = ''.join(format_tb(tb))

            logger.exception('Job "%s" raised an exception', job)

            # This is to prevent cyclic references that would lead to memory leaks
            import traceback
            traceback.clear_frames(tb)
            del tb
        else:
            logger.info('Job "%s" executed successfully', job)

    return events


class BasePoolExecutor(BaseExecutor):
    @abstractmethod
    def __init__(self, pool):
        super(BasePoolExecutor, self).__init__()
        self._pool = pool

    def _do_submit_job(self, job, run_times):
        def callback(f):
            exc, tb = (f.exception_info() if hasattr(f, 'exception_info') else
                       (f.exception(), getattr(f.exception(), '__traceback__', None)))
            if exc:
                self._run_job_error(job.id, exc, tb)
            else:
                self._run_job_success(job.id, f.result())

        try:
            f = self._pool.submit(run_job, job, run_times, self._logger.name)
        except BrokenProcessPool:
            self._logger.warning('Process pool is broken; replacing pool with a fresh instance')
            self._pool = self._pool.__class__(self._pool._max_workers)
            f = self._pool.submit(run_job, job, job._jobstore_alias, run_times, self._logger.name)

        f.add_done_callback(callback)

    def shutdown(self, wait=True):
        self._pool.shutdown(wait)


class ThreadPoolExecutor(BasePoolExecutor):
    """
    An executor that runs jobs in a concurrent.futures thread pool.
    Plugin alias: ``threadpool``
    :param max_workers: the maximum number of spawned threads.
    :param pool_kwargs: dict of keyword arguments to pass to the underlying
        ThreadPoolExecutor constructor
    """

    def __init__(self, max_workers=10, pool_kwargs=None):
        pool_kwargs = pool_kwargs or {}
        pool = concurrent.futures.ThreadPoolExecutor(int(max_workers), **pool_kwargs)
        super(ThreadPoolExecutor, self).__init__(pool)