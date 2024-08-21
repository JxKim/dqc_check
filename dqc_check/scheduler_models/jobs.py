from abc import ABCMeta, abstractmethod
import logging

from inspect import ismethod, isclass
from uuid import uuid4

import six

from dqc_check.scheduler_models import BaseTrigger

try:
    from collections.abc import Iterable, Mapping
except ImportError:
    from collections import Iterable, Mapping


def get_callable_name(func):
    """
    Returns the best available display name for the given function/callable.

    :rtype: str

    """
    from inspect import isfunction,isbuiltin
    if ismethod(func):
        self = func.__self__
        cls = self if isclass(self) else type(self)
        return f"{cls.__qualname__}.{func.__name__}"
    elif isclass(func) or isfunction(func) or isbuiltin(func):
        return func.__qualname__
    elif hasattr(func, '__call__') and callable(func.__call__):
        # instance of a class with a __call__ method
        return type(func).__qualname__

    raise TypeError('Unable to determine a name for %r -- maybe it is not a callable?' % func)

def datetime_repr(dateval):
    return dateval.strftime('%Y-%m-%d %H:%M:%S %Z') if dateval else 'None'

class JobLookupError(KeyError):
    """Raised when the job store cannot find a job for update or removal."""

    def __init__(self, job_id):
        super(JobLookupError, self).__init__(u'No job by the id of %s was found' % job_id)


class ConflictingIdError(KeyError):
    """Raised when the uniqueness of job IDs is being violated."""

    def __init__(self, job_id):
        super(ConflictingIdError, self).__init__(
            u'Job identifier (%s) conflicts with an existing job' % job_id)


class TransientJobError(ValueError):
    """
    Raised when an attempt to add transient (with no func_ref) job to a persistent job store is
    detected.
    """

    def __init__(self, job_id):
        super(TransientJobError, self).__init__(
            u'Job (%s) cannot be added to this job store because a reference to the callable '
            u'could not be determined.' % job_id)

class Job(object):
    """
    该实例不需要用户自行实例化，通过调用scheduler调用add_job方法，即可以实现实例化
    Contains the options given when scheduling callables and its current schedule and other state.
    This class should never be instantiated by the user.

    :var str id: the unique identifier of this job
    :var str name: the description of this job
    :var func: the callable to execute
    :var tuple|list args: positional arguments to the callable
    :var dict kwargs: keyword arguments to the callable
    :var bool coalesce: whether to only run the job once when several run times are due
    :var trigger: the trigger object that controls the schedule of this job
    :var str executor: the name of the executor that will run this job
    :var int misfire_grace_time: the time (in seconds) how much this job's execution is allowed to
        be late (``None`` means "allow the job to run no matter how late it is")
    :var int max_instances: the maximum number of concurrently executing instances allowed for this
        job
    :var datetime.datetime next_run_time: the next scheduled run time of this job

    .. note::
        The ``misfire_grace_time`` has some non-obvious effects on job execution. See the
        :ref:`missed-job-executions` section in the documentation for an in-depth explanation.
    """

    __slots__ = ('_scheduler', '_jobstore_alias', 'id', 'trigger', 'executor', 'func',
                 'args', 'kwargs', 'name', 'misfire_grace_time', 'coalesce', 'max_instances',
                 'next_run_time')

    # 需要在kwargs当中定义name=xxx，这可以通过调用scheduler.add_job的时候来实现
    def __init__(self, scheduler, id=None, **kwargs):
        super(Job, self).__init__()
        self._scheduler = scheduler
        self._jobstore_alias = None
        self._modify(id=id or uuid4().hex, **kwargs)

    def modify(self, **changes):
        """
        Makes the given changes to this job and saves it in the associated job store.

        Accepted keyword arguments are the same as the variables on this class.

        .. seealso:: :meth:`~apscheduler.schedulers.base.BaseScheduler.modify_job`

        :return Job: this job instance

        """
        self._scheduler.modify_job(self.id, self._jobstore_alias, **changes)
        return self

    def reschedule(self, trigger, **trigger_args):
        """
        Shortcut for switching the trigger on this job.

        .. seealso:: :meth:`~apscheduler.schedulers.base.BaseScheduler.reschedule_job`

        :return Job: this job instance

        """
        self._scheduler.reschedule_job(self.id, self._jobstore_alias, trigger, **trigger_args)
        return self

    def pause(self):
        """
        Temporarily suspend the execution of this job.

        .. seealso:: :meth:`~apscheduler.schedulers.base.BaseScheduler.pause_job`

        :return Job: this job instance

        """
        self._scheduler.pause_job(self.id, self._jobstore_alias)
        return self

    def resume(self):
        """
        Resume the schedule of this job if previously paused.

        .. seealso:: :meth:`~apscheduler.schedulers.base.BaseScheduler.resume_job`

        :return Job: this job instance

        """
        self._scheduler.resume_job(self.id, self._jobstore_alias)
        return self

    def remove(self):
        """
        Unschedules this job and removes it from its associated job store.

        .. seealso:: :meth:`~apscheduler.schedulers.base.BaseScheduler.remove_job`

        """
        self._scheduler.remove_job(self.id, self._jobstore_alias)

    @property
    def pending(self):
        """
        Returns ``True`` if the referenced job is still waiting to be added to its designated job
        store.

        """
        return self._jobstore_alias is None

    #
    # Private API
    #

    def _get_run_times(self, now):
        """
        Computes the scheduled run times between ``next_run_time`` and ``now`` (inclusive).

        :type now: datetime.datetime
        :rtype: list[datetime.datetime]

        """
        run_times = []
        next_run_time = self.next_run_time
        while next_run_time and next_run_time <= now:
            run_times.append(next_run_time)
            next_run_time = self.trigger.get_next_fire_time(next_run_time, now)

        return run_times

    def _modify(self, **changes):
        """
        审核校验changes，是否可以修改到Job当中去

        """
        approved = {}

        if 'id' in changes:
            value = changes.pop('id')
            if not isinstance(value, six.string_types):
                raise TypeError("id must be a nonempty string")
            if hasattr(self, 'id'):
                raise ValueError('The job ID may not be changed')
            approved['id'] = value

        if 'func' in changes or 'args' in changes or 'kwargs' in changes:
            func = changes.pop('func') if 'func' in changes else self.func
            args = changes.pop('args') if 'args' in changes else self.args
            kwargs = changes.pop('kwargs') if 'kwargs' in changes else self.kwargs


            if not hasattr(self, 'name') and changes.get('name', None) is None:
                changes['name'] = get_callable_name(func)

            if isinstance(args, six.string_types) or not isinstance(args, Iterable):
                raise TypeError('args must be a non-string iterable')
            if isinstance(kwargs, six.string_types) or not isinstance(kwargs, Mapping):
                raise TypeError('kwargs must be a dict-like object')



            approved['func'] = func
            approved['args'] = args
            approved['kwargs'] = kwargs

        if 'name' in changes:
            value = changes.pop('name')
            if not value or not isinstance(value, six.string_types):
                raise TypeError("name must be a nonempty string")
            approved['name'] = value

        if 'misfire_grace_time' in changes:
            value = changes.pop('misfire_grace_time')
            if value is not None and (not isinstance(value, six.integer_types) or value <= 0):
                raise TypeError('misfire_grace_time must be either None or a positive integer')
            approved['misfire_grace_time'] = value

        if 'coalesce' in changes:
            value = bool(changes.pop('coalesce'))
            approved['coalesce'] = value

        if 'max_instances' in changes:
            value = changes.pop('max_instances')
            if not isinstance(value, six.integer_types) or value <= 0:
                raise TypeError('max_instances must be a positive integer')
            approved['max_instances'] = value

        if 'trigger' in changes:
            trigger = changes.pop('trigger')
            if not isinstance(trigger, BaseTrigger):
                raise TypeError('Expected a trigger instance, got %s instead' %
                                trigger.__class__.__name__)

            approved['trigger'] = trigger

        if 'executor' in changes:
            value = changes.pop('executor')
            if not isinstance(value, six.string_types):
                raise TypeError('executor must be a string')
            approved['executor'] = value

        if 'next_run_time' in changes:
            value = changes.pop('next_run_time')
            approved['next_run_time'] = value

        if changes:
            raise AttributeError('The following are not modifiable attributes of Job: %s' %
                                 ', '.join(changes))

        for key, value in six.iteritems(approved):
            setattr(self, key, value)

    def __eq__(self, other):
        if isinstance(other, Job):
            return self.id == other.id
        return NotImplemented

    def __repr__(self):
        return '<Job (id=%s name=%s)>' % (self.id, self.name)

    def __str__(self):
        return self.__unicode__()

    def __unicode__(self):
        if hasattr(self, 'next_run_time'):
            status = ('next run at: ' + datetime_repr(self.next_run_time) if
                      self.next_run_time else 'paused')
        else:
            status = 'pending'

        return u'%s (trigger: %s, %s)' % (self.name, self.trigger, status)

class BaseJobStore(six.with_metaclass(ABCMeta)):
    """Abstract base class that defines the interface that every job store must implement."""

    _scheduler = None
    _alias = None
    _logger = logging.getLogger('jobstores')

    def start(self, scheduler, alias):
        """
        Called by the scheduler when the scheduler is being started or when the job store is being
        added to an already running scheduler.

        :param apscheduler.schedulers.base.BaseScheduler scheduler: the scheduler that is starting
            this job store
        :param str|unicode alias: alias of this job store as it was assigned to the scheduler
        """

        self._scheduler = scheduler
        self._alias = alias
        self._logger = logging.getLogger('jobstores.%s' % alias)

    def shutdown(self):
        """Frees any resources still bound to this job store."""

    def _fix_paused_jobs_sorting(self, jobs):
        for i, job in enumerate(jobs):
            if job.next_run_time is not None:
                if i > 0:
                    paused_jobs = jobs[:i]
                    del jobs[:i]
                    jobs.extend(paused_jobs)
                break

    @abstractmethod
    def lookup_job(self, job_id):
        """
        Returns a specific job, or ``None`` if it isn't found..

        The job store is responsible for setting the ``scheduler`` and ``jobstore`` attributes of
        the returned job to point to the scheduler and itself, respectively.

        :param str|unicode job_id: identifier of the job
        :rtype: Job
        """

    @abstractmethod
    def get_due_jobs(self, now):
        """
        Returns the list of jobs that have ``next_run_time`` earlier or equal to ``now``.
        The returned jobs must be sorted by next run time (ascending).

        :param datetime.datetime now: the current (timezone aware) datetime
        :rtype: list[Job]
        """

    @abstractmethod
    def get_next_run_time(self):
        """
        Returns the earliest run time of all the jobs stored in this job store, or ``None`` if
        there are no active jobs.

        :rtype: datetime.datetime
        """

    @abstractmethod
    def get_all_jobs(self):
        """
        Returns a list of all jobs in this job store.
        The returned jobs should be sorted by next run time (ascending).
        Paused jobs (next_run_time == None) should be sorted last.

        The job store is responsible for setting the ``scheduler`` and ``jobstore`` attributes of
        the returned jobs to point to the scheduler and itself, respectively.

        :rtype: list[Job]
        """

    @abstractmethod
    def add_job(self, job):
        """
        Adds the given job to this store.

        :param Job job: the job to add
        :raises ConflictingIdError: if there is another job in this store with the same ID
        """

    @abstractmethod
    def update_job(self, job):
        """
        Replaces the job in the store with the given newer version.

        :param Job job: the job to update
        :raises JobLookupError: if the job does not exist
        """

    @abstractmethod
    def remove_job(self, job_id):
        """
        Removes the given job from this store.

        :param str|unicode job_id: identifier of the job
        :raises JobLookupError: if the job does not exist
        """

    @abstractmethod
    def remove_all_jobs(self):
        """Removes all jobs from this store."""

    def __repr__(self):
        return '<%s>' % self.__class__.__name__

class MemoryJobStore(BaseJobStore):
    """
    Stores jobs in an array in RAM. Provides no persistence support.

    Plugin alias: ``memory``
    """

    def __init__(self):
        super(MemoryJobStore, self).__init__()
        # list of (job, timestamp), sorted by next_run_time and job id (ascending)
        # 包含任务及下次执行时间，所以当使用add_job的时候，会调用相关方法，将job添加到这个列表里面
        self._jobs = []
        self._jobs_index = {}  # id -> (job, timestamp) lookup table

    def lookup_job(self, job_id):
        return self._jobs_index.get(job_id, (None, None))[0]

    def get_due_jobs(self, now):
        # 此处的job并没有把now之前的任务删除掉，会常驻内存
        now_timestamp = now.timestamp()
        pending = []
        # 此处的job，都是按照执行时间升序排序的
        for job, timestamp in self._jobs:
            if timestamp is None or timestamp > now_timestamp:
                break
            pending.append(job)

        return pending

    def get_next_run_time(self):
        return self._jobs[0][0].next_run_time if self._jobs else None

    def get_all_jobs(self):
        return [j[0] for j in self._jobs]

    def add_job(self, job):
        if job.id in self._jobs_index:
            raise ConflictingIdError(job.id)

        # 此处的重点是获取到了任务的下次执行时间，在此基础上面去把任务插入到self._jobs当中去
        timestamp = job.next_run_time.timestamp()
        # 通过时间戳来获取到需要将任务放入到self._jobs的具体什么位置
        index = self._get_job_index(timestamp, job.id)
        self._jobs.insert(index, (job, timestamp))
        self._jobs_index[job.id] = (job, timestamp)

    def update_job(self, job):
        old_job, old_timestamp = self._jobs_index.get(job.id, (None, None))
        if old_job is None:
            raise JobLookupError(job.id)

        # If the next run time has not changed, simply replace the job in its present index.
        # Otherwise, reinsert the job to the list to preserve the ordering.
        old_index = self._get_job_index(old_timestamp, old_job.id)
        new_timestamp = job.next_run_time.timestamp()
        if old_timestamp == new_timestamp:
            self._jobs[old_index] = (job, new_timestamp)
        else:
            del self._jobs[old_index]
            new_index = self._get_job_index(new_timestamp, job.id)
            self._jobs.insert(new_index, (job, new_timestamp))

        self._jobs_index[old_job.id] = (job, new_timestamp)

    def remove_job(self, job_id):
        job, timestamp = self._jobs_index.get(job_id, (None, None))
        if job is None:
            raise JobLookupError(job_id)

        index = self._get_job_index(timestamp, job_id)
        del self._jobs[index]
        del self._jobs_index[job.id]

    def remove_all_jobs(self):
        self._jobs = []
        self._jobs_index = {}

    def shutdown(self):
        self.remove_all_jobs()

    def _get_job_index(self, timestamp, job_id):
        """
        Returns the index of the given job, or if it's not found, the index where the job should be
        inserted based on the given timestamp.

        :type timestamp: int
        :type job_id: str

        """
        lo, hi = 0, len(self._jobs)
        timestamp = float('inf') if timestamp is None else timestamp
        while lo < hi:
            mid = (lo + hi) // 2
            mid_job, mid_timestamp = self._jobs[mid]
            mid_timestamp = float('inf') if mid_timestamp is None else mid_timestamp
            if mid_timestamp > timestamp:
                hi = mid
            elif mid_timestamp < timestamp:
                lo = mid + 1
            elif mid_job.id > job_id:
                hi = mid
            elif mid_job.id < job_id:
                lo = mid + 1
            else:
                return mid

        return lo