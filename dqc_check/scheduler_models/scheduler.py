
# 定义抽象基类
from abc import ABCMeta, abstractmethod

# 定义线程同步方法
from threading import RLock
from threading import Event

# 定义时间相关变量
from datetime import datetime, timedelta
from logging import getLogger
import warnings
import sys

# from tzlocal import get_localzone
import six
from threading import Thread
from dqc_check.scheduler_models import SchedulerAlreadyRunningError,SchedulerNotRunningError,MaxInstancesReachedError
from dqc_check.scheduler_models import ConflictingIdError,JobLookupError,BaseJobStore,Job,MemoryJobStore
from dqc_check.scheduler_models import BaseTrigger,CronTrigger
from dqc_check.scheduler_models import BaseExecutor,ThreadPoolExecutor



try:
    from collections.abc import MutableMapping
except ImportError:
    from collections import MutableMapping

try:
    from importlib.metadata import entry_points
except ModuleNotFoundError:
    from importlib_metadata import entry_points

try:
    from threading import TIMEOUT_MAX
except ImportError:
    TIMEOUT_MAX = 4294967  # Maximum value accepted by Event.wait() on Windows

def timedelta_seconds(delta):
    """
    Converts the given timedelta to seconds.

    :type delta: timedelta
    :rtype: float

    """
    return delta.days * 24 * 60 * 60 + delta.seconds + \
        delta.microseconds / 1000000.0



#: constant indicating a scheduler's stopped state
STATE_STOPPED = 0
#: constant indicating a scheduler's running state (started and processing jobs)
STATE_RUNNING = 1
#: constant indicating a scheduler's paused state (started but not processing jobs)
STATE_PAUSED = 2


class _Undefined(object):
    def __nonzero__(self):
        return False

    def __bool__(self):
        return False

    def __repr__(self):
        return '<undefined>'


undefined = _Undefined()  #: a unique object that only signifies that no value is defined

class BaseScheduler(six.with_metaclass(ABCMeta)):
    """
    Abstract base class for all schedulers.

    Takes the following keyword arguments:

    :param str|logging.Logger logger: logger to use for the scheduler's logging (defaults to
        apscheduler.scheduler)
    :param str|datetime.tzinfo timezone: the default time zone (defaults to the local timezone)
    :param int|float jobstore_retry_interval: the minimum number of seconds to wait between
        retries in the scheduler's main loop if the job store raises an exception when getting
        the list of due jobs
    :param dict job_defaults: default values for newly added jobs
    :param dict jobstores: a dictionary of job store alias -> job store instance or configuration
        dict
    :param dict executors: a dictionary of executor alias -> executor instance or configuration
        dict

    :ivar int state: current running state of the scheduler (one of the following constants from
        ``apscheduler.schedulers.base``: ``STATE_STOPPED``, ``STATE_RUNNING``, ``STATE_PAUSED``)

    .. seealso:: :ref:`scheduler-config`
    """
    # The `group=...` API is only available in the backport, used in <=3.7, and in std>=3.10.

    #
    # Public API
    #

    def __init__(self):
        super(BaseScheduler, self).__init__()
        self._executors = {}
        self._executors_lock = self._create_lock()
        self._jobstores = {} # 当前不需要其他的jobstore，仅需要一个内存级别的jobstore就可以了
        self._jobstores_lock = self._create_lock()
        self._pending_jobs = []
        self.state = STATE_STOPPED
        # 对相关属性进行配置
        if self.state != STATE_STOPPED:
            raise SchedulerAlreadyRunningError
        # Set general options
        self._logger = getLogger('scheduler')

        self.jobstore_retry_interval = float(10)

        # 设置job的默认参数
        # job_defaults = {}
        self._job_defaults = {
            'misfire_grace_time': 300,
            'coalesce': True,
            'max_instances': 3
        }


    def start(self, paused=False):
        """
        Start the configured executors and job stores and begin processing scheduled jobs.

        :param bool paused: if ``True``, don't start job processing until :meth:`resume` is called
        :raises SchedulerAlreadyRunningError: if the scheduler is already running
        :raises RuntimeError: if running under uWSGI with threads disabled

        """
        # 任务已经开始，不能调用start方法
        if self.state != STATE_STOPPED:
            raise SchedulerAlreadyRunningError


        with self._executors_lock:
            # Create a default executor if nothing else is configured
            if 'default' not in self._executors:
                # 添加执行器
                self.add_executor(self._create_default_executor(), 'default')

            # Start all the executors
            for alias, executor in six.iteritems(self._executors):
                executor.start(self, alias)

        with self._jobstores_lock:
            # Create a default job store if nothing else is configured
            if 'default' not in self._jobstores:
                self.add_jobstore(self._create_default_jobstore(), 'default')

            # Start all the job stores
            for alias, store in six.iteritems(self._jobstores):
                # start方法会将store的调度器设置为当前的调度器
                store.start(self, alias)

            # Schedule all pending jobs
            for job, jobstore_alias, replace_existing in self._pending_jobs:
                # 将任务从pending list当中添加到job store当中去
                self._real_add_job(job, jobstore_alias, replace_existing)
            del self._pending_jobs[:]

        self.state = STATE_PAUSED if paused else STATE_RUNNING
        self._logger.info('Scheduler started')

        if not paused:
            self.wakeup()

    @abstractmethod
    def shutdown(self, wait=True):
        """
        Shuts down the scheduler, along with its executors and job stores.

        Does not interrupt any currently running jobs.

        :param bool wait: ``True`` to wait until all currently executing jobs have finished
        :raises SchedulerNotRunningError: if the scheduler has not been started yet

        """
        if self.state == STATE_STOPPED:
            raise SchedulerNotRunningError

        self.state = STATE_STOPPED

        # Shut down all executors
        with self._executors_lock, self._jobstores_lock:
            for executor in six.itervalues(self._executors):
                executor.shutdown(wait)

            # Shut down all job stores
            for jobstore in six.itervalues(self._jobstores):
                jobstore.shutdown()

        self._logger.info('Scheduler has been shut down')

    def pause(self):
        """
        Pause job processing in the scheduler.

        This will prevent the scheduler from waking up to do job processing until :meth:`resume`
        is called. It will not however stop any already running job processing.

        """
        if self.state == STATE_STOPPED:
            raise SchedulerNotRunningError
        elif self.state == STATE_RUNNING:
            self.state = STATE_PAUSED # 将当前状态调整为STATE_PAUSED
            self._logger.info('Paused scheduler job processing')

    def resume(self):
        """Resume job processing in the scheduler."""
        if self.state == STATE_STOPPED:
            raise SchedulerNotRunningError
        elif self.state == STATE_PAUSED:
            self.state = STATE_RUNNING
            self._logger.info('Resumed scheduler job processing')
            self.wakeup()

    @property
    def running(self):
        """
        Return ``True`` if the scheduler has been started.

        This is a shortcut for ``scheduler.state != STATE_STOPPED``.

        """
        return self.state != STATE_STOPPED

    def add_executor(self, executor, alias='default', **executor_opts):
        """
        Adds an executor to this scheduler.

        Any extra keyword arguments will be passed to the executor plugin's constructor, assuming
        that the first argument is the name of an executor plugin.

        :param str|unicode|executors.BaseExecutor executor: either an executor
            instance or the name of an executor plugin
        :param str|unicode alias: alias for the scheduler
        :raises ValueError: if there is already an executor by the given alias

        """
        with self._executors_lock:
            if alias in self._executors:
                raise ValueError('This scheduler already has an executor by the alias of "%s"' %
                                 alias)

            if isinstance(executor, BaseExecutor):
                self._executors[alias] = executor
            else:
                raise TypeError('Expected an executor instance or a string, got %s instead' %
                                executor.__class__.__name__)

            # Start the executor right away if the scheduler is running
            if self.state != STATE_STOPPED:
                executor.start(self, alias)

    def remove_executor(self, alias, shutdown=True):
        """
        Removes the executor by the given alias from this scheduler.

        :param str|unicode alias: alias of the executor
        :param bool shutdown: ``True`` to shut down the executor after
            removing it

        """
        with self._executors_lock:
            executor = self._lookup_executor(alias)
            del self._executors[alias]

        if shutdown:
            executor.shutdown()

    def add_jobstore(self, jobstore, alias='default', **jobstore_opts):
        """
        Adds a job store to this scheduler.

        Any extra keyword arguments will be passed to the job store plugin's constructor, assuming
        that the first argument is the name of a job store plugin.

        :param str|unicode|jobs.BaseJobStore jobstore: job store to be added
        :param str|unicode alias: alias for the job store
        :raises ValueError: if there is already a job store by the given alias

        """
        with self._jobstores_lock:
            if alias in self._jobstores:
                raise ValueError('This scheduler already has a job store by the alias of "%s"' %
                                 alias)

            if isinstance(jobstore, BaseJobStore):
                self._jobstores[alias] = jobstore
            else:
                raise TypeError('Expected a job store instance or a string, got %s instead' %
                                jobstore.__class__.__name__)

            # Start the job store right away if the scheduler isn't stopped
            if self.state != STATE_STOPPED:
                jobstore.start(self, alias)


        # Notify the scheduler so it can scan the new job store for jobs
        if self.state != STATE_STOPPED:
            self.wakeup()

    def remove_jobstore(self, alias, shutdown=True):
        """
        Removes the job store by the given alias from this scheduler.

        :param str|unicode alias: alias of the job store
        :param bool shutdown: ``True`` to shut down the job store after removing it

        """
        with self._jobstores_lock:
            jobstore = self._lookup_jobstore(alias)
            del self._jobstores[alias]

        if shutdown:
            jobstore.shutdown()


    # 需要有一个apply和job_list的一个对应关系，以防有测试的任务需要处理
    def add_job(self, func, trigger, args=None, kwargs=None, id=None, name=None,
                misfire_grace_time=180, coalesce=undefined, max_instances=5,
                next_run_time=undefined, jobstore='default', executor='default',
                replace_existing=False):
        """
        当任务还没有开始时，将其添加到pending_jobs列表当中
        add_job(func, trigger=None, args=None, kwargs=None, id=None, \
            name=None, misfire_grace_time=undefined, coalesce=undefined, \
            max_instances=undefined, next_run_time=undefined, \
            jobstore='default', executor='default', \
            replace_existing=False, **trigger_args)
        trigger是必传的参数，为DateTrigger或者是CronTrigger
        """
        job_kwargs = {
            'trigger': trigger,
            'executor': executor,
            'func': func,
            'args': tuple(args) if args is not None else (),
            'kwargs': dict(kwargs) if kwargs is not None else {},
            'id': id,
            'name': name,
            'misfire_grace_time': misfire_grace_time,
            'coalesce': coalesce,
            'max_instances': max_instances,
            'next_run_time': next_run_time
        }
        job_kwargs = dict((key, value) for key, value in six.iteritems(job_kwargs) if
                          value is not undefined)
        job = Job(self, **job_kwargs)

        # 懒加载，只有当真正开始scheduler的时候，才将job添加到job_store当中去
        with self._jobstores_lock:
            if self.state == STATE_STOPPED:
                self._pending_jobs.append((job, jobstore, replace_existing))
                self._logger.info('Adding job tentatively -- it will be properly scheduled when '
                                  'the scheduler starts')
            else:
                self._real_add_job(job, jobstore, replace_existing)

        return job

    def scheduled_job(self, trigger, args=None, kwargs=None, id=None, name=None,
                      misfire_grace_time=undefined, coalesce=undefined, max_instances=undefined,
                      next_run_time=undefined, jobstore='default', executor='default'):
        """
        scheduled_job(trigger, args=None, kwargs=None, id=None, \
            name=None, misfire_grace_time=undefined, \
            coalesce=undefined, max_instances=undefined, \
            next_run_time=undefined, jobstore='default', \
            executor='default',**trigger_args)

        A decorator version of :meth:`add_job`, except that ``replace_existing`` is always
        ``True``.

        .. important:: The ``id`` argument must be given if scheduling a job in a persistent job
        store. The scheduler cannot, however, enforce this requirement.

        """
        def inner(func):
            self.add_job(func, trigger, args, kwargs, id, name, misfire_grace_time, coalesce,
                         max_instances, next_run_time, jobstore, executor, True)
            return func
        return inner

    def modify_job(self, job_id, jobstore=None, **changes):
        """
        Modifies the properties of a single job.

        Modifications are passed to this method as extra keyword arguments.

        :param str|unicode job_id: the identifier of the job
        :param str|unicode jobstore: alias of the job store that contains the job
        :return Job: the relevant job instance

        """
        with self._jobstores_lock:
            job, jobstore = self._lookup_job(job_id, jobstore)
            job._modify(**changes)
            if jobstore:
                self._lookup_jobstore(jobstore).update_job(job)


        # Wake up the scheduler since the job's next run time may have been changed
        if self.state == STATE_RUNNING:
            self.wakeup()

        return job

    def reschedule_job(self, job_id, jobstore=None, **trigger_args):
        """
        Constructs a new trigger for a job and updates its next run time.

        Extra keyword arguments are passed directly to the trigger's constructor.

        :param str|unicode job_id: the identifier of the job
        :param str|unicode jobstore: alias of the job store that contains the job
        :param trigger: alias of the trigger type or a trigger instance
        :return Job: the relevant job instance

        """
        trigger = self._create_trigger(trigger_args) # 注意此处的设计，通过一个方法，将用户传入trigger, 或者是构造trigger合二为一，避免了通过if else 来判断
        now = datetime.now()
        next_run_time = trigger.get_next_fire_time(None, now)
        return self.modify_job(job_id, jobstore, trigger=trigger, next_run_time=next_run_time)

    def pause_job(self, job_id, jobstore=None):
        """
        1、暂停任务，将任务的下次执行时间置为空
        Causes the given job not to be executed until it is explicitly resumed.

        :param str|unicode job_id: the identifier of the job
        :param str|unicode jobstore: alias of the job store that contains the job
        :return Job: the relevant job instance

        """
        return self.modify_job(job_id, jobstore, next_run_time=None)

    def resume_job(self, job_id, jobstore=None):
        """
        Resumes the schedule of the given job, or removes the job if its schedule is finished.

        :param str|unicode job_id: the identifier of the job
        :param str|unicode jobstore: alias of the job store that contains the job
        :return Job|None: the relevant job instance if the job was rescheduled, or ``None`` if no
            next run time could be calculated and the job was removed

        """
        with self._jobstores_lock:
            job, jobstore = self._lookup_job(job_id, jobstore)
            now = datetime.now()
            next_run_time = job.trigger.get_next_fire_time(None, now)
            if next_run_time:
                return self.modify_job(job_id, jobstore, next_run_time=next_run_time)
            else:
                self.remove_job(job.id, jobstore)

    def get_jobs(self, jobstore=None):
        """
        Returns a list of pending jobs (if the scheduler hasn't been started yet) and scheduled
        jobs, either from a specific job store or from all of them.

        If the scheduler has not been started yet, only pending jobs can be returned because the
        job stores haven't been started yet either.

        :param str|unicode jobstore: alias of the job store
        :param bool pending: **DEPRECATED**
        :rtype: list[Job]

        """

        with self._jobstores_lock:
            jobs = []
            if self.state == STATE_STOPPED:
                for job, alias, replace_existing in self._pending_jobs:
                    if jobstore is None or alias == jobstore:
                        jobs.append(job)
            else:
                for alias, store in six.iteritems(self._jobstores):
                    if jobstore is None or alias == jobstore:
                        jobs.extend(store.get_all_jobs())

            return jobs

    def get_job(self, job_id, jobstore=None):
        """
        Returns the Job that matches the given ``job_id``.

        :param str|unicode job_id: the identifier of the job
        :param str|unicode jobstore: alias of the job store that most likely contains the job
        :return: the Job by the given ID, or ``None`` if it wasn't found
        :rtype: Job

        """
        with self._jobstores_lock:
            try:
                return self._lookup_job(job_id, jobstore)[0]
            except JobLookupError:
                return

    def remove_job(self, job_id, jobstore=None):
        """
        Removes a job, preventing it from being run any more.

        :param str|unicode job_id: the identifier of the job
        :param str|unicode jobstore: alias of the job store that contains the job
        :raises JobLookupError: if the job was not found

        """
        jobstore_alias = None
        with self._jobstores_lock:
            # Check if the job is among the pending jobs
            if self.state == STATE_STOPPED:
                for i, (job, alias, replace_existing) in enumerate(self._pending_jobs):
                    if job.id == job_id and jobstore in (None, alias):
                        del self._pending_jobs[i]
                        jobstore_alias = alias
                        break
            else:
                # Otherwise, try to remove it from each store until it succeeds or we run out of
                # stores to check
                for alias, store in six.iteritems(self._jobstores):
                    if jobstore in (None, alias):
                        try:
                            store.remove_job(job_id)
                            jobstore_alias = alias
                            break
                        except JobLookupError:
                            continue

        if jobstore_alias is None:
            raise JobLookupError(job_id)


        self._logger.info('Removed job %s', job_id)

    def remove_all_jobs(self, jobstore=None):
        """
        Removes all jobs from the specified job store, or all job stores if none is given.

        :param str|unicode jobstore: alias of the job store

        """
        with self._jobstores_lock:
            if self.state == STATE_STOPPED:
                if jobstore:
                    self._pending_jobs = [pending for pending in self._pending_jobs if
                                          pending[1] != jobstore]
                else:
                    self._pending_jobs = []
            else:
                for alias, store in six.iteritems(self._jobstores):
                    if jobstore in (None, alias):
                        store.remove_all_jobs()


    def print_jobs(self, jobstore=None, out=None):
        """
        print_jobs(jobstore=None, out=sys.stdout)

        Prints out a textual listing of all jobs currently scheduled on either all job stores or
        just a specific one.

        :param str|unicode jobstore: alias of the job store, ``None`` to list jobs from all stores
        :param file out: a file-like object to print to (defaults to  **sys.stdout** if nothing is
            given)

        """
        out = out or sys.stdout
        with self._jobstores_lock:
            if self.state == STATE_STOPPED:
                print(u'Pending jobs:', file=out)
                if self._pending_jobs:
                    for job, jobstore_alias, replace_existing in self._pending_jobs:
                        if jobstore in (None, jobstore_alias):
                            print(u'    %s' % job, file=out)
                else:
                    print(u'    No pending jobs', file=out)
            else:
                for alias, store in sorted(six.iteritems(self._jobstores)):
                    if jobstore in (None, alias):
                        print(u'Jobstore %s:' % alias, file=out)
                        jobs = store.get_all_jobs()
                        if jobs:
                            for job in jobs:
                                print(u'    %s' % job, file=out)
                        else:
                            print(u'    No scheduled jobs', file=out)

    @abstractmethod
    def wakeup(self):
        """
        Notifies the scheduler that there may be jobs due for execution.
        Triggers :meth:`_process_jobs` to be run in an implementation specific manner.
        """

    #
    # Private API
    #


    def _create_default_executor(self):
        """Creates a default executor store, specific to the particular scheduler type."""
        return ThreadPoolExecutor()

    def _create_default_jobstore(self):
        """Creates a default job store, specific to the particular scheduler type."""
        return MemoryJobStore()

    def _create_trigger(self, trigger_args):
        trigger=CronTrigger(**trigger_args)
        return trigger

    def _lookup_executor(self, alias):
        """
        Returns the executor instance by the given name from the list of executors that were added
        to this scheduler.

        :type alias: str
        :raises KeyError: if no executor by the given alias is not found

        """
        try:
            return self._executors[alias]
        except KeyError:
            raise KeyError('No such executor: %s' % alias)

    def _lookup_jobstore(self, alias):
        """
        Returns the job store instance by the given name from the list of job stores that were
        added to this scheduler.

        :type alias: str
        :raises KeyError: if no job store by the given alias is not found

        """
        try:
            return self._jobstores[alias]
        except KeyError:
            raise KeyError('No such job store: %s' % alias)

    def _lookup_job(self, job_id, jobstore_alias):
        """
        Finds a job by its ID.

        :type job_id: str
        :param str jobstore_alias: alias of a job store to look in
        :return tuple[Job, str]: a tuple of job, jobstore alias (jobstore alias is None in case of
            a pending job)
        :raises JobLookupError: if no job by the given ID is found.

        """
        if self.state == STATE_STOPPED:
            # Check if the job is among the pending jobs
            for job, alias, replace_existing in self._pending_jobs:
                if job.id == job_id:
                    return job, None
        else:
            # Look in all job stores
            for alias, store in six.iteritems(self._jobstores):
                if jobstore_alias in (None, alias):
                    job = store.lookup_job(job_id)
                    if job is not None:
                        return job, alias

        raise JobLookupError(job_id)


    def _real_add_job(self, job, jobstore_alias, replace_existing):
        """
        1、调整job的参数
        2、将job添加到job_store当中去
        :param Job job: the job to add
        :param bool replace_existing: ``True`` to use update_job() in case the job already exists
            in the store

        """
        # Fill in undefined values with defaults
        replacements = {}
        for key, value in six.iteritems(self._job_defaults):
            if not hasattr(job, key):
                replacements[key] = value


        # 此处计算出了任务的下次执行时间，注意是通过trigger来计算得出的
        if not hasattr(job, 'next_run_time'):
            now = datetime.now()
            replacements['next_run_time'] = job.trigger.get_next_fire_time(None, now) # 这里获取到下次执行时间，和misfire_time 没有关系

        # Apply any replacements
        job._modify(**replacements)

        # 通过jobstore_alias获取到jobstore，从而将job添加到jobstore当中去
        store = self._lookup_jobstore(jobstore_alias)
        try:
            store.add_job(job)
        except ConflictingIdError:
            if replace_existing:
                store.update_job(job)
            else:
                raise

        # Mark the job as no longer pending
        job._jobstore_alias = jobstore_alias


        self._logger.info('Added job "%s" to job store "%s"', job.name, jobstore_alias)

        # Notify the scheduler about the new job
        if self.state == STATE_RUNNING:
            self.wakeup()




    def _create_lock(self):
        """Creates a reentrant lock object."""
        return RLock()

    def _process_jobs(self):
        """
        遍历jobstore里面的job，等待下一个需要执行的任务到达

        If the ``get_due_jobs()`` call raises an exception, a new wakeup is scheduled in at least
        ``jobstore_retry_interval`` seconds.

        """
        if self.state == STATE_PAUSED:
            self._logger.debug('Scheduler is paused -- not processing jobs')
            return None

        self._logger.debug('Looking for jobs to run')
        now = datetime.now()
        next_wakeup_time = None


        with self._jobstores_lock:
            for jobstore_alias, jobstore in six.iteritems(self._jobstores):
                try:
                    # 在内存jobstore当中获取到需要执行的任务有哪些
                    due_jobs = jobstore.get_due_jobs(now)
                except Exception as e:
                    # Schedule a wakeup at least in jobstore_retry_interval seconds
                    self._logger.warning('Error getting due jobs from job store %r: %s',jobstore_alias, e)
                    retry_wakeup_time = now + timedelta(seconds=self.jobstore_retry_interval)
                    if not next_wakeup_time or next_wakeup_time > retry_wakeup_time:
                        next_wakeup_time = retry_wakeup_time

                    continue

                for job in due_jobs:
                    # Look up the job's executor
                    try:
                        # 此处的job.executor是一个str，在job初始化的时候实现的
                        executor = self._lookup_executor(job.executor)
                    except BaseException:
                        self._logger.error(
                            'Executor lookup ("%s") failed for job "%s" -- removing it from the '
                            'job store', job.executor, job)
                        self.remove_job(job.id, jobstore_alias)
                        continue

                    run_times = job._get_run_times(now)
                    run_times = run_times[-1:] if run_times and job.coalesce else run_times
                    if run_times:
                        try:
                            # 提交任务，让executor去执行相关的任务
                            executor.submit_job(job, run_times)
                        except MaxInstancesReachedError:
                            self._logger.warning(
                                'Execution of job "%s" skipped: maximum number of running '
                                'instances reached (%d)', job, job.max_instances)
                        except BaseException:
                            self._logger.exception('Error submitting job "%s" to executor "%s"',
                                                   job, job.executor)


                        # Update the job if it has a next execution time.
                        # Otherwise remove it from the job store.
                        # 获取到下次执行时间
                        job_next_run_time = job.trigger.get_next_fire_time(run_times[-1], now)
                        if job_next_run_time:
                            job._modify(next_run_time=job_next_run_time) # 调整当前任务的下次执行时间
                            jobstore.update_job(job) # 因为可能需要重新对任务进行排序，所以需要对jobstore进行update，没办法做到事件驱动：job变更之后，自动变更job store
                        else:
                            self.remove_job(job.id, jobstore_alias)

                # Set a new next wakeup time if there isn't one yet or
                # the jobstore has an even earlier one

                # 对于整个job_store而言获取到下个wakeup时间，当下个wakeup时间到的时候，会再次执行相关任务
                jobstore_next_run_time = jobstore.get_next_run_time()
                # 在这个期间，不断对next_wakeup_time进行重新赋值（因为是在遍历jobstore）
                if jobstore_next_run_time and (next_wakeup_time is None or
                                               jobstore_next_run_time < next_wakeup_time):
                    next_wakeup_time = jobstore_next_run_time


        # Determine the delay until this method should be called again
        if self.state == STATE_PAUSED:
            wait_seconds = None
            self._logger.debug('Scheduler is paused; waiting until resume() is called')
        elif next_wakeup_time is None:
            wait_seconds = None
            self._logger.debug('No jobs; waiting until a job is added')
        else:
            now = datetime.now()
            wait_seconds = min(max(timedelta_seconds(next_wakeup_time - now), 0), TIMEOUT_MAX)
            self._logger.debug('Next wakeup is due at %s (in %f seconds)', next_wakeup_time,
                               wait_seconds)

        return wait_seconds


class BackgroundScheduler(BaseScheduler):
    """
    A scheduler that runs in the background using a separate thread
    (:meth:`~apscheduler.schedulers.base.BaseScheduler.start` will return immediately).

    Extra options:

    ========== =============================================================================
    ``daemon`` Set the ``daemon`` option in the background thread (defaults to ``True``, see
               `the documentation
               <https://docs.python.org/3.4/library/threading.html#thread-objects>`_
               for further details)
    ========== =============================================================================
    """

    _thread = None
    _event = None



    def start(self, *args, **kwargs):
        if self._event is None or self._event.is_set():
            self._event = Event()
        self._daemon=True
        BaseScheduler.start(self, *args, **kwargs)
        # self._main_loop()
        self._thread = Thread(target=self._main_loop, name='BackgroundScheduler')
        self._thread.daemon = self._daemon
        self._thread.start()

    def _main_loop(self):
        """
        这是最主要的循环，通过这个循环，来实现不停地执行相关任务
        """
        wait_seconds = TIMEOUT_MAX
        while self.state != STATE_STOPPED:
            self._event.wait(wait_seconds)
            self._event.clear()
            wait_seconds = self._process_jobs()
    def shutdown(self, *args, **kwargs):
        super(BackgroundScheduler, self).shutdown(*args, **kwargs)
        self._thread.join()
        del self._thread

    def wakeup(self):
        # 会将值置为
        self._event.set()