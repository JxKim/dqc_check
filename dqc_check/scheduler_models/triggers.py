from datetime import datetime, timedelta

# from tzlocal import get_localzone
import six
from abc import ABCMeta, abstractmethod
from datetime import timedelta, datetime
import random

from dqc_check.scheduler_models import (BaseField, MonthField, WeekField, DayOfMonthField, DayOfWeekField, DEFAULT_VALUES)


def datetime_ceil(dateval):
    """
    Rounds the given datetime object upwards.

    :type dateval: datetime

    """
    if dateval.microsecond > 0:
        return dateval + timedelta(seconds=1, microseconds=-dateval.microsecond)
    return dateval


def datetime_repr(dateval):
    return dateval.strftime('%Y-%m-%d %H:%M:%S %Z') if dateval else 'None'


def normalize(dt):
    return datetime.fromtimestamp(dt.timestamp(), dt.tzinfo)


class BaseTrigger(six.with_metaclass(ABCMeta)):
    """Abstract base class that defines the interface that every trigger must implement."""

    __slots__ = ()

    @abstractmethod
    def get_next_fire_time(self, previous_fire_time, now):
        """
        Returns the next datetime to fire on, If no such datetime can be calculated, returns
        ``None``.

        :param datetime.datetime previous_fire_time: the previous time the trigger was fired
        :param datetime.datetime now: current datetime
        """

    def _apply_jitter(self, next_fire_time, jitter, now):
        """
        Randomize ``next_fire_time`` by adding a random value (the jitter).

        :param datetime.datetime|None next_fire_time: next fire time without jitter applied. If
            ``None``, returns ``None``.
        :param int|None jitter: maximum number of seconds to add to ``next_fire_time``
            (if ``None`` or ``0``, returns ``next_fire_time``)
        :param datetime.datetime now: current datetime
        :return datetime.datetime|None: next fire time with a jitter.
        """
        if next_fire_time is None or not jitter:
            return next_fire_time

        return next_fire_time + timedelta(seconds=random.uniform(0, jitter))


class CronTrigger(BaseTrigger):
    """
    可以先实例化，然后在调用scheduler.add_job的时候，传入进去
    Triggers when current time matches all specified time constraints,
    similarly to how the UNIX cron scheduler works.

    :param int|str year: 4-digit year
    :param int|str month: month (1-12)
    :param int|str day: day of month (1-31)
    :param int|str week: ISO week (1-53)
    :param int|str day_of_week: number or name of weekday (0-6 or mon,tue,wed,thu,fri,sat,sun)
    :param int|str hour: hour (0-23)
    :param int|str minute: minute (0-59)
    :param int|str second: second (0-59)
    :param datetime|str start_date: earliest possible date/time to trigger on (inclusive)
    :param datetime|str end_date: latest possible date/time to trigger on (inclusive)
    :param datetime.tzinfo|str timezone: time zone to use for the date/time calculations (defaults
        to scheduler timezone)
    :param int|None jitter: delay the job execution by ``jitter`` seconds at most

    .. note:: The first weekday is always **monday**.
    """
    # 正则表达式的五个字段： minute, hour, day of month, month, day of week
    FIELD_NAMES = ('year', 'month', 'day', 'week', 'day_of_week', 'hour', 'minute', 'second')
    FIELDS_MAP = {
        'year': BaseField,
        'month': MonthField,
        'week': WeekField,
        'day': DayOfMonthField,
        'day_of_week': DayOfWeekField,
        'hour': BaseField,
        'minute': BaseField,
        'second': BaseField
    }

    __slots__ = 'start_date', 'end_date', 'fields'

    def __init__(self, year=None, month=None, day=None, week=None, day_of_week=None, hour=None,
                 minute=None, second=None, start_date=datetime.now(), end_date=datetime.now().replace(year=2124)):
        # if timezone:
        #     self.timezone = astimezone(timezone)
        # elif isinstance(start_date, datetime) and start_date.tzinfo:
        #     self.timezone = start_date.tzinfo
        # elif isinstance(end_date, datetime) and end_date.tzinfo:
        #     self.timezone = end_date.tzinfo
        # else:
        #     self.timezone = get_localzone()

        self.start_date = start_date
        self.end_date = end_date
        values = dict(
            (key, value) for (key, value) in six.iteritems(locals()) if key in self.FIELD_NAMES and value is not None)
        self.fields = []
        assign_defaults = False
        for field_name in self.FIELD_NAMES:
            # 从传入的实参当中获取到相关的值
            if field_name in values:
                exprs = values.pop(field_name)
                is_default = False
                assign_defaults = not values
            elif assign_defaults:
                exprs = DEFAULT_VALUES[field_name]
                is_default = True
            else:
                exprs = '*'
                is_default = True

            # 构建field实例
            field_class = self.FIELDS_MAP[field_name]
            field = field_class(field_name, exprs, is_default)
            # 定义当前的时间字段值
            self.fields.append(field)

    @classmethod
    def from_crontab(cls, expr):
        """
        Create a :class:`~CronTrigger` from a standard crontab expression.

        See https://en.wikipedia.org/wiki/Cron for more information on the format accepted here.

        :param expr: minute, hour, day of month, month, day of week
        :param datetime.tzinfo|str timezone: time zone to use for the date/time calculations (
            defaults to scheduler timezone)
        :return: a :class:`~CronTrigger` instance

        """
        values = expr.split()
        if len(values) != 5:
            raise ValueError('Wrong number of fields; got {}, expected 5'.format(len(values)))

        return cls(minute=values[0], hour=values[1], day=values[2], month=values[3],
                   day_of_week=values[4])

    def _increment_field_value(self, dateval, fieldnum):
        """
        时间单位上面的进位，比方说天到达最后一天了，就把月份加一，然后把其他单位全部都置为0
        Increments the designated field and resets all less significant fields to their minimum
        values.

        :type dateval: datetime
        :type fieldnum: int
        :return: a tuple containing the new date, and the number of the field that was actually
            incremented
        :rtype: tuple
        """

        values = {}
        i = 0
        while i < len(self.fields):
            field = self.fields[i]
            if not field.REAL:
                if i == fieldnum:
                    fieldnum -= 1
                    i -= 1
                else:
                    i += 1
                continue

            if i < fieldnum:
                values[field.name] = field.get_value(dateval)
                i += 1
            elif i > fieldnum:
                values[field.name] = field.get_min(dateval)
                i += 1
            else:
                value = field.get_value(dateval)
                maxval = field.get_max(dateval)
                if value == maxval:
                    fieldnum -= 1
                    i -= 1
                else:
                    values[field.name] = value + 1
                    i += 1

        difference = datetime(**values) - dateval.replace(tzinfo=None)
        return normalize(dateval + difference), fieldnum

    def _set_field_value(self, dateval, fieldnum, new_value):
        """
        设置某个字段的字段值
        """
        values = {}
        for i, field in enumerate(self.fields):
            if field.REAL:
                if i < fieldnum:
                    # 对于fieldnum之前的日期字段，直接从dateval当中取值即可
                    values[field.name] = field.get_value(dateval)
                elif i > fieldnum:
                    # 对于fieldnum之后的日期字段，获取到最小值，
                    values[field.name] = field.get_min(dateval)
                else:
                    values[field.name] = new_value

        # 返回一个日期值即可
        return datetime(**values)

    def get_next_fire_time(self, previous_fire_time, now):
        """
        获取到下次的触发时间
        """
        if previous_fire_time:
            start_date = min(now, previous_fire_time + timedelta(microseconds=1))
            if start_date == previous_fire_time:
                start_date += timedelta(microseconds=1)
        else:
            # 获取到计算的开始日期
            start_date = max(now, self.start_date) if self.start_date else now

        fieldnum = 0
        next_date = datetime_ceil(start_date)
        while 0 <= fieldnum < len(self.fields):
            field = self.fields[fieldnum]
            # 当前是12点
            curr_value = field.get_value(next_date)
            # 假如我是每天执行，当前时间是6月最后一天，6月30日，那么next_value就会返回为None
            # 获取到的是10点，也就是下次执行时间是7月1日10点
            next_value = field.get_next_value(next_date)

            if next_value is None:
                # 比方说本次是这个月最后一天了，那么就需要通过更上一级的时间单位进位，
                # No valid value was found
                # 传入 start_date=datetime(year=2024,month=6,day=30,minute=11,xxx)
                next_date, fieldnum = self._increment_field_value(next_date, fieldnum - 1)
            elif next_value > curr_value:
                # A valid, but higher than the starting value, was found
                if field.REAL:
                    # 把对应的字段设置成对应的值，next_date的不同字段，例如，年，月，日，时，分，秒，等，都会被设值，从而获取到一个完整的next_date
                    next_date = self._set_field_value(next_date, fieldnum, next_value)
                    fieldnum += 1
                else:
                    next_date, fieldnum = self._increment_field_value(next_date, fieldnum)
            else:
                # A valid value was found, no changes necessary
                fieldnum += 1

            # Return if the date has rolled past the end date
            if self.end_date and next_date > self.end_date:
                return None

        if fieldnum >= 0:
            return min(next_date, self.end_date) if self.end_date else next_date

    def __str__(self):
        options = ["%s='%s'" % (f.name, f) for f in self.fields if not f.is_default]
        return 'cron[%s]' % (', '.join(options))

    def __repr__(self):
        options = ["%s='%s'" % (f.name, f) for f in self.fields if not f.is_default]
        if self.start_date:
            options.append("start_date=%r" % datetime_repr(self.start_date))
        if self.end_date:
            options.append("end_date=%r" % datetime_repr(self.end_date))

        return "<%s (%s)>" % (
            self.__class__.__name__, ', '.join(options))


class DateTrigger(BaseTrigger):
    """
    Triggers once on the given datetime. If ``run_date`` is left empty, current time is used.

    :param datetime|str run_date: the date/time to run the job at
    """

    __slots__ = 'run_date'

    def __init__(self, run_date=None):
        self.run_date = run_date

    def get_next_fire_time(self, previous_fire_time, now):
        return self.run_date if previous_fire_time is None else None

    def __str__(self):
        return 'date[%s]' % datetime_repr(self.run_date)

    def __repr__(self):
        return "<%s (run_date='%s')>" % (self.__class__.__name__, datetime_repr(self.run_date))
