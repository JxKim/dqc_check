"""Fields represent CronTrigger options which map to :class:`~datetime.datetime` fields."""

from calendar import monthrange
import re

import six

from dqc_check.scheduler_models import (
    AllExpression, RangeExpression, WeekdayPositionExpression, LastDayOfMonthExpression,
    WeekdayRangeExpression, MonthRangeExpression)


__all__ = ('MIN_VALUES', 'MAX_VALUES', 'DEFAULT_VALUES', 'BaseField', 'WeekField',
           'DayOfMonthField', 'DayOfWeekField')


MIN_VALUES = {'year': 1970, 'month': 1, 'day': 1, 'week': 1, 'day_of_week': 0, 'hour': 0,
              'minute': 0, 'second': 0}
MAX_VALUES = {'year': 9999, 'month': 12, 'day': 31, 'week': 53, 'day_of_week': 6, 'hour': 23,
              'minute': 59, 'second': 59}
DEFAULT_VALUES = {'year': '*', 'month': 1, 'day': 1, 'week': '*', 'day_of_week': '*', 'hour': 0,
                  'minute': 0, 'second': 0}
SEPARATOR = re.compile(' *, *')


class BaseField(object):
    # 对于小时，分，秒等，完全使用该Base类即可
    REAL = True # 此处表示是否是真实的时间单位，对于星期，和周几，就不是真实的时间单位
    COMPILERS = [AllExpression, RangeExpression]


    def __init__(self, name, exprs, is_default=False):
        self.name = name
        self.is_default = is_default
        self.compile_expressions(exprs)

    def get_min(self, dateval):
        return MIN_VALUES[self.name]

    def get_max(self, dateval):
        return MAX_VALUES[self.name]

    def get_value(self, dateval):
        return getattr(dateval, self.name)

    def get_next_value(self, dateval):
        # 所有的子类都没有实现这个方法，这个方法仅在父类实现，获取到所有的正则表达式当中的下一个日期的最小值
        smallest = None
        for expr in self.expressions:
            value = expr.get_next_value(dateval, self)
            if smallest is None or (value is not None and value < smallest):
                smallest = value

        return smallest

    def compile_expressions(self, exprs):
        self.expressions = []

        # Split a comma-separated expression list, if any
        # 注意此处的split方法，不是字符串的split方法，而是正则表达式对象的split方法
        for expr in SEPARATOR.split(str(exprs).strip()):
            self.compile_expression(expr)

    def compile_expression(self, expr):
        for compiler in self.COMPILERS:
            match = compiler.value_re.match(expr)
            if match:
                compiled_expr = compiler(**match.groupdict())

                try:
                    compiled_expr.validate_range(self.name)
                except ValueError as e:
                    exc = ValueError('Error validating expression {!r}: {}'.format(expr, e))
                    six.raise_from(exc, None)

                self.expressions.append(compiled_expr)
                return

        raise ValueError('Unrecognized expression "%s" for field "%s"' % (expr, self.name))

    def __eq__(self, other):
        return isinstance(self, self.__class__) and self.expressions == other.expressions

    def __str__(self):
        expr_strings = (str(e) for e in self.expressions)
        return ','.join(expr_strings)

    def __repr__(self):
        return "%s('%s', '%s')" % (self.__class__.__name__, self.name, self)


class WeekField(BaseField):
    REAL = False # 第几周是真实的时间单位，所以此处的值为False

    def get_value(self, dateval):
        # isocalendar 会返回 哪一年，哪一个星期，以及周几
        # 因为第几周不是一个真实的时间单位，所以没有办法从dateeval直接获取到属性值，也就是BaseField当中的get_value来获取到值
        return dateval.isocalendar()[1]


class DayOfMonthField(BaseField):
    COMPILERS = BaseField.COMPILERS + [WeekdayPositionExpression, LastDayOfMonthExpression]


    def get_max(self, dateval):
        # 获取到当月的天数的最大值，对于不同的月份，有不同的天数，所以此处override了BaseField的方法
        return monthrange(dateval.year, dateval.month)[1]


class DayOfWeekField(BaseField):
    REAL = False
    COMPILERS = BaseField.COMPILERS + [WeekdayRangeExpression]

    def get_value(self, dateval):
        # 获取到是本周第几天，以0开始计数，星期一是第0天
        return dateval.weekday()


class MonthField(BaseField):
    COMPILERS = BaseField.COMPILERS + [MonthRangeExpression]
