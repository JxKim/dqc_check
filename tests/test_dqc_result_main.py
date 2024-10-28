import unittest
import datetime
import sys
sys.path.append('D:\PycharmProjects\dqc_check\dqc_check')
from dqc_check.dqc_result_main import _get_earliest_schedule_time

class TestDqcResultMain(unittest.TestCase):

    def test_set_time(self):
        t = datetime.datetime.strptime('15:20:00', '%H:%M:%S')
        schedule_time="19:20,18:20,15:20"
        res=_get_earliest_schedule_time(schedule_time)
        assert t==res


if __name__ == '__main__':
    unittest.main()