import unittest
import datetime
import sys
sys.path.append('D:\PycharmProjects\dqc_check\dqc_check')
from dqc_check.models import HiveConnectionException,OtherDatabaseException,NoPartitionException,AccessDeniedException,DivideZeroException,SyntaxException
class TestModel(unittest.TestCase):

    def test_exception_msg(self):
        """
        测试 异常类是否能够正常运行
        :return:
        """
        t1=HiveConnectionException('abc')
        assert '查询任务因Hive连接异常，异常详细信息为：abc'==t1.args[0]

    def test_other_database_msg(self):
        t1 = OtherDatabaseException('abc')
        assert '查询任务其他数据库相关异常，异常信息为：abc' == t1.args[0]

    def test_no_partition_msg(self):
        t1 = NoPartitionException('abc')
        assert '查询任务因没有指定分区异常，异常详细信息为：abc' == t1.args[0]

    def test_access_denied(self):
        t1 = AccessDeniedException('abc')
        assert '查询任务权限异常，异常详细信息为：abc' == t1.args[0]

    def test_divide_msg(self):
        t1 = DivideZeroException('abc')
        assert '查询任务除零异常，异常详细信息为：abc' == t1.args[0]

    def test_syntax_msg(self):
        t1 = SyntaxException('abc')
        assert '查询任务因语法异常，异常详细信息为：abc' == t1.args[0]


if __name__ == '__main__':
    unittest.main()
