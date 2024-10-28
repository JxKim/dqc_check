# 通过导入当前模板，即可创建一个session对象，然后可以完成整个ORM的操作
import time
from heapq import heappush, heappop
from typing import Optional, List, Tuple,Dict
from sqlalchemy import Column, Integer, String, DECIMAL, ForeignKey, create_engine, DateTime, func, Text
from sqlalchemy.sql import text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, scoped_session
from app import app
from dataclasses import dataclass
import logging
from datetime import datetime

Base = declarative_base()
# logging.basicConfig(level=logging.ERROR, format='%(asctime)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
logger = logging.getLogger(__name__)


class DqcCheckRules(Base):
    __tablename__ = app.config.dqc_check_rules_table_name
    id = Column(Integer, primary_key=True)
    rule_level = Column(String(10))
    rule_description = Column(String(100))

    def __repr__(self):
        return f'<rule_id={self.id}, rule_level={self.rule_level}, rule_description={self.rule_description}>'


class DqcCheckRulesApply(Base):
    # 这个是检测项的定义，但是在实际进行检测的时候，还需要有检测项的具体实例，类似于调度器当中的，工作流定义和工作流实例的区别
    __tablename__ = app.config.dqc_check_rules_apply_table_name

    apply_id = Column(Integer, primary_key=True, comment='当前正在检测的一个ID')
    tbl_name = Column(String(100), comment='表名称')
    col_name = Column(String(100), comment='字段名称')
    rule_id = Column(Integer, comment='规则字段ID')
    operator = Column(String(10), comment='操作符')
    threshold = Column(DECIMAL(5, 2), comment='阈值')
    filter_condition = Column(String(500), comment='过滤条件')
    checked_times = Column(Integer, comment='已检测次数，程序自动更新', server_default=text("0"))
    last_check_time = Column(String(30), comment='上次检测时间')
    last_check_result = Column(String(10),comment='上次检测结果') # todo 添加字段last_check_result
    apply_sql = Column(Text, comment='自定义SQL')
    desc = Column("job_desc", String(100), comment='任务描述')  # 后期根据这项来获取到每天对应结果具体检测的任务有哪些，然后再对结果进行输出
    tbl_type = Column(String(10), comment="表类型：1：结果表，2：底表，默认为底表", server_default=text("底表"))
    schedule_time = Column(String(50), comment='执行时间列表，comma分隔，例如: 12:00,11:30,11:45等')
    day=Column(String(50),comment='执行日期，1-31当中选择日期，默认每天',server_default=text("'*'"))
    month=Column(String(50),comment='执行月份，1-12当中选择值，多个月份按照逗号隔开，例如：1,3,5等，默认每年',server_default=text("'*'"))
    day_of_week=Column(String(50),comment='具体执行星期',server_default=text("'*'"))
    year=Column(String(50),comment='执行年份',server_default=text("'*'"))
    schedule_status = Column(Integer, comment='是否启用，1为启用，0为停用，默认为1', server_default=text("1"))
    report_nums = Column(Integer, comment='该子检测项关联检测报告任务数', server_default=text("0"))
    create_time = Column(DateTime, comment='创建时间', server_default=func.current_timestamp())
    update_time = Column(DateTime, comment='创建时间', server_default=func.current_timestamp(), onupdate=func.now())
    retry_times = Column(Integer, comment='重试次数，单位：分钟', server_default=text("25"))
    retry_interval = Column(Integer, comment='重试时间间隔，单位：分钟', server_default=text("30"))
    apply_priority = Column(Integer,comment='任务优先级，表行数任务优先级最大，其余任务当前优先级保持一致')
    creator=Column(String(10),comment='操作人')
    updator=Column(String(10),comment='更新人，如果是系统内部对数据进行更新，那updator就是system，否则就是员工姓名')
    creator_email=Column(String(50),comment='操作人')
    fail_alarm=Column(Integer,comment='1:失败告警，0：成功告警（成功告警使用不同的告警模版）',default=1)
    sql_desc=Column(String(100),comment='自定义SQL描述',default='')
    notifier=Column(String(100),comment='通知人',default='')
    def __repr__(self):
        return f'<apply_id={self.apply_id} tbl_name={self.tbl_name} col_name={self.col_name} rule_id={self.rule_id} schedule_status={self.schedule_status}> schedule_time={self.schedule_time}'


class DqcCheckRulesApplyRecords(Base):
    """
        数据表：dqc_check_rules_apply_records 所对应的Model类

    """
    __tablename__ = app.config.dqc_check_rules_apply_records_table_name

    check_id = Column(Integer, primary_key=True, autoincrement=True, comment='单次check_id')
    check_status = Column(Integer, default=0, comment="任务检测状态,1:已调度启动，2:已执行完成")
    apply_id = Column(Integer, comment='所对应的检测任务')
    check_result = Column(String(10), comment='检测是否通过，pass表示未通过，fail表示未通过')
    actual_value = Column(DECIMAL(20, 2), comment='检测的实际结果值') # actual_value数据类型原为decimal(10,2)
    check_sql = Column(Text, comment='本次检测对应的SQL')
    has_alarmed = Column(Integer, comment='是否已告警')
    create_time = Column(DateTime, comment='创建时间', server_default=func.current_timestamp())
    update_time = Column(DateTime, comment='创建时间', server_default=func.current_timestamp(), onupdate=func.now())

    def __repr__(self):
        return f'<check_id={self.check_id} check_sql={self.check_sql}>'


class DqcCheckReports(Base):
    __tablename__ = app.config.dqc_check_reports_table_name
    report_id = Column(Integer, primary_key=True, autoincrement=True, comment='检测报告ID')
    report_name = Column(String(100), unique=True, comment='检测报告名称，用于显示在检测报告上方')
    report_status = Column(Integer, default=0, comment='检测报告启用状态：0:暂停启用，1:启用中')
    check_applys = Column(String(255), comment='当前检测报告所对应的检测项的id，对应DqcCheckRulesApply表的id')
    create_time = Column(DateTime, comment='创建时间', server_default=func.current_timestamp())
    schedule_time = Column(String(100), comment='调度执行时间', default='')
    schedule_day = Column(String(100), comment='调度执行日期', default='-1')
    update_time = Column(DateTime, comment='创建时间', server_default=func.current_timestamp(), onupdate=func.now())

    def __repr__(self):
        return f'<report_id={self.report_id} report_name={self.report_name} check_applys={self.check_applys}>'


class DqcCheckRulesApplyStatusControl(Base):
    __tablename__ = app.config.dqc_check_rules_apply_status_control_table_name
    to_turn_off=Column(Integer,comment='是否关闭')
    create_time=Column(DateTime,primary_key=True,comment='创建时间')

    def __repr__(self):
        return f'<to_turn_off={self.to_turn_off} create_time={self.create_time}>'

@dataclass
class check_job_status:
    """
    用于在报告任务当中检测，通过dataclass的装饰器，可以免去__init__方法
    """
    job_status: str
    job_batch: Optional[int]
    dqc_check_rules_apply_records: Optional[DqcCheckRulesApplyRecords]
    dqc_check_rules_apply: Optional[DqcCheckRulesApply]


class SyntaxException(Exception):
    """
    SQL语法异常
    """
    def __init__(self,msg,record):
        super().__init__(f'查询任务因语法异常，异常详细信息为：{msg}')
        self.record=record

    pass

class DivideZeroException(Exception):
    """
    除0异常
    """
    def __init__(self,msg,record):
        super().__init__(f'查询任务除零异常，异常详细信息为：{msg}')
        self.record = record

class AccessDeniedException(Exception):
    """
    没有权限异常
    """
    def __init__(self,msg,record):
        super().__init__(f'查询任务权限异常，异常详细信息为：{msg}')
        self.record=record

class NoPartitionException(Exception):
    """
    查询Hive分区表没有指定分区
    """
    def __init__(self,msg,record):

        super().__init__(f'查询任务因没有指定分区异常，异常详细信息为：{msg}')
        self.record=record


class HiveConnectionException(Exception):
    """
    Hive连接异常
    """
    def __init__(self,msg,record):
        super().__init__(f'查询任务因Hive连接异常，异常详细信息为：{msg}')
        self.record=record


class OtherDatabaseException(Exception):
    """
    其他数据库异常
    """
    def __init__(self,msg,record):
        super().__init__(f'查询任务其他数据库相关异常，异常信息为：{msg}')
        self.record=record

# 当前线上的代码可以通过pymysql驱动跑通
engine = create_engine(
    f"mysql+pymysql://{app.config.result_db_user}:{app.config.result_db_pwd}@{app.config.result_db_host}:{app.config.result_db_port}/{app.config.result_db_name}",
    pool_size=20, # 默认情况下是有5个连接，会存在获取不到连接超时的情况
    max_overflow=10,
    pool_timeout=60
)
session_factory = sessionmaker(bind=engine)
Session = scoped_session(session_factory)