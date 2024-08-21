#
from logging import INFO,WARNING,DEBUG,ERROR
from dataclasses import dataclass
from collections import defaultdict
@dataclass
class BaseConfig:
    presto_conn_host = 'presto_host'
    prest_conn_port = '8443'
    presto_user_name = 'presto_user_name'
    presto_user_passsword = 'presto_password'
    admin_user = 'admin_user_email@xxx.com'
    hive_host='hive_host'
    hive_port=10001
    hive_username='hive_username'
    hive_pwd='hive_pwd'
    hive_db='hive_db'
    new_tidb_host = 'tidb_host'
    new_tidb_port = 3306
    new_tidb_username = 'tidb_user'
    new_tidb_password = 'tidb_password'
    new_tidb_db = 'tidb_db'
    logger_level = DEBUG
    # 用于企微通知的key: 如下key后面的内容，https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=xxx
    qiwei_key = "6005413c-xxxx-xxxx-xxxx-1fea99d4326c"
    user_email_to_phone=defaultdict(str,{
        'xxx@credithc.com':'xxx',
        'xx1@credithc.com':'xxx',
        'xx2@credithc.com':'xxx',
        'xx3@credithc.com':'xxx',
        'xx4@credithc.com':'xxx',
        'xx5@credithc.com':'xxx',
        'xx6@credithc.com':'xxx'
    })
    dqc_check_rules_apply_status_control_table_name='dqc_check_rules_apply_status_control'
    main_thread_sleep_time=600
    to_send_msgs = 1
    max_workers = 10

@dataclass
class DevConfig(BaseConfig):
    to_send_msgs = 0
    max_workers = 10
    pass

@dataclass
class TestConfig(BaseConfig):
    pass

@dataclass
class ProdConfig(BaseConfig):
    pass

