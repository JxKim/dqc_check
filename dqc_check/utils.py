import warnings
import requests
import urllib3
from pyhive import presto
from pyhive import hive
from pyhive.exc import DatabaseError, OperationalError
from pymysql.err import ProgrammingError
from typing import *
import logging
from requests.auth import HTTPBasicAuth
from app import app
from conn_properties import DevConfig
from models import SyntaxException, DivideZeroException, OtherDatabaseException, AccessDeniedException, \
    NoPartitionException

logger = logging.getLogger('utils')


def presto_execute(psql: str, **kwargs) -> List[Tuple]:
    """

    :param psql:
    :return:
    """
    import time
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    requests.packages.urllib3.disable_warnings
    warnings.filterwarnings("ignore")
    schema = 'dp_ods'

    retry_time = 3
    curs = None
    conn = None
    while retry_time:
        try:
            conn = presto.connect(host=app.config.presto_conn_host, port=app.config.prest_conn_port, protocol='https',
                                  schema=schema,
                                  username=app.config.presto_user_name,
                                  requests_kwargs={'verify': False,
                                                   'auth': HTTPBasicAuth(app.config.presto_user_name,
                                                                         app.config.presto_user_passsword)})
            curs = conn.cursor()
            curs.execute(psql)
            result = curs.fetchall()
            return result
        except DatabaseError as e:
            if e.args[0]['errorCode'] == 1:
                # 当前SQL有语法错误，包括
                logger.error('当前SQL有语法错误')
                raise SyntaxException(e.args[0]['message'])
            elif e.args[0]['errorCode'] == 8:
                logger.error('除数为0异常')
                raise DivideZeroException(e.args[0]['message'])
            elif e.args[0]['errorCode'] == 4:
                logger.error('无权限异常')
                raise AccessDeniedException(e.args[0]['message'])
            elif e.args[0]['errorCode'] == 50003:
                logger.error("重复SQL提交，等待重试")
                time.sleep(360)
                retry_time -= 1
            else:
                logger.error("其他数据库侧异常")
                raise OtherDatabaseException(e.args[0])
        except Exception as e:
            raise OtherDatabaseException(e.args)
        finally:
            if conn:
                conn.close()


def send_email_dev(sender, to: List, msg, subject):
    from email.header import Header
    import smtplib
    from email.mime.multipart import MIMEMultipart
    from email.mime.text import MIMEText
    email = MIMEMultipart('alternative')
    email['From'] = sender
    email['To'] = Header(','.join(to))
    email['Subject'] = Header(subject, 'utf-8')
    email.attach(MIMEText(msg, "html", "utf-8"))
    with smtplib.SMTP_SSL('smtp.163.com', 994) as smtp_client:
        smtp_client.login("jinxin231213@credithc.com", "gL3jS9sbhdC52e9k")
        smtp_client.sendmail("jinxin231213@credithc.com", to, email.as_string())


def send_email_prod(sender, to: List, msg, subject):
    ccs = [""]
    from hctools.email.sendEmail import sendEmailAtta, sendEmailNoAtta
    sendEmailNoAtta(subject, to, ccs, msg, "", mail_host="smtp.exmail.qq.com", mail_port=465,
                    mail_user="jinxin231213@credithc.com", mail_pw="WOshikim0921",
                    sender="jinxin231213@credithc.com")


def notify_wechat_msgs(template, mention_list=None, **kwargs):
    """
    将指定路径下面的模板，按照kwargs当中的数据实例化以后，发送消息到企微当中
    :param template:模板路径
    :param kwargs:
    :return:
    """
    logger.debug('进入到notify_wechat_msgs方法中')
    if app.config.to_send_msgs == 0:
        logger.debug(f'测试环境，进入到notify_wechat_msgs当中：{template}')
        return
    logger.debug('向企微推送信息')
    import requests
    import json
    notify_group_token = f'https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key={app.config.qiwei_key}'
    with open(template, 'r', encoding='utf-8') as f:
        msg = f.read()
    logger.info(msg)
    for key, value in kwargs.items():
        value = str(value) or ''
        msg = msg.replace('{' + f'{key}' + '}', value)
    logger.info(msg)
    header = {
        'Content-Type': 'application/json'
    }
    message_data = {
        "msgtype": "markdown",
        "markdown": {
            "content": f"""{msg}"""
        }
    }
    response = requests.post(notify_group_token, json.dumps(message_data), headers=header)

    logger.info(f'调用企微接口结果:{response.text}')
    if mention_list:
        message_data = {
            "msgtype": "text",
            "text": {
                "content": "",
                "mentioned_list": [],
                "mentioned_mobile_list": mention_list
            }
        }
        requests.post(notify_group_token, json.dumps(message_data), headers=header)


def hive_execute(hql, query_type=1, **kwargs):
    """
    仅用于查询表行数
    :return list[tuple] 返回类型和presto查询保持一致
    """
    conn = None
    curser = None
    try:
        # 创建连接
        conn = hive.connect(host=app.config.hive_host, port=app.config.hive_port, auth='LDAP',
                            username=app.config.hive_username, password=app.config.hive_pwd,
                            database=app.config.hive_db)
        curser = conn.cursor()
        print(hql)
        curser.execute(hql)
        logger.info("执行Hive SQL完毕")
        if query_type == 1:
            data = curser.fetchall()
            return data
    except OperationalError as e:
        if 'HiveAccessControlException' in e.args[0].status.errorMessage:
            raise AccessDeniedException(e.args[0].status.errorMessage)
        elif 'ParseException' in e.args[0].status.errorMessage:
            raise SyntaxException(e.args[0].status.errorMessage)
        elif 'Queries against partitioned tables without a partition' in e.args[0].status.errorMessage:
            raise NoPartitionException(e.args[0].status.errorMessage)
        else:
            raise OtherDatabaseException(e.args[0].status.errorMessage)
    except Exception as e:
        raise OtherDatabaseException(e)
    finally:
        if curser:
            curser.close()
        if conn:
            conn.close()


def tidb_execute(sql, query_type=1, **kwargs):
    """
    使用tidb执行查询操作
    :param query_type 操作类型，当为1的时候，表示为查询
    :return tuple 返回数据类型为tuple
    """
    import pymysql
    connection = None
    cursor = None
    logger.debug('进入到tidb查询')
    try:
        connection = pymysql.connect(host=kwargs['host'], port=3306, user=app.config.result_db_user,
                                     password=app.config.result_db_pwd,
                                     db=app.config.result_db_name)
        cursor = connection.cursor()
        cursor.execute(sql)
        if query_type == 2:
            connection.commit()
        else:
            res = cursor.fetchall()
            return res
    except ProgrammingError as e:
        raise SyntaxException(e.args)
    except Exception as e:
        raise OtherDatabaseException(f'查询tidb出现其他异常：{e.args}')
    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()


if app.config == DevConfig:
    send_email = send_email_dev
else:
    send_email = send_email_prod

if __name__ == '__main__':
    send_email_dev('jinxin231213@credithc.com', ['jinxin231213@credithc.com'], msg='sss', subject='t1')