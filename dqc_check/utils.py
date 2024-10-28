import threading
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
    NoPartitionException,HiveConnectionException

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


def get_image_data(html_content, size=(1100, 2300)):
    # HTML内容
    # 将DataFrame转换为HTML
    # 通过subprocess调用wkhtmltoimage，将标准输出重定向到管道
    import subprocess
    import imgkit
    path = "/usr/local/bin/wkhtmltoimage"
    config = imgkit.config(wkhtmltoimage=path)
    proc = subprocess.Popen([config.wkhtmltoimage,
                             '--quiet',
                             '--width', str(size[0]),
                             '--height', str(size[1]),
                             '-', '-'],
                            stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    # 将HTML内容写入wkhtmltoimage的标准输入，并获取标准输出和标准错误
    image_data, error = proc.communicate(input=html_content.encode('utf-8'))
    # with open("1原图2.png",'wb') as file:
    #     file.write(image_data)
    return image_data


def get_columns_total_avg_width(datas, headers):
    """
    计算所有列数据的平均字符数的总和
    :param datas: list[Tuple]，每个Tuple是一行数据
    :param headers: 数据的表头，每个表头对应一列
    :return: 所有列的平均字符数的总和
    """
    # 初始化一个列表，用于存储每列的字符总数
    import copy
    columns_char_counts = [0] * len(headers)
    column_max = -99
    change_lines_row=0
    logger.info(f'当前data为：{datas},当前header为：{headers}')
    new_data=copy.deepcopy(datas)
    new_data.append(headers)
    # 遍历数据，累加每列的字符数
    for row in new_data:
        line_change_row_cnt=[0]
        for i, value in enumerate(row):
            if get_str_length(str(value))>100:
                columns_char_counts[i] = max(columns_char_counts[i], 100)
                line_change_row_cnt.append(int(get_str_length(str(value))/100))
            else:
                columns_char_counts[i] = max(columns_char_counts[i], get_str_length(str(value)))
        change_lines_row+=max(line_change_row_cnt)

    # 计算每列的平均字符数
    # columns_avg_widths = [char_count / len(datas) for char_count in columns_char_counts]

    # 计算所有列的平均字符数的总和
    total_avg_width = sum(columns_char_counts)
    logger.info(f'当前header为：{headers},当前total_avg_width为:{total_avg_width}，当前需要换行的行数为：{change_lines_row},当前格列任务的宽度为：{columns_char_counts}')
    # 默认每个字符所占据的宽度为50，后期再调整
    return columns_char_counts,change_lines_row


def get_image_data_by_detail(table_name, data_detail: List[Tuple], data_headers: List[str],
                             path="/usr/local/bin/wkhtmltoimage") -> bytes:
    """
    传入表名称，表数据详情，表头等参数，将html文本渲染成图片的二进制格式，无需调整图片尺寸大小，会动态调整
    :param table_name 表名称
    :param data_detail 表数据详情，例如数据有三个列：id,姓名，年龄，那么data_detail即为：[(1,'John',21),(2,'Kim',22)]
    :param data_headers 表头：以上例子当中为：['id','姓名','年龄']
    :param path wkhtmltoimage可执行文件位置，在服务器上面执行不需要传入进去
    :return: 图片二进制字节数组
    """
    import subprocess
    import imgkit
    html_template = """

        <html>
			<head>
			            <meta charset="utf-8">
			            <STYLE TYPE="text/css" MEDIA=screen>

			                table.dataframe {
			                    border-collapse: collapse;
			                }
			                table {
		                     table-layout: auto;
			                border-collapse: collapse;
			                margin-left: auto;   /* 左边距自动 */
			                margin-right: auto;  /* 右边距自动 */
	                        margin-bottom: 4px; /* 控制表格底部的间隙*/
			                width: 90%;
			                }
			                p {
			    font-family: 'Microsoft YaHei UI Light', sans-serif;
			    font-size: 25px;
			    text-align: center;
			    font-weight: bold;margin-bottom: 20px;margin-top: 40px;
	  margin-left: 20px; /* 左边距 */
	  margin-right: 20px; /* 右边距 */

			  }
							/*tr是控制一整行的数据*/
			                table.dataframe tr {
			                  vertical-align: middle;
			                  height:50px; /* 控制表每一行的高度 */
							  font-size: 24px;
							  height: 50px;
			                }
							/*th是控制表头的数据*/
			                table.dataframe th {
			                    vertical-align: middle;
			                    font-size: 24px;
			                    padding: 2px;
			                    text-align: center;
			                    background-color: #A3D1E4;
			                    font-weight: bolder;
		                        width: auto;
			                }
							/*td是控制表内部数据*/
			                table.dataframe td {
			                    text-align: center;
			                    padding: 2px;
		                        width: auto;
		                        font-size: 20px;
			                }
			                body {
			                    font-family: "Microsoft YaHei",Arial,sans-serif;
			                }
			               .flex-container {
			                    display: flex;
			                    flex-wrap: wrap;
			                    justify-content: center;
			               }
			                .flex-container > div {
			                    margin: 10px;
			                }
	                        tr:nth-child(even) {
	                        background-color: #f2f2f2;
	                        }

	  /* 为第奇数行设置另一种背景色 */
	            tr:nth-child(odd) {
	    background-color: #ffffff;
	  }
			            </STYLE>
			</head>

			<body>
			    <div>
			      <p style="font-size: 35px">table_name_format</p>
			         <table border="1" class="dataframe" >
			  <thead>
			    <tr align="center" style="color:Black;font-family:'Microsoft YaHei',sans-serif;">
					table_header_format
			    </tr>
			  </thead>
	  <tbody>
			table_data_format
	  </tbody>
	</table>
	    </div>
	    </body>
	</html>
    """
    table_header_template = """
    <th>{0}</th>
    """
    table_data_template = """
        <tr>
	                                  {0}
	                                </tr>
    """
    headers_list = [table_header_template.format(header) for header in data_headers]
    headers_str = " ".join(headers_list)
    data_list = [table_data_template.format(" ".join(["<td>{0}</td>".format(row_col) for row_col in row])) for row in
                 data_detail]
    data_str = " ".join(data_list)
    columns_total_width,change_line_rows = get_columns_total_avg_width(data_detail, data_headers)
    # 宽度=表左边缘宽度+表所有列的宽度+表右边缘宽度
    image_width = 10 + columns_total_width + 10
    logger.info(f'当前检测明细为：{data_headers},当前宽度为：{image_width}')
    # 高度=表名称上边缘+表名称高度+表名称下边缘+表头高度+表数据行高度+表下边缘高度
    image_heigth = 10 + 100 + 40 + 70 + (len(data_detail)+change_line_rows) * 50 + 10
    logger.info(f'当前高度为:{image_heigth}')
    html_content = html_template.replace('table_data_format', data_str).replace('table_header_format',
                                                                                headers_str).replace(
        'table_name_format', table_name)
    # logger.info(f"当前入参为：{html_content}")
    config = imgkit.config(wkhtmltoimage=path)
    proc = subprocess.Popen([config.wkhtmltoimage,
                             '--quiet',
                             '--width', str(int(image_width)),
                             '--height', str(int(image_heigth)),
                             '-', '-'],
                            stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    image_data, error = proc.communicate(input=html_content.encode('utf-8'))
    logger.info(f"当前任务error为：{error}")
    return image_data


def get_multi_image_data_by_detail(data_details: Dict, path="/usr/local/bin/wkhtmltoimage") -> bytes:
    """
    将多个数据集渲染成图片
    :param data_details:
    :param path:
    :param th_font_size:
    :param td_font_size:
    :param bh_font_size:
    :return:
    """
    import subprocess
    import imgkit
    html_template = """

        <html>
			<head>
			            <meta charset="utf-8">
			            <STYLE TYPE="text/css" MEDIA=screen>

			                table.dataframe {
			                    border-collapse: collapse;
			                }
			                table {
		                     table-layout: auto;
			                border-collapse: collapse;
			                margin-left: auto;   /* 左边距自动 */
			                margin-right: auto;  /* 右边距自动 */
	                        margin-bottom: 4px; /* 控制表格底部的间隙*/
			                width: 90%;
			                }
			                p {
			    font-family: 'Microsoft YaHei UI Light', sans-serif;
			    font-size: 25px;
			    text-align: center;
			    font-weight: bold;margin-bottom: 20px;margin-top: 40px;
	  margin-left: 20px; /* 左边距 */
	  margin-right: 20px; /* 右边距 */

			  }
							/*tr是控制一整行的数据*/
			                table.dataframe tr {
			                  vertical-align: middle;
			                  height:50px; /* 控制表每一行的高度 */
							  font-size: 24px;
							  height: 50px;
			                }
							/*th是控制表头的数据*/
			                table.dataframe th {
			                    vertical-align: middle;
			                    font-size: 24px;
			                    padding: 0px;
			                    text-align: center;
			                    background-color: #A3D1E4;
			                    font-weight: bolder;

			                }
							/*td是控制表内部数据*/
			                table.dataframe td {
			                    text-align: center;
			                    padding: 0px;

		                        font-size: 20px;
			                }
			                body {
			                    font-family: "Microsoft YaHei",Arial,sans-serif;
			                }
			               .flex-container {
			                    display: flex;
			                    flex-wrap: wrap;
			                    justify-content: center;
			               }
			                .flex-container > div {
			                    margin: 10px;
			                }
	                        tr:nth-child(even) {
	                        background-color: #f2f2f2;
	                        }

	  /* 为第奇数行设置另一种背景色 */
	            tr:nth-child(odd) {
	    background-color: #ffffff;
	  }
			            </STYLE>
			</head>

			<body>
			    <div>
			      table_and_table_names
	    </div>
	    </body>
	</html>
    """
    table_header_template = """
    <th>{0}</th>
    """
    table_data_template = """
        <tr>
	                                  {0}
	                                </tr>
    """
    singel_table_format = """
        <p style="font-size: 35px">table_name_format</p>
			         <table border="1" class="dataframe" >
			  <thead>
			    <tr align="center" style="color:Black;font-family:'Microsoft YaHei',sans-serif;">
					table_header_format
			    </tr>
			  </thead>
	  <tbody>
			table_data_format
	  </tbody>
	</table>
    """
    table_width = []
    table_height = []
    table_html = []
    for table_name, table in data_details.items():
        data_headers = table[0]
        data_detail = table[1]
        headers_list = [table_header_template.format(header) for header in data_headers]
        headers_str = " ".join(headers_list)
        data_list = []
        each_column_width,add_line_row=get_columns_total_avg_width(data_detail,data_headers)
        total_column_width=sum(each_column_width)
        for row in data_detail:
            if row[1] == '异常':
                data_list.append(table_data_template.format(
                    " ".join(["""<td style="color: red;width:{width}%">{row_col}</td>""".format(row_col=row_col,width=float(each_column_width[index]*100/total_column_width)) for index,row_col in enumerate(row)])))
            else:
                data_list.append(
                    table_data_template.format(" ".join(["""<td style="width:{width}%">{row_col}</td>""".format(row_col=row_col,width=float(each_column_width[index]*100/total_column_width)) for index,row_col in enumerate(row)])))
        data_str = " ".join(data_list)
        columns_total_width=sum(each_column_width)
        # 宽度=表左边缘宽度+表所有列的宽度+表右边缘宽度
        image_width = 10 + columns_total_width*20 + 10
        table_width.append(image_width)
        logger.info(f'当前data_headers为：{data_headers},当前宽度为：{image_width}')
        # 高度=表名称上边缘+表名称高度+表名称下边缘+表头高度+表数据行高度+表下边缘高度
        if table_name == '异常检测项明细':
            image_heigth = 10 + 100 + 40 + 70 + len(data_detail) * 70 + 10
        else:
            image_heigth = 10 + 100 + 40 + 70 + len(data_detail) * 50 + 10+add_line_row*20
        logger.info(f'当前data_header为：{data_headers},当前高度为:{image_heigth}')
        table_height.append(image_heigth)
        html_content = singel_table_format.replace('table_data_format', data_str).replace('table_header_format',
                                                                                          headers_str).replace(
            'table_name_format', table_name)
        logger.info(f'当前Html为：{html_content}')
        table_html.append(html_content)
    image_width = max(table_width)
    logger.info(f'当前data_headers为：{data_details},当前宽度为：{image_width}')
    image_heigth = sum(table_height)
    logger.info(f'当前data_headers为：{data_details},当前高度为：{image_heigth}')
    html_content = html_template.replace('table_and_table_names', ' '.join(table_html))
    config = imgkit.config(wkhtmltoimage=path)
    proc = subprocess.Popen([config.wkhtmltoimage,
                             '--quiet',
                             '--width', str(int(image_width)),
                             '--height', str(int(image_heigth)),
                             '-', '-'],
                            stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    image_data, error = proc.communicate(input=html_content.encode('utf-8'))
    logger.warning(f"当前任务error为：{error}")
    return image_data

def get_str_length(value):
    logger.info('进入到get_str_length')
    import string
    total_length=0
    for i in value:
        total_length+=1 if i in string.ascii_letters or i=='.' or i in ('1','2','3','4','5','6','7','8','9','0') else 1.5
    logger.info('get_str_length执行完成')
    return total_length
def send_pic_to_entWechat(image: bytes, key='6005413c-09ca-404e-964b-1fea99d4326c', notifier=""):
    """
    数据发送到企微
    :param image:
    :return:
    """
    import base64
    import hashlib
    import json
    base64_encoded = base64.b64encode(image)
    md = hashlib.md5()
    md.update(image)
    md5_hash = md.hexdigest()
    # headers = {"Content-Type": "text/plain"}
    wechat_token = f"https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key={key}"
    import requests
    content = {
        "msgtype": "image",
        "image": {
            "base64": base64_encoded.decode(),
            "md5": f"{md5_hash}"
        }
    }

    response = requests.post(wechat_token, json=content)
    logger.info(f'当前调用企微返回的结果为：{response.text}')
    logger.info(f'当前调用企微返回的状态码为：{response.status_code}')
    notify_list = []
    to_notify = {
        "msgtype": "text",
        "text": {
            "content": "",
            "mentioned_list": [],
            "mentioned_mobile_list": notify_list
        }
    }

    header = {
        'Content-Type': 'application/json'
    }
    if notifier:
        notify_list += notifier.split(',')
        requests.post(wechat_token, json.dumps(to_notify), headers=header)

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
    retry_time=3
    while retry_time:
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
            if 'TSocket read 0 bytes' in e.args[0]: # 对于Hive连接异常，单独处理
                raise HiveConnectionException(e.args[0])

            logger.warning(f'执行hive_execute出现异常:{e.args[0]}')
            if retry_time:
                import time as t_time
                t_time.sleep(30)
                retry_time -= 1
            else:
                logger.error('执行Hive最终失败，告警')
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
