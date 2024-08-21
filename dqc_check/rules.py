from collections import namedtuple
from models import DqcCheckRulesApply,Session,DqcCheckRulesApplyRecords,SyntaxException,DivideZeroException,AccessDeniedException,OtherDatabaseException,NoPartitionException

from utils import presto_execute,hive_execute,tidb_execute
from datetime import datetime
from app import app
import logging
logger = logging.getLogger(__name__)
session=Session()
RuleDesc=namedtuple("RuleDesc","func description")

def _convert(filter_condition:str)->str:
    """
    参数转换
    :param filter_condition:
    :return:
    """
    import time
    import re
    from datetime import timedelta,datetime
    pattern = r'\$\[yyyy-MM-dd(?:-(\d+))?\]'
    matches=re.finditer(pattern,filter_condition)
    for match in matches:
        param=match.group(0)
        day_delta=int(match.group(1) or 0)
        filter_condition=filter_condition.replace(param,(datetime.now()-timedelta(day_delta)).strftime('%Y-%m-%d'),1)
    return filter_condition

def _get_result(value:float,threshold,operator,apply_id,query_sql:str,new_check_record:DqcCheckRulesApplyRecords)->DqcCheckRulesApplyRecords:
    """
    构造record对象
    :param value:
    :param threshold:
    :param operator:
    :param apply_id:
    :param query_sql:
    :param new_check_record:
    :return:
    """
    new_check_record.has_alarmed=False
    new_check_record.check_sql=query_sql
    new_check_record.actual_value=value
    if operator=='>':
        new_check_record.check_result='失败' if value>threshold else '成功'
    elif operator=='=':
        new_check_record.check_result = '失败' if value == threshold else '成功'
    elif operator=='<':
        new_check_record.check_result = '失败' if value < threshold else '成功'
    elif operator=='!=':
        new_check_record.check_result = '失败' if value != threshold else '成功'
    elif operator=='>=':
        new_check_record.check_result = '失败' if value != threshold else '成功'
    elif operator=='<=':
        new_check_record.check_result = '失败' if value != threshold else '成功'
    new_check_record.check_status=2
    return new_check_record


def _optimize_sql_count(check_apply:DqcCheckRulesApply,**kwargs):
    """
    优化SQL count查询
    """
    if kwargs['filter_condition']:
        filter_condition=' where '+kwargs['filter_condition']
    else:
        filter_condition=''
    if check_apply.threshold==0:
        sql=f"select * from {kwargs['tbl_name']} {filter_condition} limit 2"
    else:
        sql=f"select count(*) from {kwargs['tbl_name']} {filter_condition}"
    return sql
def _table_nums_check(check_apply:DqcCheckRulesApply,record:DqcCheckRulesApplyRecords,last_check_sql:str) ->DqcCheckRulesApplyRecords:
    logger.debug("执行表行数检测")
    kwargs={}
    tbl_name_list = check_apply.tbl_name.split('.')
    filter_condition=''
    if len(tbl_name_list)<2:
        raise SyntaxException('表名配置异常')
    tbl_name = str(tbl_name_list[-2]) + '.' + str(tbl_name_list[-1])
    if check_apply.filter_condition is not None and len(check_apply.filter_condition)>0:
        filter_condition=_convert(check_apply.filter_condition)

    # 判断是否是tidb数据表还是hive数据表
    if 'report_db_xhdc' in check_apply.tbl_name:
        if len(tbl_name_list) <3: # catalog, database, table_name
            raise SyntaxException('查询tidb表请配置tidb catalog')
        host= app.config.new_tidb_host if tbl_name_list[0]=='tidb_xh_159' else app.config.result_db_host
        kwargs['host']=host
        func=tidb_execute
    elif 'tidb' in check_apply.tbl_name and 'report_db_xhdc' not in check_apply.tbl_name:
        func=presto_execute
        tbl_name=check_apply.tbl_name
    else:
        func=hive_execute
    if last_check_sql:
        sql=last_check_sql # 如果上次有失败的任务，那么这次执行的SQL和本次执行的SQL保持一致
    else:
        sql=_optimize_sql_count(check_apply,tbl_name=tbl_name,filter_condition=filter_condition)


    logger.debug(f"当前拼接的SQL为:{sql}")
    try:
        res=func(sql,**kwargs)
        table_num=float(res[0][0]) if check_apply.threshold !=0 else len(res) #
        operator=check_apply.operator
        threshold=check_apply.threshold
        new_check_record=_get_result(table_num,threshold,operator,check_apply.apply_id,sql,record)

        logger.info(f"表行数检测结果为：{new_check_record.check_result}")
        # check_apply.checked_times+=1
        # check_apply.last_check_time=datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        #session.commit()
        return new_check_record
    except AccessDeniedException as e:
        raise AccessDeniedException(f"查询SQL因权限问题失败：\n SQL:{sql}\nMESSAGE:{e.args[0]}")
    except SyntaxException as e:
        raise SyntaxException(f"当前apply_id：{check_apply.apply_id}查询SQL因语法相关问题失败：\n SQL:{sql}\nMESSAGE:{e.args[0]}")
    except NoPartitionException as e:
        raise NoPartitionException(f'当前apply_id:{check_apply.apply_id}查询分区表，没有指定分区，查询失败：\n SQL:{sql}\nMESSAGE:{e.args[0]}')
    except OtherDatabaseException as e:
        raise OtherDatabaseException(e)

def _clean_sql(sql):
    # 当用户传入的是自定义SQL时，用以去除掉注释和所有的换行符
    import re
    sql = re.sub(r'--.*', '', sql)
    sql = re.sub(r'/\*.*\*/', '', sql, flags=re.DOTALL)  # 多行注释
    # 去掉换行符
    sql = sql.replace('\n', ' ').replace('\r', '')
    # 去掉多余的空格
    sql = re.sub(r'\s+', ' ', sql)
    return sql
def table_days_7_30_fluctuate(check_apply:DqcCheckRulesApply) ->DqcCheckRulesApplyRecords:
    result:DqcCheckRulesApplyRecords=None
    sql=f""""""

    return result

def _column_unique_nums(check_apply:DqcCheckRulesApply,record:DqcCheckRulesApplyRecords,last_check_sql:str) ->DqcCheckRulesApplyRecords:
    '''
    检测字段唯一值个数
    :param table_full_name:
    :param column_name:
    :return:
    '''
    logger.info("执行字段唯一性检测")
    filter_condition = ''
    if last_check_sql:
        sql=last_check_sql
    else:
        if len(check_apply.filter_condition) > 0:
            filter_condition = '  where  ' + _convert(check_apply.filter_condition)
        sql = f"""select {check_apply.col_name},count(*) as total from {check_apply.tbl_name} {filter_condition} group by {check_apply.col_name} having count(*)>1"""
    try:
        res=presto_execute(sql)
        duplicate_col_count=len(res)
        print(f"{duplicate_col_count}长度")
        operator=check_apply.operator
        threshold=check_apply.threshold
        new_check_record = _get_result(duplicate_col_count, threshold, operator, check_apply.apply_id,sql,record)

        logger.info(f"字段唯一性检测结果为：{new_check_record.check_result}")
        # check_apply.checked_times+=1
        # check_apply.last_check_time=datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        #session.commit()
        return new_check_record
    except AccessDeniedException as e:
        raise AccessDeniedException(f"查询SQL因权限问题失败：\n SQL:{sql}\nMESSAGE:{e.args[0]}")
    except SyntaxException as e:
        # 对异常再包装一个具体的SQL，然后返给用户，前面需要将当前apply删除掉
        raise SyntaxException(f"查询SQL因语法相关问题失败：\n SQL:{sql}\nMESSAGE:{e.args[0]}")
    except DivideZeroException as e:
        # 当前任务不会存在除0异常
        pass


def _column_null_nums(check_apply:DqcCheckRulesApply,record:DqcCheckRulesApplyRecords,last_check_sql:str)->DqcCheckRulesApplyRecords:
    '''
    字段空值检测
    :param check_apply:
    :return:
    '''
    logger.info("开始执行空值检测")
    col_name = check_apply.col_name
    tbl_name = check_apply.tbl_name
    filter_condition = ""
    if last_check_sql:
        sql=last_check_sql
    else:
        if len(check_apply.filter_condition) > 0:
            filter_condition = "  where  " + _convert(check_apply.filter_condition)
        sql = f"""select count(if({col_name} is null,1,null)) as null_values from {tbl_name} {filter_condition}"""
    try:
        res=presto_execute(sql)
        actual_value=float(res[0][0])
        new_check_record=_get_result(actual_value,check_apply.threshold,check_apply.operator,check_apply.apply_id,sql,record)
        logger.info(f"空值检测结果为：{new_check_record.check_result}")
        # check_apply.checked_times+=1
        # check_apply.last_check_time=datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        return new_check_record
    except SyntaxException as e:
        raise SyntaxException(f"查询SQL因语法相关问题失败：\n SQL:{sql}\n,MESSAGE:{e.args[0]}")
    except Exception as e:
        raise Exception("")

def _user_defined_sql(check_apply:DqcCheckRulesApply,record:DqcCheckRulesApplyRecords,last_check_sql:str)->DqcCheckRulesApplyRecords:
    '''
    用户自定义SQL检测
    :param check_apply:
    :return:
    '''
    logger.info("开始执行自定义SQL检测")
    if last_check_sql:
        sql=last_check_sql
    else:
        sql=check_apply.apply_sql #只有在自定义SQL的时候，该字段的值才非空
        sql=_convert(sql)
    try:
        res=presto_execute(sql)
        sql=_clean_sql(sql)
        actual_result=float(res[0][0])
        operator = check_apply.operator
        threshold = check_apply.threshold
        new_check_record=_get_result(actual_result,threshold,operator,check_apply.apply_id,sql,record)

        # check_apply.checked_times+=1
        logger.info(f"自定义SQL检测结果为：{new_check_record.check_result}")
        # check_apply.last_check_time=datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        #session.commit()
        return new_check_record
    except DivideZeroException as e:
        # 对于除零异常，直接视为任务失败了，处理好record数据之后，直接返回即可
        sql=_clean_sql(sql)
        record.has_alarmed=False
        record.check_sql=sql
        record.check_result='失败'
        record.check_status=2
    except SyntaxException as e:
        raise SyntaxException(f"查询SQL因语法相关问题失败：\n SQL:{sql}\n,MESSAGE:{e.args[0]}")
    except AccessDeniedException as e:
        raise AccessDeniedException(f"查询SQL因权限问题失败：\n SQL:{sql}\nMESSAGE:{e.args[0]}")
    except Exception as e:
        raise Exception(e.args[0])


def _two_tbls_data_check(check_apply:DqcCheckRulesApply,record:DqcCheckRulesApplyRecords)->DqcCheckRulesApplyRecords:
    """
    查询两个表数据是否一致
    :param check_apply:
    :param record:
    :return:
    """



# 如果需要添加检验规则，可在此处添加，然后在数据库当中添加相应的规则
rule_dict={
    1:RuleDesc(_table_nums_check,"表行数检查，固定值"),
    2:RuleDesc(_column_unique_nums,"字段唯一性检测"),
    3:RuleDesc(_column_null_nums,'字段空值个数检测'),
    4:RuleDesc(_user_defined_sql,"用户自定义SQL") #对于用户自定义的SQL，不需要将规则存放在SQL里面，
}