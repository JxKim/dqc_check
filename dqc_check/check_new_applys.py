"""
轮询检测单个检测项，并调用检测任务
未完成：
2、apply表当中添加责任人，填写时可以通过责任人在群里进行告知
3、如何只捕捉apply表部分字段的更新，例如只捕捉schedule_time、col_name、apply_sql等字段的变更，而不捕捉通知群等相关字段的变更
4、对两个表数据一致性的校验
5、如何在凌晨两点到六点之间，让任务停止运行？
已完成：
1、配置完任务之后，需要立即进行检测，判断任务配置是否成功，不成功需要进行告知，否则将任务添加到调度队列当中



# bug
1、time_schedule.schedule_dict包含了test类型的SingleTask


20240715:
    1、对于一个检测实例，保证在跨天时，任务检测SQL保持一致，当前可能在跨天重试之后，yyyy-MM-dd-1 会被重新赋值
    2、检测前需要判定当前日期是否需要进行检测，例如对于某些任务，仅在周末进行检测
"""
import os
import time as t_m
from models import (Session, DqcCheckRulesApply, DqcCheckRulesApplyRecords,
                    DqcCheckRulesApplyStatusControl,func,SyntaxException,DivideZeroException,AccessDeniedException,OtherDatabaseException,NoPartitionException,HiveConnectionException)
from utils import notify_wechat_msgs,send_email
from sqlalchemy.orm.session import Session as TypeSession
from sqlalchemy import or_, desc,and_
from typing import List, Optional
from datetime import datetime, timedelta
from app import app
import threading
import logging
import traceback
from scheduler_models.scheduler import BackgroundScheduler
from scheduler_models.triggers import CronTrigger,DateTrigger
from collections import defaultdict
from conn_properties import DevConfig,ProdConfig
from traceback import format_exc
today = datetime.today()
logging.basicConfig(level=app.config.logger_level, format='%(asctime)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
logger = logging.getLogger(__name__)
basedir=os.path.abspath(os.path.dirname(__file__))
# 以下两个变量用于在线程内出现异常时，整个进程直接终止，
check_new_apply_not_exit=True
wait_for_apply_not_exit=True
sched=BackgroundScheduler()
# apply 到 job dict，like {1:{'schedule':[],'retry':[],'test':[]}}
apply_to_job_dict=defaultdict(lambda :defaultdict(list))

def check_new_apply():
    """
    对dqc_check_rules_apply表每5分钟进行一次check任务，捕获新增以及变更的任务
    """
    from datetime import datetime
    import re
    session: Optional[TypeSession] = None

    initialization = True
    global check_new_apply_not_exit
    logger_prefix = "check_new_apply"
    try:
        session = Session()
        while check_new_apply_not_exit:
            try:
                # 1、对时间列表的初始化
                if initialization:
                    logger.debug(f"{logger_prefix}:initialization:begin")
                    on_schedule_applys: List[DqcCheckRulesApply] = session.query(DqcCheckRulesApply).filter(DqcCheckRulesApply.schedule_status == 1).all()
                    for apply in on_schedule_applys:
                        _set_crontab_trigger(apply)
                    initialization = False

                    sched.start() # 启动调度器
                    t_m.sleep(app.config.sleep_time)
                    logger.debug(f"{logger_prefix}:initialization:end")
                    last_check_time = datetime.now().replace(second=0,microsecond=0)
                else:
                    # 2、对当前时间列表进行增量更新
                    now=datetime.now().replace(microsecond=0)
                    logger.debug(f"{logger_prefix}:incremental:begin")
                    logger.debug(f"{logger_prefix}:incremental:check_time_range:From {last_check_time} To {now}")
                    increment_applys:List[DqcCheckRulesApply] = session.query(DqcCheckRulesApply).filter(
                        or_(
                            # 创建时间在上次检查时间和当前时间之间
                            DqcCheckRulesApply.create_time.between(last_check_time, datetime.now()),
                            # 同时满足以下条件
                            and_(
                                # 更新时间在上次检查时间和当前时间之间
                                DqcCheckRulesApply.update_time.between(last_check_time, datetime.now()),
                                # 更新人updator为空或者不为system
                                or_(
                                    DqcCheckRulesApply.updator == None,
                                    DqcCheckRulesApply.updator != 'system'
                                )
                            )
                        )
                    ).all()
                    logger.debug(f'当前increment_applys数据量为:{increment_applys}')
                    for apply in increment_applys:
                        # 将所有时间点全部添加到TimeSchedule当中去
                        if apply.create_time == apply.update_time:
                            res=_set_crontab_trigger(apply)
                            # 当用户设置的时间没有问题时。。新添加任务，需要通知用户配置成功
                            if res:
                                tri=DateTrigger(run_date=datetime.now()+timedelta(seconds=10))
                                sched.add_job(execute_single_apply,trigger=tri,args=(apply.apply_id,'test'),name=str(apply)+'_test')
                        elif apply.schedule_status==0:
                            # 删除任务
                            logger.info(f"{logger_prefix}:incremental:delete task {apply.apply_id}")
                            _clear_apply_jobs(apply.apply_id)

                        else:
                            # 更新任务
                            logger.info(f"{logger_prefix}:incremental:update task {apply.apply_id}")
                            # 使用replace逻辑：先将老的task全部删除，然后再创建新的task
                            _clear_apply_jobs(apply.apply_id) # 这里需要调整一下，清理job，只能将schedule和test类型的job清理掉，而不能清理掉retry类型的job
                            res=_set_crontab_trigger(apply)
                            # 对更新任务，也需要对任务进行测试
                            if res:
                                date_tri=DateTrigger(run_date=datetime.now()+timedelta(seconds=10))
                                sched.add_job(execute_single_apply,trigger=date_tri,args=(apply.apply_id,'test'),name=str(apply)+'_test')
                    logger.debug(f"{logger_prefix}:incremental:end")
                    last_check_time = now+timedelta(milliseconds=1)
                    #import time # 此处会存在变量作用域逃逸的问题，所以需要重新引入
                    t_m.sleep(app.config.sleep_time)
                session.commit()
            except Exception as e:
                # 非用户配置类异常，程序异常，直接退出
                err_msg=e.args[0]
                stack_msgs=traceback.format_exc()
                logger.error(f"当前错误信息为：{stack_msgs}")
                send_email(app.config.admin_email, [app.config.admin_email],f'监控任务配置异常：{err_msg}','监控配置任务主循环异常')
                # check_new_apply_not_exit=False
                t_m.sleep(3*app.config.sleep_time)
                # raise Exception('程序异常，需要及时处理')

                # notify_wechat_msgs(f'{basedir}/templates/messages/exception.txt',mention_list=['18810652711'],exception_args=err_msg,stack_msgs=stack_msgs) # 当前有任务报错的时候，通过邮件提醒
                # check_new_apply_not_exit=False
                # raise Exception('程序异常，需要及时处理')
    finally:
        if session:
            session.close()


def _clear_apply_jobs(apply_id):
    for job_id in apply_to_job_dict[apply_id]['schedule']:
        sched.remove_job(job_id)

    del apply_to_job_dict[apply_id]

def _set_crontab_trigger(apply:DqcCheckRulesApply):
    """
    :params apply 设置的规则
    """
    try:
        import re
        result=[]
        pattern = r'\s*[,|，]\s*'
        res = re.sub(pattern, '&', apply.schedule_time).replace('：', ':').replace(' ', '').split('&')
        for time in res:
            crontab = ' '.join(
            [time.split(':')[1], time.split(':')[0], str(apply.day), str(apply.month),
             str(apply.day_of_week)])
            logger.info(f'当前任务crontab表达式为：{crontab}')
            trigger = CronTrigger.from_crontab(crontab)
            result.append(trigger)
    except TypeError:
        notify_wechat_msgs(f'{basedir}/templates/messages/config_error_template.txt',
                           mention_list=[app.config.user_email_to_phone[apply.creator+ app.config.email_prefix]],
                           tbl_name=apply.tbl_name,
                           err_msg=f'当前apply:{apply}调度时间设置为空或空字符串，请重新设置，已将当前任务暂停，请及时修改')
        apply.schedule_status=0
        apply.updator='system'

    except IndexError:
        notify_wechat_msgs(f'{basedir}/templates/messages/config_error_template.txt',
                           mention_list=[app.config.user_email_to_phone[apply.creator + app.config.email_prefix]],
                           tbl_name=apply.tbl_name,
                           err_msg=f'当前apply:{apply}调度存在时间设置不符合模式: hour:minute，请重新设置，已将当前任务暂停，请及时修改')
        apply.schedule_status=0
        apply.updator = 'system'
    except Exception as e :
        notify_wechat_msgs(f'{basedir}/templates/messages/config_error_template.txt',
                           mention_list=[app.config.user_email_to_phone[apply.creator+ app.config.email_prefix] ],
                           tbl_name=apply.tbl_name,
                           err_msg=f'当前apply:{apply}调度时间设置存在异常:{e.args[0]}，请重新设置，已将当前任务暂停，请及时修改')
        apply.schedule_status=0
        apply.updator = 'system'
    else:
        for trigger in result:
            job = sched.add_job(execute_single_apply, trigger=trigger, args=(apply.apply_id, 'schedule'),
                                name=str(apply) + '_schedule')
            apply_to_job_dict[apply.apply_id]['schedule'].append(job.id)
        return True


def execute_single_apply(apply_id:int,type:str,last_check_sql=None,retry_times=0):
    """
    使用独立的线程执行单个check任务，任务失败，则重新将任务添加到time_schedule当中。
    对于多种不同的错误类型，需要去定义多种处理方式
    :param task_list: 同一个数据表的多个检测任务，按照表的优先顺序来进行排队检测
    """
    from rules import rule_dict
    threading.currentThread().setName(name=f'Apply:{apply_id} execute')
    logger_prefix = f'Apply_id:{apply_id} execute'
    logger.debug(f"{logger_prefix} start")
    session: Optional[TypeSession] = None
    try:
        session = Session()
        # 执行单个检测任务
        apply: DqcCheckRulesApply = session.query(DqcCheckRulesApply).filter(
            DqcCheckRulesApply.apply_id == apply_id).first()
        rule_desc = rule_dict.get(int(apply.rule_id))
        record = DqcCheckRulesApplyRecords(  # 对record进行初始化
            apply_id=apply.apply_id,
            check_status=1
        )
        session.add(record)
        session.commit()  # 此处提交一次，以获取到apply的id
        execute_func = rule_desc.func
        record: DqcCheckRulesApplyRecords = execute_func(apply, record,last_check_sql)
        all_records = (session.query(DqcCheckRulesApplyRecords).filter(
                                                        DqcCheckRulesApplyRecords.apply_id == apply.apply_id,
                                                                 func.date(DqcCheckRulesApplyRecords.create_time) == datetime.now().date(),
                                                                 DqcCheckRulesApplyRecords.check_sql == record.check_sql)
                                                        .order_by(desc(DqcCheckRulesApplyRecords.create_time)).all())
        logger.debug(f'当前查询出的all_records值为：{all_records}')
        if type=='test':
            # 测试任务，只要不报错，就是配置成功，但是需要告知本次的检测结果，以及本次的检测SQL，
            if record:
                next_run_time=_get_apply_next_run_time(apply_id)
                notify_wechat_msgs(f'{basedir}/templates/messages/config_success.txt',
                                   mention_list=[app.config.user_email_to_phone[apply.creator+app.config.email_prefix]],
                                   tbl_name=apply.tbl_name,check_content=_get_apply_desc(apply),
                                   this_time_check_result='成功' if record.check_result=='成功' else '异常',
                                   next_schedule_time=next_run_time,
                                   check_sql=record.check_sql
                                   )
        elif type in ('retry','schedule'):
            # 重试任务
            if record and record.check_result == '失败':
                if apply.retry_times and retry_times < apply.retry_times:
                    date_trigger = DateTrigger(run_date=datetime.now() + timedelta(minutes=int(apply.retry_interval)))
                    sched.add_job(execute_single_apply, trigger=date_trigger,args=(apply.apply_id, 'retry', record.check_sql, retry_times + 1),name=str(apply) + '_retry')
                if apply.tbl_name == "tidb_hjlc.report_db_xhdc.ads_wechat_report_metrics":
                    # 企微报表类型的告警
                    notify_wechat_msgs(f'{basedir}/templates/messages/qiwei_message.txt',
                                       mention_list=[app.config.user_email_to_phone[apply.creator+app.config.email_prefix]]+[app.config.user_email_to_phone[email] for email in apply.notifier.split(',')],
                                       report_name=apply.desc,
                                       formatted_now=datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                                       )
                elif apply.fail_alarm==1:
                    notify_wechat_msgs(f'{basedir}/templates/messages/check_templates.txt',
                                       mention_list=[app.config.user_email_to_phone[apply.creator+app.config.email_prefix]]+[app.config.user_email_to_phone[email] for email in apply.notifier.split(',')],
                                       **_prepare_data(check_record=record, check_apply=apply, type="异常"))
                    # 添加重试任务

            elif record and record.check_result == '等待重试':
                if retry_times<=10:
                    date_trigger = DateTrigger(run_date=datetime.now() + timedelta(minutes=30)) # 暂时设定为10分钟之后进行重试
                    sched.add_job(execute_single_apply, trigger=date_trigger,args=(apply.apply_id, 'retry', record.check_sql, retry_times + 1),
                              name=str(apply) + '_retry')
                else:
                    # 给出报错信息，告知数据加工异常
                    notify_wechat_msgs(f'{basedir}/templates/messages/check_templates.txt',
                                       mention_list=[app.config.user_email_to_phone[apply.creator+app.config.email_prefix]]+[app.config.user_email_to_phone[email] for email in apply.notifier.split(',')],
                                       **_prepare_data(check_record=record, check_apply=apply, type="任务执行异常"))


            elif record and record.check_result == '成功':
                if apply.fail_alarm==0:
                    notify_wechat_msgs(f'{basedir}/templates/messages/check_success.txt',
                                       mention_list=[app.config.user_email_to_phone[apply.creator+app.config.email_prefix]]+[app.config.user_email_to_phone[email] for email in apply.notifier.split(',')],
                                       tbl_name=apply.tbl_name,
                                       apply_desc=_get_apply_desc(apply), actual_value=record.actual_value, desc=apply.desc)
                elif len(all_records)>=2 and all_records[1].check_result == '失败' and apply.fail_alarm == 1:
                    # 当前任务失败之后，成功了，需要进行恢复告警
                    notify_wechat_msgs(f'{basedir}/templates/messages/check_templates.txt',
                                       mention_list=[app.config.user_email_to_phone[
                                                         apply.creator + app.config.email_prefix]] + [
                                                        app.config.user_email_to_phone[email] for email in
                                                        apply.notifier.split(',')],
                                       **_prepare_data(check_record=record, check_apply=apply, type="异常恢复"))


    except (SyntaxException,AccessDeniedException,OtherDatabaseException,NoPartitionException) as e:
        # 此处默认是认为首次检测？可能是修改之后的检测，例如修改阈值，告警条件等
        apply.schedule_status=0 # 先将当前任务暂停，
        apply.updator='system'
        err_msg=e.args[0]
        logger.error(f"错误信息为：{err_msg}")
        # 通知到企微群当中去，配置错误
        notify_wechat_msgs(f'{basedir}/templates/messages/config_error_template.txt', mention_list=[app.config.user_email_to_phone[apply.creator+app.config.email_prefix]],tbl_name=apply.tbl_name, err_msg=err_msg)
        _clear_apply_jobs(apply_id)
    except HiveConnectionException as e:
        date_trigger = DateTrigger(run_date=datetime.now() + timedelta(minutes=5))  # 暂时设定为5分钟之后进行重试
        sched.add_job(execute_single_apply, trigger=date_trigger,
                      args=(apply.apply_id, 'retry', record.check_sql, retry_times + 1),
                      name=str(apply) + '_retry')

    except Exception as e:
        apply.schedule_status=0
        apply.updator = 'system'
        err_msg = format_exc()
        logger.error(f"监控项：{apply}；错误信息为：{err_msg}")
        send_email(app.config.admin_email, [app.config.admin_email], f'监控项：{apply} /n 监控任务配置异常：{err_msg}','监控任务检测项出现错误')
        #notify_wechat_msgs(f'{basedir}/templates/messages/config_error_template.txt',mention_list=[app.config.user_email_to_phone[apply.creator+app.config.email_prefix]], tbl_name=apply.tbl_name,err_msg=err_msg)
        _clear_apply_jobs(apply_id)
    finally:
        session.commit()
        logger.info(f"{logger_prefix} 检测完成")
        if session: session.close()


def _prepare_data(check_record: DqcCheckRulesApplyRecords, check_apply: DqcCheckRulesApply, type: str):
    """
    准备发送数据
    :param check_record:
    :param check_apply:
    :param type:
    :return:
    """
    from datetime import datetime
    if check_apply.rule_id == 4:  # 对于自定义SQL进行单独讨论
        check_desc = check_apply.sql_desc if check_apply.sql_desc and check_apply.sql_desc != '' else "自定义SQL"
    elif check_apply.rule_id == 1:
        check_desc = "表行数检测"
    else:
        rule_description = "字段唯一性检测" if check_apply.rule_id == 2 else "字段空值个数检测"
        check_desc = f"{check_apply.col_name}字段{rule_description}"
    formatted_now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    actual_value, threshold = _convert_actual_value_threshold(check_record.actual_value, check_apply.threshold)
    if type == "异常":
        description = f"{check_apply.tbl_name}表，{check_desc}，本次检测实际值为{actual_value}，已触发异常条件：{check_apply.operator} {threshold} 请及时关注"
        state = """<font color="warning">数据异常</font>"""
        first_title = "数据质量监控异常"
        warn_type='red'
    elif type== "任务执行异常":
        description = f"{check_apply.tbl_name}表，{check_desc}，数据质量检测任务异常：可能存在的原因为：底表无数据，请排查"
        state = """<font color="warning">数据异常</font>"""
        first_title = "数据质量检测任务异常"
    elif type == "异常恢复":
        description = f"{check_apply.tbl_name}表，{check_desc}，本次检测实际值为{actual_value}，已恢复正常"
        state = """异常恢复"""
        first_title = "数据质量异常恢复"
        warn_type='info'
    data_dict={
        'first_title':first_title,
        'desc':check_apply.desc,
        'description':description,
        'formatted_now':formatted_now,
        'apply_id':check_apply.apply_id,
        'check_sql':check_record.check_sql,
        'warn_type':warn_type
    }
    return data_dict



def _convert_actual_value_threshold(actual_value, threshold):
    if actual_value - int(actual_value) == 0:
        actual_value = int(actual_value)
    if threshold - int(threshold) == 0:
        threshold = int(threshold)

    return actual_value, threshold

def _get_apply_desc(apply:DqcCheckRulesApply):
    """
    获取到单个apply所对应的描述
    :param apply:
    :return:
    """
    if apply.rule_id==1:
        return '表行数检测'
    if apply.rule_id==2:
        return f'{apply.col_name}字段唯一性检测'
    if apply.rule_id==3:
        return f'{apply.col_name}字段空值个数检测'
    if apply.rule_id==4:
        return '自定义SQL检测：'+str(apply.sql_desc) or '自定义SQL' # 使用自定义SQL的描述

def _get_apply_next_run_time(apply_id):
    """
    获取到单个任务的下次执行时间
    """
    try:
        job_ids=apply_to_job_dict[apply_id]['schedule']
        next_run_times=[sched.get_job(job_id).next_run_time for job_id in job_ids]
        return min(next_run_times)
    except Exception:
        return None



def _validate_apply(apply:DqcCheckRulesApply):
    """
    传入参数合法性校验
    """




def main():
    import sys
    # time.sleep(2222)
    check_apply = threading.Thread(target=check_new_apply,)
    start_time=datetime.now()
    check_apply.daemon=True
    check_apply.start()
    global check_new_apply_not_exit
    session=Session()
    not_turn_off=True
    while not_turn_off:
        try:
            status_control:DqcCheckRulesApplyStatusControl=session.query(DqcCheckRulesApplyStatusControl).filter(DqcCheckRulesApplyStatusControl.create_time>start_time).order_by(desc(DqcCheckRulesApplyStatusControl.create_time)).first()
            logger.debug(f'current_not_turn_off:{status_control}')
            if status_control and status_control.to_turn_off==1:
                not_turn_off=False
            else:
                t_m.sleep(app.config.main_thread_sleep_time)
            session.commit()
        except Exception as e:
            err_msg=e.args
            send_email(app.config.admin_email, [app.config.admin_email], f'监控任务配置异常：{err_msg}','监控任务检查停止信号出现问题')
            t_m.sleep(3*app.config.main_thread_sleep_time)


    # if app.config==ProdConfig:
    #     check_apply.join()
    # check_execute_apply.join()
    # 当时间没有到凌晨1点，且两个主要线程没有因为异常退出时，程序继续，否则程序直接退出
    # while datetime.now().hour != 1 and check_new_apply_not_exit and wait_for_apply_not_exit:
    #     time.sleep(1800) # 沉睡30分钟
    # # 要想让整个进程全部停止，需要将另外两个线程也都停止了才行，否则的话，主线程停止了，但是另外两个线程还在继续
    # check_new_apply_not_exit = False
    # wait_for_apply_not_exit = False
    # sys.exit(0) # 等待凌晨1点任务自动终止

if __name__ == '__main__':
    main()