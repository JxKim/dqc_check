"""
对于任务的批量检测报告：
当前逻辑：
    对特定的任务实施检测，对于失败的任务，等待所有任务都是同一批次的失败之后，就推送相关任务报告，否则等待其他任务重试

数据填报的逻辑：
    首次检测，全部失败，不推送，当有一个成功时，推送相关数据

两种触发场景不一致
场景一：当所有失败任务都是同一个批次的时候，触发
场景二：当有成功任务时，就触发，

通过传参兼容上面两种场景：

"""

import time
from models import DqcCheckRulesApply,DqcCheckRulesApplyRecords,Session,check_job_status,func
from typing import Dict,List,Tuple
from sqlalchemy import desc,not_
import threading
from threading import Thread
import logging
from collections import namedtuple
from utils import get_image_data,send_pic_to_entWechat,get_image_data_by_detail,get_multi_image_data_by_detail
logger=logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO,format='%(asctime)s - %(levelname)s - %(message)s',datefmt='%Y-%m-%d %H:%M:%S')
input_parameters=namedtuple('InputParameters','job_name key alarm_type notifier')
shared_dict:Dict[str,check_job_status]={} #一个线程，用以获取到当前是否还有其他的apply需要去执行
lock=threading.Lock()
main_thread_finish=False

def main_threading_job(check_input:input_parameters,check_tbls:List[str]):
    """
    主线程的工作，每次轮训这个dict。
    成功时告警：
        每次有轮训到新的成功任务，就发一次告警
    失败时告警：
        对于成功任务，不需要处理，对于失败任务，需要判断当前失败任务，是否都是同一批检测完成了，如果是的，那么就发送一次报告，否则，不发送报告
    :return:
    """
    all_finished=False
    threading.currentThread().setName("主线程")
    current_thread_name =threading.currentThread().getName()
    logger.info(f"主线程{current_thread_name}开始：当前传入的参数为{check_input}")
    logger.info(f"所有的任务为：{shared_dict}")
    last_batch=0
    last_succeeded_tasks_nums=0
    last_succeeded_tasks_set=set()
    global main_thread_finish
    while not all_finished:
        failed_applys = []
        not_started_applys=[]
        succeed_applys=[]
        for apply,check_job_res in shared_dict.items():
            if check_job_res.job_status=="失败":
                failed_applys.append(check_job_res)
            elif check_job_res.job_status=="成功":
                succeed_applys.append(check_job_res)
            else:
                not_started_applys.append(check_job_res)

        succeeded_tasks_nums=len(succeed_applys)
        succeeded_tasks_sets=[job.dqc_check_rules_apply.apply_id for job in succeed_applys]
        # 当前失败任务全部都为0，而且当前未开启的任务全部都为0
        if len(not_started_applys)>0:
            logger.info("主线程仍有任务未完成，等待")
            time.sleep(30)


        elif len(failed_applys)==0 and len(not_started_applys)==0:
            if check_input.alarm_type=='1':
                logger.info(f"{current_thread_name}所有任务都已成功，当前成功列表为：{succeed_applys}")
                logger.info(f"当前参数为:{check_input}")
                notify_job_status(succeed_applys,failed_applys,check_input,check_tbls)
                # 使用全局变量，非局部变量
                main_thread_finish = True
                logger.info('当前任务已全部执行完成')
                return
            else:
                notify_job_status_type_2(succeed_applys, succeed_applys, failed_applys, check_input)
                main_thread_finish=True
                return
        elif len(failed_applys)==0:
            logger.info(f"{current_thread_name}任务尚未开始，等待20secs，等待任务列表为：{not_started_applys}")
            time.sleep(20)
        else:
            if check_input.alarm_type=='1':
                # 失败时，一个批次的失败才会触发
                # 只有在全部失败批次号都一致时，才推送消息，否则等待
                job_batchs=[apply_job.job_batch for apply_job in failed_applys]
                logger.info(f"{current_thread_name}所有失败任务批次为：{job_batchs}")
                max_job_batch=max(job_batchs)
                is_same_batch=True
                for batch in job_batchs:
                    if batch != max_job_batch:
                        is_same_batch=False
                        break
                if is_same_batch and max_job_batch>last_batch:
                    logger.info(f"{current_thread_name}当前批次任务已经全部完成，批次为:{max_job_batch}\n，当前批次失败任务为：{failed_applys}\n，当前成功批次为：{succeed_applys}\n,发送通知，继续等待180secs")
                    notify_job_status(succeed_applys,failed_applys,check_input,check_tbls)
                    last_batch=max_job_batch
                    time.sleep(180)
                else:
                    logger.info(f"{current_thread_name}当前批次还有失败任务尚未完成,等待30secs")
                    time.sleep(30)
            else:
                # 成功时，有单个任务成功就会触发
                if succeeded_tasks_nums>last_succeeded_tasks_nums:
                    # 有新的成功的任务
                    new_succeeded_tasks=[]
                    for job in succeed_applys:
                        if job.dqc_check_rules_apply.apply_id not in last_succeeded_tasks_set:
                            new_succeeded_tasks.append(job)
                    # new_succeeded_tasks=succeeded_tasks_sets-last_succeeded_tasks_set 当前check_job_status不能被序列化，所以不可以使用set的方式
                    notify_job_status_type_2(new_succeeded_tasks,succeed_applys,failed_applys,check_input)

                logger.info(f'{current_thread_name}当前有部分任务成功，继续等待')
                time.sleep(180)
        last_succeeded_tasks_set=succeeded_tasks_sets
        last_succeeded_tasks_nums = succeeded_tasks_nums







def update_dict(to_be_updated:Dict[str,check_job_status]):
    with lock:
        shared_dict.update(to_be_updated)

def _check_single_apply_loop(apply_id:int):
    """
    轮训检测单个apply的检测状态，如果成功，更新shared_dict,否则等待下次执行完成
    :return:
    """
    from datetime import datetime
    session=Session()
    threading.currentThread().setName(f"{apply_id}")
    current_thread_name =threading.currentThread().getName()
    logger.info(f"{current_thread_name}开始进行循环检测")
    last_apply_record_id=None
    check_result='失败'
    has_checked_time=0
    job_batch=0
    while check_result=='失败':
        # 获取到最新的check_apply_record和最新的apply（在apply当中，有last_checked_time字段，用以标记最近一次查询时间，所以需要更新）
        latest_apply_record:DqcCheckRulesApplyRecords=session.query(DqcCheckRulesApplyRecords).filter(
                    DqcCheckRulesApplyRecords.apply_id == apply_id,
                    func.date(DqcCheckRulesApplyRecords.create_time)== datetime.now().date()
        ).order_by(desc(DqcCheckRulesApplyRecords.create_time)).first()
        latest_apply = session.query(DqcCheckRulesApply).filter(DqcCheckRulesApply.apply_id == apply_id).first()
        check_status:check_job_status=shared_dict.get(str(apply_id))
        check_status.dqc_check_rules_apply = latest_apply
        if latest_apply_record is not None:
            logger.info(f"{current_thread_name}当前检测id为{latest_apply_record.check_id}，上次为:{last_apply_record_id}")
        if latest_apply_record is None:
            # 当前任务还没有进行调度
            time.sleep(50)
            logger.info(f"当前线程{current_thread_name}对应任务暂时没有调度，继续等待")
            has_checked_time+=1
            if has_checked_time==2:
                #检测多次都没有check_record ID，都没有结果，说明当前apply_id并没有进行调度（有可能是当前apply_id实际已不使用，但是没有在数据表当中标记），算是有一个超时时间，总共是50*5=4mins
                # update_dict({str(apply_id):check_job_status(job_status="废弃",job_batch=None,dqc_check_rules_apply_records=None)})
                logger.info(f"{current_thread_name}apply_id{apply_id}没有上具体检测调度，已废弃")
                check_status.job_status="废弃"
                logger.info(f"当前线程{current_thread_name}执行完成")
                session.close()
                return
        elif latest_apply_record.check_result=="成功":
            # the job succeeded,return
            # update_dict({str(apply_id): check_job_status(job_status="成功", job_batch=None,dqc_check_rules_apply_records=latest_apply_record)})
            logger.info(f"{current_thread_name}当前apply成功，子线程返回")
            check_status.job_status="成功"
            check_status.dqc_check_rules_apply_records=latest_apply_record
            logger.info(f"当前线程{current_thread_name}执行完成")
            session.close()
            return
        else:
            if latest_apply_record.check_status==1:
                # 任务执行中，无需更新shared_dict
                logger.info(f"当前线程{current_thread_name}检测任务正在进行，等待30secs")
                session.commit()
                time.sleep(30)

            elif (latest_apply_record.check_result=="失败" or latest_apply_record.check_result=="等待重试") and latest_apply_record.check_id==last_apply_record_id:
                # 上一批次的失败，无需更新shared_dict
                logger.info(f"当前线程{current_thread_name}检测任务仍是上次失败任务，等待300secs")
                session.commit()
                time.sleep(20) #等待5分钟
            else:
                # 新的批次的失败（the first fail or a new fail）
                job_batch+=1
                # update_dict({str(apply_id): check_job_status(job_status="失败", job_batch=job_batch,dqc_check_rules_apply_records=latest_apply_record)})
                check_status.job_status="失败"
                check_status.job_batch=job_batch
                check_status.dqc_check_rules_apply_records=latest_apply_record
                last_apply_record_id=latest_apply_record.check_id
                logger.info(f"当前线程{current_thread_name}检测任务执行再次失败，继续等待下次任务，等待300secs")
                session.commit()
                time.sleep(10) # todo 生产环境下，时间可以不用这么短

def _get_rule_name(rule_id:int):
    if rule_id==1:
        return '表行数'
    elif rule_id==2:
        return '字段唯一性检测'
    elif rule_id==3:
        return '字段空值个数检测'
    else :
        return '自定义SQL'

def _convert_actual_value_threshold(actual_value, threshold):
    if not actual_value or not threshold:
        return actual_value,threshold
    actual_value=actual_value or 0
    threshold=threshold or 0
    if actual_value - int(actual_value) == 0:
        actual_value = int(actual_value)
    if threshold - int(threshold) == 0:
        threshold = int(threshold)

    return actual_value, threshold

def _get_earliest_schedule_time(schedule_time):
    """
    获取到某一个apply的最早调度时间
    :param schedule_time:
    :return:
    """
    import re
    import datetime
    result = []
    min_datetime=datetime.datetime.strptime('23:59:59','%H:%M:%S')
    pattern = r'\s*[,|，]\s*'
    res = re.sub(pattern, '&', schedule_time).replace('：', ':').replace(' ', '').split('&')
    for time in res:
        t=datetime.datetime.strptime(time.split(':')[0]+':'+time.split(':')[1]+':00','%H:%M:%S')
        min_datetime=min(t,min_datetime)
    return min_datetime


def update_applys_status(check_input:input_parameters):
    """
    调度主线程和多个子线程。
    主线程：轮训所有applys的状态，触发企微推送
    子线程：轮训单个apply_id所对应的任务状态，更新shared_dict,通过shared_dict完成和主线程之间的通信
    :param applys_status:
    :return:
    """
    #
    from datetime import datetime
    now_time=datetime.now()
    t = datetime.strptime('9:00:00', '%H:%M:%S')
    #time.sleep(60) #此处需要等待所有的apply全部都调起来了，等待时间不好做决策
    session=Session()
    all_applys:List[DqcCheckRulesApply]=(session.query(DqcCheckRulesApply)
                                     .filter(DqcCheckRulesApply.desc.contains(check_input.job_name)) # 获取到当前相关任务
                                     .filter(DqcCheckRulesApply.schedule_status==1) #获取到在调度状态的任务
                                     # .filter(_get_earliest_schedule_time(DqcCheckRulesApply.schedule_time)<=t) # 获取到调度时间在9点之前的任务
                                     .all())

    applys=[apply  for apply in all_applys if _get_earliest_schedule_time(apply.schedule_time)<=t]
    del all_applys
    def get_tbl_type(tbl_type):
        if len(tbl_type)==3:
            return tbl_type
        else:
            return tbl_type+'#'
    check_tbls=list(set(get_tbl_type(apply.tbl_type)+apply.tbl_name for apply in applys))

    logger.info(f"需要检测的表为：{check_tbls}")
    check_tbls.sort(key=lambda x:x,reverse=True)
    all_data_dict={str(apply.apply_id):check_job_status(job_status="等待执行",job_batch=None,dqc_check_rules_apply_records=None,dqc_check_rules_apply=apply) for apply in applys}
    update_dict(all_data_dict) # 对所有的任务进行初始化
    # 启动主线程，子线程，
    main_thread=Thread(target=main_threading_job,args=(check_input,check_tbls))

    main_thread.daemon=True
    sub_threads=[]
    for apply in applys:
        sub_thread=Thread(target=_check_single_apply_loop,args=(apply.apply_id,))
        sub_threads.append(sub_thread)
        sub_thread.daemon=True
        sub_thread.start()
    main_thread.start()
    from datetime import datetime
    start_time = datetime.now()
    not_turn_off=True
    # 判定主线程是否执行完成，如果执行完成，那么就停止，否则的话等待，此外，在ds也可以通过直接kill掉任务，来让整个进程终止
    global main_thread_finish
    while (not main_thread_finish) and not_turn_off:
        # status_control: DqcCheckRulesApplyReportsStatus = session.query(DqcCheckRulesApplyReportsStatus).filter(
        #     DqcCheckRulesApplyReportsStatus.create_time > start_time).order_by(
        #     desc(DqcCheckRulesApplyReportsStatus.create_time)).first()
        # logger.debug(f'current_not_turn_off:{status_control}')
        # if status_control and status_control.to_turn_off == 1:
        #     not_turn_off = False
        # else:
        time.sleep(60)
        # session.commit()
    session.close()



def _get_check_desc(col_name,rule_id):
    """
    获取到检测对应的描述，对于表级别规则，仅返回 检测类型，对于字段级别，返回：字段+检测内容
    """
    if rule_id==4:
        return "自定义SQL"
    elif rule_id==1:
        return "表行数检测"
    else:
        check_desc="字段唯一性检测" if rule_id==2 else "空值检测"
        return f"{col_name}字段{check_desc}"

def _convert_condition(operator):
    return {
        ">":"大于",
        "<":"小于",
        "=":"等于",
        ">=":"大于等于",
        "<=":"小于等于",
        "!=":"不等于"
    }.get(operator)

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
        return apply.sql_desc or '自定义SQL'


def notify_job_status(succeed_applys_list:List[check_job_status],failed_job_list:List[check_job_status],check_input:input_parameters,check_tbls:List[str]):
    """
    对成功任务列表和失败任务列表进行告知
    :param succeed_applys_list:成功子检测项列表
    :param failed_job_list:失败子检测项列表
    :param check_input:传递参数，包含作业名称
    :param check_tbls:检测的table_list
    """
    from datetime import datetime
    all_check_items_list=[]
    failed_check_items_list=[]
    current_date=datetime.now().strftime('%Y-%m-%d')

    logger.info(f"当前faild_job_list为：\n{failed_job_list}")
    logger.info(f"当前succeed_job_list为：\n{succeed_applys_list}")
    logger.info(f"当前check_tbls_list为：\n{succeed_applys_list}")
    tbl_check_items:Dict[str,set]=dict()
    succeed_tbls:Dict[str,DqcCheckRulesApply]=dict() #检测成功表
    failed_tbls:Dict[str,List[Tuple[DqcCheckRulesApply, DqcCheckRulesApplyRecords]]]=dict() #检测失败表（存在单项失败即为失败）
    for succeed_apply in succeed_applys_list:
        items=tbl_check_items.get(succeed_apply.dqc_check_rules_apply.tbl_name,set())
        items.add(_get_apply_desc(succeed_apply.dqc_check_rules_apply))
        tbl_check_items[succeed_apply.dqc_check_rules_apply.tbl_name]=items
        succeed_tbls.update({succeed_apply.dqc_check_rules_apply.tbl_name:succeed_apply.dqc_check_rules_apply})
    for fail_apply in failed_job_list:
        items = tbl_check_items.get(fail_apply.dqc_check_rules_apply.tbl_name, set())
        items.add(_get_apply_desc(fail_apply.dqc_check_rules_apply))
        tbl_check_items[fail_apply.dqc_check_rules_apply.tbl_name] = items
        fail_list=failed_tbls.get(fail_apply.dqc_check_rules_apply.tbl_name)
        tbl_name=fail_apply.dqc_check_rules_apply.tbl_name
        if fail_list is None:#当前表还没有失败项
            failed_tbls.update({tbl_name:[(fail_apply.dqc_check_rules_apply,fail_apply.dqc_check_rules_apply_records)]})
        else:
            fail_list.append((fail_apply.dqc_check_rules_apply,fail_apply.dqc_check_rules_apply_records))

    for index,single_tbl in enumerate(check_tbls):
        single_tbl=single_tbl[3:] #去除掉表类型
        apply_items=tbl_check_items.get(single_tbl)
        apply_items_list=sorted(apply_items)
        if single_tbl in succeed_tbls and single_tbl not in failed_tbls:
            all_check_items_list.append((single_tbl,'正常',succeed_tbls.get(single_tbl).tbl_type,succeed_tbls.get(single_tbl).desc,index,','.join(apply_items_list),succeed_tbls.get(single_tbl).last_check_time))
        elif single_tbl in failed_tbls:
            logger.info(f"当前表为：{single_tbl}")
            logger.info(f"当前failed_tbls为：{failed_tbls}")
            all_check_items_list.append((single_tbl, '异常', failed_tbls.get(single_tbl)[0][0].tbl_type,
                                         failed_tbls.get(single_tbl)[0][0].desc, index, ','.join(apply_items_list),failed_tbls.get(single_tbl)[0][0].last_check_time))
            all_fail_job_for_table=failed_tbls.get(single_tbl)
            for job in all_fail_job_for_table:
                apply=job[0]
                record=job[1]
                actual_value,threshold=_convert_actual_value_threshold(record.actual_value,apply.threshold)
                failed_check_items_list.append((index,_get_check_desc(apply.col_name,apply.rule_id),actual_value,f"{_convert_condition(apply.operator)}{threshold}",record.check_sql,apply.last_check_time))

        else: # 特殊的情况，当前数据表没有相关的检测任务
            pass # todo 需要补充，部分任务可能没有对应的检测项



    logger.info(f"当前所有检测的items为：{all_check_items_list}")
    current_time=datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    if failed_check_items_list:
        logger.info(f'当前失败项为:{failed_check_items_list}')

        data_details={
            f'{current_date}高管会数据质量检测结果清单（截至{current_time}）':(['检测目标','状态','类型','影响范围','检测编号','检测项','检测时间'],all_check_items_list),

        }
        fail_data_details={
            f'{current_date}高管会数据质量检测异常检测项明细': (
            ['检测编号', '检测内容描述', '检测值', '告警条件', '检测SQL', '检测时间'], failed_check_items_list)
        }

    else:
        data_details = {
            f'{current_date}高管会数据质量检测结果清单（截至{current_time}）': (
            ['检测目标', '状态', '类型', '影响范围', '检测编号', '检测项','检测时间'], all_check_items_list)
        }
        fail_data_details = {}
    if fail_data_details:
        send_pic_to_entWechat(get_multi_image_data_by_detail(data_details),check_input.key) #
        send_pic_to_entWechat(get_multi_image_data_by_detail(fail_data_details),check_input.key,notifier=check_input.notifier) #
    else:
        send_pic_to_entWechat(get_multi_image_data_by_detail(data_details), check_input.key,notifier=check_input.notifier)  #




def notify_job_status_type_2(new_succeeded_tasks:List[check_job_status],succeeded_job_list:List[check_job_status],failed_job_list:List[check_job_status],check_input:input_parameters):
    """
    对于填报类型的任务的推送
    """
    # 1、本次填报的内容为：
    # 2、尚未填报的内容为
    notify_content = """
    # 数据填报内容检测清单
    ## 本次填报内容为：
    {this_checked_content}
    ## 已填报清单：
    {checked_tbls}
    ## 尚未填报清单：
    {unchecked_tbls}
    """
    this_checked_content='\n'.join([str(task.dqc_check_rules_apply.desc)+', '+str(task.dqc_check_rules_apply.tbl_name)+', 填报数据行数为： '+str(task.dqc_check_rules_apply_records.actual_value) for task in new_succeeded_tasks])
    checked_tbls='\n'.join([str(task.dqc_check_rules_apply.desc) for task in succeeded_job_list])
    unchecked_tbls='\n'.join([str(task.dqc_check_rules_apply.desc) for task in failed_job_list])
    import requests
    import json
    notify_group_token = f'https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key={check_input.key}'
    header = {
        'Content-Type': 'application/json'
    }
    message_data = {
        "msgtype": "markdown",
        "markdown": {
            "content": f"""{notify_content.format(this_checked_content=this_checked_content,checked_tbls=checked_tbls,unchecked_tbls=unchecked_tbls)}"""
        }
    }
    requests.post(notify_group_token, json.dumps(message_data), headers=header)
    notify_list=[]
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
    if check_input.notifier:
        notify_list += check_input.notifier.split(',')
        requests.post(notify_group_token, json.dumps(to_notify), headers=header)





def config_parser() ->input_parameters:
    import argparse
    parser = argparse.ArgumentParser()
    # 当前任务名称也即影响范围，仅到数据表的层面，不到具体指标的层面
    parser.add_argument(
        "--job_name",
        required=True,
        help="任务名称，需要和单表检测任务当中的desc描述保持一致"
    )

    parser.add_argument(
        "--key",
        required=True,
        help="机器人token"
    )

    parser.add_argument(
        "--alarm_type",
        default='1',
        help="成功时触发告警：有任务成功就推送，失败时触发告警：有任务失败的时候，需要等待失败任务全部是同一个批次的时候，才会触发告警"
    )

    parser.add_argument(
        "--notifier",
        default='',
        help="通知人，具体将消息推送给谁"
    )
    # 影响范围，目前只到结果表的粒度，不会到指标的粒度，后期会添加上到指标的粒度
    # 获取到命令行传入的对应的参数
    args = parser.parse_args()
    # 2、清洗用户传入的参数数据，封装成input_parameters对象
    check_input = input_parameters(job_name=args.job_name,key=args.key,alarm_type=args.alarm_type,notifier=args.notifier)
    logger.info(f"当前传入的check_input参数为：{check_input}")
    return check_input

def main():
    # apply_status用以记录所有的apply
    check_input=config_parser()
    update_applys_status(check_input)

if __name__ == '__main__':
    main()
    ## todo list
    """
    1、在单项检测任务当中，需要去定义表类型，tbl_type:结果表 底表
    2、
    """