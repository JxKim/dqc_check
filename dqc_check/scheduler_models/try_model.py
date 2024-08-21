import logging
logging.basicConfig(level=logging.ERROR)
def t1():
    import sys
    from traceback import format_tb
    try:
        t={
            'k1':'v1'
        }
        print(t['k2'])
    except Exception as e:
        print('当前sys.exc_info为',sys.exc_info())
        print(f'当前sys.exc_info类型为：{type(sys.exc_info())}')
        exc, tb = sys.exc_info()[1:]
        formatted_tb = ''.join(format_tb(tb))
        print(f'exc为:{exc}')
        print(f'tb为:{tb}')
        print(f'当前formatted_tb为：{formatted_tb}')
        import traceback
        traceback.clear_frames(tb)

def raise_exception():
    t = {
        'k1': 'v1'
    }
    print(t['k2'])


from scheduler import BackgroundScheduler
from triggers import DateTrigger,CronTrigger
from datetime import datetime,timedelta

import time

sched=BackgroundScheduler()
sched.start()
for t in range(5):
    from pympler import tracker, muppy, summary

    tr = tracker.SummaryTracker()
    all_objs = muppy.get_objects()
    sum = summary.summarize(all_objs)
    print('总内存占用为：')
    summary.print_(sum)
    print('\n\n')
    print('内存差为：')
    tr.print_diff()
    for i in range(1000):
        now = datetime.now()+timedelta(days=1)
        tri = CronTrigger.from_crontab('30 17 * * *')
        sched.add_job(func=raise_exception,trigger=tri)
    time.sleep(5)




import time
time.sleep(700)