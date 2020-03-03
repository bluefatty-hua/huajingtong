# -*- coding: utf8 -*-

import argparse
import pymysql
from datetime import datetime
from datetime import date
from datetime import timedelta
import io
import os
import logging
from script.py_script.common.log import init_logging
from script.py_script.common.sent_email import send_email
from script.py_script.common.config import LOG_DIR
from script.py_script.common.config import XJL_ETL_DB
from script.py_script.common.config import TO_AGENT
import sys
from warnings import filterwarnings

reload(sys)
sys.setdefaultencoding('utf8')

filterwarnings('ignore', category=pymysql.Warning)

cur_date = (date.today() + timedelta(days=-1)).strftime('%Y-%m-%d')

# 连接数据库
conn = pymysql.Connect(host=XJL_ETL_DB['host'], port=XJL_ETL_DB['port'], user=XJL_ETL_DB['user'],
                       password=XJL_ETL_DB['password'])
cursor = conn.cursor()


def run_sql(sql_param):
    sql = '''SELECT n.dt, m.platform, (n.revenue = m.revenue AND n.anchor_cnt = m.anchor_cnt AND n.live_cnt = m.live_cnt)
             FROM bireport.rpt_day_all_new n
             INNER JOIN stage.rs_monitored_tmp0 m ON n.dt = m.dt AND n.platform = m.platform
             WHERE newold_state = 'all'
               AND active_state = 'all'
               AND revenue_level = 'all'
               -- t-2
               AND n.dt = '2020-03-02' - INTERVAL 1 DAY;'''
    try:
        cursor.execute(
        )
        print(cursor.fetchall())
    except Exception as err:
        logging.exception(err)
        text = '{err}\n{sql}'.format(err=err, sql=sql)
        send_email(TO_AGENT['email'], 'monitored.sql', '', text)
        conn.rollback()
        return


if __name__ == '__main__':
    param = {'cur_date': cur_date}
    run_sql(param)
