# -*- coding: utf8 -*-

import argparse
import pymysql
from datetime import datetime
from datetime import date
from datetime import timedelta
import io
import os
import logging
from common.log import init_logging
from common.sent_email import send_email
from common.config import LOG_DIR
from common.config import XJL_ETL_DB
from common.config import TO_AGENT
import sys
from warnings import filterwarnings

cur_date = (date.today() + timedelta(days=-1)).strftime('%Y-%m-%d')

# 连接数据库
conn = pymysql.Connect(host=XJL_ETL_DB['host'], port=XJL_ETL_DB['port'], user=XJL_ETL_DB['user'],
                       password=XJL_ETL_DB['password'])
cursor = conn.cursor()


def run_sql(sql_param):
    judge_sql = '''SELECT n.dt, m.platform, (n.revenue = m.revenue AND n.anchor_cnt = m.anchor_cnt AND n.live_cnt = m.live_cnt)
             FROM bireport.rpt_day_all_new n
             INNER JOIN stage.monitored m ON n.dt = m.dt AND n.platform = m.platform
             WHERE newold_state = 'all'
               AND active_state = 'all'
               AND revenue_level = 'all'
               -- t-2
               AND n.dt = '{cur_date}' - INTERVAL 1 DAY;'''.format(cur_date=cur_date)
    insert_sql = '''
            REPLACE INTO stage.monitored
            SELECT dt, platform, anchor_cnt, live_cnt, revenue
            FROM bireport.rpt_day_all_ne
            WHERE newold_state = 'all'
              AND active_state = 'all'
              AND revenue_level = 'all'
              -- t-1
              AND dt = '{cur_date}';'''.format(cur_date=cur_date)
    i = 0
    sql = ''
    try:
        sql = judge_sql
        cursor.execute(judge_sql)
        result = cursor.fetchall()
        # ((datetime.date(2020, 3, 1), 'all', 1),
        #  (datetime.date(2020, 3, 1), 'bilibili', 1),
        #  (datetime.date(2020, 3, 1), 'DouYin', 1),
        #  (datetime.date(2020, 3, 1), 'FanXing', 1),
        #  (datetime.date(2020, 3, 1), 'HUYA', 1),
        #  (datetime.date(2020, 3, 1), 'NOW', 1),
        #  (datetime.date(2020, 3, 1), 'YY', 1))
        for t in result:
            print(t)
            if t[2] == 1:
                pass
            else:
                i += 1
                send_email(TO_AGENT['email'], 'monitored.sql', '', 'ERROR:' + t[1] + '数据有误')
        if i == 0:
            sql = insert_sql
            cursor.execute(insert_sql)
            conn.commit()
    except Exception as err:
        logging.exception(err)
        text = '{err}\n{sql}'.format(err=err, sql=sql)
        send_email(TO_AGENT['email'], 'monitored.sql', '', text)
        conn.rollback()
        return


if __name__ == '__main__':
    param = {'cur_date': cur_date}
    run_sql(param)
