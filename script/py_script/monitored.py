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
from monitored_config import MONITOR_TABLE
from monitored_config import MONITOR_DIC
import sys
from warnings import filterwarnings

log_path = LOG_DIR + datetime.now().strftime('%Y%m%d')
if not os.path.exists(log_path):
    os.makedirs(log_path)
log_file = log_path + '/etl_' + 'monitored.log'
init_logging({'console_log_level': logging.INFO, 'file_log_level': logging.INFO, 'log_file': log_file})

cur_date = (date.today() + timedelta(days=-1)).strftime('%Y-%m-%d')
judge_date = (date.today() + timedelta(days=-1)).strftime('%Y-%m-%d')

# 连接数据库
conn = pymysql.Connect(host=XJL_ETL_DB['host'], port=XJL_ETL_DB['port'], user=XJL_ETL_DB['user'],
                       password=XJL_ETL_DB['password'])
cursor = conn.cursor()


def run_sql(sql_dic, sql_param):
    judge_sql = sql_dic['judge_sql'].format(cur_date=sql_param['cur_date'])
    result_insert_sql = sql_dic['result_insert_sql'].format(cur_date=sql_param['cur_date'])
    insert_sql = sql_dic['insert_sql'].format(cur_date=sql_param['cur_date'])
    i = 0
    text = ''
    sql = ''
    try:
        sql = judge_sql
        logging.info('judge_sql--------/n{}'.format(judge_sql))
        cursor.execute(judge_sql)
        result = cursor.fetchall()
        cursor.execute(result_insert_sql)
        logging.info('judge_sql--------/n{}'.format(result_insert_sql))
        logging.info('\n' + str(result).replace('), (', '),\n ('))
        # (datetime.date(2020, 3, 1), 'all', 1, 1)
        # (datetime.date(2020, 3, 1), 'bilibili', 1, 1)
        # (datetime.date(2020, 3, 1), 'DouYin', 1, 1)
        # (datetime.date(2020, 3, 1), 'FanXing', 1, 1)
        # (datetime.date(2020, 3, 1), 'HUYA', 1, 1)
        # (datetime.date(2020, 3, 1), 'NOW', 1, 1)
        # (datetime.date(2020, 3, 1), 'YY', 1, 1)
        for t in result:
            print(t)
            if t[2] == 1 and t[3] == 1:
                pass
            else:
                i += 1
                text += 'ERROR: ' + t[1] + ('-{judge_date}-数据有误&{cur_date}-数据缺失' if t[2] != 1 and t[3] != 1 else (
                    '-{cur_date}-数据缺失' if t[2] == 1 and t[3] != 1 else '-{judge_date}-数据有误')).format(
                    judge_date=judge_date,
                    cur_date=cur_date) + '\n'
        if i == 0:
            sql = insert_sql
            cursor.execute(insert_sql)
            logging.info('judge_sql--------/n{}'.format(insert_sql))
            conn.commit()
        else:
            send_email(TO_AGENT['email'], 'monitored.sql', '', text)
    except Exception as err:
        logging.exception(err)
        text = '{err}\n{sql}'.format(err=err, sql=sql)
        send_email(TO_AGENT['email'], 'monitored.sql', '', text)
        conn.rollback()
        return


if __name__ == '__main__':
    logging.info('cur_date: ' + cur_date + ' judge_date: ' + judge_date)
    for table in MONITOR_TABLE:
        # print(MONITOR_DIC[table])
        param = {'cur_date': cur_date}
        run_sql(MONITOR_DIC[table], param)
    cursor.close()
    conn.close()
