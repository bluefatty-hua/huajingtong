# -*- coding: utf8 -*-

# 注意传参格式 eg: slq_file_path ['{'start_date':'2019-01-01','end_date':'2019-12-31','platform_id':'1004,1005,1000'}']
import argparse

import pymysql
import sys
from datetime import datetime
from datetime import date
from datetime import timedelta
import io
import os
import logging
from log import init_logging
from config import LOG_DIR
import sys

reload(sys)
sys.setdefaultencoding('utf8')

# 链接数据库
conn = pymysql.Connect(host='127.0.0.1', user='wh_user', password='Nd^93)9f@445Fv')
cursor = conn.cursor()

# 解析参数
parser = argparse.ArgumentParser()
parser.add_argument('-s', '--start_date', help='开始时间')
parser.add_argument('-e', '--end_date', help='结束时间')
parser.add_argument('-f', '--sql_file', help='指定执行SQL文件')
parser.add_argument('-p', '--platform_id', help='指定平台ID')
parser.add_argument('-l', '--log_file', help='指定的log文件')
args = parser.parse_args()

# 配置LOG
LOG_NAME = args.sql_file.split('/')[-1]
if args.log_file == None:
    log_path = LOG_DIR + datetime.now().strftime('%Y%m%d')
    if not os.path.exists(log_path):
        os.makedirs(log_path)
    log_file = log_path + '/' + LOG_NAME
else:
    log_file = args.log_file
init_logging({'console_log_level': logging.INFO, 'file_log_level': logging.INFO, 'log_file': log_file})


def run_sql(sql_param, file):
    with io.open(file, 'r', encoding='utf8') as fr:
        for sql in fr.read().split(';'):
            try:
                if len(sql) > 10:
                    sql = (sql + ';').replace('/n', '').format(**sql_param)
                    cursor.execute(sql)
                    conn.commit()
                    logging.info('------------------------------DONE------------------------------\n', sql)
            except Exception as err:
                logging.info('------------------------------ERROR SQL------------------------------\n', sql)
                logging.exception(err)
                logging.info('--ROLLBACK-->>>>>>>>>>>>>>>>>')
                conn.rollback()


def format_param_dict(args):
    # 获取所有平台ID
    cursor.execute('select id from warehouse.platform;')
    # platform_id = cursor.fetchall()
    platform_id = str([pf_id for pf_id in [list(t)[0] for t in cursor.fetchall()]]).replace('[', '(').replace(']', ')')

    # 设置默认终止日期：前一天, 开始时间：7天前
    start_date = (date.today() + timedelta(days=-7)).strftime('%Y-%m-%d')
    end_date = (date.today() + timedelta(days=-1)).strftime('%Y-%m-%d')

    param = dict()
    param['start_date'] = args.start_date if args.start_date else start_date
    param['end_date'] = args.end_date if args.end_date else end_date
    param['platform_id'] = args.platform_id if args.platform_id else platform_id
    logging.info('------------------------------PARAM------------------------------\n', param)
    return param


if __name__ == '__main__':
    # 部署项目路径
    # project_path = '/services/xjl_etl/jobs_sql/'  # 项目跟目录/repo/xjl_etl/jobs_sql/
    project_path = '/services/xjl_etl/script/run_sql/'  # TEST
    sql_file = project_path + args.sql_file
    logging.info('SQl_FILE>>>>>>>>>>>>>>>>>>>>>>>>>', sql_file)

    # 格式化参数字典
    param_dic = format_param_dict(args)

    # 执行SQL脚本
    try:
        run_sql(param_dic, sql_file)
    finally:
        cursor.close()
        conn.close()
