# -*- coding: utf8 -*-

# 注意传参格式 eg: slq_file_path ['{'start_date':'2019-01-01','end_date':'2019-12-31','platform_id':'1004,1005,1000'}']
import argparse
import pymysql
from datetime import datetime
from datetime import date
from datetime import timedelta
import io
import os
import logging
from script.py_script.common.log import init_logging
from script.py_script.common.config import LOG_DIR
from script.py_script.common.config import XJL_ETL_DB
import sys

reload(sys)
sys.setdefaultencoding('utf8')

# 链接数据库
conn = pymysql.Connect(host=XJL_ETL_DB['host'], port=XJL_ETL_DB['port'], user=XJL_ETL_DB['user'], password=XJL_ETL_DB['password'])
cursor = conn.cursor()

# 设置默认终止日期：前一天, 开始时间：7天前, （t-1）月第一天
start_date = (date.today() + timedelta(days=-7)).strftime('%Y-%m-%d')
end_date = (date.today() + timedelta(days=-1)).strftime('%Y-%m-%d')
month = (date.today() + timedelta(days=-1)).strftime('%Y-%m-01')

# 获取所有平台ID
cursor.execute('select id from warehouse.platform;')
# platform_id = cursor.fetchall()
platform_id = str([pf_id for pf_id in [list(t)[0] for t in cursor.fetchall()]]).replace('[', '(').replace(']', ')')

# 解析参数
parser = argparse.ArgumentParser()
parser.add_argument('-s', '--start_date', default=start_date, help='开始时间 xxxx-xx-xx')
parser.add_argument('-e', '--end_date', default=end_date, help='结束时间 xxxx-xx-xx')
parser.add_argument('-m', '--month', default=month, help='月 xxxx-xx-01')
parser.add_argument('-p', '--platform_id', default=platform_id, help='指定平台ID')
parser.add_argument('-l', '--log_file', default=None, help='指定的log文件')
parser.add_argument('-f', '--sql_file', help='指定执行SQL文件')
args = parser.parse_args()

# 配置LOG
LOG_NAME = args.sql_file.split('/')[-1].replace('.sql', '.log')
if args.log_file == None:
    log_path = LOG_DIR + datetime.now().strftime('%Y%m%d')
    if not os.path.exists(log_path):
        os.makedirs(log_path)
    log_file = log_path + '/etl_' + LOG_NAME
else:
    log_file = args.log_file
init_logging({'console_log_level': logging.INFO, 'file_log_level': logging.INFO, 'log_file': log_file})


def run_sql(sql_param, file):
    logging.info('RUN>>>>>>>>>>>>>>>>>>>>>>>>>>...')
    with io.open(file, 'r', encoding='utf8') as fr:
        for sql in fr.read().split(';'):
            try:
                if len(sql) > 10:
                    sql = (sql + ';').replace('/n', '').format(**sql_param) + '\n'
                    cursor.execute(sql)
                    conn.commit()
                    logging.info('-----------------------------SUCCESS----------------------------\n{}'.format(sql))
            except Exception as err:
                logging.info('----------------------------ERROR SQL---------------------------\n{}'.format(sql))
                logging.exception(err)
                logging.info('ROLLBACK>>>>>>>>>>>>>>>>>>>>>>>>>>...')
                conn.rollback()
                break


def format_param_dict(args):
    param = {
        'start_date': args.start_date,
        'end_date': args.end_date,
        'month': args.month,
        'platform_id': args.platform_id
    }
    logging.info('------------------------------PARAM-----------------------------')
    logging.info(param)
    return param


if __name__ == '__main__':
    logging.info('------------------------------START-----------------------------')
    logging.info('start_time: {}'.format(datetime.now().strftime('%Y-%m-%d %H:%M:%S')))

    # 部署项目路径
    # project_path = '/services/xjl_etl/jobs_sql/'  # 项目跟目录/repo/xjl_etl/jobs_sql/
    project_path = '/services/xjl_etl/script/py_script/'  # TEST
    sql_file = project_path + args.sql_file
    logging.info('SQl_FILE: {}'.format(sql_file))

    # 格式化参数字典
    param_dic = format_param_dict(args)

    # 执行SQL脚本
    try:
        run_sql(param_dic, sql_file)
        conn.commit()
        logging.info('------------------------------DONE------------------------------')
    except Exception as err:
        logging.exception(err)
    finally:
        cursor.close()
        conn.close()
        logging.info('end_time: {}'.format(datetime.now().strftime('%Y-%m-%d %H:%M:%S')))

