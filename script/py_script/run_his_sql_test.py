# -*- coding: utf8 -*-
import argparse
import pymysql
from datetime import datetime
import io
import os
import logging
from log import init_logging
from config import LOG_DIR
from config import PROJECT_DIR
from config import PROJECT_TEST_DIR
from config import XJL_ETL_DB
import sys
import arrow

reload(sys)
sys.setdefaultencoding('utf8')

# 连接数据库
conn = pymysql.Connect(host=XJL_ETL_DB['host'], port=XJL_ETL_DB['port'], user=XJL_ETL_DB['user'],
                       password=XJL_ETL_DB['password'])
cursor = conn.cursor()

# 设置默认终止日期：前一天, 开始时间：7天前, （t-1）月第一天
start_date = arrow.now().shift(days=-1).format('YYYY-MM-') + '01'
end_date = arrow.now().shift(days=-1).format('YYYY-MM-') + '01'
cur_date = arrow.now().shift(days=-1).format('YYYY-MM-DD')
month = arrow.now().shift(days=-1).format('YYYY-MM-') + '01'

# 解析参数
parser = argparse.ArgumentParser()
parser.add_argument('-s', '--start_date', default=start_date, help='开始时间 xxxx-xx-xx')
parser.add_argument('-e', '--end_date', default=end_date, help='结束时间 xxxx-xx-xx')
parser.add_argument('-l', '--log_file', default=None, help='指定的log文件')
parser.add_argument('-f', '--sql_file', help='指定执行SQL文件')
args = parser.parse_args()

# 配置LOG
LOG_NAME = args.sql_file.split('/')[-1].replace('.sql', '.log')
if args.log_file == None:
    log_path = LOG_DIR + datetime.now().strftime('%Y%m%d')
    if not os.path.exists(log_path):
        os.makedirs(log_path)
    log_file = log_path + '/etl_his_' + LOG_NAME
else:
    log_file = args.log_file
init_logging({'console_log_level': logging.INFO, 'file_log_level': logging.INFO, 'log_file': log_file})


def run_sql(sql_param, file):
    logging.info('RUN>>>>>>>>>>>>>>>>>>>>>>>>>>........................................')
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
                logging.info('ROLLBACK>>>>>>>>>>>>>>>>>>>>>...........................................')
                conn.rollback()
                break


def format_param_dict(args):
    param = {
        'cur_date': cur_date,
        'start_date': args.start_date,
        'end_date': args.end_date
    }
    logging.info('------------------------------PARAM-----------------------------')
    logging.info(param)
    return param


if __name__ == '__main__':
    logging.info('------------------------------START-----------------------------')
    logging.info('start_time: {}'.format(datetime.now().strftime('%Y-%m-%d %H:%M:%S')))

    # 被执行SQL文件
    # sql_file = PROJECT_TEST_DIR + args.sql_file  # PROJECT_TEST_DIR = '/services/xjl_etl/script/py_script/'

    sql_file = PROJECT_DIR + args.sql_file
    logging.info('SQl_FILE: {}'.format(sql_file))
    # 格式化参数字典
    param_dic = format_param_dict(args)
    param_dic['month'] = param_dic['start_date']
    print(param_dic)
    try:
        while 1:
            # 执行SQL脚本
            run_sql(param_dic, sql_file)
            logging.info('-----------------------{}--DONE------------------------------'.format(
                param_dic['month']))
            # 判断是否终止条件
            if param_dic['month'] == param_dic['end_date']:
                break
            # 执行完，月份加1
            param_dic['month'] = arrow.get(param_dic['month']).shift(months=+1).format('YYYY-MM-DD')
    except Exception as err:
        logging.exception(err)
    finally:
        cursor.close()
        conn.close()
        logging.info('end_time: {}'.format(datetime.now().strftime('%Y-%m-%d %H:%M:%S')))
