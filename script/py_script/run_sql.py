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
from common.config import PROJECT_DIR
from common.config import DEBUG
import sys
from warnings import filterwarnings

reload(sys)
sys.setdefaultencoding('utf8')
filterwarnings('ignore', category=pymysql.Warning)
# 链接数据库
conn = pymysql.Connect(host=XJL_ETL_DB['host'], port=XJL_ETL_DB['port'], user=XJL_ETL_DB['user'],
                       password=XJL_ETL_DB['password'])
cursor = conn.cursor()

# 设置默认终止日期：前一天, 开始时间：7天前, （t-1）月第一天
cur_date = (date.today() + timedelta(days=-1)).strftime('%Y-%m-%d')
start_date = (date.today() + timedelta(days=-7)).strftime('%Y-%m-%d')
end_date = (date.today() + timedelta(days=-1)).strftime('%Y-%m-%d')
month = (date.today() + timedelta(days=-1)).strftime('%Y-%m-01')

# 解析参数
parser = argparse.ArgumentParser()
parser.add_argument('-s', '--start_date', default=start_date, help='开始时间 xxxx-xx-xx')
parser.add_argument('-e', '--end_date', default=end_date, help='结束时间 xxxx-xx-xx')
parser.add_argument('-m', '--month', default=month, help='月 xxxx-xx-01')
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
                    logging.info('-----------------------------RUNNING----------------------------\n{}'.format(sql))
                    cursor.execute(sql)
                    conn.commit()
                    logging.info('-----------------------------SUCCESS----------------------------')
            except Exception as err:
                logging.error('----------------------------ERROR SQL---------------------------\n{}\n'.format(sql))
                logging.error('----------------------------ERROR Info--------------------------')
                logging.error(err)
                if DEBUG==False:
                    text = '{err}\n{sql}'.format(err=err, sql=sql)
                    subject = file.split('/')[-1]
                    send_email(TO_AGENT['email'], subject, '', text)
                logging.info('ROLLBACK>>>>>>>>>>>>>>>>>>>>>>>>>>...')
                conn.rollback()
                return
    logging.info('------------------------------DONE------------------------------')


def format_param_dict(args):
    param = {
        'cur_date': cur_date,
        'start_date': args.start_date,
        'end_date': args.end_date,
        'month': args.month
    }
    logging.info('------------------------------PARAM-----------------------------')
    logging.info(param)
    return param


if __name__ == '__main__':
    logging.info('------------------------------START-----------------------------')
    logging.info('start_time: {}'.format(datetime.now().strftime('%Y-%m-%d %H:%M:%S')))

    # 部署项目路径
    project_path = PROJECT_DIR  # 项目跟目录/repo/xjl_etl/jobs_sql/
    # project_path = '/services/xjl_etl/script/py_script/'  # TEST
    sql_file = project_path + args.sql_file
    logging.info('SQl_FILE: {}'.format(sql_file))

    # 格式化参数字典
    param_dic = format_param_dict(args)

    # 执行SQL脚本
    try:
        run_sql(param_dic, sql_file)
        conn.commit()
    except Exception as err:
        logging.exception(err)
    finally:
        cursor.close()
        conn.close()
        logging.info('end_time: {}'.format(datetime.now().strftime('%Y-%m-%d %H:%M:%S')))
