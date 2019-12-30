# -*- coding: utf8 -*-

# 注意传参格式 eg: "{'start_date':'2019-01-01','end_date':'2019-12-31','platform_id':'1004,1005,1000'}"

import pymysql
import sys
from datetime import date
from datetime import date
from datetime import timedelta
import io
import sys

reload(sys)
sys.setdefaultencoding('utf8')

conn = pymysql.Connect(host='127.0.0.1', user='root', password='123456')
cursor = conn.cursor()


# format path Todo


def run_sql(sql_param):
    with io.open('./rs_test_sql.sql', 'r', encoding='utf8') as fr:
        for sql in fr.read().split(';'):
            if len(sql) > 10:
                sql = (sql + ';').replace('/n', '').format(**sql_param)
                print(sql)
                cursor.execute(sql)


def format_param_dict(param_lst):
    # 获取所有平台ID
    cursor.execute('select id from warehouse.platform;')
    platform_id = [pf_id for pf_id in [list(t)[0] for t in cursor.fetchall()]]
    # platform_id = cursor.fetchall()

    # 设置默认终止日期 前一天
    end_date = (date.today() + timedelta(days=-1)).strftime('%Y-%m-%d')

    if len(param_lst) > 1:
        param = eval(param_lst[1])
        print(type(param), '/n', param)
        param['start_date'] = param['start_date'] if param.get('start_date') else '2000-01-01'
        param['end_date'] = param['end_date'] if param.get('end_date') else end_date
        param['platform_id'] = param['platform_id'] if param.get('platform_id') else str(platform_id)[
                                                                                     :-2] + ')'
    else:
        param = {'start_date': '2019-01-01',
                 'end_date': end_date,
                 'platform_id': str(platform_id).replace('[', '').replace(']', '')
                 }
    return param


if __name__ == '__main__':
    print(sys.argv)
    param_dic = format_param_dict(sys.argv)
    print(param_dic)
    run_sql(param_dic)
