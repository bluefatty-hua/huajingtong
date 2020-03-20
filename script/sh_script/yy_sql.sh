#!/bin/bash
/usr/bin/python /services/xjl_etl/script/py_script/run_sql.py -f ods/ods_yy.sql -s 2019-12-01 -e 2020-03-19

# /usr/bin/python /services/xjl_etl/script/py_script/run_sql.py -f dw/delete_dw_yy_anchor_tags.sql

/usr/bin/python /services/xjl_etl/script/py_script/run_his_sql.py -f dw/dw_yy_day.sql -s 2019-12-01 -e 2020-03-01
/usr/bin/python /services/xjl_etl/script/py_script/run_his_sql.py -f dw/dw_yy_month.sql -s 2019-12-01 -e 2020-03-01

/usr/bin/python /services/xjl_etl/script/py_script/run_his_sql.py -f rpt/rpt_yy_month.sql -s 2019-12-01 -e 2020-03-01
/usr/bin/python /services/xjl_etl/script/py_script/run_his_sql.py -f rpt/rpt_yy_day.sql -s 2019-12-01 -e 2020-03-01
