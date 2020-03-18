#!/bin/bash
/usr/bin/python /services/xjl_etl/script/py_script/run_sql.py -f ods/ods_now.sql

# /usr/bin/python /services/xjl_etl/script/py_script/run_sql.py -f dw/delete_dw_now_anchor_tags.sql

/usr/bin/python /services/xjl_etl/script/py_script/run_sql.py -f dw/dw_now_day.sql
/usr/bin/python /services/xjl_etl/script/py_script/run_sql.py -f dw/dw_now_month.sql

/usr/bin/python /services/xjl_etl/script/py_script/run_sql.py -f rpt/rpt_now_month.sql
/usr/bin/python /services/xjl_etl/script/py_script/run_sql.py -f rpt/rpt_now_day.sql
