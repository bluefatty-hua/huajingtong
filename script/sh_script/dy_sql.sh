#!/bin/bash
/usr/bin/python /services/xjl_etl/script/py_script/run_sql.py -f ods/ods_dy.sql

# /usr/bin/python /services/xjl_etl/script/py_script/run_sql.py -f dw/delete_dw_dy_anchor_tags.sql

/usr/bin/python /services/xjl_etl/script/py_script/run_sql.py -f dw/dw_dy_day.sql
/usr/bin/python /services/xjl_etl/script/py_script/run_sql.py -f dw/dw_dy_month.sql

/usr/bin/python /services/xjl_etl/script/py_script/run_sql.py -f rpt/rpt_dy_day.sql
/usr/bin/python /services/xjl_etl/script/py_script/run_sql.py -f rpt/rpt_dy_month.sql
