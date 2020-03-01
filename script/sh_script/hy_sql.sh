#!/bin/bash
/usr/bin/python /services/xjl_etl/script/py_script/run_sql.py -f ods/ods_hy.sql

/usr/bin/python /services/xjl_etl/script/py_script/run_sql.py -f dw/dw_hy_anchor_tags.sql

/usr/bin/python /services/xjl_etl/script/py_script/run_sql.py -f dw/dw_hy_day.sql
/usr/bin/python /services/xjl_etl/script/py_script/run_sql.py -f dw/dw_hy_month.sql

/usr/bin/python /services/xjl_etl/script/py_script/run_sql.py -f rpt/rpt_hy_month.sql
/usr/bin/python /services/xjl_etl/script/py_script/run_sql.py -f rpt/rpt_hy_day.sql