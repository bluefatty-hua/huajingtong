# !/bin/bash
/usr/bin/python /services/xjl_etl/script/py_script/run_sql.py -f ods/ods_dy_xjl.sql

/usr/bin/python /services/xjl_etl/script/py_script/run_sql.py -f dw/dw_dy_xjl_anchor_tags.sql

/usr/bin/python /services/xjl_etl/script/py_script/run_sql.py -f dw/dw_dy_xjl_day.sql
/usr/bin/python /services/xjl_etl/script/py_script/run_sql.py -f dw/dw_dy_xjl_month.sql

/usr/bin/python /services/xjl_etl/script/py_script/run_sql.py -f rpt/rpt_dy_xjl_day.sql

/usr/bin/python /services/xjl_etl/script/py_script/run_sql.py -f rpt/rpt_dy_xjl_month.sql
