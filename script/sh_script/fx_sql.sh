# !/bin/bash
/usr/bin/python /services/xjl_etl/script/py_script/run_sql.py -f ods/ods_fx.sql

/usr/bin/python /services/xjl_etl/script/py_script/run_sql.py -f dw/dw_fx_anchor_tags.sql

/usr/bin/python /services/xjl_etl/script/py_script/run_sql.py -f dw/dw_fx_day.sql
/usr/bin/python /services/xjl_etl/script/py_script/run_sql.py -f dw/dw_fx_month.sql

/usr/bin/python /services/xjl_etl/script/py_script/run_sql.py -f rpt/rpt_fx_month.sql
/usr/bin/python /services/xjl_etl/script/py_script/run_sql.py -f rpt/rpt_fx_day.sql
