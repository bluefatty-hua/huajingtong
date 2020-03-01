#!/bin/bash
/usr/bin/python /services/xjl_etl/script/py_script/run_sql.py -f ods/ods_bb.sql

/usr/bin/python /services/xjl_etl/script/py_script/run_sql.py -f dw/dw_bb_anchor_tags.sql
/usr/bin/python /services/xjl_etl/script/py_script/run_sql.py -f dw/dw_bb_day.sql
/usr/bin/python /services/xjl_etl/script/py_script/run_sql.py -f dw/dw_bb_month.sql

/usr/bin/python /services/xjl_etl/script/py_script/run_sql.py -f rpt/rpt_bb_day.sql
/usr/bin/python /services/xjl_etl/script/py_script/run_sql.py -f rpt/rpt_bb_month.sql
