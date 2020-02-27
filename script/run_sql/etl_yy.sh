#!/bin/bash


/usr/bin/python /services/xjl_etl/script/run_sql/run_sql.py -f ods/ods_yy.sql

/usr/bin/python /services/xjl_etl/script/run_sql/run_sql.py -f dw/dw_yy_anchor_tags.sql

/usr/bin/python /services/xjl_etl/script/run_sql/run_sql.py -f dw/dw_yy_day.sql
/usr/bin/python /services/xjl_etl/script/run_sql/run_sql.py -f dw/dw_yy_month.sql

/usr/bin/python /services/xjl_etl/script/run_sql/run_sql.py -f rpt/rpt_yy_month.sql
/usr/bin/python /services/xjl_etl/script/run_sql/run_sql.py -f rpt/rpt_all_month.sql

/usr/bin/python /services/xjl_etl/script/run_sql/run_sql.py -f rpt/rpt_yy_day.sql
/usr/bin/python /services/xjl_etl/script/run_sql/run_sql.py -f rpt/rpt_all_day.sql
/usr/bin/python /services/xjl_etl/script/run_sql/run_sql.py -f rpt/rpt_all_day_compare.sql

/usr/bin/python /services/xjl_etl/script/run_sql/run_sql.py -f rpt/rpt_yy_month.sql -m 2020-01-01

