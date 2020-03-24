#!/bin/bash
this_month=`date -d '1 days ago' +%Y-%m-01`
r30_month=`date -d '1 months ago' +%Y-%m-01`
r60_month=`date -d '2 months ago' +%Y-%m-01`
r90_month=`date -d '3 months ago' +%Y-%m-01`
r120_month=`date -d '4 months ago' +%Y-%m-01`
cd /services/xjl_etl



/usr/bin/python script/py_script/run_sql.py -f ods/ods_bb.sql

# /usr/bin/python /services/xjl_etl/script/py_script/run_sql.py -f dw/delete_dw_bb_anchor_tags.sql
/usr/bin/python script/py_script/run_sql.py -f dw/bb/dw_bb_day.sql -m $this_month
/usr/bin/python script/py_script/run_sql.py -f dw/bb/dw_bb_day_new.sql -m $this_month
/usr/bin/python script/py_script/run_sql.py -f dw/bb/dw_bb_day_retention_r30.sql -m $r30_month
/usr/bin/python script/py_script/run_sql.py -f dw/bb/dw_bb_day_retention_r60.sql -m $r60_month
/usr/bin/python script/py_script/run_sql.py -f dw/bb/dw_bb_day_retention_r90.sql -m $r90_month
/usr/bin/python script/py_script/run_sql.py -f dw/bb/dw_bb_day_retention_r120.sql -m $r120_month
/usr/bin/python script/py_script/run_sql.py -f dw/bb/dw_bb_day_guild.sql -m $this_month


/usr/bin/python script/py_script/run_sql.py -f dw/bb/dw_bb_month.sql
/usr/bin/python script/py_script/run_sql.py -f rpt/bb/rpt_bb_day.sql
/usr/bin/python script/py_script/run_sql.py -f rpt/bb/rpt_bb_month.sql

