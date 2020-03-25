#!/bin/bash
if [ ! -n "$1" ] ;then
    now=$(date +%Y-%m-01)
    this_month=`date -d "$now 1 days ago" +%Y-%m-01`
else
    this_month=$(date +%Y-%m-01  --date=$1)
fi

r30_month=`date -d "$this_month 1 months ago" +%Y-%m-01`
r60_month=`date -d "$this_month 2 months ago" +%Y-%m-01`
r90_month=`date -d "$this_month 3 months ago" +%Y-%m-01`
r120_month=`date -d "$this_month 4 months ago" +%Y-%m-01`
cd /services/xjl_etl
/usr/bin/python script/py_script/run_sql.py -f ods/ods_yy.sql 

# /usr/bin/python /services/xjl_etl/script/py_script/run_sql.py -f dw/delete_dw_yy_anchor_tags.sql

/usr/bin/python script/py_script/run_sql.py -f dw/yy/dw_yy_day.sql -m $this_month
/usr/bin/python script/py_script/run_sql.py -f dw/yy/dw_yy_day_new.sql -m $this_month
/usr/bin/python script/py_script/run_sql.py -f dw/yy/dw_yy_day_retention_r30.sql -m $r30_month
/usr/bin/python script/py_script/run_sql.py -f dw/yy/dw_yy_day_retention_r60.sql -m $r60_month
/usr/bin/python script/py_script/run_sql.py -f dw/yy/dw_yy_day_retention_r90.sql -m $r90_month
/usr/bin/python script/py_script/run_sql.py -f dw/yy/dw_yy_day_retention_r120.sql -m $r120_month
/usr/bin/python script/py_script/run_sql.py -f dw/yy/dw_yy_day_guild.sql -m $this_month

/usr/bin/python script/py_script/run_sql.py -f dw/yy/dw_yy_month.sql -m $this_month
/usr/bin/python script/py_script/run_sql.py -f rpt/yy/rpt_yy_day.sql -m $this_month
/usr/bin/python script/py_script/run_sql.py -f rpt/yy/rpt_yy_month.sql -m $this_month

