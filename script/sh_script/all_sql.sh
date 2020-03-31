#!/bin/bash
if [ ! -n "$1" ] ;then
    now=$(date +%Y-%m-%d)
    this_month=`date -d "$now 1 days ago" +%Y-%m-01`
else
    this_month=$(date +%Y-%m-01  --date=$1)
fi

r30_month=`date -d "$this_month 1 months ago" +%Y-%m-01`
r60_month=`date -d "$this_month 2 months ago" +%Y-%m-01`
r90_month=`date -d "$this_month 3 months ago" +%Y-%m-01`
r120_month=`date -d "$this_month 4 months ago" +%Y-%m-01`
cd /services/xjl_etl


/usr/bin/python script/py_script/run_sql.py -f rpt/rpt_all_month.sql -m $this_month
/usr/bin/python script/py_script/run_sql.py -f rpt/rpt_all_month_compare.sql -m $this_month

# /usr/bin/python /services/xjl_etl/script/py_script/run_sql.py -f rpt/rpt_yy_day.sql
/usr/bin/python script/py_script/run_sql.py -f rpt/rpt_all_day.sql -m $this_month
/usr/bin/python script/py_script/run_sql.py -f rpt/rpt_all_day_compare.sql -m $this_month

# /usr/bin/python script/py_script/monitored.py -m $this_month
#/usr/bin/python /services/xjl_etl/script/py_script/monitored_test.py
