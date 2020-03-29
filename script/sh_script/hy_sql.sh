#!/bin/bash
if [ ! -n "$1" ] ;then
    hy=$(date +%Y-%m-%d)
    this_month=`date -d "$hy 1 days ago" +%Y-%m-01`
else
    this_month=$(date +%Y-%m-01  --date=$1)
fi

r30_month=`date -d "$this_month 1 months ago" +%Y-%m-01`
r60_month=`date -d "$this_month 2 months ago" +%Y-%m-01`
r90_month=`date -d "$this_month 3 months ago" +%Y-%m-01`
r120_month=`date -d "$this_month 4 months ago" +%Y-%m-01`
cd /services/xjl_etl



/usr/bin/python script/py_script/run_sql.py -f ods/ods_hy.sql 

/usr/bin/python script/py_script/run_sql.py -f dw/hy/dw_hy_day.sql -m $this_month
/usr/bin/python script/py_script/run_sql.py -f dw/hy/dw_hy_day_new.sql -m $this_month
/usr/bin/python script/py_script/run_sql.py -f dw/hy/dw_hy_day_guild.sql -m $this_month


/usr/bin/python script/py_script/run_sql.py -f dw/hy/dw_hy_month.sql -m $this_month
/usr/bin/python script/py_script/run_sql.py -f dw/hy/dw_hy_month_new.sql -m $this_month
/usr/bin/python script/py_script/run_sql.py -f dw/hy/dw_hy_month_guild.sql -m $this_month

/usr/bin/python script/py_script/run_sql.py -f rpt/hy/rpt_hy_day.sql -m $this_month
/usr/bin/python script/py_script/run_sql.py -f rpt/hy/rpt_hy_month.sql -m $this_month
/usr/bin/python script/py_script/run_sql.py -f rpt/hy/rpt_hy_anchor.sql -m $this_month


# # # 更新留存数据
/usr/bin/python script/py_script/run_sql.py -f dw/hy/dw_hy_day_retention_r30.sql -m $r30_month
/usr/bin/python script/py_script/run_sql.py -f dw/hy/dw_hy_day_retention_r60.sql -m $r60_month
/usr/bin/python script/py_script/run_sql.py -f dw/hy/dw_hy_day_retention_r90.sql -m $r90_month
/usr/bin/python script/py_script/run_sql.py -f dw/hy/dw_hy_day_retention_r120.sql -m $r120_month



# # #留存数据更新了，全部报表都要刷一遍
/usr/bin/python script/py_script/run_sql.py -f rpt/hy/rpt_hy_day.sql -m $r120_month
/usr/bin/python script/py_script/run_sql.py -f rpt/hy/rpt_hy_month.sql -m $r120_month

/usr/bin/python script/py_script/run_sql.py -f rpt/hy/rpt_hy_day.sql -m $r90_month
/usr/bin/python script/py_script/run_sql.py -f rpt/hy/rpt_hy_month.sql -m $r90_month

/usr/bin/python script/py_script/run_sql.py -f rpt/hy/rpt_hy_day.sql -m $r60_month
/usr/bin/python script/py_script/run_sql.py -f rpt/hy/rpt_hy_month.sql -m $r60_month

/usr/bin/python script/py_script/run_sql.py -f rpt/hy/rpt_hy_day.sql -m $r30_month
/usr/bin/python script/py_script/run_sql.py -f rpt/hy/rpt_hy_month.sql -m $r30_month