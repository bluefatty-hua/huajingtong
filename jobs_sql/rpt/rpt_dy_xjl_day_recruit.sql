USE bireport;
delete from rpt_day_dy_xjl_recruit;
insert into `rpt_day_dy_xjl_recruit`
SELECT MIN(dt) AS dt,xjl_week,recruit_team,
CONCAT('累计主播数:',MAX(anchor_cnt),'<br>日均开播数:',ROUND(AVG(live_cnt),1),'<br>总新开播:',SUM(new_live_cnt),'<br>总短视频:',SUM(aweme_cnt),'<br>总流水:',ROUND(SUM(revenue)/10,0))
AS total,
MAX(IF(xjl_day_of_week=0,
CONCAT(dt,'<br>','累计主播数:',anchor_cnt,'<br>开播数:',live_cnt,'<br>新开播:',new_live_cnt,'<br>短视频:',aweme_cnt,'<br>流水:',ROUND(revenue/10,0)),
'')) AS day1,
MAX(IF(xjl_day_of_week=1,
CONCAT(dt,'<br>','累计主播数:',anchor_cnt,'<br>开播数:',live_cnt,'<br>新开播:',new_live_cnt,'<br>短视频:',aweme_cnt,'<br>流水:',ROUND(revenue/10,0)),
'')) AS day2,
MAX(IF(xjl_day_of_week=2,
CONCAT(dt,'<br>','累计主播数:',anchor_cnt,'<br>开播数:',live_cnt,'<br>新开播:',new_live_cnt,'<br>短视频:',aweme_cnt,'<br>流水:',ROUND(revenue/10,0)),
'')) AS day3,
MAX(IF(xjl_day_of_week=3,
CONCAT(dt,'<br>','累计主播数:',anchor_cnt,'<br>开播数:',live_cnt,'<br>新开播:',new_live_cnt,'<br>短视频:',aweme_cnt,'<br>流水:',ROUND(revenue/10,0)),
'')) AS day4,
MAX(IF(xjl_day_of_week=4,
CONCAT(dt,'<br>','累计主播数:',anchor_cnt,'<br>开播数:',live_cnt,'<br>新开播:',new_live_cnt,'<br>短视频:',aweme_cnt,'<br>流水:',ROUND(revenue/10,0)),
'')) AS day5,
MAX(IF(xjl_day_of_week=5,
CONCAT(dt,'<br>','累计主播数:',anchor_cnt,'<br>开播数:',live_cnt,'<br>新开播:',new_live_cnt,'<br>短视频:',aweme_cnt,'<br>流水:',ROUND(revenue/10,0)),
'')) AS day6,
MAX(IF(xjl_day_of_week=6,
CONCAT(dt,'<br>','累计主播数:',anchor_cnt,'<br>开播数:',live_cnt,'<br>新开播:',new_live_cnt,'<br>短视频:',aweme_cnt,'<br>流水:',ROUND(revenue/10,0)),
'')) AS day7
FROM 
(SELECT 
dt,
WEEK(dt - INTERVAL 5 DAY,0) AS xjl_week,
WEEKDAY(dt - INTERVAL 4 DAY) AS xjl_day_of_week,
IFNULL(recruit_team,'未知') AS recruit_team,
COUNT(anchor_uid) AS anchor_cnt,
SUM(IF(duration>3600,1,0)) AS live_cnt,
SUM(IF(first_live_date = dt,1,0)) AS new_live_cnt,
SUM(IFNULL(aweme_cnt,0)) AS aweme_cnt,
SUM(IFNULL(revenue,0)) AS revenue
FROM 
warehouse.`dw_dy_xjl_day_anchor_live_recruit`
GROUP BY recruit_team,dt) t
GROUP BY xjl_week,recruit_team;


delete from rpt_day_dy_xjl_recruit_compare;

insert into rpt_day_dy_xjl_recruit_compare
SELECT dt,
'新开播' AS idx,
SUM(IF(recruit_team='招募一组',new_live_cnt,0)) AS team1,
SUM(IF(recruit_team='招募二组',new_live_cnt,0)) AS team2,
SUM(IF(recruit_team='招募三组',new_live_cnt,0)) AS team3,
SUM(IF(recruit_team='招募四组',new_live_cnt,0)) AS team4,
SUM(IF(recruit_team='招募五组',new_live_cnt,0)) AS team5,
SUM(IF(recruit_team='招募六组',new_live_cnt,0)) AS team6,
SUM(IF(recruit_team='招募七组',new_live_cnt,0)) AS team7,
SUM(IF(recruit_team='未知',new_live_cnt,0)) AS unknow
FROM 
(SELECT 
dt,
IFNULL(recruit_team,'未知') AS recruit_team,
COUNT(anchor_uid) AS anchor_cnt,
SUM(IF(duration>3600,1,0)) AS live_cnt,
SUM(IF(first_live_date = dt,1,0)) AS new_live_cnt,
SUM(IFNULL(aweme_cnt,0)) AS aweme_cnt,
SUM(IFNULL(revenue,0)) AS revenue
FROM 
warehouse.`dw_dy_xjl_day_anchor_live_recruit`
GROUP BY recruit_team,dt) t
GROUP BY dt;

insert into rpt_day_dy_xjl_recruit_compare
SELECT dt,
'流水' AS idx,
SUM(IF(recruit_team='招募一组',round(revenue,0),0)) AS team1,
SUM(IF(recruit_team='招募二组',round(revenue,0),0)) AS team2,
SUM(IF(recruit_team='招募三组',round(revenue,0),0)) AS team3,
SUM(IF(recruit_team='招募四组',round(revenue,0),0)) AS team4,
SUM(IF(recruit_team='招募五组',round(revenue,0),0)) AS team5,
SUM(IF(recruit_team='招募六组',round(revenue,0),0)) AS team6,
SUM(IF(recruit_team='招募七组',round(revenue,0),0)) AS team7,
SUM(IF(recruit_team='未知',round(revenue,0),0)) AS unknow
FROM 
(SELECT 
dt,
IFNULL(recruit_team,'未知') AS recruit_team,
COUNT(anchor_uid) AS anchor_cnt,
SUM(IF(duration>3600,1,0)) AS live_cnt,
SUM(IF(first_live_date = dt,1,0)) AS new_live_cnt,
SUM(IFNULL(aweme_cnt,0)) AS aweme_cnt,
SUM(IFNULL(revenue,0)) AS revenue
FROM 
warehouse.`dw_dy_xjl_day_anchor_live_recruit`
GROUP BY recruit_team,dt) t
GROUP BY dt;

insert into rpt_day_dy_xjl_recruit_compare
SELECT dt,
'短视频数' AS idx,
SUM(IF(recruit_team='招募一组',aweme_cnt,0)) AS team1,
SUM(IF(recruit_team='招募二组',aweme_cnt,0)) AS team2,
SUM(IF(recruit_team='招募三组',aweme_cnt,0)) AS team3,
SUM(IF(recruit_team='招募四组',aweme_cnt,0)) AS team4,
SUM(IF(recruit_team='招募五组',aweme_cnt,0)) AS team5,
SUM(IF(recruit_team='招募六组',aweme_cnt,0)) AS team6,
SUM(IF(recruit_team='招募七组',aweme_cnt,0)) AS team7,
SUM(IF(recruit_team='未知',aweme_cnt,0)) AS unknow
FROM 
(SELECT 
dt,
IFNULL(recruit_team,'未知') AS recruit_team,
COUNT(anchor_uid) AS anchor_cnt,
SUM(IF(duration>3600,1,0)) AS live_cnt,
SUM(IF(first_live_date = dt,1,0)) AS new_live_cnt,
SUM(IFNULL(aweme_cnt,0)) AS aweme_cnt,
SUM(IFNULL(revenue,0)) AS revenue
FROM 
warehouse.`dw_dy_xjl_day_anchor_live_recruit`
GROUP BY recruit_team,dt) t
GROUP BY dt;