-- - rpt_day_all_new ----
-- 1、B站
DELETE
FROM bireport.rpt_day_all
WHERE platform = 'bilibili'
  AND dt BETWEEN '{start_date}' AND '{end_date}';
INSERT INTO bireport.rpt_day_all
(dt,
 platform,
 revenue_level,
 newold_state,
 active_state,
 anchor_cnt,
 live_cnt,
 duration,
 revenue,
 guild_income,
 anchor_income)
SELECT dt,
       'bilibili' AS platform,
       revenue_level,
       newold_state,
       active_state,
       anchor_cnt,
       live_cnt,
       duration,
       revenue,
       guild_income,
       anchor_income
FROM bireport.rpt_day_bb_guild
WHERE remark = 'all'
  AND dt BETWEEN '{start_date}' AND '{end_date}'
;


-- 2、抖音
DELETE
FROM bireport.rpt_day_all
WHERE platform = 'DouYin'
  AND dt BETWEEN '{start_date}' AND '{end_date}';
INSERT INTO bireport.rpt_day_all
(dt,
 platform,
 revenue_level,
 newold_state,
 active_state,
 anchor_cnt,
 live_cnt,
 duration,
 revenue,
 guild_income,
 anchor_income)
SELECT dt,
       'DouYin'                 as platform,
       revenue_level,
       newold_state,
       active_state,
       anchor_cnt,
       live_cnt,
       duration,
       revenue,
       IFNULL(guild_income, 0)  AS guild_income,
       IFNULL(anchor_income, 0) AS anchor_income
FROM bireport.rpt_day_dy_guild
WHERE backend_account_id = 'all'
  AND dt BETWEEN '{start_date}' AND '{end_date}'
;


-- 3、繁星
DELETE
FROM bireport.rpt_day_all
WHERE platform = 'FanXing'
  AND dt BETWEEN '{start_date}' AND '{end_date}';
INSERT INTO bireport.rpt_day_all
(dt,
 platform,
 revenue_level,
 newold_state,
 active_state,
 anchor_cnt,
 live_cnt,
 duration,
 revenue,
 guild_income,
 anchor_income)
SELECT dt,
       'FanXing' AS platform,
       revenue_level,
       newold_state,
       active_state,
       anchor_cnt,
       live_cnt,
       duration,
       revenue,
       guild_income,
       anchor_income
FROM bireport.rpt_day_fx_guild
WHERE backend_account_id = 'all'
  AND dt BETWEEN '{start_date}' AND '{end_date}'
;


-- 4、虎牙
DELETE
FROM bireport.rpt_day_all
WHERE platform = 'HUYA'
  AND dt BETWEEN '{start_date}' AND '{end_date}';
INSERT INTO bireport.rpt_day_all
(dt,
 platform,
 revenue_level,
 newold_state,
 active_state,
 anchor_cnt,
 live_cnt,
 duration,
 revenue,
 guild_income,
 anchor_income)
SELECT dt,
       'HUYA' as platform,
       revenue_level,
       newold_state,
       active_state,
       anchor_cnt,
       live_cnt,
       duration,
       revenue,
       0      AS guild_income,
       0      AS anchor_income
FROM bireport.rpt_day_hy_guild
WHERE channel_type = 'all'
  AND dt BETWEEN '{start_date}' AND '{end_date}'
;


-- 5、NOW
DELETE
FROM bireport.rpt_day_all
WHERE platform = 'NOW'
  AND dt BETWEEN '{start_date}' AND '{end_date}';
INSERT INTO bireport.rpt_day_all
(dt,
 platform,
 revenue_level,
 newold_state,
 active_state,
 anchor_cnt,
 live_cnt,
 duration,
 revenue,
 guild_income,
 anchor_income)
SELECT dt,
       'NOW' as platform,
       revenue_level,
       newold_state,
       active_state,
       anchor_cnt,
       live_cnt,
       duration,
       revenue,
       guild_income,
       anchor_income
FROM bireport.rpt_day_now_guild
WHERE backend_account_id = 'all'
  AND city = 'all'
  AND dt BETWEEN '{start_date}' AND '{end_date}'
;


-- 6、YY
DELETE
FROM bireport.rpt_day_all
WHERE platform = 'YY'
  AND dt BETWEEN '{start_date}' AND '{end_date}';
INSERT INTO bireport.rpt_day_all
(dt,
 platform,
 revenue_level,
 newold_state,
 active_state,
 anchor_cnt,
 live_cnt,
 duration,
 revenue,
 guild_income,
 anchor_income)
SELECT dt,
       'YY' AS platform,
       revenue_level,
       newold_state,
       active_state,
       anchor_cnt,
       live_cnt,
       duration,
       revenue,
       guild_income,
       anchor_income
FROM bireport.rpt_day_yy_guild
WHERE channel_num = 'all'
  AND dt BETWEEN '{start_date}' AND '{end_date}'
;


REPLACE INTO bireport.rpt_day_all (dt, platform, revenue_level, newold_state, active_state, anchor_cnt, live_cnt,
                                   duration, revenue, guild_income, anchor_income)
SELECT dt,
       'all'              AS platform,
       revenue_level,
       newold_state,
       active_state,
       SUM(anchor_cnt)    AS anchor_cnt,
       SUM(live_cnt)      AS live_cnt,
       SUM(duration)      AS duration,
       SUM(revenue)       AS revenue,
       SUM(guild_income)  AS guild_income,
       SUM(anchor_income) AS anchor_income
FROM bireport.rpt_day_all
WHERE platform != 'all'
  AND dt BETWEEN '{start_date}' AND '{end_date}'
GROUP BY dt, revenue_level, newold_state, active_state
;


-- 报表用，计算上周、上月同期数据---
DELETE
FROM bireport.rpt_day_all_view
WHERE dt BETWEEN '{start_date}' AND '{end_date}';
INSERT INTO bireport.rpt_day_all_view
SELECT t1.dt,
       t1.platform,
       t1.revenue_level,
       t1.newold_state,
       t1.active_state,
       t1.anchor_cnt,
       t2.anchor_cnt                                                   AS anchor_cnt_lastweek,
       t3.anchor_cnt                                                   AS anchor_cnt_lastmonth,
       t1.live_cnt,
       t2.live_cnt                                                     AS live_cnt_lastweek,
       t3.live_cnt                                                     AS live_cnt_lastmonth,
       IF(t1.anchor_cnt > 0, ROUND(t1.live_cnt / t1.anchor_cnt, 3), 0) AS live_ratio,
       IF(t2.anchor_cnt > 0, ROUND(t2.live_cnt / t2.anchor_cnt, 3), 0) AS live_ratio_lastweek,
       IF(t3.anchor_cnt > 0, ROUND(t3.live_cnt / t3.anchor_cnt, 3), 0) AS live_ratio_lastmonth,
       ROUND(t1.duration / 3600, 1)                                    AS duration,
       ROUND(t2.duration / 3600, 1)                                    AS duration_lastweek,
       ROUND(t3.duration / 3600, 1)                                    AS duration_lastmonth,
       t1.revenue,
       t2.revenue                                                      AS revenue_lastweek,
       t3.revenue                                                      AS revenue_lastmonth,
       IF(t1.live_cnt > 0, ROUND(t1.revenue / t1.live_cnt, 0), 0)      AS revenue_per_live,
       IF(t2.live_cnt > 0, ROUND(t2.revenue / t2.live_cnt, 0), 0)      AS revenue_per_live_lastweek,
       IF(t3.live_cnt > 0, ROUND(t3.revenue / t3.live_cnt, 0), 0)      AS revenue_per_live_lastmonth
FROM bireport.rpt_day_all t1
         LEFT JOIN bireport.rpt_day_all t2
                   ON t1.dt - INTERVAL 7 DAY = t2.dt
                       AND t1.platform = t2.platform
                       AND t1.revenue_level = t2.revenue_level
                       AND t1.newold_state = t2.newold_state
                       AND t1.active_state = t2.active_state
         LEFT JOIN bireport.rpt_day_all t3
                   ON t1.dt - INTERVAL 1 MONTH = t3.dt
                       AND t1.platform = t3.platform
                       AND t1.revenue_level = t3.revenue_level
                       AND t1.newold_state = t3.newold_state
                       AND t1.active_state = t3.active_state
WHERE t1.dt BETWEEN '{start_date}' AND '{end_date}'
;

