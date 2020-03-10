-- -- 1、B站
-- REPLACE INTO bireport.rpt_month_all_guild
-- SELECT dt,
--        platform_id,
--        platform,
--        channel_num,
--        anchor_cnt         AS anchor_cnt,
--        live_cnt           AS live_cnt,
--        revenue            AS revenue,
--        revenue_orig       AS revenue_orig,
--        guild_income       AS guild_income,
--        guild_income_orig  AS guild_income_orig,
--        anchor_income      AS anchor_income,
--        anchor_income_orig AS anchor_income_orig
-- FROM (SELECT dt,
--              platform_id,
--              platform,
--              backend_account_id AS channel_num,
--              anchor_cnt,
--              live_cnt,
--              revenue,
--              revenue_orig,
--              guild_income,
--              guild_income_orig,
--              anchor_income,
--              anchor_income_orig
--       FROM bireport.rpt_month_bb_guild) t
-- WHERE dt = '{month}'
-- ;
--
--
-- -- 4、虎牙
-- REPLACE INTO bireport.rpt_month_all_guild
-- SELECT dt,
--        platform_id,
--        platform,
--        channel_num,
--        CASE WHEN anchor_cnt >= 0 THEN anchor_cnt ELSE 0 END                 AS anchor_cnt,
--        CASE WHEN live_cnt >= 0 THEN live_cnt ELSE 0 END                     AS live_cnt,
--        CASE WHEN revenue >= 0 THEN revenue ELSE 0 END                       AS revenue,
--        CASE WHEN revenue_orig >= 0 THEN revenue_orig ELSE 0 END             AS revenue_orig,
--        CASE WHEN guild_income >= 0 THEN guild_income ELSE 0 END             AS guild_income,
--        CASE WHEN guild_income_orig >= 0 THEN guild_income_orig ELSE 0 END   AS guild_income_orig,
--        CASE WHEN anchor_income >= 0 THEN anchor_income ELSE 0 END           AS anchor_income,
--        CASE WHEN anchor_income_orig >= 0 THEN anchor_income_orig ELSE 0 END AS anchor_income_orig
-- FROM (
--          SELECT dt,
--                 platform_id,
--                 platform,
--                 channel_num,
--                 anchor_cnt,
--                 live_cnt,
--                 revenue,
--                 revenue_orig,
--                 guild_income,
--                 guild_income_orig,
--                 anchor_income,
--                 anchor_income_orig
--          FROM bireport.rpt_month_hy_guild
--      ) t
-- WHERE dt = '{month}'
-- ;
--
--
--
-- REPLACE INTO bireport.rpt_month_all
-- (dt,
--  platform,
--  anchor_cnt,
--  live_cnt,
--  revenue,
--  guild_income,
--  anchor_income)
-- SELECT t.dt                 AS dt,
--        t.platform           AS platform,
--        SUM(t.anchor_cnt)    AS anchor_cnt,
--        SUM(t.live_cnt)      AS live_cnt,
--        SUM(t.revenue)       AS revenue,
--        SUM(t.guild_income)  AS guild_income,
--        SUM(t.anchor_income) AS anchor_income
-- FROM bireport.rpt_month_all_guild t
-- WHERE dt = '{month}'
-- GROUP BY t.platform, t.dt
-- ;


-- ===========================================================================================
-- rpt_month_all_new
-- 1、B站
DELETE
FROM bireport.rpt_month_all
WHERE platform = 'bilibili'
  AND dt = '{month}';
INSERT INTO bireport.rpt_month_all
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
       platform,
       revenue_level,
       newold_state,
       active_state,
       anchor_cnt,
       live_cnt,
       duration,
       revenue,
       guild_income,
       anchor_income
FROM bireport.rpt_month_bb_guild
WHERE backend_account_id = 0
  AND dt = '{month}'
;


-- 2、抖音
DELETE
FROM bireport.rpt_month_all
WHERE platform = 'DouYin'
  AND dt = '{month}';
INSERT INTO bireport.rpt_month_all
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
       Platform,
       revenue_level,
       newold_state,
       active_state,
       anchor_cnt,
       live_cnt,
       duration,
       revenue,
       guild_income,
       anchor_income
FROM bireport.rpt_month_dy_guild
WHERE backend_account_id = 'all'
  AND dt = '{month}'
;


-- 3、繁星
DELETE
FROM bireport.rpt_month_all
WHERE platform = 'FanXing'
  AND dt = '{month}';
INSERT INTO bireport.rpt_month_all
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
       platform,
       revenue_level,
       newold_state,
       active_state,
       anchor_cnt,
       live_cnt,
       duration,
       revenue,
       guild_income,
       anchor_income
FROM bireport.rpt_month_fx_guild
WHERE backend_account_id = 'all'
  AND dt = '{month}'
;


-- 4、虎牙
DELETE
FROM bireport.rpt_month_all
WHERE platform = '虎牙'
  AND dt = '{month}';
INSERT INTO bireport.rpt_month_all
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
       platform,
       revenue_level,
       newold_state,
       active_state,
       anchor_cnt,
       live_cnt,
       duration,
       revenue,
       guild_income,
       anchor_income
FROM bireport.rpt_month_hy_guild
WHERE channel_type = 'all'
  AND dt = '{month}'
;


-- 5、NOW
DELETE
FROM bireport.rpt_month_all
WHERE platform = 'NOW'
  AND dt = '{month}';
INSERT INTO bireport.rpt_month_all
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
       platform,
       revenue_level,
       newold_state,
       active_state,
       anchor_cnt,
       live_cnt,
       duration,
       revenue,
       guild_income,
       anchor_income
FROM bireport.rpt_month_now_guild
WHERE backend_account_id = 'all'
  AND city = 'all'
  AND dt = '{month}'
;


-- 6、YY
DELETE
FROM bireport.rpt_month_all
WHERE platform = 'YY'
  AND dt = '{month}';
INSERT INTO bireport.rpt_month_all
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
       platform,
       revenue_level,
       newold_state,
       active_state,
       anchor_cnt,
       live_cnt,
       duration,
       revenue,
       guild_income,
       anchor_income
FROM bireport.rpt_month_yy_guild
WHERE channel_num = 'all'
  AND dt = '{month}'
;


-- ALL
DELETE
FROM bireport.rpt_month_all
WHERE platform != 'all'
  AND dt = '{month}';
INSERT INTO bireport.rpt_month_all
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
FROM bireport.rpt_month_all
WHERE platform != 'all'
  AND dt = '{month}'
GROUP BY dt, revenue_level, newold_state, active_state
;

-- 报表用，计算上周、上月同期数据---
DELETE
FROM bireport.rpt_month_all_view
WHERE dt = '{month}';
INSERT INTO bireport.rpt_month_all_view
SELECT t1.dt,
       t1.platform,
       t1.revenue_level,
       t1.newold_state,
       t1.active_state,
       t1.anchor_cnt,
       t3.anchor_cnt                                                   AS anchor_cnt_lastmonth,
       t1.live_cnt,
       t3.live_cnt                                                     AS live_cnt_lastmonth,
       IF(t1.anchor_cnt > 0, ROUND(t1.live_cnt / t1.anchor_cnt, 3), 0) AS live_ratio,
       IF(t3.anchor_cnt > 0, ROUND(t3.live_cnt / t3.anchor_cnt, 3), 0) AS live_ratio_lastmonth,
       ROUND(t1.duration / 3600, 1)                                    AS duration,
       ROUND(t3.duration / 3600, 1)                                    AS duration_lastmonth,
       t1.revenue,
       t3.revenue                                                      AS revenue_lastmonth,
       IF(t1.live_cnt > 0, ROUND(t1.revenue / t1.live_cnt, 0), 0)      AS revenue_per_live,
       IF(t3.live_cnt > 0, ROUND(t3.revenue / t3.live_cnt, 0), 0)      AS revenue_per_live_lastmonth
FROM bireport.rpt_month_all t1
         LEFT JOIN bireport.rpt_month_all t3
                   ON t1.dt - INTERVAL 1 MONTH = t3.dt
                       AND t1.platform = t3.platform
                       AND t1.revenue_level = t3.revenue_level
                       AND t1.newold_state = t3.newold_state
                       AND t1.active_state = t3.active_state
WHERE t1.dt = '{month}'
;
