-- 公会每月流水、公会收入、主播收入
-- DROP TABLE IF EXISTS bireport.rpt_month_yy_guild;
-- CREATE TABLE bireport.rpt_month_yy_guild AS
-- DELETE
-- FROM bireport.rpt_month_yy_guild
-- WHERE dt = '{month}';
-- INSERT INTO bireport.rpt_month_yy_guild
-- SELECT t0.dt,
--        t0.platform_id,
--        pf.platform_name                                                       AS platform,
--        t0.channel_num,
--        t0.anchor_cnt,
--        t0.anchor_live_cnt                                                     AS live_cnt,
--        -- 平台流水
--        t0.anchor_bluediamond                                                  AS anchor_bluediamond_revenue,
--        ROUND(t0.guild_commission / 1000, 2)                                   AS guild_commission_revenue,
--        ROUND((t0.anchor_bluediamond + t0.guild_commission) / 1000 * 2, 2)     AS revenue,
--        t0.anchor_bluediamond + t0.guild_commission                            AS revenue_orig,
--        -- 公会收入
--        t0.guild_income_bluediamond                                            AS guild_income_bluediamond,
--        ROUND((t0.guild_income_bluediamond + t0.guild_commission) / 1000, 2)   AS guild_income,
--        t0.guild_income_bluediamond + t0.guild_commission                      AS guild_income_orig,
--        -- 主播收入
--        ROUND((t0.anchor_bluediamond - t0.guild_income_bluediamond) / 1000, 2) AS anchor_income,
--        t0.anchor_bluediamond - t0.guild_income_bluediamond                    AS anchor_income_orig
-- FROM (SELECT dt,
--              platform_id,
--              backend_account_id,
--              channel_num,
--              SUM(anchor_cnt)               AS anchor_cnt,
--              SUM(anchor_live_cnt)          AS anchor_live_cnt,
--              SUM(anchor_bluediamond)       AS anchor_bluediamond,
--              SUM(guild_income_bluediamond) AS guild_income_bluediamond,
--              SUM(guild_commission)         AS guild_commission
--       FROM warehouse.dw_yy_month_guild_live
--       WHERE comment = 'orig'
--         AND dt = '{month}'
--       GROUP BY dt,
--                platform_id,
--                backend_account_id,
--                channel_num
--      ) t0
--          lEFT JOIN warehouse.platform pf ON pf.id = t0.platform_id
-- ;
-- 
-- 
-- DELETE
-- FROM bireport.rpt_month_all_guild
-- WHERE platform_id = 1000
--   AND dt = '{month}';
-- INSERT INTO bireport.rpt_month_all_guild
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
-- FROM (SELECT dt,
--              platform_id,
--              platform,
--              channel_num,
--              anchor_cnt,
--              live_cnt,
--              revenue,
--              revenue_orig,
--              guild_income,
--              guild_income_orig,
--              anchor_income,
--              anchor_income_orig
--       FROM bireport.rpt_month_yy_guild) t
-- WHERE dt = '{month}'
-- ;


-- rpt_month_yy_guild_new
DELETE
FROM bireport.rpt_month_yy_guild
WHERE dt = '{month}';
INSERT INTO bireport.rpt_month_yy_guild
SELECT gl.dt,
       gl.platform_id,
       pf.platform_name                                             AS platform_name,
       gl.channel_num,
       gl.revenue_level,
       gl.newold_state,
       gl.active_state,
       gl.anchor_cnt,
       gl.anchor_live_cnt                                           AS live_cnt,
       gl.duration,
       -- 平台流水
       gl.anchor_bluediamond                                        AS anchor_bluediamond_revenue,
       gl.guild_commission / 1000                                   AS guild_commssion_revenue,
       (gl.anchor_bluediamond + gl.guild_commission) * 2 / 1000     AS revenue,
       gl.anchor_bluediamond + gl.guild_commission                  AS revenue_orig,
       -- 公会收入
       gl.guild_income_bluediamond,
       (gl.guild_income_bluediamond + gl.guild_commission) / 1000   AS guild_income,
       gl.guild_income_bluediamond + gl.guild_commission            AS guild_income_orig,
       -- 主播收入
       (gl.anchor_income_bluediamond + gl.anchor_commission) / 1000 AS anchor_income,
       gl.anchor_income_bluediamond + gl.anchor_commission          AS anchor_income_orig
FROM warehouse.dw_yy_month_guild_live gl
         LEFT JOIN warehouse.platform pf ON gl.platform_id = pf.id
WHERE comment = 'orig'
  AND gl.dt = '{month}'
;


REPLACE INTO bireport.rpt_month_yy_guild (dt, platform_id, platform, channel_num, revenue_level, newold_state,
                                          active_state,
                                          anchor_cnt, live_cnt, duration, anchor_bluediamond_revenue,
                                          guild_commission_revenue, revenue, revenue_orig, guild_income_bluediamond,
                                          guild_income, guild_income_orig, anchor_income, anchor_income_orig)
SELECT *
FROM (SELECT dt,
             MAX(platform_id)                AS platform_id,
             MAX(platform)                      platform,
             IFNULL(channel_num, 'all')      AS channel_num,
             IFNULL(revenue_level, 'all')    AS revenue_level,
             IFNULL(newold_state, 'all')     AS newold_state,
             IFNULL(active_state, 'all')     AS active_state,
             SUM(anchor_cnt)                 AS anchor_cnt,
             SUM(live_cnt)                   AS live_cnt,
             SUM(duration)                   AS duration,
             SUM(anchor_bluediamond_revenue) AS anchor_bluediamond_revenue,
             SUM(guild_commission_revenue)   AS guild_commission_revenue,
             SUM(revenue)                    AS revenue,
             SUM(revenue_orig)               AS revenue_orig,
             SUM(guild_income_bluediamond)   AS guild_income_bluediamond,
             SUM(guild_income)               AS guild_income,
             SUM(guild_income_orig)          AS guild_income_orig,
             SUM(anchor_income)              AS anchor_income,
             SUM(anchor_income_orig)         AS anchor_income_orig
      FROM bireport.rpt_month_yy_guild
      WHERE channel_num != 'all'
        AND revenue_level != 'all'
        AND newold_state != 'all'
        AND active_state != 'all'
        AND dt = '{month}'
      GROUP BY dt, channel_num, revenue_level, newold_state, active_state
      WITH ROLLUP

      UNION

      SELECT dt,
             MAX(platform_id)                AS platform_id,
             MAX(platform)                      platform,
             IFNULL(channel_num, 'all')      AS channel_num,
             IFNULL(revenue_level, 'all')    AS revenue_level,
             IFNULL(newold_state, 'all')     AS newold_state,
             IFNULL(active_state, 'all')     AS active_state,
             SUM(anchor_cnt)                 AS anchor_cnt,
             SUM(live_cnt)                   AS live_cnt,
             SUM(duration)                   AS duration,
             SUM(anchor_bluediamond_revenue) AS anchor_bluediamond_revenue,
             SUM(guild_commission_revenue)   AS guild_commission_revenue,
             SUM(revenue)                    AS revenue,
             SUM(revenue_orig)               AS revenue_orig,
             SUM(guild_income_bluediamond)   AS guild_income_bluediamond,
             SUM(guild_income)               AS guild_income,
             SUM(guild_income_orig)          AS guild_income_orig,
             SUM(anchor_income)              AS anchor_income,
             SUM(anchor_income_orig)         AS anchor_income_orig
      FROM bireport.rpt_month_yy_guild
      WHERE channel_num != 'all'
        AND revenue_level != 'all'
        AND newold_state != 'all'
        AND active_state != 'all'
        AND dt = '{month}'
      GROUP BY dt, revenue_level, newold_state, active_state, channel_num
      WITH ROLLUP

      UNION

      SELECT dt,
             MAX(platform_id)                AS platform_id,
             MAX(platform)                      platform,
             IFNULL(channel_num, 'all')      AS channel_num,
             IFNULL(revenue_level, 'all')    AS revenue_level,
             IFNULL(newold_state, 'all')     AS newold_state,
             IFNULL(active_state, 'all')     AS active_state,
             SUM(anchor_cnt)                 AS anchor_cnt,
             SUM(live_cnt)                   AS live_cnt,
             SUM(duration)                   AS duration,
             SUM(anchor_bluediamond_revenue) AS anchor_bluediamond_revenue,
             SUM(guild_commission_revenue)   AS guild_commission_revenue,
             SUM(revenue)                    AS revenue,
             SUM(revenue_orig)               AS revenue_orig,
             SUM(guild_income_bluediamond)   AS guild_income_bluediamond,
             SUM(guild_income)               AS guild_income,
             SUM(guild_income_orig)          AS guild_income_orig,
             SUM(anchor_income)              AS anchor_income,
             SUM(anchor_income_orig)         AS anchor_income_orig
      FROM bireport.rpt_month_yy_guild
      WHERE channel_num != 'all'
        AND revenue_level != 'all'
        AND newold_state != 'all'
        AND active_state != 'all'
        AND dt = '{month}'
      GROUP BY dt, newold_state, active_state, channel_num, revenue_level
      WITH ROLLUP

      UNION

      SELECT dt,
             MAX(platform_id)                AS platform_id,
             MAX(platform)                      platform,
             IFNULL(channel_num, 'all')      AS channel_num,
             IFNULL(revenue_level, 'all')    AS revenue_level,
             IFNULL(newold_state, 'all')     AS newold_state,
             IFNULL(active_state, 'all')     AS active_state,
             SUM(anchor_cnt)                 AS anchor_cnt,
             SUM(live_cnt)                   AS live_cnt,
             SUM(duration)                   AS duration,
             SUM(anchor_bluediamond_revenue) AS anchor_bluediamond_revenue,
             SUM(guild_commission_revenue)   AS guild_commission_revenue,
             SUM(revenue)                    AS revenue,
             SUM(revenue_orig)               AS revenue_orig,
             SUM(guild_income_bluediamond)   AS guild_income_bluediamond,
             SUM(guild_income)               AS guild_income,
             SUM(guild_income_orig)          AS guild_income_orig,
             SUM(anchor_income)              AS anchor_income,
             SUM(anchor_income_orig)         AS anchor_income_orig
      FROM bireport.rpt_month_yy_guild
      WHERE channel_num != 'all'
        AND revenue_level != 'all'
        AND newold_state != 'all'
        AND active_state != 'all'
        AND dt = '{month}'
      GROUP BY dt, active_state, channel_num, revenue_level, newold_state
      WITH ROLLUP

      UNION

      SELECT dt,
             MAX(platform_id)                AS platform_id,
             MAX(platform)                      platform,
             IFNULL(channel_num, 'all')      AS channel_num,
             IFNULL(revenue_level, 'all')    AS revenue_level,
             IFNULL(newold_state, 'all')     AS newold_state,
             IFNULL(active_state, 'all')     AS active_state,
             SUM(anchor_cnt)                 AS anchor_cnt,
             SUM(live_cnt)                   AS live_cnt,
             SUM(duration)                   AS duration,
             SUM(anchor_bluediamond_revenue) AS anchor_bluediamond_revenue,
             SUM(guild_commission_revenue)   AS guild_commission_revenue,
             SUM(revenue)                    AS revenue,
             SUM(revenue_orig)               AS revenue_orig,
             SUM(guild_income_bluediamond)   AS guild_income_bluediamond,
             SUM(guild_income)               AS guild_income,
             SUM(guild_income_orig)          AS guild_income_orig,
             SUM(anchor_income)              AS anchor_income,
             SUM(anchor_income_orig)         AS anchor_income_orig
      FROM bireport.rpt_month_yy_guild
      WHERE channel_num != 'all'
        AND revenue_level != 'all'
        AND newold_state != 'all'
        AND active_state != 'all'
        AND dt = '{month}'
      GROUP BY dt, active_state, revenue_level, channel_num, newold_state
      WITH ROLLUP
     ) t
WHERE dt IS NOT NULL
;


-- 报表用，计算上周、上月同期数据---
DELETE
FROM bireport.rpt_month_yy_guild_view
WHERE dt = '{month}';
INSERT INTO bireport.rpt_month_yy_guild_view
SELECT t1.dt,
       t1.channel_num,
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
       IF(t3.live_cnt > 0, ROUND(t3.revenue / t3.live_cnt, 0), 0)      AS revenue_per_live_lastmonth,
       0                                                               AS guild_income,
       0                                                               AS anchor_income
FROM bireport.rpt_month_yy_guild t1
         LEFT JOIN bireport.rpt_month_yy_guild t3
                   ON t1.dt - INTERVAL 1 MONTH = t3.dt
                       AND t1.channel_num = t3.channel_num
                       AND t1.revenue_level = t3.revenue_level
                       AND t1.newold_state = t3.newold_state
                       AND t1.active_state = t3.active_state
WHERE t1.dt = '{month}';


-- 报表用，计算指标占比---
DELETE
FROM bireport.rpt_month_yy_guild_view_compare
WHERE dt = '{month}';
INSERT INTO bireport.rpt_month_yy_guild_view_compare
SELECT *
FROM (SELECT dt,
             channel_num,
             revenue_level,
             newold_state,
             active_state,
             '主播数'      AS idx,
             anchor_cnt AS val
      FROM bireport.rpt_month_yy_guild
      WHERE revenue_level != 'all'
        AND dt = '{month}'
      UNION
      SELECT dt,
             channel_num,
             revenue_level,
             newold_state,
             active_state,
             '开播数'    AS idx,
             live_cnt AS val
      FROM bireport.rpt_month_yy_guild
      WHERE revenue_level != 'all'
        AND dt = '{month}'
      UNION
      SELECT dt,
             channel_num,
             revenue_level,
             newold_state,
             active_state,
             '流水'    AS idx,
             revenue AS val
      FROM bireport.rpt_month_yy_guild
      WHERE revenue_level != 'all'
        AND dt = '{month}'
      UNION
      SELECT dt,
             channel_num,
             revenue_level,
             newold_state,
             active_state,
             '开播人均流水'                     AS idx,
             round(revenue / live_cnt, 0) AS val
      FROM bireport.rpt_month_yy_guild
      WHERE revenue_level != 'all'
        AND dt = '{month}'
        AND live_cnt > 0) t
;

