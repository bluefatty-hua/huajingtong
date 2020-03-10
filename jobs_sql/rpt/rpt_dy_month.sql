-- 公会每月流水、公会收入、主播收入
-- DROP TABLE IF EXISTS bireport.rpt_month_dy_guild;
-- CREATE TABLE bireport.rpt_month_dy_guild AS
-- DELETE
-- FROM bireport.rpt_month_dy_guild
-- WHERE dt = '{month}';
-- INSERT INTO bireport.rpt_month_dy_guild
-- SELECT DATE_FORMAT(al.dt, '%Y-%m-01'),
--        al.platform_id,
--        al.platform_name                      AS                 platform,
--        al.backend_account_id,
--        COUNT(DISTINCT al.anchor_uid)         AS                 anchor_cnt,
--        COUNT(DISTINCT IF(al.live_status = 1, anchor_uid, NULL)) ASlive_cnt,
--        SUM(IFNULL(al.revenue, 0)) / 10       AS                 revenue,
--        SUM(IFNULL(al.revenue, 0))            AS                 revenue_orig,
--        SUM(IFNULL(al.guild_income, 0)) / 10  AS                 guild_income,
--        SUM(IFNULL(al.guild_income, 0))       AS                 guild_income_orig,
--        SUM(IFNULL(al.anchor_income, 0)) / 10 AS                 anchor_income,
--        SUM(IFNULL(al.anchor_income, 0))      AS                 anchor_income_orig
-- FROM warehouse.dw_dy_day_anchor_live al
-- WHERE dt >= '{month}'
--   AND dt < '{month}' + INTERVAL 1 MONTH
-- GROUP BY DATE_FORMAT(al.dt, '%Y-%m-01'),
--          al.platform_id,
--          al.platform_name,
--          al.backend_account_id
-- ;
-- 
-- 
-- 
-- DELETE
-- FROM bireport.rpt_month_all_guild
-- WHERE platform_id = 1005
--   AND dt = '{month}';
-- INSERT INTO bireport.rpt_month_all_guild
-- SELECT dt,
--        platform_id,
--        platform,
--        backend_account_id,
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
--              backend_account_id AS backend_account_id,
--              anchor_cnt,
--              live_cnt,
--              revenue,
--              revenue_orig,
--              guild_income,
--              guild_income_orig,
--              anchor_income,
--              anchor_income_orig
--       FROM bireport.rpt_month_dy_guild) t
-- WHERE dt >= '{month}'
--   AND dt < '{month}' + INTERVAL 1 MONTH
-- ;


-- rpt_month_dy_guild_new
DELETE
FROM bireport.rpt_month_dy_guild
WHERE dt = '{month}';
INSERT INTO bireport.rpt_month_dy_guild
SELECT gl.dt,
       gl.platform_id,
       gl.platform_name      AS platform,
       gl.backend_account_id,
       gl.revenue_level,
       gl.newold_state,
       gl.active_state,
       gl.anchor_cnt,
       gl.anchor_live_cnt    AS live_cnt,
       gl.duration,
       gl.revenue / 10       AS revenue,
       gl.revenue            AS revenue_orig,
       gl.guild_income / 10  AS guild_income,
       gl.revenue            AS guild_income_orig,
       gl.anchor_income / 10 AS anchor_income,
       gl.revenue            AS anchor_income_orig
FROM warehouse.dw_dy_month_guild_live gl
WHERE dt >= '{month}'
  AND dt < '{month}' + INTERVAL 1 MONTH
;


REPLACE INTO bireport.rpt_month_dy_guild (dt, platform_id, platform, backend_account_id, revenue_level,
                                          newold_state, active_state, anchor_cnt, live_cnt, duration, revenue,
                                          revenue_orig, guild_income, guild_income_orig, anchor_income,
                                          anchor_income_orig)
SELECT *
FROM (SELECT dt,
             MAX(platform_id)                  AS platform_id,
             MAX(platform)                        platform,
             IFNULL(backend_account_id, 'all') AS backend_account_id,
             IFNULL(revenue_level, 'all')      AS revenue_level,
             IFNULL(newold_state, 'all')       AS newold_state,
             IFNULL(active_state, 'all')       AS active_state,
             SUM(anchor_cnt)                   AS anchor_cnt,
             SUM(live_cnt)                     AS live_cnt,
             SUM(duration)                     AS duration,
             SUM(revenue)                      AS revenue,
             SUM(revenue_orig)                 AS revenue_orig,
             SUM(guild_income)                 AS guild_income,
             SUM(guild_income_orig)            AS guild_income_orig,
             SUM(anchor_income)                AS anchor_income,
             SUM(anchor_income_orig)           AS anchor_income_orig
      FROM bireport.rpt_month_dy_guild
      WHERE backend_account_id != 'all' AND revenue_level != 'all' AND newold_state != 'all' AND active_state != 'all'
        AND dt = '{month}'
      GROUP BY dt, backend_account_id, revenue_level, newold_state, active_state
      WITH ROLLUP

      UNION

      SELECT dt,
             MAX(platform_id)                  AS platform_id,
             MAX(platform)                        platform,
             IFNULL(backend_account_id, 'all') AS backend_account_id,
             IFNULL(revenue_level, 'all')      AS revenue_level,
             IFNULL(newold_state, 'all')       AS newold_state,
             IFNULL(active_state, 'all')       AS active_state,
             SUM(anchor_cnt)                   AS anchor_cnt,
             SUM(live_cnt)                     AS live_cnt,
             SUM(duration)                     AS duration,
             SUM(revenue)                      AS revenue,
             SUM(revenue_orig)                 AS revenue_orig,
             SUM(guild_income)                 AS guild_income,
             SUM(guild_income_orig)            AS guild_income_orig,
             SUM(anchor_income)                AS anchor_income,
             SUM(anchor_income_orig)           AS anchor_income_orig
      FROM bireport.rpt_month_dy_guild
      WHERE backend_account_id != 'all' AND revenue_level != 'all' AND newold_state != 'all' AND active_state != 'all'
        AND dt = '{month}'
      GROUP BY dt, revenue_level, newold_state, active_state, backend_account_id
      WITH ROLLUP

      UNION

      SELECT dt,
             MAX(platform_id)                  AS platform_id,
             MAX(platform)                        platform,
             IFNULL(backend_account_id, 'all') AS backend_account_id,
             IFNULL(revenue_level, 'all')      AS revenue_level,
             IFNULL(newold_state, 'all')       AS newold_state,
             IFNULL(active_state, 'all')       AS active_state,
             SUM(anchor_cnt)                   AS anchor_cnt,
             SUM(live_cnt)                     AS live_cnt,
             SUM(duration)                     AS duration,
             SUM(revenue)                      AS revenue,
             SUM(revenue_orig)                 AS revenue_orig,
             SUM(guild_income)                 AS guild_income,
             SUM(guild_income_orig)            AS guild_income_orig,
             SUM(anchor_income)                AS anchor_income,
             SUM(anchor_income_orig)           AS anchor_income_orig
      FROM bireport.rpt_month_dy_guild
      WHERE backend_account_id != 'all' AND revenue_level != 'all' AND newold_state != 'all' AND active_state != 'all'
        AND dt = '{month}'
      GROUP BY dt, newold_state, active_state, backend_account_id, revenue_level
      WITH ROLLUP

      UNION

      SELECT dt,
             MAX(platform_id)                  AS platform_id,
             MAX(platform)                        platform,
             IFNULL(backend_account_id, 'all') AS backend_account_id,
             IFNULL(revenue_level, 'all')      AS revenue_level,
             IFNULL(newold_state, 'all')       AS newold_state,
             IFNULL(active_state, 'all')       AS active_state,
             SUM(anchor_cnt)                   AS anchor_cnt,
             SUM(live_cnt)                     AS live_cnt,
             SUM(duration)                     AS duration,
             SUM(revenue)                      AS revenue,
             SUM(revenue_orig)                 AS revenue_orig,
             SUM(guild_income)                 AS guild_income,
             SUM(guild_income_orig)            AS guild_income_orig,
             SUM(anchor_income)                AS anchor_income,
             SUM(anchor_income_orig)           AS anchor_income_orig
      FROM bireport.rpt_month_dy_guild
      WHERE backend_account_id != 'all' AND revenue_level != 'all' AND newold_state != 'all' AND active_state != 'all'
        AND dt = '{month}'
      GROUP BY dt, active_state, backend_account_id, revenue_level, newold_state
      WITH ROLLUP

      UNION

      SELECT dt,
             MAX(platform_id)                  AS platform_id,
             MAX(platform)                        platform,
             IFNULL(backend_account_id, 'all') AS backend_account_id,
             IFNULL(revenue_level, 'all')      AS revenue_level,
             IFNULL(newold_state, 'all')       AS newold_state,
             IFNULL(active_state, 'all')       AS active_state,
             SUM(anchor_cnt)                   AS anchor_cnt,
             SUM(live_cnt)                     AS live_cnt,
             SUM(duration)                     AS duration,
             SUM(revenue)                      AS revenue,
             SUM(revenue_orig)                 AS revenue_orig,
             SUM(guild_income)                 AS guild_income,
             SUM(guild_income_orig)            AS guild_income_orig,
             SUM(anchor_income)                AS anchor_income,
             SUM(anchor_income_orig)           AS anchor_income_orig
      FROM bireport.rpt_month_dy_guild
      WHERE backend_account_id != 'all' AND revenue_level != 'all' AND newold_state != 'all' AND active_state != 'all'
        AND dt = '{month}'
      GROUP BY dt, backend_account_id, newold_state, revenue_level, active_state
      WITH ROLLUP
     ) t
WHERE dt IS NOT NULL
;


-- 报表用，计算上周、上月同期数据---
DELETE
FROM bireport.rpt_month_dy_guild_view
WHERE dt = '{month}';
INSERT INTO bireport.rpt_month_dy_guild_view
SELECT t1.dt,
       t1.backend_account_id,
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
FROM bireport.rpt_month_dy_guild t1
         LEFT JOIN bireport.rpt_month_dy_guild t3
                   ON t1.dt - INTERVAL 1 MONTH = t3.dt
                       AND t1.backend_account_id = t3.backend_account_id
                       AND t1.revenue_level = t3.revenue_level
                       AND t1.newold_state = t3.newold_state
                       AND t1.active_state = t3.active_state
WHERE t1.dt = '{month}'
;


-- 报表用，计算指标占比---
DELETE
FROM bireport.rpt_month_dy_guild_view_compare
WHERE dt = '{month}';
INSERT INTO bireport.rpt_month_dy_guild_view_compare
SELECT *
FROM (SELECT dt,
             backend_account_id,
             revenue_level,
             newold_state,
             active_state,
             '主播数'      AS idx,
             anchor_cnt AS val
      FROM bireport.rpt_month_dy_guild
      WHERE revenue_level != 'all'
        AND dt = '{month}'
      UNION
      SELECT dt,
             backend_account_id,
             revenue_level,
             newold_state,
             active_state,
             '开播数'    AS idx,
             live_cnt AS val
      FROM bireport.rpt_month_dy_guild
      WHERE revenue_level != 'all'
        AND dt = '{month}'
      UNION
      SELECT dt,
             backend_account_id,
             revenue_level,
             newold_state,
             active_state,
             '流水'    AS idx,
             revenue AS val
      FROM bireport.rpt_month_dy_guild
      WHERE revenue_level != 'all'
        AND dt = '{month}'
      UNION
      SELECT dt,
             backend_account_id,
             revenue_level,
             newold_state,
             active_state,
             '开播人均流水'                     AS idx,
             round(revenue / live_cnt, 0) AS val
      FROM bireport.rpt_month_dy_guild
      WHERE revenue_level != 'all'
        AND dt = '{month}'
        AND live_cnt > 0) t
;


