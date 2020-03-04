-- 公会每月流水、公会收入、主播收入
-- DROP TABLE IF EXISTS bireport.rpt_month_hy_guild;
-- CREATE TABLE bireport.rpt_month_hy_guild AS
-- REPLACE INTO stage.hy_guild_income_rate
-- SELECT channel_num, AVG(guild_income / revenue) avg_rate
-- FROM bireport.rpt_month_hy_guild
-- WHERE revenue > 0
--   AND guild_income / revenue > 0.09
-- GROUP BY channel_num
-- ;
--
--
-- REPLACE INTO bireport.rpt_month_hy_guild
-- SELECT t0.dt,
--        t0.platform_id,
--        pf.platform_name                     AS platform,
--        t0.channel_num,
--        t0.sign_count                        AS anchor_cnt,
--        t1.anchor_live_cnt                   AS live_cnt,
--        t0.revenue,
--        t0.revenue                           AS revenue_orig,
--        t0.revenue * ig.avg_rate             AS guild_income,
--        t0.revenue * ig.avg_rate             AS guild_income_orig,
--        t0.revenue * ig.avg_rate * 0.7 / 0.3 AS anchor_income,
--        t0.revenue * ig.avg_rate             AS anchor_incom_orig
-- FROM warehouse.dw_huya_month_guild_live_true t0
--          LEFT JOIN (SELECT CONCAT(DATE_FORMAT(t.dt, '%Y-%m'), '-01')                          AS dt,
--                            t.channel_id,
--                            COUNT(DISTINCT t.anchor_uid)                                       AS anchor_cnt,
--                            COUNT(DISTINCT
--                                  CASE WHEN t.live_status = 1 THEN t.anchor_uid ELSE NULL END) AS anchor_live_cnt,
--                            SUM(t.revenue)                                                     AS revenue
--                     FROM warehouse.dw_huya_day_anchor_live t
--                     WHERE dt >= '{month}'
--                       AND dt < '{month}' + INTERVAL 1 MONTH
--                     GROUP BY CONCAT(DATE_FORMAT(t.dt, '%Y-%m'), '-01'),
--                              t.channel_id) t1 ON t0.dt = t1.dt AND t0.channel_id = t1.channel_id
--          LEFT JOIN stage.hy_guild_income_rate ig ON t0.channel_num = ig.channel_num
--          lEFT JOIN warehouse.platform pf ON pf.id = t0.platform_id
-- ;
--
--
-- REPLACE INTO bireport.rpt_month_hy_guild
-- SELECT t0.dt,
--        t0.platform_id,
--        pf.platform_name                                                          AS platform,
--        t0.channel_num,
--        t0.sign_count                                                             AS anchor_cnt,
--        t1.anchor_live_cnt                                                        AS live_cnt,
--        t0.revenue,
--        t0.revenue                                                                AS revenue_orig,
--        (t0.gift_income + t0.guard_income + t0.noble_income) / 1000               AS guild_income,
--        t0.gift_income + t0.guard_income + t0.noble_income                        AS guild_income_orig,
--        (t0.gift_income + t0.guard_income + t0.noble_income) * 0.7 / (0.3 * 1000) AS anchor_income,
--        t0.gift_income + t0.guard_income + t0.noble_income                        AS anchor_incom_orig
-- FROM warehouse.dw_huya_month_guild_live_true t0
--          INNER JOIN (SELECT CONCAT(DATE_FORMAT(t.dt, '%Y-%m'), '-01')                          AS dt,
--                             t.channel_id,
--                             COUNT(DISTINCT t.anchor_uid)                                       AS anchor_cnt,
--                             COUNT(DISTINCT
--                                   CASE WHEN t.live_status = 1 THEN t.anchor_uid ELSE NULL END) AS anchor_live_cnt,
--                             SUM(t.income)                                                      AS anchor_income
--                      FROM warehouse.ods_huya_day_anchor_live t
--                      WHERE dt >= '{month}'
--                        AND dt < '{month}' + INTERVAL 1 MONTH
--                      GROUP BY CONCAT(DATE_FORMAT(t.dt, '%Y-%m'), '-01'),
--                               t.channel_id) t1 ON t0.dt = t1.dt AND t0.channel_id = t1.channel_id
--          lEFT JOIN warehouse.platform pf ON pf.id = t0.platform_id
-- ;


-- rpt_month_now_guild_new
DELETE
FROM bireport.rpt_month_hy_guild
WHERE dt = '{month}';
REPLACE INTO bireport.rpt_month_hy_guild
SELECT *
FROM warehouse.dw_huya_month_guild_live
WHERE dt = '{month}'
;


REPLACE INTO bireport.rpt_month_hy_guild
SELECT t.dt,
       t.platform_id,
       t.platform,
       t.channel_type,
       t.channel_num,
       t.revenue_level,
       t.newold_state,
       t.active_state,
       t.anchor_cnt,
       t.live_cnt,
       t.duration,
       t.revenue,
       t.revenue_orig,
       t.guild_income,
       t.guild_income_orig,
       t.anchor_income,
       t.anchor_income_orig
FROM (
         SELECT dt,
                MAX(platform_id)             AS platform_id,
                MAX(platform)                AS platform,
                IFNULL(channel_type, 'all')  AS channel_type,
                IFNULL(channel_num, 'all')   AS channel_num,
                IFNULL(revenue_level, 'all') AS revenue_level,
                IFNULL(newold_state, 'all')  AS newold_state,
                IFNULL(active_state, 'all')  AS active_state,
                SUM(anchor_cnt)              AS anchor_cnt,
                SUM(live_cnt)                AS live_cnt,
                SUM(duration)                AS duration,
                SUM(revenue)                 AS revenue,
                SUM(revenue_orig)            AS revenue_orig,
                SUM(guild_income)            AS guild_income,
                SUM(guild_income_orig)       AS guild_income_orig,
                SUM(anchor_income)           AS anchor_income,
                SUM(anchor_income_orig)      AS anchor_income_orig
         FROM bireport.rpt_month_hy_guild
         WHERE channel_type != 'all'
           AND channel_num != 'all'
           AND revenue_level != 'all'
           AND newold_state != 'all'
           AND active_state != 'all'
           AND dt = '{month}'
         GROUP BY dt, channel_type, channel_num, revenue_level, newold_state, active_state
         WITH ROLLUP

         UNION

         SELECT dt,
                MAX(platform_id)             AS platform_id,
                MAX(platform)                AS platform,
                IFNULL(channel_type, 'all')  AS channel_type,
                IFNULL(channel_num, 'all')   AS channel_num,
                IFNULL(revenue_level, 'all') AS revenue_level,
                IFNULL(newold_state, 'all')  AS newold_state,
                IFNULL(active_state, 'all')  AS active_state,
                SUM(anchor_cnt)              AS anchor_cnt,
                SUM(live_cnt)                AS live_cnt,
                SUM(duration)                AS duration,
                SUM(revenue)                 AS revenue,
                SUM(revenue_orig)            AS revenue_orig,
                SUM(guild_income)            AS guild_income,
                SUM(guild_income_orig)       AS guild_income_orig,
                SUM(anchor_income)           AS anchor_income,
                SUM(anchor_income_orig)      AS anchor_income_orig
         FROM bireport.rpt_month_hy_guild
         WHERE channel_type != 'all'
           AND channel_num != 'all'
           AND revenue_level != 'all'
           AND newold_state != 'all'
           AND active_state != 'all'
           AND dt = '{month}'
         GROUP BY dt, channel_num, revenue_level, newold_state, active_state, channel_type
         WITH ROLLUP

         UNION

         SELECT dt,
                MAX(platform_id)             AS platform_id,
                MAX(platform)                AS platform,
                IFNULL(channel_type, 'all')  AS channel_type,
                IFNULL(channel_num, 'all')   AS channel_num,
                IFNULL(revenue_level, 'all') AS revenue_level,
                IFNULL(newold_state, 'all')  AS newold_state,
                IFNULL(active_state, 'all')  AS active_state,
                SUM(anchor_cnt)              AS anchor_cnt,
                SUM(live_cnt)                AS live_cnt,
                SUM(duration)                AS duration,
                SUM(revenue)                 AS revenue,
                SUM(revenue_orig)            AS revenue_orig,
                SUM(guild_income)            AS guild_income,
                SUM(guild_income_orig)       AS guild_income_orig,
                SUM(anchor_income)           AS anchor_income,
                SUM(anchor_income_orig)      AS anchor_income_orig
         FROM bireport.rpt_month_hy_guild
         WHERE channel_type != 'all'
           AND channel_num != 'all'
           AND revenue_level != 'all'
           AND newold_state != 'all'
           AND active_state != 'all'
           AND dt = '{month}'
         GROUP BY dt, revenue_level, newold_state, active_state, channel_type, channel_num
         WITH ROLLUP

         UNION

         SELECT dt,
                MAX(platform_id)             AS platform_id,
                MAX(platform)                AS platform,
                IFNULL(channel_type, 'all')  AS channel_type,
                IFNULL(channel_num, 'all')   AS channel_num,
                IFNULL(revenue_level, 'all') AS revenue_level,
                IFNULL(newold_state, 'all')  AS newold_state,
                IFNULL(active_state, 'all')  AS active_state,
                SUM(anchor_cnt)              AS anchor_cnt,
                SUM(live_cnt)                AS live_cnt,
                SUM(duration)                AS duration,
                SUM(revenue)                 AS revenue,
                SUM(revenue_orig)            AS revenue_orig,
                SUM(guild_income)            AS guild_income,
                SUM(guild_income_orig)       AS guild_income_orig,
                SUM(anchor_income)           AS anchor_income,
                SUM(anchor_income_orig)      AS anchor_income_orig
         FROM bireport.rpt_month_hy_guild
         WHERE channel_type != 'all'
           AND channel_num != 'all'
           AND revenue_level != 'all'
           AND newold_state != 'all'
           AND active_state != 'all'
           AND dt = '{month}'
         GROUP BY dt, newold_state, active_state, channel_type, channel_num, revenue_level
         WITH ROLLUP

         UNION

         SELECT dt,
                MAX(platform_id)             AS platform_id,
                MAX(platform)                AS platform,
                IFNULL(channel_type, 'all')  AS channel_type,
                IFNULL(channel_num, 'all')   AS channel_num,
                IFNULL(revenue_level, 'all') AS revenue_level,
                IFNULL(newold_state, 'all')  AS newold_state,
                IFNULL(active_state, 'all')  AS active_state,
                SUM(anchor_cnt)              AS anchor_cnt,
                SUM(live_cnt)                AS live_cnt,
                SUM(duration)                AS duration,
                SUM(revenue)                 AS revenue,
                SUM(revenue_orig)            AS revenue_orig,
                SUM(guild_income)            AS guild_income,
                SUM(guild_income_orig)       AS guild_income_orig,
                SUM(anchor_income)           AS anchor_income,
                SUM(anchor_income_orig)      AS anchor_income_orig
         FROM bireport.rpt_month_hy_guild
         WHERE channel_type != 'all'
           AND channel_num != 'all'
           AND revenue_level != 'all'
           AND newold_state != 'all'
           AND active_state != 'all'
           AND dt = '{month}'
         GROUP BY dt, active_state, channel_type, channel_num, revenue_level, newold_state
         WITH ROLLUP

         UNION

         SELECT dt,
                MAX(platform_id)             AS platform_id,
                MAX(platform)                AS platform,
                IFNULL(channel_type, 'all')  AS channel_type,
                IFNULL(channel_num, 'all')   AS channel_num,
                IFNULL(revenue_level, 'all') AS revenue_level,
                IFNULL(newold_state, 'all')  AS newold_state,
                IFNULL(active_state, 'all')  AS active_state,
                SUM(anchor_cnt)              AS anchor_cnt,
                SUM(live_cnt)                AS live_cnt,
                SUM(duration)                AS duration,
                SUM(revenue)                 AS revenue,
                SUM(revenue_orig)            AS revenue_orig,
                SUM(guild_income)            AS guild_income,
                SUM(guild_income_orig)       AS guild_income_orig,
                SUM(anchor_income)           AS anchor_income,
                SUM(anchor_income_orig)      AS anchor_income_orig
         FROM bireport.rpt_month_hy_guild
         WHERE channel_type != 'all'
           AND channel_num != 'all'
           AND revenue_level != 'all'
           AND newold_state != 'all'
           AND active_state != 'all'
           AND dt = '{month}'
         GROUP BY dt, channel_num, channel_type, active_state, revenue_level, newold_state
         WITH ROLLUP

         UNION

         SELECT dt,
                MAX(platform_id)             AS platform_id,
                MAX(platform)                AS platform,
                IFNULL(channel_type, 'all')  AS channel_type,
                IFNULL(channel_num, 'all')   AS channel_num,
                IFNULL(revenue_level, 'all') AS revenue_level,
                IFNULL(newold_state, 'all')  AS newold_state,
                IFNULL(active_state, 'all')  AS active_state,
                SUM(anchor_cnt)              AS anchor_cnt,
                SUM(live_cnt)                AS live_cnt,
                SUM(duration)                AS duration,
                SUM(revenue)                 AS revenue,
                SUM(revenue_orig)            AS revenue_orig,
                SUM(guild_income)            AS guild_income,
                SUM(guild_income_orig)       AS guild_income_orig,
                SUM(anchor_income)           AS anchor_income,
                SUM(anchor_income_orig)      AS anchor_income_orig
         FROM bireport.rpt_month_hy_guild
         WHERE channel_type != 'all'
           AND channel_num != 'all'
           AND revenue_level != 'all'
           AND newold_state != 'all'
           AND active_state != 'all'
           AND dt = '{month}'
         GROUP BY dt, newold_state, channel_type, channel_num, revenue_level, active_state
         WITH ROLLUP

         UNION

         SELECT dt,
                MAX(platform_id)             AS platform_id,
                MAX(platform)                AS platform,
                IFNULL(channel_type, 'all')  AS channel_type,
                IFNULL(channel_num, 'all')   AS channel_num,
                IFNULL(revenue_level, 'all') AS revenue_level,
                IFNULL(newold_state, 'all')  AS newold_state,
                IFNULL(active_state, 'all')  AS active_state,
                SUM(anchor_cnt)              AS anchor_cnt,
                SUM(live_cnt)                AS live_cnt,
                SUM(duration)                AS duration,
                SUM(revenue)                 AS revenue,
                SUM(revenue_orig)            AS revenue_orig,
                SUM(guild_income)            AS guild_income,
                SUM(guild_income_orig)       AS guild_income_orig,
                SUM(anchor_income)           AS anchor_income,
                SUM(anchor_income_orig)      AS anchor_income_orig
         FROM bireport.rpt_month_hy_guild
         WHERE channel_type != 'all'
           AND channel_num != 'all'
           AND revenue_level != 'all'
           AND newold_state != 'all'
           AND active_state != 'all'
           AND dt = '{month}'
         GROUP BY dt, active_state, channel_type, newold_state, revenue_level, channel_num
         WITH ROLLUP
     ) t
WHERE dt IS NOT NULL
;



UPDATE bireport.rpt_month_hy_guild a
    INNER JOIN warehouse.dw_huya_month_guild_live_true b
    ON a.dt = b.dt AND a.channel_num = b.channel_num
SET a.revenue = b.revenue
WHERE a.dt <= '2019-11-01'
  AND a.dt = '{month}'
  AND a.channel_type <> 'all'
  AND a.channel_num <> 'all'
  AND a.newold_state = 'all'
  AND a.active_state = 'all'
  AND a.revenue_level = 'all'
;


UPDATE bireport.rpt_month_hy_guild a
    INNER JOIN (SELECT gl.dt, ai.channel_type, SUM(gl.revenue) AS revenue
                FROM warehouse.dw_huya_month_guild_live_true gl
                         LEFT JOIN warehouse.ods_hy_account_info ai ON gl.channel_id = ai.channel_id
                GROUP BY gl.dt, ai.channel_type) b
    ON a.dt = b.dt AND a.channel_type = b.channel_type
SET a.revenue = b.revenue
WHERE a.dt <= '2019-11-01'
  AND a.dt = '{month}'
  AND a.channel_type <> 'all'
  AND a.channel_num = 'all'
  AND a.newold_state = 'all'
  AND a.active_state = 'all'
  AND a.revenue_level = 'all'
;


UPDATE bireport.rpt_month_hy_guild a
    INNER JOIN (SELECT dt, SUM(revenue) AS revenue FROM warehouse.dw_huya_month_guild_live_true GROUP BY dt) b
    ON a.dt = b.dt
SET a.revenue = b.revenue
WHERE a.dt <= '2019-11-01'
  AND a.dt = '{month}'
  AND a.channel_type = 'all'
  AND a.channel_num = 'all'
  AND a.newold_state = 'all'
  AND a.active_state = 'all'
  AND a.revenue_level = 'all'
;


-- 报表用，计算上周、上月同期数据---
DELETE
FROM bireport.rpt_month_hy_guild_view
WHERE dt = '{month}';
REPLACE INTO bireport.rpt_month_hy_guild_view
SELECT t1.dt,
       t1.channel_type,
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
       IF(t1.live_cnt > 0, ROUND(t1.`revenue` / t1.live_cnt, 0), 0)    AS revenue_per_live,
       IF(t3.live_cnt > 0, ROUND(t3.`revenue` / t3.live_cnt, 0), 0)    AS revenue_per_live_lastmonth,
       0                                                               AS `guild_income`,
       0                                                               AS `anchor_income`
FROM bireport.rpt_month_hy_guild t1
         LEFT JOIN bireport.rpt_month_hy_guild t3
                   ON t1.dt - INTERVAL 1 MONTH = t3.dt
                       AND t1.channel_type = t3.channel_type
                       AND t1.channel_num = t3.channel_num
                       AND t1.revenue_level = t3.revenue_level
                       AND t1.newold_state = t3.newold_state
                       AND t1.active_state = t3.active_state
WHERE t1.dt = '{month}'
;


-- 报表用，计算指标占比---
DELETE
FROM bireport.rpt_month_hy_guild_view_compare
WHERE dt = '{month}';
REPLACE INTO bireport.rpt_month_hy_guild_view_compare
SELECT *
FROM (SELECT dt,
             channel_num,
             revenue_level,
             newold_state,
             active_state,
             '主播数'      AS idx,
             anchor_cnt AS val
      FROM bireport.rpt_month_hy_guild
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
      FROM bireport.rpt_month_hy_guild
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
      FROM bireport.rpt_month_hy_guild
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
      FROM bireport.rpt_month_hy_guild
      WHERE revenue_level != 'all'
        AND dt = '{month}'
        AND live_cnt > 0
     ) t
;

