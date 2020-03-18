-- =======================================================================
-- 计算每月相对前一月新增主播数。流失主播数、净增长主播数
DELETE
FROM stage.stage_rpt_now_month_anchor_live_contrast
WHERE dt = '{month}';
INSERT IGNORE INTO stage.stage_rpt_now_month_anchor_live_contrast
SELECT dt,
       platform_name,
       platform_id,
       anchor_no,
       dt + INTERVAL 1 MONTH AS last_dt,
       dt - INTERVAL 1 MONTH AS next_dt
FROM warehouse.dw_now_month_anchor_live al
WHERE dt = '{month}'
;


-- 新增主播（在t-1天主播列表，不在t-2天的列表）
-- CREATE TABLE stage.stage_now_day_anchor_add_loss AS
DELETE
FROM stage.stage_rpt_now_month_anchor_add_loss
WHERE add_loss_state = 'add'
  AND dt = '{month}';
INSERT INTO stage.stage_rpt_now_month_anchor_add_loss
SELECT al1.dt, al1.platform_name, al1.platform_id, al1.anchor_no, 'add' AS add_loss_state
FROM stage.stage_rpt_now_month_anchor_live_contrast al1
         LEFT JOIN stage.stage_rpt_now_month_anchor_live_contrast al2
                   ON al1.dt = al2.last_dt AND al1.anchor_no = al2.anchor_no
WHERE al2.anchor_no IS NULL
  AND al1.dt = '{month}'
;


-- 流失主播（在t-2天主播列表，不在t-1天的列表）
DELETE
FROM stage.stage_rpt_now_month_anchor_add_loss
WHERE add_loss_state = 'loss'
  AND dt = '{month}';
INSERT INTO stage.stage_rpt_now_month_anchor_add_loss
SELECT al1.last_dt, al1.platform_name, al1.platform_id, al1.anchor_no, 'loss' AS add_loss_state
FROM stage.stage_rpt_now_month_anchor_live_contrast al1
         LEFT JOIN stage.stage_rpt_now_month_anchor_live_contrast al2
                   ON al1.dt = al2.next_dt AND al1.anchor_no = al2.anchor_no
WHERE al2.anchor_no IS NULL
  AND al1.last_dt <= '{cur_date}'
  AND al1.dt = '{month}'
;


# CREATE TABLE stage.stage_rpt_now_month_guild_live
DELETE
FROM stage.stage_rpt_now_month_guild_live
WHERE dt = '{month}';
INSERT INTO stage.stage_rpt_now_month_guild_live
SELECT DATE_FORMAT(al.dt, '%Y-%m-01')                                                              AS dt,
       al.platform_id,
       al.platform_name,
       al.backend_account_id,
       al.city,
       al.newold_state,
       al.active_state,
       al.revenue_level,
       COUNT(DISTINCT IF(add_loss_state <> 'loss', al.anchor_no, NULL))                            AS anchor_cnt,
       COUNT(DISTINCT IF(add_loss_state = 'add', al.anchor_no, NULL))                              AS add_anchor_cnt,
       COUNT(DISTINCT IF(add_loss_state = 'loss', al.anchor_no, NULL))                             AS loss_anchor_cnt,
       COUNT(DISTINCT IF(add_loss_state = 'add', al.anchor_no, NULL)) -
       COUNT(DISTINCT IF(add_loss_state = 'loss', al.anchor_no, NULL))                             AS increase_anchor_cnt,
       COUNT(DISTINCT IF(add_loss_state <> 'loss' AND al.duration > 0, al.anchor_no, NULL))        AS anchor_live_cnt,
       SUM(IF(add_loss_state <> 'loss' AND al.duration > 0, al.duration, 0))                       AS duration,
       SUM(IF(add_loss_state <> 'loss' AND al.revenue > 0, al.revenue, 0))                         AS revenue,
       SUM(IF(add_loss_state <> 'loss' AND al.revenue > 0, al.revenue, 0))                         AS revenue_orig,
       SUM(IF(add_loss_state <> 'loss' AND al.revenue * 0.6 * 0.5 > 0, al.revenue * 0.6 * 0.5,
              0))                                                                                  AS anchor_income,
       SUM(IF(add_loss_state <> 'loss' AND al.revenue * 0.6 * 0.5 > 0, al.revenue * 0.6 * 0.5,
              0))                                                                                  AS anchor_income_orig,
       SUM(IF(add_loss_state <> 'loss' AND al.revenue * 0.6 * 0.5 > 0, al.revenue * 0.6 * 0.5, 0)) AS guild_income,
       SUM(IF(add_loss_state <> 'loss' AND al.revenue * 0.6 * 0.5 > 0, al.revenue * 0.6 * 0.5,
              0))                                                                                  AS guild_income_orig
FROM (
         SELECT al.*, CASE WHEN aal.add_loss_state IS NULL THEN '' ELSE aal.add_loss_state END AS add_loss_state
         FROM warehouse.dw_now_month_anchor_live al
                  LEFT JOIN stage.stage_rpt_now_month_anchor_add_loss aal
                            ON al.dt = aal.dt AND al.anchor_no = aal.anchor_no
         WHERE al.dt = '{month}'
         UNION ALL
         SELECT al.dt + INTERVAL 1 MONTH                                                 AS dt,
                al.platform_id,
                al.platform_name,
                al.backend_account_id,
                al.city,
                al.active_state,
                al.newold_state,
                al.revenue_level,
                al.anchor_no,
                al.live_days,
                al.duration,
                al.revenue,
                CASE WHEN aal.add_loss_state IS NULL THEN '' ELSE aal.add_loss_state END AS add_loss_state
         FROM warehouse.dw_now_month_anchor_live al
                  INNER JOIN stage.stage_rpt_now_month_anchor_add_loss aal
                             ON al.dt + INTERVAL 1 MONTH = aal.dt AND al.anchor_no = aal.anchor_no
         WHERE aal.add_loss_state = 'loss'
           AND al.dt = '{month}'
     ) al
GROUP BY DATE_FORMAT(al.dt, '%Y-%m-01'),
         al.platform_id,
         al.platform_name,
         al.backend_account_id,
         al.city,
         al.newold_state,
         al.active_state,
         al.revenue_level
;

-- rpt_month_yy_guild_new
DELETE
FROM bireport.rpt_month_now_guild
WHERE dt = '{month}';
INSERT INTO bireport.rpt_month_now_guild
SELECT gl.dt,
       gl.platform_id,
       pf.platform_name   AS platform,
       gl.backend_account_id,
       gl.city,
       gl.revenue_level,
       gl.newold_state,
       gl.active_state,
       gl.anchor_cnt,
       gl.add_anchor_cnt,
       gl.loss_anchor_cnt,
       gl.increase_anchor_cnt,
       gl.anchor_live_cnt AS live_cnt,
       gl.duration,
       -- 平台流水
       gl.revenue,
       gl.revenue         AS revenue_orig,
       -- 公会收入
       gl.guild_income,
       gl.guild_income_orig,
       -- 主播收入
       gl.anchor_income,
       gl.anchor_income_orig
# FROM warehouse.dw_now_month_guild_live gl
FROM stage.stage_rpt_now_month_guild_live gl
         LEFT JOIN warehouse.platform pf ON gl.platform_id = pf.id
WHERE gl.dt = '{month}'
;



REPLACE INTO bireport.rpt_month_now_guild (dt, platform_id, platform, backend_account_id, city, revenue_level,
                                           newold_state, active_state, anchor_cnt, add_anchor_cnt, loss_anchor_cnt,
                                           increase_anchor_cnt, live_cnt, duration, revenue, revenue_orig, guild_income,
                                           guild_income_orig, anchor_income, anchor_income_orig)
SELECT *
FROM (SELECT dt,
             MAX(platform_id)                  AS platform_id,
             MAX(platform)                     AS platform,
             IFNULL(backend_account_id, 'all') AS backend_account_id,
             IFNULL(city, 'all')               AS city,
             IFNULL(revenue_level, 'all')      AS revenue_level,
             IFNULL(newold_state, 'all')       AS newold_state,
             IFNULL(active_state, 'all')       AS active_state,
             SUM(anchor_cnt)                   AS anchor_cnt,
             SUM(add_anchor_cnt)               AS add_anchor_cnt,
             SUM(loss_anchor_cnt)              AS loss_anchor_cnt,
             SUM(increase_anchor_cnt)          AS increase_anchor_cnt,
             SUM(live_cnt)                     AS live_cnt,
             SUM(duration)                     AS duration,
             SUM(revenue)                      AS revenue,
             SUM(revenue_orig)                 AS revenue_orig,
             SUM(guild_income)                 AS guild_income,
             SUM(guild_income_orig)            AS guild_income_orig,
             SUM(anchor_income)                AS anchor_income,
             SUM(anchor_income_orig)           AS anchor_income_orig
      FROM bireport.rpt_month_now_guild
      WHERE backend_account_id != 'all'
        AND revenue_level != 'all'
        AND newold_state != 'all'
        AND active_state != 'all'
        AND dt = '{month}'
      GROUP BY dt, backend_account_id, city, revenue_level, newold_state, active_state
      WITH ROLLUP

      UNION

      SELECT dt,
             MAX(platform_id)                  AS platform_id,
             MAX(platform)                     AS platform,
             IFNULL(backend_account_id, 'all') AS backend_account_id,
             IFNULL(city, 'all')               AS city,
             IFNULL(revenue_level, 'all')      AS revenue_level,
             IFNULL(newold_state, 'all')       AS newold_state,
             IFNULL(active_state, 'all')       AS active_state,
             SUM(anchor_cnt)                   AS anchor_cnt,
             SUM(add_anchor_cnt)               AS add_anchor_cnt,
             SUM(loss_anchor_cnt)              AS loss_anchor_cnt,
             SUM(increase_anchor_cnt)          AS increase_anchor_cnt,
             SUM(live_cnt)                     AS live_cnt,
             SUM(duration)                     AS duration,
             SUM(revenue)                      AS revenue,
             SUM(revenue_orig)                 AS revenue_orig,
             SUM(guild_income)                 AS guild_income,
             SUM(guild_income_orig)            AS guild_income_orig,
             SUM(anchor_income)                AS anchor_income,
             SUM(anchor_income_orig)           AS anchor_income_orig
      FROM bireport.rpt_month_now_guild
      WHERE backend_account_id != 'all'
        AND revenue_level != 'all'
        AND newold_state != 'all'
        AND active_state != 'all'
        AND dt = '{month}'
      GROUP BY dt, city, revenue_level, newold_state, active_state, backend_account_id
      WITH ROLLUP

      UNION

      SELECT dt,
             MAX(platform_id)                  AS platform_id,
             MAX(platform)                     AS platform,
             IFNULL(backend_account_id, 'all') AS backend_account_id,
             IFNULL(city, 'all')               AS city,
             IFNULL(revenue_level, 'all')      AS revenue_level,
             IFNULL(newold_state, 'all')       AS newold_state,
             IFNULL(active_state, 'all')       AS active_state,
             SUM(anchor_cnt)                   AS anchor_cnt,
             SUM(add_anchor_cnt)               AS add_anchor_cnt,
             SUM(loss_anchor_cnt)              AS loss_anchor_cnt,
             SUM(increase_anchor_cnt)          AS increase_anchor_cnt,
             SUM(live_cnt)                     AS live_cnt,
             SUM(duration)                     AS duration,
             SUM(revenue)                      AS revenue,
             SUM(revenue_orig)                 AS revenue_orig,
             SUM(guild_income)                 AS guild_income,
             SUM(guild_income_orig)            AS guild_income_orig,
             SUM(anchor_income)                AS anchor_income,
             SUM(anchor_income_orig)           AS anchor_income_orig
      FROM bireport.rpt_month_now_guild
      WHERE backend_account_id != 'all'
        AND revenue_level != 'all'
        AND newold_state != 'all'
        AND active_state != 'all'
        AND dt = '{month}'
      GROUP BY dt, city, newold_state, revenue_level, active_state, backend_account_id
      WITH ROLLUP

      UNION

      SELECT dt,
             MAX(platform_id)                  AS platform_id,
             MAX(platform)                     AS platform,
             IFNULL(backend_account_id, 'all') AS backend_account_id,
             IFNULL(city, 'all')               AS city,
             IFNULL(revenue_level, 'all')      AS revenue_level,
             IFNULL(newold_state, 'all')       AS newold_state,
             IFNULL(active_state, 'all')       AS active_state,
             SUM(anchor_cnt)                   AS anchor_cnt,
             SUM(add_anchor_cnt)               AS add_anchor_cnt,
             SUM(loss_anchor_cnt)              AS loss_anchor_cnt,
             SUM(increase_anchor_cnt)          AS increase_anchor_cnt,
             SUM(live_cnt)                     AS live_cnt,
             SUM(duration)                     AS duration,
             SUM(revenue)                      AS revenue,
             SUM(revenue_orig)                 AS revenue_orig,
             SUM(guild_income)                 AS guild_income,
             SUM(guild_income_orig)            AS guild_income_orig,
             SUM(anchor_income)                AS anchor_income,
             SUM(anchor_income_orig)           AS anchor_income_orig
      FROM bireport.rpt_month_now_guild
      WHERE backend_account_id != 'all'
        AND revenue_level != 'all'
        AND newold_state != 'all'
        AND active_state != 'all'
        AND dt = '{month}'
      GROUP BY dt, city, active_state, newold_state, revenue_level, backend_account_id
      WITH ROLLUP

      UNION

      SELECT dt,
             MAX(platform_id)                  AS platform_id,
             MAX(platform)                     AS platform,
             IFNULL(backend_account_id, 'all') AS backend_account_id,
             IFNULL(city, 'all')               AS city,
             IFNULL(revenue_level, 'all')      AS revenue_level,
             IFNULL(newold_state, 'all')       AS newold_state,
             IFNULL(active_state, 'all')       AS active_state,
             SUM(anchor_cnt)                   AS anchor_cnt,
             SUM(add_anchor_cnt)               AS add_anchor_cnt,
             SUM(loss_anchor_cnt)              AS loss_anchor_cnt,
             SUM(increase_anchor_cnt)          AS increase_anchor_cnt,
             SUM(live_cnt)                     AS live_cnt,
             SUM(duration)                     AS duration,
             SUM(revenue)                      AS revenue,
             SUM(revenue_orig)                 AS revenue_orig,
             SUM(guild_income)                 AS guild_income,
             SUM(guild_income_orig)            AS guild_income_orig,
             SUM(anchor_income)                AS anchor_income,
             SUM(anchor_income_orig)           AS anchor_income_orig
      FROM bireport.rpt_month_now_guild
      WHERE backend_account_id != 'all'
        AND revenue_level != 'all'
        AND newold_state != 'all'
        AND active_state != 'all'
        AND dt = '{month}'
      GROUP BY dt, revenue_level, newold_state, active_state, backend_account_id, city
      WITH ROLLUP

      UNION

      SELECT dt,
             MAX(platform_id)                  AS platform_id,
             MAX(platform)                     AS platform,
             IFNULL(backend_account_id, 'all') AS backend_account_id,
             IFNULL(city, 'all')               AS city,
             IFNULL(revenue_level, 'all')      AS revenue_level,
             IFNULL(newold_state, 'all')       AS newold_state,
             IFNULL(active_state, 'all')       AS active_state,
             SUM(anchor_cnt)                   AS anchor_cnt,
             SUM(add_anchor_cnt)               AS add_anchor_cnt,
             SUM(loss_anchor_cnt)              AS loss_anchor_cnt,
             SUM(increase_anchor_cnt)          AS increase_anchor_cnt,
             SUM(live_cnt)                     AS live_cnt,
             SUM(duration)                     AS duration,
             SUM(revenue)                      AS revenue,
             SUM(revenue_orig)                 AS revenue_orig,
             SUM(guild_income)                 AS guild_income,
             SUM(guild_income_orig)            AS guild_income_orig,
             SUM(anchor_income)                AS anchor_income,
             SUM(anchor_income_orig)           AS anchor_income_orig
      FROM bireport.rpt_month_now_guild
      WHERE backend_account_id != 'all'
        AND revenue_level != 'all'
        AND newold_state != 'all'
        AND active_state != 'all'
        AND dt = '{month}'
      GROUP BY dt, newold_state, active_state, backend_account_id, city, revenue_level
      WITH ROLLUP

      UNION

      SELECT dt,
             MAX(platform_id)                  AS platform_id,
             MAX(platform)                     AS platform,
             IFNULL(backend_account_id, 'all') AS backend_account_id,
             IFNULL(city, 'all')               AS city,
             IFNULL(revenue_level, 'all')      AS revenue_level,
             IFNULL(newold_state, 'all')       AS newold_state,
             IFNULL(active_state, 'all')       AS active_state,
             SUM(anchor_cnt)                   AS anchor_cnt,
             SUM(add_anchor_cnt)               AS add_anchor_cnt,
             SUM(loss_anchor_cnt)              AS loss_anchor_cnt,
             SUM(increase_anchor_cnt)          AS increase_anchor_cnt,
             SUM(live_cnt)                     AS live_cnt,
             SUM(duration)                     AS duration,
             SUM(revenue)                      AS revenue,
             SUM(revenue_orig)                 AS revenue_orig,
             SUM(guild_income)                 AS guild_income,
             SUM(guild_income_orig)            AS guild_income_orig,
             SUM(anchor_income)                AS anchor_income,
             SUM(anchor_income_orig)           AS anchor_income_orig
      FROM bireport.rpt_month_now_guild
      WHERE backend_account_id != 'all'
        AND revenue_level != 'all'
        AND newold_state != 'all'
        AND active_state != 'all'
        AND dt = '{month}'
      GROUP BY dt, active_state, backend_account_id, city, revenue_level, newold_state
      WITH ROLLUP
     ) t
WHERE dt IS NOT NULL
;


-- 报表用，计算上周、上月同期数据---
DELETE
FROM bireport.rpt_month_now_guild_view
WHERE dt = '{month}';
INSERT INTO bireport.rpt_month_now_guild_view
SELECT t1.dt,
       t1.backend_account_id,
       t1.city,
       t1.revenue_level,
       t1.newold_state,
       t1.active_state,
       t1.anchor_cnt,
       t1.add_anchor_cnt,
       t1.loss_anchor_cnt,
       t1.increase_anchor_cnt,
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
FROM bireport.rpt_month_now_guild t1
         LEFT JOIN bireport.rpt_month_now_guild t3
                   ON t1.dt - INTERVAL 1 MONTH = t3.dt
                       AND t1.backend_account_id = t3.backend_account_id
                       AND t1.city = t3.city
                       AND t1.revenue_level = t3.revenue_level
                       AND t1.newold_state = t3.newold_state
                       AND t1.active_state = t3.active_state
WHERE t1.dt = '{month}'
;


-- 报表用，计算指标占比---
DELETE
FROM bireport.rpt_month_now_guild_view_compare
WHERE dt = '{month}';
INSERT INTO bireport.rpt_month_now_guild_view_compare
SELECT *
FROM (SELECT dt,
             backend_account_id,
             revenue_level,
             newold_state,
             active_state,
             '主播数'      AS idx,
             anchor_cnt AS val
      FROM bireport.rpt_month_now_guild
      WHERE revenue_level != 'all'
        AND city = 'all'
        AND dt = '{month}'
      UNION
      SELECT dt,
             backend_account_id,
             revenue_level,
             newold_state,
             active_state,
             '开播数'    AS idx,
             live_cnt AS val
      FROM bireport.rpt_month_now_guild
      WHERE revenue_level != 'all'
        AND city = 'all'
        AND dt = '{month}'
      UNION
      SELECT dt,
             backend_account_id,
             revenue_level,
             newold_state,
             active_state,
             '流水'    AS idx,
             revenue AS val
      FROM bireport.rpt_month_now_guild
      WHERE revenue_level != 'all'
        AND city = 'all'
        AND dt = '{month}'
      UNION
      SELECT dt,
             backend_account_id,
             revenue_level,
             newold_state,
             active_state,
             '开播人均流水'                     AS idx,
             round(revenue / live_cnt, 0) AS val
      FROM bireport.rpt_month_now_guild
      WHERE revenue_level != 'all'
        AND city = 'all'
        AND dt = '{month}'
        AND live_cnt > 0
     ) t
;
