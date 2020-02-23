DELETE
FROM bireport.rpt_day_all
WHERE platform = 'FanXing'
  AND dt BETWEEN '{start_date}' AND '{end_date}';
INSERT INTO bireport.rpt_day_all
SELECT gl.dt,
       gl.platform_name,
       SUM(anchor_cnt)                          AS anchor_cnt,
       SUM(live_cnt)                            AS live_cnt,
       SUM(gl.anchor_income) / 125 / 0.4        AS revenue,
       SUM(gl.anchor_income) / 125 / 0.4 * 0.09 AS guild_income,
       SUM(gl.anchor_income) / 125              AS anchor_income
FROM warehouse.dw_fx_day_guild_live gl
WHERE gl.dt BETWEEN '{start_date}' AND '{end_date}'
GROUP BY gl.dt,
         gl.platform_name
;


DELETE
FROM bireport.rpt_day_fx_guild
WHERE dt BETWEEN '{start_date}' AND '{end_date}';
INSERT INTO bireport.rpt_day_fx_guild
SELECT gl.dt,
       gl.platform_id,
       gl.platform_name                         AS platform,
       gl.backend_account_id,
       SUM(gl.anchor_cnt)                       AS anchor_cnt,
       SUM(gl.live_cnt)                         AS live_cnt,
       SUM(gl.anchor_income) / 125 / 0.4        AS revenue,
       SUM(gl.anchor_income)                    AS revenue_orig,
       SUM(gl.anchor_income) / 125 / 0.4 * 0.09 AS guild_income,
       SUM(gl.anchor_income)                    AS guild_income_orig,
       SUM(anchor_income) / 125                 AS anchor_income,
       SUM(anchor_income)                       AS anchor_income_orig
FROM warehouse.dw_bb_day_guild_live gl
WHERE gl.dt BETWEEN '{start_date}' AND '{end_date}'
GROUP BY gl.dt,
         gl.platform_id,
         gl.platform_name,
         gl.backend_account_id
;


REPLACE INTO bireport.rpt_day_fx_guild
(dt, backend_account_id, anchor_cnt, live_cnt, revenue, guild_income, anchor_income)
SELECT dt,
       'all' AS backend_account_id,
       anchor_cnt,
       live_cnt,
       revenue,
       guild_income,
       anchor_income
FROM bireport.rpt_day_all
WHERE platform = 'FanXing'
  AND dt BETWEEN '{start_date}'
    AND '{end_date}';


-- ---------------------rpt_day_fx_guild_new-----------------------------
DELETE
FROM bireport.rpt_day_fx_guild_new
WHERE dt BETWEEN '{start_date}' AND '{end_date}';
INSERT INTO bireport.rpt_day_fx_guild_new
SELECT gl.dt,
       gl.platform_id,
       gl.platform_name                         AS platform,
       gl.backend_account_id,
       gl.revenue_level,
       gl.newold_state,
       gl.active_state,
       SUM(gl.anchor_cnt)                       AS anchor_cnt,
       SUM(gl.live_cnt)                         AS live_cnt,
       SUM(gl.duration)                         AS duration,
       SUM(gl.anchor_income) / 125 / 0.4        AS revenue,
       SUM(gl.anchor_income)                    AS revenue_orig,
       SUM(gl.anchor_income) / 125 / 0.4 * 0.09 AS guild_income,
       SUM(gl.anchor_income)                    AS guild_income_orig,
       SUM(anchor_income) / 125                 AS anchor_income,
       SUM(anchor_income)                       AS anchor_income_orig
FROM warehouse.dw_fx_day_guild_live gl
WHERE gl.dt BETWEEN '{start_date}' AND '{end_date}'
GROUP BY gl.dt,
         gl.platform_id,
         gl.platform_name,
         gl.backend_account_id,
         gl.revenue_level,
         gl.newold_state,
         gl.active_state
;


-- 补充汇总数据
REPLACE INTO bireport.rpt_day_fx_guild_new (dt, platform_id, platform, backend_account_id, revenue_level, newold_state,
                                            active_state, anchor_cnt, live_cnt, duration, revenue, revenue_orig,
                                            guild_income, guild_income_orig, anchor_income, anchor_income_orig)
SELECT *
FROM (SELECT dt,
             MAX(platform_id)                  AS platform_id,
             MAX(platform)                     AS platform,
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
      FROM bireport.rpt_day_fx_guild_new
      WHERE (backend_account_id != 'all' OR revenue_level != 'all' OR newold_state != 'all' OR active_state != 'all')
        AND dt BETWEEN '{start_date}' AND '{end_date}'
      GROUP BY dt, backend_account_id, revenue_level, newold_state, active_state
      WITH ROLLUP

      UNION

      SELECT dt,
             MAX(platform_id)                  AS platform_id,
             MAX(platform)                     AS platform,
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
      FROM bireport.rpt_day_fx_guild_new
      WHERE (backend_account_id != 'all' OR revenue_level != 'all' OR newold_state != 'all' OR active_state != 'all')
        AND dt BETWEEN '{start_date}' AND '{end_date}'
      GROUP BY dt, revenue_level, newold_state, active_state, backend_account_id
      WITH ROLLUP

      UNION

      SELECT dt,
             MAX(platform_id)                  AS platform_id,
             MAX(platform)                     AS platform,
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
      FROM bireport.rpt_day_fx_guild_new
      WHERE (backend_account_id != 'all' OR revenue_level != 'all' OR newold_state != 'all' OR active_state != 'all')
        AND dt BETWEEN '{start_date}' AND '{end_date}'
      GROUP BY dt, newold_state, active_state, backend_account_id, revenue_level
      WITH ROLLUP

      UNION

      SELECT dt,
             MAX(platform_id)                  AS platform_id,
             MAX(platform)                     AS platform,
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
      FROM bireport.rpt_day_fx_guild_new
      WHERE (backend_account_id != 'all' OR revenue_level != 'all' OR newold_state != 'all' OR active_state != 'all')
        AND dt BETWEEN '{start_date}' AND '{end_date}'
      GROUP BY dt, active_state, backend_account_id, revenue_level, newold_state
      WITH ROLLUP

      UNION

      SELECT dt,
             MAX(platform_id)                  AS platform_id,
             MAX(platform)                     AS platform,
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
      FROM bireport.rpt_day_fx_guild_new
      WHERE (backend_account_id != 'all' OR revenue_level != 'all' OR newold_state != 'all' OR active_state != 'all')
        AND dt BETWEEN '{start_date}' AND '{end_date}'
      GROUP BY dt, backend_account_id, newold_state, revenue_level, active_state
      WITH ROLLUP
     ) t
WHERE t.dt IS NOT NULL
;


-- - rpt_day_all_new ----

REPLACE INTO bireport.rpt_day_all_new
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
FROM bireport.rpt_day_fx_guild_new
WHERE backend_account_id = 'all'
  AND dt BETWEEN '{start_date}' AND '{end_date}'
;


-- 报表用，计算上周、上月同期数据---
REPLACE INTO bireport.rpt_day_fx_guild_new_view
SELECT t1.dt,
       t1.backend_account_id,
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
FROM bireport.rpt_day_fx_guild_new t1
         LEFT JOIN bireport.rpt_day_fx_guild_new t2
                   ON t1.dt - INTERVAL 7 DAY = t2.dt
                       AND t1.backend_account_id = t2.backend_account_id
                       AND t1.revenue_level = t2.revenue_level
                       AND t1.newold_state = t2.newold_state
                       AND t1.active_state = t2.active_state
         LEFT JOIN bireport.rpt_day_fx_guild_new t3
                   ON t1.dt - INTERVAL 1 MONTH = t3.dt
                       AND t1.backend_account_id = t3.backend_account_id
                       AND t1.revenue_level = t3.revenue_level
                       AND t1.newold_state = t3.newold_state
                       AND t1.active_state = t3.active_state
WHERE t1.dt BETWEEN '{start_date}' AND '{end_date}'
;


-- 报表用，计算指标占比---
REPLACE INTO bireport.rpt_day_fx_guild_new_view_compare
SELECT *
FROM (SELECT dt,
             backend_account_id,
             revenue_level,
             newold_state,
             active_state,
             '主播数'      AS idx,
             anchor_cnt AS val
      FROM bireport.rpt_day_fx_guild_new
      WHERE revenue_level != 'all'
        AND dt BETWEEN '{start_date}' AND '{end_date}'
      UNION
      SELECT dt,
             backend_account_id,
             revenue_level,
             newold_state,
             active_state,
             '开播数'    AS idx,
             live_cnt AS val
      FROM bireport.rpt_day_fx_guild_new
      WHERE revenue_level != 'all'
        AND dt BETWEEN '{start_date}' AND '{end_date}'
      UNION
      SELECT dt,
             backend_account_id,
             revenue_level,
             newold_state,
             active_state,
             '流水'    AS idx,
             revenue AS val
      FROM bireport.rpt_day_fx_guild_new
      WHERE revenue_level != 'all'
        AND dt BETWEEN '{start_date}' AND '{end_date}'
      UNION
      SELECT dt,
             backend_account_id,
             revenue_level,
             newold_state,
             active_state,
             '开播人均流水'                     AS idx,
             round(revenue / live_cnt, 0) AS val
      FROM bireport.rpt_day_fx_guild_new
      WHERE revenue_level != 'all'
        AND dt BETWEEN '{start_date}' AND '{end_date}'
        AND live_cnt > 0) t
;
