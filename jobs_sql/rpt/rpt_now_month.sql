-- 公会每月流水、公会收入、主播收入
-- DROP TABLE IF EXISTS bireport.rpt_month_now_guild;
-- CREATE TABLE bireport.rpt_month_now_guild AS
DELETE
FROM bireport.rpt_month_now_guild
WHERE DATE_FORMAT(dt, '%Y-%m') BETWEEN DATE_FORMAT('{start_date}', '%Y-%m') AND DATE_FORMAT('{end_date}', '%Y-%m');
INSERT INTO bireport.rpt_month_now_guild
SELECT gl.dt,
       gl.platform_id,
       gl.platform_name                     AS platform,
       gl.backend_account_id,
       gr.anchor_cnt_true                   AS anchor_cnt,
       gl.live_cnt,
       gl.revenue_rmb                       AS revenue,
       gl.revenue_rmb                       AS revenue_orig,
       round(gl.revenue_rmb * 0.6 * 0.5, 2) AS guild_income,
       gl.revenue_rmb * 0.6 * 0.5           AS guild_income_orig,
       round(gl.revenue_rmb * 0.6 * 0.5, 2) AS anchor_income,
       gl.revenue_rmb * 0.6 * 0.5           AS anchor_income_orig
FROM (SELECT gl.dt,
             gl.platform_id,
             pf.platform_name,
             gl.backend_account_id,
             SUM(anchor_cnt)      AS anchor_cnt,
             SUM(anchor_live_cnt) AS live_cnt,
             SUM(revenue_rmb)     AS revenue_rmb
      FROM warehouse.dw_now_month_guild_live gl
               lEFT JOIN warehouse.platform pf ON pf.id = gl.platform_id
       WHERE DATE_FORMAT(dt, '%Y-%m') BETWEEN DATE_FORMAT('{start_date}', '%Y-%m') AND DATE_FORMAT('{end_date}', '%Y-%m')
      GROUP BY gl.dt,
               gl.platform_id,
               pf.platform_name,
               gl.backend_account_id) gl
         LEFT JOIN warehouse.dw_now_month_guild_live_true gr
                   ON gl.dt = gr.dt AND gl.backend_account_id = gr.backend_account_id
;



DELETE
FROM bireport.rpt_month_all_guild
WHERE platform_id = 1003
  AND dt BETWEEN DATE_FORMAT('{start_date}', '%Y-%m-01') AND DATE_FORMAT('{end_date}', '%Y-%m-01');
INSERT INTO bireport.rpt_month_all_guild
SELECT dt,
       platform_id,
       platform,
       channel_num,
       CASE WHEN anchor_cnt >= 0 THEN anchor_cnt ELSE 0 END                 AS anchor_cnt,
       CASE WHEN live_cnt >= 0 THEN live_cnt ELSE 0 END                     AS live_cnt,
       CASE WHEN revenue >= 0 THEN revenue ELSE 0 END                       AS revenue,
       CASE WHEN revenue_orig >= 0 THEN revenue_orig ELSE 0 END             AS revenue_orig,
       CASE WHEN guild_income >= 0 THEN guild_income ELSE 0 END             AS guild_income,
       CASE WHEN guild_income_orig >= 0 THEN guild_income_orig ELSE 0 END   AS guild_income_orig,
       CASE WHEN anchor_income >= 0 THEN anchor_income ELSE 0 END           AS anchor_income,
       CASE WHEN anchor_income_orig >= 0 THEN anchor_income_orig ELSE 0 END AS anchor_income_orig
FROM (SELECT dt,
             platform_id,
             platform,
             backend_account_id AS channel_num,
             anchor_cnt,
             live_cnt,
             revenue,
             revenue_orig,
             guild_income,
             guild_income_orig,
             anchor_income,
             anchor_income_orig
      FROM bireport.rpt_month_now_guild) t
WHERE dt BETWEEN DATE_FORMAT('{start_date}', '%Y-%m-01') AND DATE_FORMAT('{end_date}', '%Y-%m-01')
;


-- rpt_month_yy_guild_new
DELETE
FROM bireport.rpt_month_now_guild_new
WHERE dt = '{month}';
INSERT INTO bireport.rpt_month_now_guild_new
SELECT gl.dt,
       gl.platform_id,
       pf.platform_name                     AS platform,
       gl.backend_account_id,
       gl.revenue_level,
       gl.newold_state,
       gl.active_state,
       gl.anchor_cnt,
       gl.anchor_live_cnt                   AS live_cnt,
       gl.duration,
       -- 平台流水
       gl.revenue_rmb                       AS revenue,
       gl.revenue_rmb                       AS revenue_orig,
       -- 公会收入
       ROUND(gl.revenue_rmb * 0.6 * 0.5, 2) AS guild_income,
       ROUND(gl.revenue_rmb * 0.6 * 0.5, 2) AS guild_income_orig,
       -- 主播收入
       ROUND(gl.revenue_rmb * 0.6 * 0.5, 2) AS anchor_income,
       ROUND(gl.revenue_rmb * 0.6 * 0.5, 2) AS anchor_income_orig
FROM warehouse.dw_now_month_guild_live gl
         LEFT JOIN warehouse.platform pf ON gl.platform_id = pf.id
WHERE dt BETWEEN DATE_FORMAT('{start_date}', '%Y-%m-01') AND DATE_FORMAT('{end_date}', '%Y-%m-01')
;



REPLACE into bireport.rpt_month_now_guild_new (dt, platform_id, platform, backend_account_id, revenue_level,
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
      FROM bireport.rpt_month_now_guild_new
      WHERE (backend_account_id != 'all' OR revenue_level != 'all' OR newold_state != 'all' OR active_state != 'all')
         AND dt BETWEEN DATE_FORMAT('{start_date}', '%Y-%m-01') AND DATE_FORMAT('{end_date}', '%Y-%m-01')
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
      FROM bireport.rpt_month_now_guild_new
      WHERE (backend_account_id != 'all' OR revenue_level != 'all' OR newold_state != 'all' OR active_state != 'all')
         AND dt BETWEEN DATE_FORMAT('{start_date}', '%Y-%m-01') AND DATE_FORMAT('{end_date}', '%Y-%m-01')
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
      FROM bireport.rpt_month_now_guild_new
      WHERE (backend_account_id != 'all' OR revenue_level != 'all' OR newold_state != 'all' OR active_state != 'all')
         AND dt BETWEEN DATE_FORMAT('{start_date}', '%Y-%m-01') AND DATE_FORMAT('{end_date}', '%Y-%m-01')
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
      FROM bireport.rpt_month_now_guild_new
      WHERE (backend_account_id != 'all' OR revenue_level != 'all' OR newold_state != 'all' OR active_state != 'all')
         AND dt BETWEEN DATE_FORMAT('{start_date}', '%Y-%m-01') AND DATE_FORMAT('{end_date}', '%Y-%m-01')
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
      FROM bireport.rpt_month_now_guild_new
      WHERE (backend_account_id != 'all' OR revenue_level != 'all' OR newold_state != 'all' OR active_state != 'all')
         AND dt BETWEEN DATE_FORMAT('{start_date}', '%Y-%m-01') AND DATE_FORMAT('{end_date}', '%Y-%m-01')
      GROUP BY dt, backend_account_id, newold_state, revenue_level, active_state
      WITH ROLLUP
    ) t
 WHERE dt IS NOT NULL
;


replace into bireport.rpt_month_all_new
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
FROM bireport.rpt_month_now_guild_new
WHERE backend_account_id = 'all'
   AND dt BETWEEN DATE_FORMAT('{start_date}', '%Y-%m-01') AND DATE_FORMAT('{end_date}', '%Y-%m-01')
;