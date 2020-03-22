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
-- =======================================================================
-- 计算每月相对前一月新增主播数。流失主播数、净增长主播数
-- DELETE
-- FROM stage.delete_stage_rpt_yy_month_anchor_live_contrast
-- WHERE dt = '{month}';
-- INSERT IGNORE INTO stage.delete_stage_rpt_yy_month_anchor_live_contrast
-- SELECT dt,
--        platform_name,
--        platform_id,
--        anchor_uid,
--        dt + INTERVAL 1 MONTH AS last_dt,
--        dt - INTERVAL 1 MONTH AS next_dt
-- FROM warehouse.dw_yy_month_anchor_live al
-- WHERE dt = '{month}'
-- ;


-- 新增主播（在t-1天主播列表，不在t-2天的列表）
-- CREATE TABLE stage.stage_yy_day_anchor_add_loss AS
DELETE
FROM stage.stage_rpt_yy_month_anchor_add_loss
WHERE add_loss_state = 'add'
  AND dt = '{month}';
INSERT IGNORE INTO stage.stage_rpt_yy_month_anchor_add_loss
-- 存在同一个主播出现在同一个频道
SELECT DATE_FORMAT(dt, '%Y-%m-01') AS dt, platform_name, platform_id, anchor_uid, add_loss_state
FROM stage.stage_rpt_yy_day_anchor_live
WHERE add_loss_state = 'add'
  AND dt >= '{month}'
  AND dt <= LAST_DAY('{month}')
;


-- 流失主播（在t-2天主播列表，不在t-1天的列表）
DELETE
FROM stage.stage_rpt_yy_month_anchor_add_loss
WHERE add_loss_state = 'loss'
  AND dt = '{moth}';
INSERT INTO stage.stage_rpt_yy_month_anchor_add_loss
SELECT DATE_FORMAT(dt, '%Y-%m-01') AS dt, platform_name, platform_id, anchor_uid, add_loss_state
FROM stage.stage_rpt_yy_day_anchor_live
WHERE add_loss_state = 'loss'
  AND dt >= '{month}'
  AND dt <= LAST_DAY('{month}')
;


DELETE
FROM stage.stage_rpt_yy_month_anchor_live
WHERE dt = '{month}';
INSERT INTO stage.stage_rpt_yy_month_anchor_live
SELECT al.*, CASE WHEN aal.add_loss_state IS NULL THEN '' ELSE aal.add_loss_state END AS add_loss_state
FROM warehouse.dw_yy_month_anchor_live al
         LEFT JOIN stage.stage_rpt_yy_month_anchor_add_loss aal
                   ON al.dt = aal.dt AND al.anchor_uid = aal.anchor_uid
WHERE al.dt = '{month}'
UNION ALL
SELECT al.dt + INTERVAL 1 MONTH                                                 AS dt,
       al.platform_id,
       al.platform_name,
       al.backend_account_id,
       al.channel_num,
       al.anchor_no,
       al.anchor_uid,
       al.revenue_level,
       al.newold_state,
       al.active_state,
       al.comment,
       al.live_days,
       al.duration,
       al.bluediamond,
       al.anchor_income_bluediamond,
       al.guild_income_bluediamond,
       al.anchor_commission,
       al.guild_commission,
       al.dt_cnt,
       CASE WHEN aal.add_loss_state IS NULL THEN '' ELSE aal.add_loss_state END AS add_loss_state
FROM warehouse.dw_yy_month_anchor_live al
         INNER JOIN stage.stage_rpt_yy_month_anchor_add_loss aal
                    ON al.dt + INTERVAL 1 MONTH = aal.dt AND al.anchor_uid = aal.anchor_uid
WHERE aal.add_loss_state = 'loss'
  AND al.dt + INTERVAL 1 MONTH = '{month}'
;


-- CREATE TABLE stage.stage_rpt_yy_month_guild_live
DELETE
FROM stage.stage_rpt_yy_month_guild_live
WHERE dt = '{month}';
INSERT INTO stage.stage_rpt_yy_month_guild_live
SELECT DATE_FORMAT(al.dt, '%Y-%m-01')                                                          AS dt,
       al.platform_id,
       al.platform_name,
       al.backend_account_id,
       al.channel_num,
       al.comment,
       al.newold_state,
       al.active_state,
       al.revenue_level,
       COUNT(DISTINCT IF(add_loss_state <> 'loss', al.anchor_uid, NULL))                       AS anchor_cnt,
       COUNT(DISTINCT IF(add_loss_state = 'add', al.anchor_uid, NULL))                         AS add_anchor_cnt,
       COUNT(DISTINCT IF(add_loss_state = 'loss', al.anchor_uid, NULL))                        AS loss_anchor_cnt,
       COUNT(DISTINCT IF(add_loss_state = 'add', al.anchor_uid, NULL)) -
       COUNT(DISTINCT IF(add_loss_state = 'loss', al.anchor_uid, NULL))                        AS increase_anchor_cnt,
       COUNT(DISTINCT IF(add_loss_state <> 'loss' AND al.duration > 0, al.anchor_uid, NULL))   AS anchor_live_cnt,
       SUM(IF(add_loss_state <> 'loss' AND al.duration > 0, al.duration, 0))                   AS duration,
       SUM(IF(add_loss_state <> 'loss' AND al.bluediamond > 0, al.bluediamond, 0))             AS bluediamond,
       SUM(IF(add_loss_state <> 'loss' AND al.anchor_income_bluediamond > 0, al.anchor_income_bluediamond,
              0))                                                                              AS anchor_income_bluediamond,
       SUM(IF(add_loss_state <> 'loss' AND al.guild_income_bluediamond > 0, al.guild_income_bluediamond,
              0))                                                                              AS guild_income_bluediamond,
       SUM(IF(add_loss_state <> 'loss' AND al.anchor_commission > 0, al.anchor_commission, 0)) AS anchor_commission,
       SUM(IF(add_loss_state <> 'loss' AND al.guild_commission > 0, al.guild_commission, 0))   AS guild_commission
FROM stage.stage_rpt_yy_month_anchor_live al
GROUP BY DATE_FORMAT(al.dt, '%Y-%m-01'),
         al.platform_id,
         al.platform_name,
         al.backend_account_id,
         al.channel_num,
         al.comment,
         al.newold_state,
         al.active_state,
         al.revenue_level
;


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
       gl.add_anchor_cnt,
       gl.loss_anchor_cnt,
       gl.increase_anchor_cnt,
       gl.anchor_live_cnt                                           AS live_cnt,
       gl.duration,
       -- 平台流水
       gl.bluediamond                                               AS bluediamond,
       gl.guild_commission / 1000                                   AS guild_commssion_revenue,
       (gl.bluediamond + gl.guild_commission) * 2 / 1000            AS revenue,
       gl.bluediamond + gl.guild_commission                         AS revenue_orig,
       -- 公会收入
       gl.guild_income_bluediamond,
       (gl.guild_income_bluediamond + gl.guild_commission) / 1000   AS guild_income,
       gl.guild_income_bluediamond + gl.guild_commission            AS guild_income_orig,
       -- 主播收入
       (gl.anchor_income_bluediamond + gl.anchor_commission) / 1000 AS anchor_income,
       gl.anchor_income_bluediamond + gl.anchor_commission          AS anchor_income_orig
-- FROM warehouse.dw_yy_month_guild_live gl
FROM stage.stage_rpt_yy_month_guild_live gl
         LEFT JOIN warehouse.platform pf ON gl.platform_id = pf.id
WHERE comment = 'orig'
  AND gl.dt = '{month}'
;


REPLACE INTO bireport.rpt_month_yy_guild (dt, platform_id, platform, channel_num, revenue_level, newold_state,
                                          active_state, anchor_cnt, add_anchor_cnt, loss_anchor_cnt,
                                          increase_anchor_cnt, live_cnt, duration, bluediamond,
                                          guild_commission_revenue, revenue, revenue_orig, guild_income_bluediamond,
                                          guild_income, guild_income_orig, anchor_income, anchor_income_orig)
SELECT *
FROM (SELECT dt,
             MAX(platform_id)              AS platform_id,
             MAX(platform)                    platform,
             IFNULL(channel_num, 'all')    AS channel_num,
             IFNULL(revenue_level, 'all')  AS revenue_level,
             IFNULL(newold_state, 'all')   AS newold_state,
             IFNULL(active_state, 'all')   AS active_state,
             SUM(anchor_cnt)               AS anchor_cnt,
             SUM(add_anchor_cnt)           AS add_anchor_cnt,
             SUM(loss_anchor_cnt)          AS loss_anchor_cnt,
             SUM(increase_anchor_cnt)      AS increase_anchor_cnt,
             SUM(live_cnt)                 AS live_cnt,
             SUM(duration)                 AS duration,
             SUM(bluediamond)              AS anchor_bluediamond_revenue,
             SUM(guild_commission_revenue) AS guild_commission_revenue,
             SUM(revenue)                  AS revenue,
             SUM(revenue_orig)             AS revenue_orig,
             SUM(guild_income_bluediamond) AS guild_income_bluediamond,
             SUM(guild_income)             AS guild_income,
             SUM(guild_income_orig)        AS guild_income_orig,
             SUM(anchor_income)            AS anchor_income,
             SUM(anchor_income_orig)       AS anchor_income_orig
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
             MAX(platform_id)              AS platform_id,
             MAX(platform)                    platform,
             IFNULL(channel_num, 'all')    AS channel_num,
             IFNULL(revenue_level, 'all')  AS revenue_level,
             IFNULL(newold_state, 'all')   AS newold_state,
             IFNULL(active_state, 'all')   AS active_state,
             SUM(anchor_cnt)               AS anchor_cnt,
             SUM(add_anchor_cnt)           AS add_anchor_cnt,
             SUM(loss_anchor_cnt)          AS loss_anchor_cnt,
             SUM(increase_anchor_cnt)      AS increase_anchor_cnt,
             SUM(live_cnt)                 AS live_cnt,
             SUM(duration)                 AS duration,
             SUM(bluediamond)              AS anchor_bluediamond_revenue,
             SUM(guild_commission_revenue) AS guild_commission_revenue,
             SUM(revenue)                  AS revenue,
             SUM(revenue_orig)             AS revenue_orig,
             SUM(guild_income_bluediamond) AS guild_income_bluediamond,
             SUM(guild_income)             AS guild_income,
             SUM(guild_income_orig)        AS guild_income_orig,
             SUM(anchor_income)            AS anchor_income,
             SUM(anchor_income_orig)       AS anchor_income_orig
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
             MAX(platform_id)              AS platform_id,
             MAX(platform)                    platform,
             IFNULL(channel_num, 'all')    AS channel_num,
             IFNULL(revenue_level, 'all')  AS revenue_level,
             IFNULL(newold_state, 'all')   AS newold_state,
             IFNULL(active_state, 'all')   AS active_state,
             SUM(anchor_cnt)               AS anchor_cnt,
             SUM(add_anchor_cnt)           AS add_anchor_cnt,
             SUM(loss_anchor_cnt)          AS loss_anchor_cnt,
             SUM(increase_anchor_cnt)      AS increase_anchor_cnt,
             SUM(live_cnt)                 AS live_cnt,
             SUM(duration)                 AS duration,
             SUM(bluediamond)              AS anchor_bluediamond_revenue,
             SUM(guild_commission_revenue) AS guild_commission_revenue,
             SUM(revenue)                  AS revenue,
             SUM(revenue_orig)             AS revenue_orig,
             SUM(guild_income_bluediamond) AS guild_income_bluediamond,
             SUM(guild_income)             AS guild_income,
             SUM(guild_income_orig)        AS guild_income_orig,
             SUM(anchor_income)            AS anchor_income,
             SUM(anchor_income_orig)       AS anchor_income_orig
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
             MAX(platform_id)              AS platform_id,
             MAX(platform)                    platform,
             IFNULL(channel_num, 'all')    AS channel_num,
             IFNULL(revenue_level, 'all')  AS revenue_level,
             IFNULL(newold_state, 'all')   AS newold_state,
             IFNULL(active_state, 'all')   AS active_state,
             SUM(anchor_cnt)               AS anchor_cnt,
             SUM(add_anchor_cnt)           AS add_anchor_cnt,
             SUM(loss_anchor_cnt)          AS loss_anchor_cnt,
             SUM(increase_anchor_cnt)      AS increase_anchor_cnt,
             SUM(live_cnt)                 AS live_cnt,
             SUM(duration)                 AS duration,
             SUM(bluediamond)              AS anchor_bluediamond_revenue,
             SUM(guild_commission_revenue) AS guild_commission_revenue,
             SUM(revenue)                  AS revenue,
             SUM(revenue_orig)             AS revenue_orig,
             SUM(guild_income_bluediamond) AS guild_income_bluediamond,
             SUM(guild_income)             AS guild_income,
             SUM(guild_income_orig)        AS guild_income_orig,
             SUM(anchor_income)            AS anchor_income,
             SUM(anchor_income_orig)       AS anchor_income_orig
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
             MAX(platform_id)              AS platform_id,
             MAX(platform)                    platform,
             IFNULL(channel_num, 'all')    AS channel_num,
             IFNULL(revenue_level, 'all')  AS revenue_level,
             IFNULL(newold_state, 'all')   AS newold_state,
             IFNULL(active_state, 'all')   AS active_state,
             SUM(anchor_cnt)               AS anchor_cnt,
             SUM(add_anchor_cnt)           AS add_anchor_cnt,
             SUM(loss_anchor_cnt)          AS loss_anchor_cnt,
             SUM(increase_anchor_cnt)      AS increase_anchor_cnt,
             SUM(live_cnt)                 AS live_cnt,
             SUM(duration)                 AS duration,
             SUM(bluediamond)              AS anchor_bluediamond_revenue,
             SUM(guild_commission_revenue) AS guild_commission_revenue,
             SUM(revenue)                  AS revenue,
             SUM(revenue_orig)             AS revenue_orig,
             SUM(guild_income_bluediamond) AS guild_income_bluediamond,
             SUM(guild_income)             AS guild_income,
             SUM(guild_income_orig)        AS guild_income_orig,
             SUM(anchor_income)            AS anchor_income,
             SUM(anchor_income_orig)       AS anchor_income_orig
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

