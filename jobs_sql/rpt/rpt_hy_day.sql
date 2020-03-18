-- DELETE
-- FROM bireport.rpt_day_all
-- WHERE platform = '虎牙'
--   AND dt BETWEEN '{start_date}' AND '{end_date}';
-- INSERT INTO bireport.rpt_day_all
-- SELECT t.dt,
--        t.platform,
--        CASE WHEN t.dt <= '2019-11-31' THEN t.anchor_cnt ELSE al.anchor_cnt END AS anchor_cnt,
--        CASE WHEN t.dt <= '2019-11-31' THEN t.live_cnt ELSE al.live_cnt END     AS live_cnt,
--        t.revenue,
--        t.guild_income,
--        t.anchor_income
-- FROM (
--          SELECT t.dt,
--                 t.platform_name                                                           AS platform,
--                 SUM(t.sign_count)                                                         AS anchor_cnt,
--                 SUM(t.live_cnt)                                                           AS live_cnt,
--                 SUM(t.revenue)                                                            AS revenue,
--                 SUM(t.gift_income + t.guard_income + t.noble_income) / 1000               AS guild_income,
--                 SUM(t.gift_income + t.guard_income + t.noble_income) * 0.7 / (0.3 * 1000) AS anchor_income
--          FROM warehouse.dw_huya_day_guild_live_true t
--          WHERE dt BETWEEN '{start_date}' AND '{end_date}'
--          GROUP BY t.dt,
--                   t.platform_name) t
--          LEFT JOIN (SELECT dt,
--                            COUNT(DISTINCT t.anchor_uid)                                       AS anchor_cnt,
--                            COUNT(DISTINCT
--                                  CASE WHEN t.live_status = 1 THEN t.anchor_uid ELSE NULL END) AS live_cnt,
--                            SUM(t.revenue)                                                     AS anchor_income
--                     FROM warehouse.dw_huya_day_anchor_live t
--                     WHERE dt BETWEEN '{start_date}' AND '{end_date}'
--                     GROUP BY dt) al ON al.dt = t.dt
-- ;
-- 
-- 
-- DELETE
-- FROM bireport.rpt_day_hy_guild
-- WHERE dt BETWEEN '{start_date}' AND '{end_date}';
-- INSERT INTO bireport.rpt_day_hy_guild
-- SELECT t.dt,
--        t.platform_id,
--        t.platform_name                                                         AS platform,
--        t.channel_num,
--        CASE WHEN t.dt <= '2019-11-31' THEN t.sign_count ELSE al.anchor_cnt END AS anchor_cnt,
--        CASE WHEN t.dt <= '2019-11-31' THEN t.live_cnt ELSE al.live_cnt END     AS live_cnt,
--        t.revenue,
--        t.revenue                                                               AS revenue_orig,
--        (t.gift_income + t.guard_income + t.noble_income) / 1000                AS guild_income,
--        t.gift_income + t.guard_income + t.noble_income                         AS guild_income_orig,
--        (t.gift_income + t.guard_income + t.noble_income) * 0.7 / (0.3 * 1000)  AS anchor_income,
--        t.gift_income + t.guard_income + t.noble_income                         AS anchor_incom_orig
-- FROM warehouse.dw_huya_day_guild_live_true t
--          LEFT JOIN (SELECT dt,
--                            t.channel_id,
--                            COUNT(DISTINCT t.anchor_uid)                                       AS anchor_cnt,
--                            COUNT(DISTINCT
--                                  CASE WHEN t.live_status = 1 THEN t.anchor_uid ELSE NULL END) AS live_cnt,
--                            SUM(t.revenue)                                                     AS revenue
--                     FROM warehouse.dw_huya_day_anchor_live t
--                     WHERE dt BETWEEN '{start_date}' AND '{end_date}'
--                     GROUP BY dt,
--                              t.channel_id) al ON al.dt = t.dt AND al.channel_id = t.channel_id
-- WHERE t.dt BETWEEN '{start_date}' AND '{end_date}'
-- ;
-- 
-- 
-- -- 补充汇总数据
-- REPLACE INTO bireport.rpt_day_hy_guild
-- (dt, channel_num, anchor_cnt, live_cnt, revenue, guild_income, anchor_income)
-- SELECT dt,
--        'all' AS channel_num,
--        anchor_cnt,
--        live_cnt,
--        revenue,
--        guild_income,
--        anchor_income
-- FROM bireport.rpt_day_all
-- WHERE platform = '虎牙'
--   AND dt BETWEEN '{start_date}' AND '{end_date}'
-- ;
-- =======================================================================
-- 计算每日相对前一天新增主播,;
-- 1、取出上月最后一天到当月倒数第二天数据
# DROP TABLE stage.stage_rpt_hy_day_anchor_live_contrast;
# CREATE TABLE stage.stage_rpt_hy_day_anchor_live_contrast AS
DELETE
FROM stage.stage_rpt_hy_day_anchor_live_contrast
WHERE dt >= '{month}'
  AND dt <= LAST_DAY('{month}');
INSERT IGNORE INTO stage.stage_rpt_hy_day_anchor_live_contrast
SELECT dt,
       platform_name,
       platform_id,
       anchor_uid,
       dt + INTERVAL 1 DAY AS last_dt,
       dt - INTERVAL 1 DAY AS next_dt
FROM warehouse.dw_huya_day_anchor_live al
WHERE dt >= '{month}'
  AND dt <= LAST_DAY('{month}')
;


-- 新增主播（在t-1天主播列表，不在t-2天的列表）
-- CREATE TABLE stage.stage_hy_day_anchor_add_loss AS
DELETE
FROM stage.stage_rpt_hy_day_anchor_add_loss
WHERE add_loss_state = 'add'
  AND dt >= '{month}';
INSERT INTO stage.stage_rpt_hy_day_anchor_add_loss
SELECT al1.dt, al1.platform_name, al1.platform_id, al1.anchor_uid, 'add' AS add_loss_state
FROM stage.stage_rpt_hy_day_anchor_live_contrast al1
         LEFT JOIN stage.stage_rpt_hy_day_anchor_live_contrast al2
                   ON al1.dt = al2.last_dt AND al1.anchor_uid = al2.anchor_uid
WHERE al2.anchor_uid IS NULL
  AND al1.dt >= '{month}'
  AND al1.dt <= LAST_DAY('{month}')
;


-- 流失主播（在t-2天主播列表，不在t-1天的列表）
DELETE
FROM stage.stage_rpt_hy_day_anchor_add_loss
WHERE add_loss_state = 'loss'
  AND dt >= '{month}'
  AND dt <= LAST_DAY('{month}');
INSERT INTO stage.stage_rpt_hy_day_anchor_add_loss
SELECT al1.last_dt, al1.platform_name, al1.platform_id, al1.anchor_uid, 'loss' AS add_loss_state
FROM stage.stage_rpt_hy_day_anchor_live_contrast al1
         LEFT JOIN stage.stage_rpt_hy_day_anchor_live_contrast al2
                   ON al1.dt = al2.next_dt AND al1.anchor_uid = al2.anchor_uid
WHERE al2.anchor_uid IS NULL
  AND al1.last_dt <= '2020-03-16'
  AND al1.dt >= '{month}'
  AND al1.dt <= LAST_DAY('{month}')
;


DELETE
FROM stage.stage_rpt_hy_day_guild_live
WHERE dt >= '{month}'
  AND dt <= LAST_DAY('{month}');
INSERT INTO stage.stage_rpt_hy_day_guild_live
SELECT dt,
       channel_type,
       channel_num,
       revenue_level,
       newold_state,
       active_state,
       COUNT(DISTINCT IF(add_loss_state <> 'loss', al.anchor_uid, NULL))                        AS anchor_cnt,
       COUNT(DISTINCT IF(add_loss_state = 'add', al.anchor_uid, NULL))                          AS add_anchor_cnt,
       COUNT(DISTINCT IF(add_loss_state = 'loss', al.anchor_uid, NULL))                         AS loss_anchor_cnt,
       COUNT(DISTINCT IF(add_loss_state = 'add', al.anchor_uid, NULL)) -
       COUNT(DISTINCT IF(add_loss_state = 'loss', al.anchor_uid, NULL))                         AS increase_anchor_cnt,
       COUNT(DISTINCT IF(add_loss_state <> 'loss' AND al.live_status = 1, al.anchor_uid, NULL)) AS anchor_live_cnt,
       SUM(IF(add_loss_state <> 'loss' AND al.duration > 0, al.duration, 0))                    AS duration,
       SUM(IF(add_loss_state <> 'loss' AND al.revenue > 0, al.revenue, 0))                      AS revenue,
       SUM(IF(add_loss_state <> 'loss' AND al.revenue_orig > 0, al.revenue_orig, 0))            AS revenue_orig
FROM (
         SELECT al.*, CASE WHEN aal.add_loss_state IS NULL THEN '' ELSE aal.add_loss_state END AS add_loss_state
         FROM warehouse.dw_huya_day_anchor_live al
                  LEFT JOIN stage.stage_rpt_hy_day_anchor_add_loss aal
                            ON al.dt = aal.dt AND al.anchor_uid = aal.anchor_uid
         WHERE al.dt >= '{month}'
           AND al.dt <= LAST_DAY('{month}')
         UNION ALL
         SELECT al.dt + INTERVAL 1 DAY                                                   AS dt,
                al.platform_id,
                al.platform_name,
                al.channel_type,
                al.channel_id,
                al.channel_num,
                al.anchor_uid,
                al.anchor_no,
                al.nick,
                al.comment,
                al.duration,
                al.live_status,
                al.revenue,
                al.revenue_orig,
                al.peak_pcu,
                al.activity_days,
                al.months,
                al.ow_percent,
                al.sign_time,
                al.sign_date,
                al.surplus_days,
                al.avatar,
                al.min_live_dt,
                al.min_sign_dt,
                al.newold_state,
                al.month_duration,
                al.month_live_days,
                al.active_state,
                al.month_revenue,
                al.revenue_level,
                al.vir_coin_name,
                al.vir_coin_rate,
                al.include_pf_amt,
                al.pf_amt_rate,
                CASE WHEN aal.add_loss_state IS NULL THEN '' ELSE aal.add_loss_state END AS add_loss_state
         FROM warehouse.dw_huya_day_anchor_live al
                  INNER JOIN stage.stage_rpt_hy_day_anchor_add_loss aal
                             ON al.dt + INTERVAL 1 DAY = aal.dt AND al.anchor_uid = aal.anchor_uid
         WHERE aal.add_loss_state = 'loss'
           AND al.dt >= '{month}'
           AND al.dt <= LAST_DAY('{month}')
     ) al
GROUP BY dt,
         channel_type,
         channel_num,
         revenue_level,
         newold_state,
         active_state
;


-- rpt_day_hy_guild_new
DELETE
FROM bireport.rpt_day_hy_guild
WHERE dt >= '{month}'
  AND dt <= LAST_DAY('{month}');
INSERT INTO bireport.rpt_day_hy_guild
SELECT dt,
       channel_type,
       channel_num,
       revenue_level,
       newold_state,
       active_state,
       SUM(anchor_cnt)          AS anchor_cnt,
       SUM(add_anchor_cnt)      AS add_anchor_cnt,
       SUM(loss_anchor_cnt)     AS loss_anchor_cnt,
       SUM(increase_anchor_cnt) AS increase_anchor_cnt,
       SUM(anchor_live_cnt)     AS live_cnt,
       SUM(duration)            AS duration,
       SUM(revenue)             AS revenue,
       SUM(revenue_orig)        AS revenue_orig
FROM stage.stage_rpt_hy_day_guild_live
WHERE dt >= '{month}'
  AND dt <= LAST_DAY('{month}')
GROUP BY dt,
         channel_type,
         channel_num,
         revenue_level,
         newold_state,
         active_state
;



REPLACE INTO bireport.rpt_day_hy_guild (dt, channel_type, channel_num, revenue_level, newold_state, active_state,
                                        anchor_cnt, add_anchor_cnt, loss_anchor_cnt, increase_anchor_cnt, live_cnt,
                                        duration, revenue, revenue_orig)
SELECT *
FROM (SELECT dt,
             IFNULL(channel_type, 'all')  AS channel_type,
             IFNULL(channel_num, 'all')   AS channel_num,
             IFNULL(revenue_level, 'all') AS revenue_level,
             IFNULL(newold_state, 'all')  AS newold_state,
             IFNULL(active_state, 'all')  AS active_state,
             SUM(anchor_cnt)              AS anchor_cnt,
             SUM(add_anchor_cnt)          AS add_anchor_cnt,
             SUM(loss_anchor_cnt)         AS loss_anchor_cnt,
             SUM(increase_anchor_cnt)     AS increase_anchor_cnt,
             SUM(live_cnt)                AS live_cnt,
             SUM(duration)                AS duration,
             SUM(revenue)                 AS revenue,
             SUM(revenue_orig)            AS revenue_orig
      FROM bireport.rpt_day_hy_guild
      WHERE channel_type != 'all'
        AND channel_num != 'all'
        AND revenue_level != 'all'
        AND newold_state != 'all'
        AND active_state != 'all'
        AND dt >= '{month}'
        AND dt <= LAST_DAY('{month}')
      GROUP BY dt, channel_type, channel_num, revenue_level, newold_state, active_state
      WITH ROLLUP

      UNION

      SELECT dt,
             IFNULL(channel_type, 'all')  AS channel_type,
             IFNULL(channel_num, 'all')   AS channel_num,
             IFNULL(revenue_level, 'all') AS revenue_level,
             IFNULL(newold_state, 'all')  AS newold_state,
             IFNULL(active_state, 'all')  AS active_state,
             SUM(anchor_cnt)              AS anchor_cnt,
             SUM(add_anchor_cnt)          AS add_anchor_cnt,
             SUM(loss_anchor_cnt)         AS loss_anchor_cnt,
             SUM(increase_anchor_cnt)     AS increase_anchor_cnt,
             SUM(live_cnt)                AS live_cnt,
             SUM(duration)                AS duration,
             SUM(revenue)                 AS revenue,
             SUM(revenue_orig)            AS revenue_orig
      FROM bireport.rpt_day_hy_guild
      WHERE channel_type != 'all'
        AND channel_num != 'all'
        AND revenue_level != 'all'
        AND newold_state != 'all'
        AND active_state != 'all'
        AND dt >= '{month}'
        AND dt <= LAST_DAY('{month}')
      GROUP BY dt, channel_num, revenue_level, newold_state, active_state, channel_type
      WITH ROLLUP

      UNION

      SELECT dt,
             IFNULL(channel_type, 'all')  AS channel_type,
             IFNULL(channel_num, 'all')   AS channel_num,
             IFNULL(revenue_level, 'all') AS revenue_level,
             IFNULL(newold_state, 'all')  AS newold_state,
             IFNULL(active_state, 'all')  AS active_state,
             SUM(anchor_cnt)              AS anchor_cnt,
             SUM(add_anchor_cnt)          AS add_anchor_cnt,
             SUM(loss_anchor_cnt)         AS loss_anchor_cnt,
             SUM(increase_anchor_cnt)     AS increase_anchor_cnt,
             SUM(live_cnt)                AS live_cnt,
             SUM(duration)                AS duration,
             SUM(revenue)                 AS revenue,
             SUM(revenue_orig)            AS revenue_orig
      FROM bireport.rpt_day_hy_guild
      WHERE channel_type != 'all'
        AND channel_num != 'all'
        AND revenue_level != 'all'
        AND newold_state != 'all'
        AND active_state != 'all'
        AND dt >= '{month}'
        AND dt <= LAST_DAY('{month}')
      GROUP BY dt, revenue_level, newold_state, active_state, channel_type, channel_num
      WITH ROLLUP

      UNION

      SELECT dt,
             IFNULL(channel_type, 'all')  AS channel_type,
             IFNULL(channel_num, 'all')   AS channel_num,
             IFNULL(revenue_level, 'all') AS revenue_level,
             IFNULL(newold_state, 'all')  AS newold_state,
             IFNULL(active_state, 'all')  AS active_state,
             SUM(anchor_cnt)              AS anchor_cnt,
             SUM(add_anchor_cnt)          AS add_anchor_cnt,
             SUM(loss_anchor_cnt)         AS loss_anchor_cnt,
             SUM(increase_anchor_cnt)     AS increase_anchor_cnt,
             SUM(live_cnt)                AS live_cnt,
             SUM(duration)                AS duration,
             SUM(revenue)                 AS revenue,
             SUM(revenue_orig)            AS revenue_orig
      FROM bireport.rpt_day_hy_guild
      WHERE channel_type != 'all'
        AND channel_num != 'all'
        AND revenue_level != 'all'
        AND newold_state != 'all'
        AND active_state != 'all'
        AND dt >= '{month}'
        AND dt <= LAST_DAY('{month}')
      GROUP BY dt, newold_state, active_state, channel_type, channel_num, revenue_level
      WITH ROLLUP

      UNION

      SELECT dt,
             IFNULL(channel_type, 'all')  AS channel_type,
             IFNULL(channel_num, 'all')   AS channel_num,
             IFNULL(revenue_level, 'all') AS revenue_level,
             IFNULL(newold_state, 'all')  AS newold_state,
             IFNULL(active_state, 'all')  AS active_state,
             SUM(anchor_cnt)              AS anchor_cnt,
             SUM(add_anchor_cnt)          AS add_anchor_cnt,
             SUM(loss_anchor_cnt)         AS loss_anchor_cnt,
             SUM(increase_anchor_cnt)     AS increase_anchor_cnt,
             SUM(live_cnt)                AS live_cnt,
             SUM(duration)                AS duration,
             SUM(revenue)                 AS revenue,
             SUM(revenue_orig)            AS revenue_orig
      FROM bireport.rpt_day_hy_guild
      WHERE channel_type != 'all'
        AND channel_num != 'all'
        AND revenue_level != 'all'
        AND newold_state != 'all'
        AND active_state != 'all'
        AND dt >= '{month}'
        AND dt <= LAST_DAY('{month}')
      GROUP BY dt, active_state, channel_type, channel_num, revenue_level, newold_state
      WITH ROLLUP

      UNION

      SELECT dt,
             IFNULL(channel_type, 'all')  AS channel_type,
             IFNULL(channel_num, 'all')   AS channel_num,
             IFNULL(revenue_level, 'all') AS revenue_level,
             IFNULL(newold_state, 'all')  AS newold_state,
             IFNULL(active_state, 'all')  AS active_state,
             SUM(anchor_cnt)              AS anchor_cnt,
             SUM(add_anchor_cnt)          AS add_anchor_cnt,
             SUM(loss_anchor_cnt)         AS loss_anchor_cnt,
             SUM(increase_anchor_cnt)     AS increase_anchor_cnt,
             SUM(live_cnt)                AS live_cnt,
             SUM(duration)                AS duration,
             SUM(revenue)                 AS revenue,
             SUM(revenue_orig)            AS revenue_orig
      FROM bireport.rpt_day_hy_guild
      WHERE channel_type != 'all'
        AND channel_num != 'all'
        AND revenue_level != 'all'
        AND newold_state != 'all'
        AND active_state != 'all'
        AND dt >= '{month}'
        AND dt <= LAST_DAY('{month}')
      GROUP BY dt, channel_num, channel_type, active_state, revenue_level, newold_state
      WITH ROLLUP

      UNION

      SELECT dt,
             IFNULL(channel_type, 'all')  AS channel_type,
             IFNULL(channel_num, 'all')   AS channel_num,
             IFNULL(revenue_level, 'all') AS revenue_level,
             IFNULL(newold_state, 'all')  AS newold_state,
             IFNULL(active_state, 'all')  AS active_state,
             SUM(anchor_cnt)              AS anchor_cnt,
             SUM(add_anchor_cnt)          AS add_anchor_cnt,
             SUM(loss_anchor_cnt)         AS loss_anchor_cnt,
             SUM(increase_anchor_cnt)     AS increase_anchor_cnt,
             SUM(live_cnt)                AS live_cnt,
             SUM(duration)                AS duration,
             SUM(revenue)                 AS revenue,
             SUM(revenue_orig)            AS revenue_orig
      FROM bireport.rpt_day_hy_guild
      WHERE channel_type != 'all'
        AND channel_num != 'all'
        AND revenue_level != 'all'
        AND newold_state != 'all'
        AND active_state != 'all'
        AND dt >= '{month}'
        AND dt <= LAST_DAY('{month}')
      GROUP BY dt, newold_state, channel_type, channel_num, revenue_level, active_state
      WITH ROLLUP

      UNION

      SELECT dt,
             IFNULL(channel_type, 'all')  AS channel_type,
             IFNULL(channel_num, 'all')   AS channel_num,
             IFNULL(revenue_level, 'all') AS revenue_level,
             IFNULL(newold_state, 'all')  AS newold_state,
             IFNULL(active_state, 'all')  AS active_state,
             SUM(anchor_cnt)              AS anchor_cnt,
             SUM(add_anchor_cnt)          AS add_anchor_cnt,
             SUM(loss_anchor_cnt)         AS loss_anchor_cnt,
             SUM(increase_anchor_cnt)     AS increase_anchor_cnt,
             SUM(live_cnt)                AS live_cnt,
             SUM(duration)                AS duration,
             SUM(revenue)                 AS revenue,
             SUM(revenue_orig)            AS revenue_orig
      FROM bireport.rpt_day_hy_guild
      WHERE channel_type != 'all'
        AND channel_num != 'all'
        AND revenue_level != 'all'
        AND newold_state != 'all'
        AND active_state != 'all'
        AND dt >= '{month}'
        AND dt <= LAST_DAY('{month}')
      GROUP BY dt, active_state, channel_type, newold_state, revenue_level, channel_num
      WITH ROLLUP
     ) t
WHERE dt IS NOT NULL
;


-- 报表用，计算上周、上月同期数据---
DELETE
FROM bireport.rpt_day_hy_guild_view
WHERE dt >= '{month}'
  AND dt <= LAST_DAY('{month}');
INSERT INTO bireport.rpt_day_hy_guild_view
SELECT t1.dt,
       t1.channel_type,
       t1.channel_num,
       t1.revenue_level,
       t1.newold_state,
       t1.active_state,
       t1.anchor_cnt,
       t1.add_anchor_cnt,
       t1.loss_anchor_cnt,
       t1.increase_anchor_cnt,
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
FROM bireport.rpt_day_hy_guild t1
         LEFT JOIN bireport.rpt_day_hy_guild t2
                   ON t1.dt - INTERVAL 7 DAY = t2.dt
                       AND t1.channel_type = t2.channel_type
                       AND t1.channel_num = t2.channel_num
                       AND t1.revenue_level = t2.revenue_level
                       AND t1.newold_state = t2.newold_state
                       AND t1.active_state = t2.active_state
         LEFT JOIN bireport.rpt_day_hy_guild t3
                   ON t1.dt - INTERVAL 1 MONTH = t3.dt
                       AND t1.channel_type = t3.channel_type
                       AND t1.channel_num = t3.channel_num
                       AND t1.revenue_level = t3.revenue_level
                       AND t1.newold_state = t3.newold_state
                       AND t1.active_state = t3.active_state
WHERE t1.dt >= '{month}'
  AND t1.dt <= LAST_DAY('{month}')
;


-- 报表用，计算指标占比---
DELETE
FROM bireport.rpt_day_hy_guild_view_compare
WHERE dt >= '{month}'
  AND dt <= LAST_DAY('{month}');
INSERT INTO bireport.rpt_day_hy_guild_view_compare
SELECT *
FROM (SELECT dt,
             channel_num,
             revenue_level,
             newold_state,
             active_state,
             '主播数'      AS idx,
             anchor_cnt AS val
      FROM bireport.rpt_day_hy_guild
      where revenue_level != 'all'
        AND channel_type = 'all'
        AND dt >= '{month}'
        AND dt <= LAST_DAY('{month}')
      UNION
      SELECT dt,
             channel_num,
             revenue_level,
             newold_state,
             active_state,
             '开播数'    AS idx,
             live_cnt AS val
      FROM bireport.rpt_day_hy_guild
      WHERE revenue_level != 'all'
        AND channel_type = 'all'
        AND dt >= '{month}'
        AND dt <= LAST_DAY('{month}')
      UNION
      SELECT dt,
             channel_num,
             revenue_level,
             newold_state,
             active_state,
             '流水'    AS idx,
             revenue AS val
      FROM bireport.rpt_day_hy_guild
      WHERE revenue_level != 'all'
        AND channel_type = 'all'
        AND dt >= '{month}'
        AND dt <= LAST_DAY('{month}')
      UNION
      SELECT dt,
             channel_num,
             revenue_level,
             newold_state,
             active_state,
             '开播人均流水'                     AS idx,
             round(revenue / live_cnt, 0) AS val
      FROM bireport.rpt_day_hy_guild
      WHERE revenue_level != 'all'
        AND channel_type = 'all'
        AND dt >= '{month}'
        AND dt <= LAST_DAY('{month}')
        AND live_cnt > 0) t
;


-- 主播数据 --- 
DELETE
FROM bireport.rpt_day_hy_anchor
WHERE dt >= '{month}'
  AND dt <= LAST_DAY('{month}');
INSERT INTO bireport.rpt_day_hy_anchor
SELECT al.dt,
       al.channel_type,
       al.channel_num,
       al.min_live_dt                   AS first_live_date,
       al.min_sign_dt                   AS sign_date,
       al.newold_state,
       al1.month_duration / 3600        AS duration_lastmonth,
       al1.month_live_days              AS live_days_lastmonth,
       al.active_state,
       al1.month_revenue                AS revenue_lastmonth,
       al.revenue_level,
       al.anchor_uid,
       al.anchor_no,
       al.nick                          AS anchor_nick_name,
       al.duration / 3600               AS duration,
       IF(al.live_status = 1, '是', '否') AS live_status,
       al.revenue
FROM warehouse.dw_huya_day_anchor_live al
         LEFT JOIN warehouse.dw_huya_day_anchor_live al1
                   ON al1.dt = DATE_FORMAT(al.dt - INTERVAL 1 MONTH, '%Y-%m-01') AND
                      al.channel_id = al1.channel_id AND
                      al.anchor_no = al1.anchor_no
WHERE al.dt >= '{month}'
  AND al.dt <= LAST_DAY('{month}')
;
