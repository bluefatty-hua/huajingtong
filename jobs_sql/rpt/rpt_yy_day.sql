-- DELETE
-- FROM bireport.rpt_day_all
-- WHERE platform = 'YY'
--   AND dt BETWEEN '{start_date}' AND '{end_date}';
-- INSERT INTO bireport.rpt_day_all
-- SELECT gl.dt,
--        pf.platform_name                                                       AS platform,
--        gl.anchor_cnt,
--        gl.anchor_live_cnt                                                     AS live_cnt,
--        -- 平台流水
--        ROUND((gl.bluediamond + gl.guild_commission) * 2 / 1000, 2)            AS revenue,
--        -- 公会收入
--        0 as guild_income,
--        -- ROUND((gl.guild_income_bluediamond + gl.guild_commission) / 1000, 2)   AS guild_income,
--        -- 主播收入
--       0 as anchor_income
--        -- ROUND((gl.anchor_income_bluediamond + gl.anchor_commission) / 1000, 2) AS anchor_income
-- FROM (SELECT al.dt,
--              al.platform_id,
--              COUNT(DISTINCT al.anchor_uid)                                                 AS anchor_cnt,
--              COUNT(DISTINCT CASE WHEN al.live_status = 1 THEN al.anchor_uid ELSE NULL END) AS anchor_live_cnt,
--              SUM(IF(al.duration > 0, al.duration, 0))                                      AS duration,
--              SUM(IF(al.bluediamond > 0, al.bluediamond, 0))                                AS bluediamond,
--              SUM(IF(al.bluediamond > 0, al.bluediamond * al.anchor_settle_rate, 0))        AS anchor_income_bluediamond,
--              SUM(IF(al.bluediamond > 0, al.bluediamond * (1 - al.anchor_settle_rate), 0))  AS guild_income_bluediamond,
--              SUM(IF(al.anchor_commission > 0, al.anchor_commission, 0))                    AS anchor_commission,
--              SUM(IF(al.guild_commission > 0, al.guild_commission, 0))                      AS guild_commission
--       FROM warehouse.ods_yy_day_anchor_live al
--       WHERE comment = 'orig'
--         AND al.dt BETWEEN '{start_date}' AND '{end_date}'
--       GROUP BY al.dt,
--                al.platform_id) gl
--          LEFT JOIN warehouse.platform pf ON gl.platform_id = pf.id
-- ;
-- 
-- 
-- DELETE
-- FROM bireport.rpt_day_yy_guild
-- WHERE dt BETWEEN '{start_date}' AND '{end_date}';
-- INSERT INTO bireport.rpt_day_yy_guild
-- SELECT gl.dt,
--        gl.platform_id,
--        pf.platform_name                                                          AS platform_name,
--        gl.channel_num,
--        SUM(gl.anchor_cnt)                                                        AS anchor_cnt,
--        SUM(gl.anchor_live_cnt)                                                   AS live_cnt,
--        -- 平台流水
--        SUM(gl.bluediamond)                                                       AS anchor_bluediamond_revenue,
--        ROUND(SUM(gl.guild_commission) / 1000, 2)                                 AS guild_commssion_revenue,
--        ROUND(SUM(gl.bluediamond + gl.guild_commission) * 2 / 1000, 2)            AS revenue,
--        SUM(gl.bluediamond + gl.guild_commission)                                 AS revenue_orig,
--        -- 公会收入
--        SUM(gl.guild_income_bluediamond),
--        0  AS guild_income,
--        SUM(gl.guild_income_bluediamond + gl.guild_commission)                    AS guild_income_orig,
--        -- 主播收入
--        0 AS anchor_income,
--        SUM(gl.anchor_income_bluediamond + gl.anchor_commission)                  AS anchor_income_orig
-- FROM warehouse.dw_yy_day_guild_live gl
--          LEFT JOIN warehouse.platform pf ON gl.platform_id = pf.id
-- WHERE comment = 'orig'
--   AND gl.dt BETWEEN '{start_date}' AND '{end_date}'
-- GROUP BY gl.dt,
--          gl.platform_id,
--          pf.platform_name,
--          gl.channel_num
-- ;
-- 
-- 
-- -- 补充汇总数据
-- REPLACE INTO bireport.rpt_day_yy_guild
-- (dt, channel_num, anchor_cnt, live_cnt, revenue, guild_income, anchor_income)
-- SELECT dt,
--        'all' AS channel_num,
--        anchor_cnt,
--        live_cnt,
--        revenue,
--        guild_income,
--        anchor_income
-- FROM bireport.rpt_day_all
-- WHERE platform = 'YY'
--   AND dt BETWEEN '{start_date}' AND '{end_date}';


-- -------- new code -------------
-- 计算每日相对前一天新增主播,;
-- 1、取出上月最后一天到当月倒数第二天数据
# DROP TABLE stage.stage_rpt_yy_day_anchor_live_contrast;
# CREATE TABLE stage.stage_rpt_yy_day_anchor_live_contrast AS
DELETE
FROM stage.stage_rpt_yy_day_anchor_live_contrast
WHERE dt >= '{month}'
  AND dt <= LAST_DAY('{month}');
INSERT IGNORE INTO stage.stage_rpt_yy_day_anchor_live_contrast
SELECT dt,
       platform_name,
       platform_id,
       anchor_uid,
       dt + INTERVAL 1 DAY AS last_dt,
       dt - INTERVAL 1 DAY AS next_dt
FROM warehouse.dw_yy_day_anchor_live al
WHERE dt >= '{month}'
  AND dt <= LAST_DAY('{month}')
;


-- 新增主播（在t-1天主播列表，不在t-2天的列表）
-- CREATE TABLE stage.stage_yy_day_anchor_add_loss AS
DELETE
FROM stage.stage_rpt_yy_day_anchor_add_loss
WHERE add_loss_state = 'add'
  AND dt >= '{month}';
INSERT INTO stage.stage_rpt_yy_day_anchor_add_loss
SELECT al1.dt, al1.platform_name, al1.platform_id, al1.anchor_uid, 'add' AS add_loss_state
FROM stage.stage_rpt_yy_day_anchor_live_contrast al1
         LEFT JOIN stage.stage_rpt_yy_day_anchor_live_contrast al2
                   ON al1.dt = al2.last_dt AND al1.anchor_uid = al2.anchor_uid
WHERE al2.anchor_uid IS NULL
  AND al1.dt >= '{month}'
  AND al1.dt <= LAST_DAY('{month}')
;


-- 流失主播（在t-2天主播列表，不在t-1天的列表）
DELETE
FROM stage.stage_rpt_yy_day_anchor_add_loss
WHERE add_loss_state = 'loss'
  AND dt >= '{month}'
  AND dt <= LAST_DAY('{month}');
INSERT INTO stage.stage_rpt_yy_day_anchor_add_loss
SELECT al1.last_dt, al1.platform_name, al1.platform_id, al1.anchor_uid, 'loss' AS add_loss_state
FROM stage.stage_rpt_yy_day_anchor_live_contrast al1
         LEFT JOIN stage.stage_rpt_yy_day_anchor_live_contrast al2
                   ON al1.dt = al2.next_dt AND al1.anchor_uid = al2.anchor_uid
WHERE al2.anchor_uid IS NULL
  AND al1.last_dt <= '2020-03-17'
  AND al1.dt >= '{month}'
  AND al1.dt <= LAST_DAY('{month}')
;


DELETE
FROM stage.stage_rpt_yy_day_guild_live
WHERE dt >= '{month}'
  AND dt <= LAST_DAY('{month}');
INSERT INTO stage.stage_rpt_yy_day_guild_live
SELECT al.dt,
       al.platform_id,
       al.platform_name,
       al.backend_account_id,
       al.channel_num,
       al.comment,
       al.newold_state,
       al.active_state,
       al.revenue_level,
       COUNT(DISTINCT IF(add_loss_state <> 'loss', al.anchor_uid, NULL))                        AS anchor_cnt,
       COUNT(DISTINCT IF(add_loss_state = 'add', al.anchor_uid, NULL))                          AS add_anchor_cnt,
       COUNT(DISTINCT IF(add_loss_state = 'loss', al.anchor_uid, NULL))                         AS loss_anchor_cnt,
       COUNT(DISTINCT IF(add_loss_state = 'add', al.anchor_uid, NULL)) -
       COUNT(DISTINCT IF(add_loss_state = 'loss', al.anchor_uid, NULL))                         AS increase_anchor_cnt,
       COUNT(DISTINCT IF(add_loss_state <> 'loss' AND al.live_status = 1, al.anchor_uid, NULL)) AS anchor_live_cnt,
       SUM(IF(add_loss_state <> 'loss' AND al.duration > 0, al.duration, 0))                    AS duration,
       SUM(IF(add_loss_state <> 'loss' AND al.bluediamond > 0, al.bluediamond, 0))              AS bluediamond,
       SUM(IF(add_loss_state <> 'loss' AND al.bluediamond > 0, al.bluediamond * al.anchor_settle_rate,
              0))                                                                               AS anchor_income_bluediamond,
       SUM(IF(add_loss_state <> 'loss' AND al.bluediamond > 0, al.bluediamond * (1 - al.anchor_settle_rate),
              0))                                                                               AS guild_income_bluediamond,
       SUM(IF(add_loss_state <> 'loss' AND al.anchor_commission > 0, al.anchor_commission, 0))  AS anchor_commission,
       SUM(IF(add_loss_state <> 'loss' AND al.guild_commission > 0, al.guild_commission, 0))    AS guild_commission
FROM (
         SELECT al.*, CASE WHEN aal.add_loss_state IS NULL THEN '' ELSE aal.add_loss_state END AS add_loss_state
         FROM warehouse.dw_yy_day_anchor_live al
                  LEFT JOIN stage.stage_rpt_yy_day_anchor_add_loss aal
                            ON al.dt = aal.dt AND al.anchor_uid = aal.anchor_uid
         WHERE al.dt >= '{month}'
           AND al.dt <= LAST_DAY('{month}')
         UNION ALL
         SELECT al.dt + INTERVAL 1 DAY                                                   AS dt,
                al.platform_id,
                al.platform_name,
                al.backend_account_id,
                al.channel_num,
                al.anchor_uid,
                al.anchor_no,
                al.anchor_nick_name,
                al.anchor_type,
                al.anchor_type_text,
                al.live_room_id,
                al.channel_id,
                al.duration,
                al.mob_duration,
                al.pc_duration,
                al.live_status,
                al.bluediamond,
                al.anchor_commission,
                al.guild_commission,
                al.vir_coin_name,
                al.vir_coin_rate,
                al.include_pf_amt,
                al.pf_amt_rate,
                al.contract_id,
                al.contract_signtime,
                al.contract_endtime,
                al.settle_method_code,
                al.settle_method_text,
                al.anchor_settle_rate,
                al.logo,
                al.comment,
                al.min_live_dt,
                al.min_sign_dt,
                al.newold_state,
                al.month_duration,
                al.month_live_days,
                al.active_state,
                al.month_revenue,
                al.revenue_level,
                CASE WHEN aal.add_loss_state IS NULL THEN '' ELSE aal.add_loss_state END AS add_loss_state
         FROM warehouse.dw_yy_day_anchor_live al
                  INNER JOIN stage.stage_rpt_yy_day_anchor_add_loss aal
                             ON al.dt + INTERVAL 1 DAY = aal.dt AND al.anchor_uid = aal.anchor_uid
         WHERE aal.add_loss_state = 'loss'
           AND al.dt >= '{month}'
           AND al.dt <= LAST_DAY('{month}')
     ) al
GROUP BY al.dt,
         al.platform_id,
         al.platform_name,
         al.backend_account_id,
         al.channel_num,
         al.comment,
         al.newold_state,
         al.active_state,
         al.revenue_level
;


-- rpt_day_yy_guild_new
DELETE
FROM bireport.rpt_day_yy_guild
WHERE dt >= '{month}'
  AND dt <= LAST_DAY('{month}');
INSERT INTO bireport.rpt_day_yy_guild
SELECT gl.dt,
       gl.platform_id,
       pf.platform_name                                    AS platform_name,
       gl.channel_num,
       gl.revenue_level,
       gl.newold_state,
       gl.active_state,
       gl.anchor_cnt,
       gl.add_anchor_cnt,
       gl.loss_anchor_cnt,
       gl.increase_anchor_cnt,
       gl.anchor_live_cnt                                  AS live_cnt,
       gl.duration,
       -- 平台流水
       gl.bluediamond                                      AS anchor_bluediamond_revenue,
       gl.guild_commission / 1000                          AS guild_commssion_revenue,
       (gl.bluediamond + gl.guild_commission) * 2 / 1000   AS revenue,
       gl.bluediamond + gl.guild_commission                AS revenue_orig,
       -- 公会收入
       gl.guild_income_bluediamond,
       0                                                   as guild_income,
       -- ROUND((gl.guild_income_bluediamond + gl.guild_commission) / 1000, 2)   AS guild_income,
       gl.guild_income_bluediamond + gl.guild_commission   AS guild_income_orig,
       -- 主播收入
       0                                                   as anchor_income,
       -- ROUND((gl.anchor_income_bluediamond + gl.anchor_commission) / 1000, 2) AS anchor_income,
       gl.anchor_income_bluediamond + gl.anchor_commission AS anchor_income_orig
# FROM warehouse.dw_yy_day_guild_live gl
FROM stage.stage_rpt_yy_day_guild_live gl
         LEFT JOIN warehouse.platform pf ON gl.platform_id = pf.id
WHERE comment = 'orig'
  AND gl.dt >= '{month}'
  AND dt <= LAST_DAY('{month}')
;


-- 补充汇总数据
REPLACE INTO bireport.rpt_day_yy_guild
(dt,
 platform_id,
 platform,
 channel_num,
 revenue_level,
 newold_state,
 active_state,
 anchor_cnt,
 add_anchor_cnt,
 loss_anchor_cnt,
 increase_anchor_cnt,
 live_cnt,
 duration,
 anchor_bluediamond_revenue,
 guild_commission_revenue,
 revenue,
 revenue_orig,
 guild_income_bluediamond,
 guild_income,
 guild_income_orig,
 anchor_income,
 anchor_income_orig)
SELECT *
FROM (SELECT dt,
             MAX(platform_id)                AS platform_id,
             MAX(platform)                   AS platform,
             IFNULL(channel_num, 'all')      AS channel_num,
             IFNULL(revenue_level, 'all')    AS revenue_level,
             IFNULL(newold_state, 'all')     AS newold_state,
             IFNULL(active_state, 'all')     AS active_state,
             SUM(anchor_cnt)                 AS anchor_cnt,
             SUM(add_anchor_cnt)             AS add_anchor_cnt,
             SUM(loss_anchor_cnt)            AS loss_anchor_cnt,
             SUM(increase_anchor_cnt)        AS increase_anchor_cnt,
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
      FROM bireport.rpt_day_yy_guild
      WHERE active_state != 'all'
        AND channel_num != 'all'
        AND revenue_level != 'all'
        AND newold_state != 'all'
        AND dt >= '{month}'
        AND dt <= LAST_DAY('{month}')
      GROUP BY dt, channel_num, revenue_level, newold_state, active_state
      WITH ROLLUP

      UNION

      SELECT dt,
             MAX(platform_id)                AS platform_id,
             MAX(platform)                   AS platform,
             IFNULL(channel_num, 'all')      AS channel_num,
             IFNULL(revenue_level, 'all')    AS revenue_level,
             IFNULL(newold_state, 'all')     AS newold_state,
             IFNULL(active_state, 'all')     AS active_state,
             SUM(anchor_cnt)                 AS anchor_cnt,
             SUM(add_anchor_cnt)             AS add_anchor_cnt,
             SUM(loss_anchor_cnt)            AS loss_anchor_cnt,
             SUM(increase_anchor_cnt)        AS increase_anchor_cnt,
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
      FROM bireport.rpt_day_yy_guild
      WHERE active_state != 'all'
        AND channel_num != 'all'
        AND revenue_level != 'all'
        AND newold_state != 'all'
        AND dt >= '{month}'
        AND dt <= LAST_DAY('{month}')
      GROUP BY dt, revenue_level, newold_state, active_state, channel_num
      WITH ROLLUP

      UNION

      SELECT dt,
             MAX(platform_id)                AS platform_id,
             MAX(platform)                   AS platform,
             IFNULL(channel_num, 'all')      AS channel_num,
             IFNULL(revenue_level, 'all')    AS revenue_level,
             IFNULL(newold_state, 'all')     AS newold_state,
             IFNULL(active_state, 'all')     AS active_state,
             SUM(anchor_cnt)                 AS anchor_cnt,
             SUM(add_anchor_cnt)             AS add_anchor_cnt,
             SUM(loss_anchor_cnt)            AS loss_anchor_cnt,
             SUM(increase_anchor_cnt)        AS increase_anchor_cnt,
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
      FROM bireport.rpt_day_yy_guild
      WHERE active_state != 'all'
        AND channel_num != 'all'
        AND revenue_level != 'all'
        AND newold_state != 'all'
        AND dt >= '{month}'
        AND dt <= LAST_DAY('{month}')
      GROUP BY dt, newold_state, active_state, channel_num, revenue_level
      WITH ROLLUP

      UNION

      SELECT dt,
             MAX(platform_id)                AS platform_id,
             MAX(platform)                   AS platform,
             IFNULL(channel_num, 'all')      AS channel_num,
             IFNULL(revenue_level, 'all')    AS revenue_level,
             IFNULL(newold_state, 'all')     AS newold_state,
             IFNULL(active_state, 'all')     AS active_state,
             SUM(anchor_cnt)                 AS anchor_cnt,
             SUM(add_anchor_cnt)             AS add_anchor_cnt,
             SUM(loss_anchor_cnt)            AS loss_anchor_cnt,
             SUM(increase_anchor_cnt)        AS increase_anchor_cnt,
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
      FROM bireport.rpt_day_yy_guild
      WHERE active_state != 'all'
        AND channel_num != 'all'
        AND revenue_level != 'all'
        AND newold_state != 'all'
        AND dt >= '{month}'
        AND dt <= LAST_DAY('{month}')
      GROUP BY dt, active_state, channel_num, revenue_level, newold_state
      WITH ROLLUP

      UNION

      SELECT dt,
             MAX(platform_id)                AS platform_id,
             MAX(platform)                   AS platform,
             IFNULL(channel_num, 'all')      AS channel_num,
             IFNULL(revenue_level, 'all')    AS revenue_level,
             IFNULL(newold_state, 'all')     AS newold_state,
             IFNULL(active_state, 'all')     AS active_state,
             SUM(anchor_cnt)                 AS anchor_cnt,
             SUM(add_anchor_cnt)             AS add_anchor_cnt,
             SUM(loss_anchor_cnt)            AS loss_anchor_cnt,
             SUM(increase_anchor_cnt)        AS increase_anchor_cnt,
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
      FROM bireport.rpt_day_yy_guild
      WHERE active_state != 'all'
        AND channel_num != 'all'
        AND revenue_level != 'all'
        AND newold_state != 'all'
        AND dt >= '{month}'
        AND dt <= LAST_DAY('{month}')
      GROUP BY dt, active_state, revenue_level, channel_num, newold_state
      WITH ROLLUP
     ) t
WHERE dt IS NOT NULL
;


-- 报表用，计算上周、上月同期数据---
DELETE
FROM bireport.rpt_day_yy_guild_view
WHERE dt >= '{month}'
  AND dt <= LAST_DAY('{month}');
INSERT INTO bireport.rpt_day_yy_guild_view
SELECT t1.dt,
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
       IF(t3.live_cnt > 0, ROUND(t3.revenue / t3.live_cnt, 0), 0)      AS revenue_per_live_lastmonth,
       t1.guild_income,
       t1.anchor_income
FROM bireport.rpt_day_yy_guild t1
         LEFT JOIN bireport.rpt_day_yy_guild t2
                   ON t1.dt - INTERVAL 7 DAY = t2.dt
                       AND t1.channel_num = t2.channel_num
                       AND t1.revenue_level = t2.revenue_level
                       AND t1.newold_state = t2.newold_state
                       AND t1.active_state = t2.active_state
         LEFT JOIN bireport.rpt_day_yy_guild t3
                   ON t1.dt - INTERVAL 1 MONTH = t3.dt
                       AND t1.channel_num = t3.channel_num
                       AND t1.revenue_level = t3.revenue_level
                       AND t1.newold_state = t3.newold_state
                       AND t1.active_state = t3.active_state
WHERE t1.dt >= '{month}'
  AND t1.dt <= LAST_DAY('{month}');


-- 报表用，计算指标占比---
DELETE
FROM bireport.rpt_day_yy_guild_view_compare
WHERE dt >= '{month}'
  AND dt <= LAST_DAY('{month}');
INSERT INTO bireport.rpt_day_yy_guild_view_compare
SELECT *
FROM (SELECT dt,
             channel_num,
             revenue_level,
             newold_state,
             active_state,
             '主播数'      AS idx,
             anchor_cnt AS val
      FROM bireport.rpt_day_yy_guild
      WHERE dt >= '{month}'
        AND dt <= LAST_DAY('{month}')
        AND revenue_level != 'all'
      UNION
      SELECT dt,
             channel_num,
             revenue_level,
             newold_state,
             active_state,
             '开播数'    AS idx,
             live_cnt AS val
      FROM bireport.rpt_day_yy_guild
      WHERE dt >= '{month}'
        AND dt <= LAST_DAY('{month}')
        AND revenue_level != 'all'
      UNION
      SELECT dt,
             channel_num,
             revenue_level,
             newold_state,
             active_state,
             '流水'    AS idx,
             revenue AS val
      FROM bireport.rpt_day_yy_guild
      WHERE dt >= '{month}'
        AND dt <= LAST_DAY('{month}')
        AND revenue_level != 'all'
      UNION
      SELECT dt,
             channel_num,
             revenue_level,
             newold_state,
             active_state,
             '开播人均流水'                     AS idx,
             round(revenue / live_cnt, 0) AS val
      FROM bireport.rpt_day_yy_guild
      WHERE dt >= '{month}'
        AND dt <= LAST_DAY('{month}')
        AND revenue_level != 'all'
        AND live_cnt > 0) t;


-- 主播数据 --- 
DELETE
FROM bireport.rpt_day_yy_anchor
WHERE dt >= '{month}'
  AND dt <= LAST_DAY('{month}');
INSERT INTO bireport.rpt_day_yy_anchor
SELECT al.dt,
       al.channel_num,
       al.min_live_dt                   AS first_live_date,
       al.min_sign_dt                   AS sign_date,
       al.newold_state,
       al1.month_duration / 3600        AS duration_lastmonth,
       al1.month_live_days              AS live_days_lastmonth,
       al.active_state,
       al1.month_revenue * 2 / 1000     AS revenue_lastmonth,
       al.revenue_level,
       al.anchor_uid,
       al.anchor_no,
       al.anchor_nick_name,
       al.anchor_type_text,
       al.duration / 3600               AS duration,
       IF(al.live_status = 1, '是', '否') AS live_status,
       al.bluediamond * 2 / 1000        AS revenue
FROM warehouse.dw_yy_day_anchor_live al
         LEFT JOIN warehouse.dw_yy_day_anchor_live al1
                   ON al1.dt = DATE_FORMAT(al.dt - INTERVAL 1 MONTH, '%Y-%m-01') AND
                      al.channel_num = al1.channel_num AND
                      al.anchor_no = al1.anchor_no
WHERE al.dt >= '{month}'
  AND al.dt <= LAST_DAY('{month}')
;
