-- 主播最早开播时间（基于现有数据）
-- DROP TABLE IF EXISTS stage.stage_yy_anchor_min_live_dt;
-- CREATE TABLE stage.stage_yy_anchor_min_live_dt
INSERT IGNORE INTO stage.stage_yy_anchor_min_live_dt
SELECT 1000 AS platform_id,
       t.anchor_no,
       MIN(t.min_live_dt)
FROM (SELECT al.anchor_no,
             MIN(dt) AS min_live_dt
      FROM warehouse.ods_yy_day_anchor_live al
      WHERE al.live_status = 1
      GROUP BY al.anchor_no
      UNION
      SELECT yj.uid             AS anchor_no,
             yj.first_live_time AS min_live_time
      FROM warehouse.ods_yujia_anchor_list yj
      WHERE platform = 'YY'
        AND first_live_time != '1970-01-01'
        AND uid = '') t
GROUP BY t.anchor_no
;


-- 主播最早签约时间（基于现有数据）,后期结合公司现有主播的签约时间
-- DROP TABLE IF EXISTS stage.stage_yy_anchor_min_sign_dt;
-- CREATE TABLE stage.stage_yy_anchor_min_sign_dt
INSERT IGNORE INTO stage.stage_yy_anchor_min_sign_dt
SELECT 1000             AS platform_id,
       t.anchor_no,
       MIN(min_sign_dt) AS min_sign_dt
FROM (SELECT al.anchor_no,
             MIN(contract_signtime) AS min_sign_dt
      FROM warehouse.ods_yy_day_anchor_live al
      WHERE al.contract_signtime IS NOT NULL
--   AND al.comment = 'orig'
      GROUP BY al.platform_id,
               al.anchor_no
      UNION
      SELECT yj.uid       AS anchor_no,
             yj.sign_time AS min_sign_dt
      FROM warehouse.ods_yujia_anchor_list yj
      WHERE platform = 'YY'
        AND yj.sign_time <> '1970-01-01'
     ) t
GROUP BY t.anchor_no
;


-- 计算每月主播开播天数，开播时长，流水
-- DROP TABLE IF EXISTS stage.stage_yy_month_anchor_live;
-- CREATE TABLE stage.stage_yy_month_anchor_live
DELETE
FROM stage.stage_yy_month_anchor_live
WHERE dt = '{month}';
INSERT INTO stage.stage_yy_month_anchor_live
SELECT t.dt,
       t.platform_id,
       t.anchor_uid,
       t.revenue,
       CASE
           WHEN t.revenue * 2 / 1000 / 10000 >= 50 THEN '50+'
           WHEN t.revenue * 2 / 1000 / 10000 >= 10 THEN '10-50'
           WHEN t.revenue * 2 / 1000 / 10000 >= 3 THEN '3-10'
           WHEN t.revenue * 2 / 1000 / 10000 > 0 THEN '0-3'
           ELSE '0' END     AS revenue_level,
       t.live_days,
       t.duration,
       CASE
           WHEN t.live_days >= 20 AND t.duration >= 60 * 60 * 60 THEN '活跃主播'
           ELSE '非活跃主播' END AS active_state
FROM (
         SELECT DATE_FORMAT(al.dt, '%Y-%m-01')                                     AS dt,
                al.platform_id,
                al.anchor_uid,
                SUM(bluediamond)                                                   AS revenue,
                COUNT(DISTINCT CASE WHEN al.live_status = 1 THEN dt ELSE NULL END) AS live_days,
                SUM(al.duration)                                                   AS duration
         FROM warehouse.ods_yy_day_anchor_live al
         WHERE comment = 'orig'
           AND dt >= '{month}'
           AND dt <= LAST_DAY('{month}')
         GROUP BY DATE_FORMAT(al.dt, '%Y-%m-01'),
                  al.platform_id,
                  al.anchor_uid) t
;


-- =====================================================================================================================
-- 计算每日相对前一天新增主播,;
-- 取出上月最后一天到当月倒数第二天数据
-- DROP TABLE stage.stage_rpt_yy_day_anchor_live_contrast;
-- CREATE TABLE stage.stage_rpt_yy_day_anchor_live_contrast AS
DELETE
FROM stage.stage_dw_yy_day_anchor_live_contrast
WHERE dt >= '{month}'
  AND dt <= LAST_DAY('{month}');
INSERT IGNORE INTO stage.stage_dw_yy_day_anchor_live_contrast
SELECT dt,
       platform_name,
       platform_id,
       anchor_uid,
       dt + INTERVAL 1 DAY AS last_dt,
       dt - INTERVAL 1 DAY AS next_dt
FROM warehouse.ods_yy_day_anchor_live al
WHERE dt >= '{month}'
  AND dt <= LAST_DAY('{month}')
;


-- 新增主播（在t-1天主播列表，不在t-2天的列表）
-- CREATE TABLE stage.stage_yy_day_anchor_add_loss AS
DELETE
FROM stage.stage_dw_yy_day_anchor_add_loss
WHERE add_loss_state = 'add'
  AND dt >= '{month}';
INSERT INTO stage.stage_dw_yy_day_anchor_add_loss
SELECT al1.dt, al1.platform_name, al1.platform_id, al1.anchor_uid, 'add' AS add_loss_state
FROM stage.stage_dw_yy_day_anchor_live_contrast al1
         LEFT JOIN stage.stage_dw_yy_day_anchor_live_contrast al2
                   ON al1.dt = al2.last_dt AND al1.anchor_uid = al2.anchor_uid
WHERE al2.anchor_uid IS NULL
  AND al1.dt >= '{month}'
  AND al1.dt <= LAST_DAY('{month}')
;


-- 流失主播（在t-2天主播列表，不在t-1天的列表）。备注：不在主播表中标志
DELETE
FROM stage.stage_dw_yy_day_anchor_add_loss
WHERE add_loss_state = 'loss'
  AND dt >= '{month}'
  AND dt <= LAST_DAY('{month}');
INSERT INTO stage.stage_dw_yy_day_anchor_add_loss
SELECT al1.last_dt, al1.platform_name, al1.platform_id, al1.anchor_uid, 'loss' AS add_loss_state
FROM stage.stage_dw_yy_day_anchor_live_contrast al1
         LEFT JOIN stage.stage_dw_yy_day_anchor_live_contrast al2
                   ON al1.dt = al2.next_dt AND al1.anchor_uid = al2.anchor_uid
WHERE al2.anchor_uid IS NULL
  AND al1.last_dt <= (SELECT MAX(dt) FROM warehouse.ods_yy_day_anchor_live)
  AND al1.dt >= '{month}'
  AND al1.dt <= LAST_DAY('{month}')
;


-- =====================================================================================================================
-- 主播开播数据
-- DROP TABLE IF EXISTS warehouse.dw_yy_day_anchor_live_duration;
-- CREATE TABLE warehouse.dw_yy_day_anchor_live_duration AS
DELETE
FROM warehouse.dw_yy_day_anchor_live_duration
WHERE dt >= '{month}'
  AND dt <= LAST_DAY('{month}');
INSERT INTO warehouse.dw_yy_day_anchor_live_duration
SELECT *
FROM warehouse.ods_yy_day_anchor_live_duration
WHERE dt >= '{month}'
  AND dt <= LAST_DAY('{month}')
;


-- 按日汇总主播佣金收入及工会分成（佣金）
-- DROP TABLE IF EXISTS warehouse.dw_yy_day_anchor_live_commission;
-- CREATE TABLE warehouse.dw_yy_day_anchor_live_commission AS
DELETE
FROM warehouse.dw_yy_day_anchor_live_commission
WHERE dt >= '{month}'
  AND dt <= LAST_DAY('{month}');
INSERT INTO warehouse.dw_yy_day_anchor_live_commission
SELECT *
FROM warehouse.ods_yy_day_anchor_live_commission
WHERE dt >= '{month}'
  AND dt <= LAST_DAY('{month}');
;


-- 主播收入（蓝钻）
-- DROP TABLE IF EXISTS warehouse.dw_yy_day_anchor_live_bluediamond;
-- CREATE TABLE warehouse.dw_yy_day_anchor_live_bluediamond AS
DELETE
FROM warehouse.dw_yy_day_anchor_live_bluediamond
WHERE dt >= '{month}'
  AND dt <= LAST_DAY('{month}');
INSERT INTO warehouse.dw_yy_day_anchor_live_bluediamond
SELECT *
FROM warehouse.ods_yy_day_anchor_live_bluediamond
WHERE dt >= '{month}'
  AND dt <= LAST_DAY('{month}')
;


-- =====================================================================================================================
-- 合并主播信息（主播列表）与开播数据
-- DROP TABLE IF EXISTS warehouse.dw_yy_day_anchor_live;
-- CREATE TABLE warehouse.dw_yy_day_anchor_live AS
DELETE
FROM warehouse.dw_yy_day_anchor_live
WHERE dt >= '{month}'
  AND dt <= LAST_DAY('{month}');
INSERT INTO warehouse.dw_yy_day_anchor_live
SELECT al.*,
       aml.min_live_dt,
       ams.min_sign_dt,
       -- 通过判断主播最小注册时间和最小开播时间，取两者之间最小的时间作为判断新老主播条件，两者为NULL则为‘未知’
       warehouse.ANCHOR_NEW_OLD(aml.min_live_dt, ams.min_sign_dt, al.dt, 180)   AS newold_state,
       IFNULL(mal.duration, 0)                                                  AS month_duration,
       IFNULL(mal.live_days, 0)                                                 AS month_live_days,
       -- 开播天数大于等于20天且开播时长大于等于60小时（t-1月累计）
       IFNULL(mal.active_state, '非活跃主播'),
       IFNULL(mal.revenue, 0)                                                   AS month_revenue,
       -- 主播流水分级（t-1月）
       IFNULL(mal.revenue_level, 0)                                             AS revenue_level,
       CASE WHEN aal.add_loss_state IS NULL THEN '' ELSE aal.add_loss_state END AS add_loss_state
FROM warehouse.ods_yy_day_anchor_live al
         LEFT JOIN stage.stage_yy_anchor_min_live_dt aml ON al.anchor_no = aml.anchor_no
         LEFT JOIN stage.stage_yy_anchor_min_sign_dt ams ON al.anchor_no = ams.anchor_no
         LEFT JOIN stage.stage_yy_month_anchor_live mal
                   ON mal.dt = DATE_FORMAT(al.dt, '%Y-%m-01') AND
                      al.anchor_uid = mal.anchor_uid
         LEFT JOIN stage.stage_dw_yy_day_anchor_add_loss aal
                   ON al.dt = aal.dt AND al.anchor_uid = aal.anchor_uid
WHERE al.dt >= '{month}'
  AND al.dt <= LAST_DAY('{month}')
  AND mal.dt = '{month}'
;


-- 刷新主播活跃及流水分档(按月)
-- UPDATE
--     warehouse.dw_yy_day_anchor_live al, stage.stage_yy_month_anchor_live mal
-- SET al.active_state    = mal.active_state,
--     al.month_duration  = mal.duration,
--     al.month_live_days = mal.live_days,
--     al.revenue_level   = mal.revenue_level,
--     al.month_revenue   = mal.revenue
-- WHERE al.anchor_uid = mal.anchor_uid
--   AND al.dt >= mal.dt
--   AND al.dt < mal.dt + INTERVAL 1 MONTH
--   AND mal.dt BETWEEN DATE_FORMAT('{start_date}', '%Y-%m-01') AND '{end_date}'
-- --   AND '{end_date}' = LAST_DAY('{end_date}')
-- ;


-- 维度 日-公会
-- 指标 主播数、开播主播数、主播流水、
-- DROP TABLE IF EXISTS warehouse.dw_yy_day_guild_live;
-- CREATE TABLE warehouse.dw_yy_day_guild_live AS
DELETE
FROM warehouse.dw_yy_day_guild_live
WHERE dt >= '{month}'
  AND dt <= LAST_DAY('{month}');;
INSERT INTO warehouse.dw_yy_day_guild_live
SELECT al.dt,
       al.platform_id,
       al.backend_account_id,
       al.channel_num,
       al.comment,
       al.newold_state,
       al.active_state,
       al.revenue_level,
       COUNT(DISTINCT al.anchor_uid)                                                 AS anchor_cnt,
       COUNT(DISTINCT CASE WHEN al.live_status = 1 THEN al.anchor_uid ELSE NULL END) AS anchor_live_cnt,
       SUM(IF(al.duration > 0, al.duration, 0))                                      AS duration,
       SUM(IF(al.bluediamond > 0, al.bluediamond, 0))                                AS bluediamond,
       SUM(IF(al.bluediamond > 0, al.bluediamond * al.anchor_settle_rate, 0))        AS anchor_income_bluediamond,
       SUM(IF(al.bluediamond > 0, al.bluediamond * (1 - al.anchor_settle_rate), 0))  AS guild_income_bluediamond,
       SUM(IF(al.anchor_commission > 0, al.anchor_commission, 0))                    AS anchor_commission,
       SUM(IF(al.guild_commission > 0, al.guild_commission, 0))                      AS guild_commission
FROM warehouse.dw_yy_day_anchor_live al
WHERE al.dt >= '{month}'
  AND al.dt <= LAST_DAY('{month}')
-- where comment <> 'from guild_anchor_sign_tran'
GROUP BY al.dt,
         al.platform_id,
         al.backend_account_id,
         al.channel_num,
         al.comment,
         al.newold_state,
         al.active_state,
         al.revenue_level
;
