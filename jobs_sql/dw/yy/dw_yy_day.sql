-- 主播最早开播时间（基于现有数据）
-- DROP TABLE IF EXISTS stage.stage_yy_anchor_min_live_dt;
-- CREATE TABLE stage.stage_yy_anchor_min_live_dt
delete from stage.stage_dw_yy_anchor_min_live_dt;
INSERT IGNORE INTO stage.stage_dw_yy_anchor_min_live_dt
(anchor_uid,min_live_dt)
SELECT 
al.anchor_uid,
MIN(dt) AS min_live_dt
FROM warehouse.ods_yy_day_anchor_live al
WHERE al.live_status = 1
GROUP BY al.anchor_uid
;


-- 主播最早签约时间（基于现有数据）,后期结合公司现有主播的签约时间
-- DROP TABLE IF EXISTS stage.stage_yy_anchor_min_sign_dt;
-- CREATE TABLE stage.stage_yy_anchor_min_sign_dt
INSERT IGNORE INTO stage.stage_dw_yy_anchor_min_sign_dt
(anchor_uid,min_sign_dt)
SELECT anchor_uid,MIN(min_sign_dt) AS min_sign_dt FROM
(
SELECT al.anchor_uid,
MIN(contract_signtime) AS min_sign_dt
FROM warehouse.ods_yy_day_anchor_live al
WHERE al.contract_signtime IS NOT NULL
GROUP BY al.anchor_uid
UNION 
SELECT anchor_uid,min_sign_dt FROM 
(SELECT anchor_no,anchor_uid FROM warehouse.ods_yy_day_anchor_live
GROUP BY anchor_no,anchor_uid) t1
JOIN 
(SELECT yj.uid       AS anchor_no,
yj.sign_time AS min_sign_dt
FROM warehouse.ods_yujia_anchor_list yj
WHERE platform = 'YY'
AND yj.sign_time <> '1970-01-01') t2
ON t1.anchor_no = t2.anchor_no
)t
GROUP BY anchor_uid;


-- 计算每月主播开播天数，开播时长，流水
-- DROP TABLE IF EXISTS stage.stage_yy_month_anchor_live;
-- CREATE TABLE stage.stage_yy_month_anchor_live
DELETE
FROM stage.stage_dw_yy_month_anchor_live
WHERE dt = '{month}';
INSERT INTO stage.stage_dw_yy_month_anchor_live
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
(
  `dt`,
  `platform_id`,
  `platform_name`,
  `backend_account_id`,
  `channel_num`,
  `anchor_uid`,
  `anchor_no`,
  `anchor_nick_name`,
  `anchor_type`,
  `anchor_type_text`,
  `live_room_id`,
  `channel_id`,
  `duration`,
  `mob_duration`,
  `pc_duration`,
  `live_status`,
  `bluediamond`,
  `anchor_commission`,
  `guild_commission`,
  `vir_coin_name`,
  `vir_coin_rate`,
  `include_pf_amt`,
  `pf_amt_rate`,
  `contract_id`,
  `contract_signtime`,
  `contract_endtime`,
  `settle_method_code`,
  `settle_method_text`,
  `anchor_settle_rate`,
  `logo`,
  `comment`,
  `min_live_dt`,
  `min_sign_dt`,
  `newold_state`,
  `month_duration`,
  `month_live_days`,
  `active_state`,
  `month_revenue`,
  `revenue_level`)
SELECT al.`dt`,
      al.`platform_id`,
      al.`platform_name`,
      al.`backend_account_id`,
      al.`channel_num`,
      al.`anchor_uid`,
      al.`anchor_no`,
      al.`anchor_nick_name`,
      al.`anchor_type`,
      al.`anchor_type_text`,
      al.`live_room_id`,
      al.`channel_id`,
      al.`duration`,
      al.`mob_duration`,
      al.`pc_duration`,
      al.`live_status`,
      al.`bluediamond`,
      al.`anchor_commission`,
      al.`guild_commission`,
      al.`vir_coin_name`,
      al.`vir_coin_rate`,
      al.`include_pf_amt`,
      al.`pf_amt_rate`,
      al.`contract_id`,
      al.`contract_signtime`,
      al.`contract_endtime`,
      al.`settle_method_code`,
      al.`settle_method_text`,
      al.`anchor_settle_rate`,
      al.`logo`,
      al.`comment`,
      aml.min_live_dt,
      ams.min_sign_dt,
      -- 通过判断主播最小注册时间和最小开播时间，取两者之间最小的时间作为判断新老主播条件，两者为NULL则为‘未知’
      warehouse.ANCHOR_NEW_OLD(aml.min_live_dt, ams.min_sign_dt, al.dt, 180) AS newold_state,
      IFNULL(mal.duration, 0)                                                AS month_duration,
      IFNULL(mal.live_days, 0)                                               AS month_live_days,
      -- 开播天数大于等于20天且开播时长大于等于60小时（t-1月累计）
      IFNULL(mal.active_state, '非活跃主播'),
      IFNULL(mal.revenue, 0)                                                 AS month_revenue,
      -- 主播流水分级（t-1月）
      IFNULL(mal.revenue_level, 0)                                           AS revenue_level
FROM warehouse.ods_yy_day_anchor_live al
         LEFT JOIN stage.stage_dw_yy_anchor_min_live_dt aml ON al.anchor_uid = aml.anchor_uid
         LEFT JOIN stage.stage_dw_yy_anchor_min_sign_dt ams ON al.anchor_uid = ams.anchor_uid
         LEFT JOIN stage.stage_dw_yy_month_anchor_live mal
                   ON mal.dt = DATE_FORMAT(al.dt, '%Y-%m-01') AND
                      al.anchor_uid = mal.anchor_uid
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


