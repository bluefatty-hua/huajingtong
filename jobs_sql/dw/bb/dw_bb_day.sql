-- 主播最早开播时间（基于现有数据）
-- DROP TABLE IF EXISTS stage.stage_bb_bb_anchor_min_live_dt;
CREATE TABLE if not EXISTS stage.stage_bb_bb_anchor_min_live_dt
(
  platform_id INT,
  anchor_no bigint,
  min_live_dt date
);
delete from  stage.stage_bb_bb_anchor_min_live_dt;
INSERT IGNORE INTO stage.stage_bb_bb_anchor_min_live_dt
SELECT 1001 AS platform_id,
       t.anchor_no,
       MIN(t.min_live_dt)
FROM (SELECT al.anchor_no,
             MIN(dt) AS min_live_dt
      FROM warehouse.ods_bb_day_anchor_live al
      WHERE al.live_status = 1
      GROUP BY al.anchor_no
      UNION
      SELECT yj.uid             AS anchor_no,
             yj.first_live_time AS min_live_time
      FROM warehouse.ods_yujia_anchor_list yj
      WHERE platform = 'B站'
        AND first_live_time != '1970-01-01') t
GROUP BY t.anchor_no
;


-- 主播最早签约时间（基于现有数据）,后期结合公司现有主播的签约时间
-- DROP TABLE IF EXISTS stage.stage_bb_bb_anchor_min_sign_dt;
-- CREATE TABLE stage.stage_bb_bb_anchor_min_sign_dt
CREATE TABLE if not EXISTS stage.stage_bb_bb_anchor_min_sign_dt
(
  platform_id INT,
  anchor_no bigint,
  min_sign_dt date
);
delete from  stage.stage_bb_bb_anchor_min_sign_dt;
INSERT IGNORE INTO stage.stage_bb_bb_anchor_min_sign_dt
SELECT 1001             AS platform_id,
       t.anchor_no,
       MIN(min_sign_dt) AS min_sign_dt
FROM (SELECT al.anchor_no,
             MIN(DATE(contract_signtime)) AS min_sign_dt
      FROM warehouse.ods_bb_day_anchor_live al
      WHERE al.contract_signtime IS NOT NULL
      GROUP BY al.anchor_no
      UNION
      SELECT yj.uid       AS anchor_no,
             yj.sign_time AS min_sign_dt
      FROM warehouse.ods_yujia_anchor_list yj
      WHERE platform = 'B站'
        AND yj.sign_time <> '1970-01-01'
     ) t
GROUP BY t.anchor_no
;

-- 计算每月主播开播天数，开播时长，流水
-- DROP TABLE IF EXISTS stage.stage_bb_bb_month_anchor_live;
-- CREATE TABLE stage.stage_bb_bb_month_anchor_live
CREATE TABLE if not EXISTS stage.stage_bb_bb_month_anchor_live
(
  dt date,
  platform_id INT,
  anchor_no bigint,
  revenue decimal(20,1),
  revenue_level varchar(10),
  live_days int,
  duration int,
  active_state varchar(30)
);
DELETE
FROM stage.stage_bb_bb_month_anchor_live
WHERE dt = '{month}';
INSERT INTO stage.stage_bb_bb_month_anchor_live
SELECT t.dt,
       t.platform_id,
       t.anchor_no,
       t.revenue,
       CASE
           WHEN t.revenue / 1000 / 10000 >= 50 THEN '50+'
           WHEN t.revenue / 1000 / 10000 >= 10 THEN '10-50'
           WHEN t.revenue / 1000 / 10000 >= 3 THEN '3-10'
           WHEN t.revenue / 1000 / 10000 > 0 THEN '0-3'
           ELSE '0' END     AS revenue_level,
       t.live_days,
       t.duration,
       CASE
           WHEN t.live_days >= 20 AND t.duration >= 60 * 60 * 60 THEN '活跃主播'
           ELSE '非活跃主播' END AS active_state
FROM (
         SELECT DATE_FORMAT(al.dt, '%Y-%m-01')                                     AS dt,
                al.platform_id,
                al.anchor_no,
                SUM(anchor_total_coin)                                             AS revenue,
                COUNT(DISTINCT CASE WHEN al.live_status = 1 THEN dt ELSE NULL END) AS live_days,
                SUM(al.duration)                                                   AS duration
         FROM warehouse.ods_bb_day_anchor_live al
         WHERE al.dt >= '{month}'
           AND al.dt <= LAST_DAY('{month}')
         GROUP BY DATE_FORMAT(al.dt, '%Y-%m-01'),
                  al.platform_id,
                  al.anchor_no) t
;


-- =======================================================================
-- DROP TABLE IF EXISTS warehouse.dw_bb_day_anchor_live;
-- CREATE TABLE warehouse.dw_bb_day_anchor_live AS
DELETE
FROM warehouse.dw_bb_day_anchor_live
WHERE dt >= '{month}'
  AND dt <= LAST_DAY('{month}');
INSERT INTO warehouse.dw_bb_day_anchor_live
(`platform_id`,
  `platform_name`,
  `dt`,
  `backend_account_id`,
  `anchor_uid`,
  `anchor_no`,
  `anchor_nick_name`,
  `anchor_status`,
  `anchor_status_text`,
  `guild_id`,
  `guild_name`,
  `guild_type`,
  `live_status`,
  `valid_live_status`,
  `live_hour`,
  `duration`,
  `valid_live_hour`,
  `valid_duration`,
  `ios_coin`,
  `android_coin`,
  `pc_coin`,
  `revenue`,
  `revenue_orig`,
  `anchor_income`,
  `special_coin`,
  `send_coin`,
  `anchor_base_coin`,
  `DAU`,
  `max_ppl`,
  `fc`,
  `contract_status`,
  `contract_status_text`,
  `contract_signtime`,
  `contract_endtime`,
  `min_live_dt`,
  `min_sign_dt`,
  `newold_state`,
  `month_duration`,
  `month_live_days`,
  `active_state`,
  `month_revenue`,
  `revenue_level`)
SELECT al.platform_id,
       al.platform_name,
       al.dt,
       al.backend_account_id,
       al.anchor_uid,
       al.anchor_no,
       al.anchor_nick_name,
       al.anchor_status,
       al.anchor_status_text,
       al.guild_id,
       al.guild_name,
       al.guild_type,
       al.live_status,
       al.valid_live_status,
       al.live_hour,
       al.duration,
       al.valid_live_hour,
       al.valid_duration,
       al.ios_coin,
       al.android_coin,
       al.pc_coin,
       ROUND(al.anchor_total_coin / 1000, 2)                                  AS revenue,
       al.anchor_total_coin                                                   AS revenue_orig,
       al.anchor_income,
       al.special_coin,
       al.send_coin,
       al.anchor_base_coin,
       al.DAU,
       al.max_ppl,
       al.fc,
       IFNULL(al.contract_status, '-1')                                         AS contract_status,
       IFNULL(al.contract_status_text, '')                                    AS contract_status_text,
       al.contract_signtime,
       al.contract_endtime,
       aml.min_live_dt,
       ams.min_sign_dt,
       -- 通过判断主播最小注册时间和最小开播时间，取两者之间最小的时间作为判断新老主播条件，两者为NULL则为‘未知’
       warehouse.ANCHOR_NEW_OLD(aml.min_live_dt, ams.min_sign_dt, al.dt, 180) AS newold_state,
       mal.duration                                                           AS month_duration,
       mal.live_days                                                          AS month_live_days,
       -- 开播天数大于等于20天且开播时长大于等于60小时（t月累计）
       mal.active_state,
       mal.revenue                                                            AS month_revenue,
       -- 主播流水分级（t月，单位：万元）
       mal.revenue_level
FROM warehouse.ods_bb_day_anchor_live al
         LEFT JOIN stage.stage_bb_bb_anchor_min_live_dt aml ON al.anchor_no = aml.anchor_no
         LEFT JOIN stage.stage_bb_bb_anchor_min_sign_dt ams ON al.anchor_no = ams.anchor_no
         LEFT JOIN stage.stage_bb_bb_month_anchor_live mal
                   ON mal.dt = '{month}' AND
                      al.anchor_no = mal.anchor_no
WHERE al.dt >= '{month}'
  AND al.dt <= LAST_DAY('{month}')
;


-- 刷新主播活跃及流水分档(按月)
-- UPDATE
--     warehouse.dw_bb_day_anchor_live al, stage.stage_bb_bb_month_anchor_live mal
-- SET al.active_state    = mal.active_state,
--     al.month_duration  = mal.duration,
--     al.month_live_days = mal.live_days,
--     al.revenue_level   = mal.revenue_level,
--     al.month_revenue   = mal.revenue
-- WHERE al.anchor_uid = mal.anchor_uid
--   AND al.dt >= mal.dt
--   AND al.dt < mal.dt + INTERVAL 1 MONTH
--   AND mal.dt BETWEEN DATE_FORMAT('{start_date}', '%Y-%m-01') AND '{end_date}'
--   AND '{end_date}' = LAST_DAY('{end_date}')
;


-- 主播
-- 汇总维度 日-公会
-- 汇总指标 开播天数，开播时长，虚拟币收入
-- DROP TABLE IF EXISTS warehouse.dw_bb_day_guild_live;
-- CREATE TABLE warehouse.dw_bb_day_guild_live AS

