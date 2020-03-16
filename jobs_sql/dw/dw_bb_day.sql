-- 主播最早开播时间（基于现有数据）
-- DROP TABLE IF EXISTS stage.stage_bb_anchor_min_live_dt;
-- CREATE TABLE stage.stage_bb_anchor_min_live_dt
INSERT IGNORE INTO stage.stage_bb_anchor_min_live_dt
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
-- DROP TABLE IF EXISTS stage.stage_bb_anchor_min_sign_dt;
-- CREATE TABLE stage.stage_bb_anchor_min_sign_dt
INSERT IGNORE INTO stage.stage_bb_anchor_min_sign_dt
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
-- DROP TABLE IF EXISTS stage.stage_bb_month_anchor_live;
-- CREATE TABLE stage.stage_bb_month_anchor_live
DELETE
FROM stage.stage_bb_month_anchor_live
WHERE dt = '{month}';
INSERT INTO stage.stage_bb_month_anchor_live
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
         WHERE (contract_status <> 2 OR contract_status IS NULL)
           AND al.dt >= '{month}'
           AND al.dt <= LAST_DAY('{month}')
         GROUP BY DATE_FORMAT(al.dt, '%Y-%m-01'),
                  al.platform_id,
                  al.anchor_no) t
;


-- =======================================================================
-- 计算每日相对前一天新增主播,;
-- 1、取出上月最后一天到当月倒数第二天数据
-- CREATE TABLE stage.stage_ods_bb_day_anchor_live_last_day AS
DELETE
FROM stage.stage_ods_bb_day_anchor_live_last_day
WHERE dt >= '{month}'
  AND dt <= LAST_DAY('{month}');
INSERT INTO stage.stage_ods_bb_day_anchor_live_last_day
SELECT dt,
       platform_name,
       platform_id,
       backend_account_id,
       anchor_no,
       dt + INTERVAL 1 DAY AS last_dt,
       dt - INTERVAL 1 DAY AS next_dt
FROM warehouse.ods_bb_day_anchor_live
WHERE (contract_status <> 2 OR contract_status IS NULL)
  AND dt >= '{month}'
  AND dt <= LAST_DAY('{month}')
;


-- 新增主播（在t-1天主播列表，不在t-2天的列表）
-- CREATE TABLE stage.stage_bb_day_anchor_add_loss AS
DELETE
FROM stage.stage_bb_day_anchor_add_loss
WHERE add_loss_state = 'add'
  AND dt >= '{month}'
  AND dt <= LAST_DAY('{month}');
INSERT INTO stage.stage_bb_day_anchor_add_loss
SELECT al1.dt, al1.platform_name, al1.platform_id, al1.anchor_no, 'add' AS add_loss_state
FROM stage.stage_ods_bb_day_anchor_live_last_day al1
         LEFT JOIN stage.stage_ods_bb_day_anchor_live_last_day al2
                   ON al1.dt = al2.last_dt AND al1.anchor_no = al2.anchor_no
WHERE al2.anchor_no IS NULL
  AND al1.dt >= '{month}'
  AND al1.dt <= LAST_DAY('{month}')
;


-- 流失主播（在t-2天主播列表，不在t-1天的列表）
DELETE
FROM stage.stage_bb_day_anchor_add_loss
WHERE add_loss_state = 'loss'
  AND dt >= '{month}'
  AND dt <= LAST_DAY('{month}');
INSERT INTO stage.stage_bb_day_anchor_add_loss
SELECT al1.last_dt, al1.platform_name, al1.platform_id, al1.anchor_no, 'loss' AS add_loss_state
FROM stage.stage_ods_bb_day_anchor_live_last_day al1
         LEFT JOIN stage.stage_ods_bb_day_anchor_live_last_day al2
                   ON al1.dt = al2.next_dt AND al1.anchor_no = al2.anchor_no
WHERE al1.dt >= '{month}'
  AND al1.dt <= LAST_DAY('{month}')
  AND al2.anchor_no IS NULL
  AND al1.last_dt <> '{cur_date}'
;


-- =======================================================================
-- DROP TABLE IF EXISTS warehouse.dw_bb_day_anchor_live;
-- CREATE TABLE warehouse.dw_bb_day_anchor_live AS
DELETE
FROM warehouse.dw_bb_day_anchor_live
WHERE dt >= '{month}'
  AND dt <= LAST_DAY('{month}');
INSERT INTO warehouse.dw_bb_day_anchor_live
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
       al.anchor_total_coin                                                   AS revenue,
       al.anchor_income,
       al.special_coin,
       al.send_coin,
       al.anchor_base_coin,
       al.DAU,
       al.max_ppl,
       al.fc,
       al.contract_status,
       al.contract_status_text,
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
         LEFT JOIN stage.stage_bb_anchor_min_live_dt aml ON al.anchor_no = aml.anchor_no
         LEFT JOIN stage.stage_bb_anchor_min_sign_dt ams ON al.anchor_no = ams.anchor_no
         LEFT JOIN stage.stage_bb_month_anchor_live mal
                   ON mal.dt = DATE_FORMAT(al.dt, '%Y-%m-01') AND
                      al.anchor_no = mal.anchor_no
WHERE al.dt >= '{month}'
  AND al.dt <= LAST_DAY('{month}')
  AND mal.dt = '{month}'
;

-- 刷新主播活跃及流水分档(按月)
-- UPDATE
--     warehouse.dw_bb_day_anchor_live al, stage.stage_bb_month_anchor_live mal
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
DELETE
FROM warehouse.dw_bb_day_guild_live
WHERE dt BETWEEN '{start_date}' AND '{end_date}';
INSERT INTO warehouse.dw_bb_day_guild_live
SELECT t.dt,
       t.platform_id,
       t.platform_name,
       t.backend_account_id,
       t.newold_state,
       t.active_state,
       t.revenue_level,
       COUNT(DISTINCT t.anchor_no)                                                AS anchor_cnt,
       COUNT(DISTINCT CASE WHEN t.live_status = 1 THEN t.anchor_no ELSE NULL END) AS live_cnt,
       SUM(t.duration)                                                            AS duration,
       SUM(t.anchor_total_coin)                                                   AS revenue,
       SUM(t.special_coin)                                                        AS special_coin,
       SUM(t.send_coin)                                                           AS send_coin,
       SUM(t.anchor_base_coin)                                                    AS anchor_base_coin,
       SUM(t.anchor_income)                                                       AS anchor_income
FROM warehouse.dw_bb_day_anchor_live t
WHERE (t.contract_status <> 2 OR t.contract_status IS NULL)
  AND t.dt BETWEEN '{start_date}' AND '{end_date}'
GROUP BY t.dt,
         t.platform_id,
         t.platform_name,
         t.backend_account_id,
         t.newold_state,
         t.active_state,
         t.revenue_level
;

