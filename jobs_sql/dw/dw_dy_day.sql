-- 主播最早开播时间（基于现有数据）
-- DROP TABLE IF EXISTS stage.stage_dy_anchor_min_live_dt;
-- CREATE TABLE stage.stage_dy_anchor_min_live_dt
INSERT IGNORE INTO stage.stage_dy_anchor_min_live_dt
SELECT 1005 AS platform_id,
       t.anchor_uid,
       MIN(t.min_live_dt)
FROM (SELECT al.anchor_uid,
             MIN(al.dt) AS min_live_dt
      FROM warehouse.ods_dy_day_anchor_live al
      WHERE al.live_status = 1
      GROUP BY al.anchor_uid
      UNION
      SELECT ai.anchor_uid,
             yj.first_live_time AS min_live_time
      FROM warehouse.ods_yujia_anchor_list yj
               INNER JOIN warehouse.ods_dy_day_anchor_info ai ON yj.uid = ai.anchor_no
      WHERE platform = '抖音'
        AND first_live_time <> '1970-01-01') t
GROUP BY t.anchor_uid
;



-- 主播最早签约时间（基于现有数据）,后期结合公司现有主播的签约时间
-- DROP TABLE IF EXISTS stage.stage_dy_anchor_min_sign_dt;
-- CREATE TABLE stage.stage_dy_anchor_min_sign_dt
INSERT IGNORE INTO stage.stage_dy_anchor_min_sign_dt
SELECT 1005             AS platform_id,
       t.anchor_uid,
       MIN(min_sign_dt) AS min_sign_dt
FROM (SELECT al.anchor_uid,
             MIN(al.sign_time) AS min_sign_dt
      FROM warehouse.ods_dy_day_anchor_live al
      WHERE al.signing_time IS NOT NULL
        AND al.signing_time <> 0
      GROUP BY al.anchor_uid
      UNION
      SELECT ai.anchor_uid,
             yj.sign_time AS min_sign_dt
      FROM warehouse.ods_yujia_anchor_list yj
               INNER JOIN warehouse.ods_dy_day_anchor_info ai ON yj.uid = ai.anchor_no
      WHERE platform = '抖音'
        AND yj.sign_time <> '1970-01-01'
     ) t
GROUP BY t.anchor_uid
;


-- 计算每月主播开播天数，开播时长，流水
-- DROP TABLE IF EXISTS stage.stage_dy_month_anchor_live;
-- CREATE TABLE stage.stage_dy_month_anchor_live
-- 按月更新主播的标签
DELETE
FROM stage.stage_dy_month_anchor_live
WHERE dt = '{month}';
INSERT INTO stage.stage_dy_month_anchor_live
SELECT t.dt,
       t.platform_id,
       t.anchor_uid,
       t.revenue,
       CASE
           WHEN t.revenue / 10 / 10000 >= 50 THEN '50+'
           WHEN t.revenue / 10 / 10000 >= 10 THEN '10-50'
           WHEN t.revenue / 10 / 10000 >= 3 THEN '3-10'
           WHEN t.revenue / 10 / 10000 > 0 THEN '0-3'
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
                SUM(revenue)                                                       AS revenue,
                COUNT(DISTINCT CASE WHEN al.live_status = 1 THEN dt ELSE NULL END) AS live_days,
                SUM(al.duration)                                                   AS duration
         FROM warehouse.ods_dy_day_anchor_live al
         WHERE dt >= '{month}'
           AND dt <= LAST_DAY('{month}')
         GROUP BY DATE_FORMAT(al.dt, '%Y-%m-01'),
                  al.platform_id,
                  al.anchor_uid) t
;


-- DROP TABLE IF EXISTS warehouse.dw_dy_day_anchor_live;
-- CREATE TABLE warehouse.dw_dy_day_anchor_live AS
DELETE
FROM warehouse.dw_dy_day_anchor_live
WHERE dt >= '{month}'
  AND dt <= LAST_DAY('{month}');
INSERT INTO warehouse.dw_dy_day_anchor_live
SELECT al.dt,
       al.platform_id,
       al.platform_name,
       al.backend_account_id,
       al.guild_name,
       al.anchor_uid,
       al.anchor_short_id,
       al.anchor_no,
       al.anchor_nick_name,
       al.last_live_time,
       al.follower_count,
       al.total_diamond,
       al.live_status,
       al.duration,
       al.revenue / 10                                                        AS revenue,
       al.revenue                                                             AS revenue_orig,
       al.live_revenue,
       al.prop_revenue,
       al.act_revenue,
       al.fan_rise,
       al.signing_type,
       al.signing_time,
       al.sign_time,
       al.anchor_settle_rate,
       al.gender,
       al.agent_id,
       al.agent_name,
       al.logo,
       IFNULL(al.revenue, 0) * IFNULL(al.anchor_settle_rate, 0) / 100         AS anchor_income,
       IFNULL(al.revenue, 0) * 0.1                                            AS guild_income,
       aml.min_live_dt,
       ams.min_sign_dt,
       -- 通过判断主播最小注册时间和最小开播时间，取两者之间最小的时间作为判断新老主播条件，两者为NULL则为‘未知’
       warehouse.ANCHOR_NEW_OLD(aml.min_live_dt, ams.min_sign_dt, al.dt, 180) AS newold_state,
       IFNULL(mal.duration, 0)                                                AS month_duration,
       IFNULL(mal.live_days, 0)                                               AS month_live_days,
       -- 开播天数大于等于20天且开播时长大于等于60小时（t月累计）
       mal.active_state,
       IFNULL(mal.revenue, 0)                                                 AS month_revenue,
       -- 主播流水分级（t月，单位：万元）
       mal.revenue_level
FROM warehouse.ods_dy_day_anchor_live al
         LEFT JOIN stage.stage_dy_anchor_min_live_dt aml ON al.anchor_uid = aml.anchor_uid
         LEFT JOIN stage.stage_dy_anchor_min_sign_dt ams ON al.anchor_uid = ams.anchor_uid
         LEFT JOIN stage.stage_dy_month_anchor_live mal
                   ON mal.dt = DATE_FORMAT(al.dt, '%Y-%m-01') AND
                      al.anchor_uid = mal.anchor_uid
WHERE al.dt >= '{month}'
  AND al.dt <= LAST_DAY('{month}')
  AND mal.dt = '{month}'
;


-- 刷新主播活跃及流水分档(按月)
-- UPDATE
--     warehouse.dw_dy_day_anchor_live al, stage.stage_dy_month_anchor_live mal
-- SET al.active_state    = mal.active_state,
--     al.month_duration  = IFNULL(mal.duration, 0),
--     al.month_live_days = IFNULL(mal.live_days, 0),
--     al.revenue_level   = mal.revenue_level,
--     al.month_revenue   = IFNULL(mal.revenue, 0)
-- WHERE al.anchor_uid = mal.anchor_uid
--   AND al.dt >= mal.dt
--   AND al.dt < mal.dt + INTERVAL 1 MONTH
--   AND mal.dt BETWEEN DATE_FORMAT('{start_date}', '%Y-%m-01') AND '{end_date}'
--   AND '{end_date}' = LAST_DAY('{end_date}')
;


-- DROP TABLE IF EXISTS warehouse.dw_dy_day_guild_live;
-- CREATE TABLE warehouse.dw_dy_day_guild_live AS
DELETE
FROM warehouse.dw_dy_day_guild_live
WHERE dt >= '{month}'
  AND dt <= LAST_DAY('{month}');
INSERT INTO warehouse.dw_dy_day_guild_live
SELECT al.dt,
       al.platform_id,
       al.platform_name,
       al.backend_account_id,
       al.newold_state,
       al.active_state,
       al.revenue_level,
       COUNT(DISTINCT al.anchor_uid)                                                 AS anchor_cnt,
       COUNT(DISTINCT CASE WHEN al.live_status = 1 THEN al.anchor_uid ELSE NULL END) AS anchor_live_cnt,
       SUM(IF(al.duration > 0, al.duration, 0))                                      AS duration,
       SUM(IF(al.revenue_orig > 0, al.revenue_orig, 0))                              AS revenue,
       SUM(IF(al.anchor_income > 0, al.anchor_income, 0))                            AS anchor_income,
       SUM(IF(al.guild_income > 0, al.guild_income, 0))                              AS guild_income
FROM warehouse.dw_dy_day_anchor_live al
WHERE al.dt >= '{month}'
  AND al.dt <= LAST_DAY('{month}')
GROUP BY al.dt,
         al.platform_id,
         al.platform_name,
         al.backend_account_id,
         al.newold_state,
         al.active_state,
         al.revenue_level
;


