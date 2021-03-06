-- 主播最早开播时间（基于现有数据）
-- DROP TABLE IF EXISTS stage.stage_now_anchor_min_live_dt;
-- CREATE TABLE stage.stage_now_anchor_min_live_dt
INSERT IGNORE INTO stage.stage_now_anchor_min_live_dt
SELECT 1003 AS platform_id,
       t.anchor_no,
       MIN(t.min_live_dt)
FROM (SELECT al.anchor_no,
             MIN(dt) AS min_live_dt
      FROM warehouse.ods_now_day_anchor_live al
      WHERE al.live_status = 1
      GROUP BY al.anchor_no
      UNION
      SELECT yj.uid             AS anchor_no,
             yj.first_live_time AS min_live_time
      FROM warehouse.ods_yujia_anchor_list yj
      WHERE platform = 'NOW'
        AND first_live_time != '1970-01-01') t
GROUP BY t.anchor_no
;


-- 主播最早签约时间（基于现有数据）,后期结合公司现有主播的签约时间
-- DROP TABLE IF EXISTS stage.stage_now_anchor_min_sign_dt;
-- CREATE TABLE stage.stage_now_anchor_min_sign_dt
INSERT IGNORE INTO stage.stage_now_anchor_min_sign_dt
SELECT 1003             AS platform_id,
       t.anchor_no,
       MIN(min_sign_dt) AS min_sign_dt
FROM (SELECT al.anchor_no,
             MIN(DATE(al.contract_sign_time)) AS min_sign_dt
      FROM warehouse.ods_now_day_anchor_live al
      WHERE al.contract_sign_time IS NOT NULL
      GROUP BY al.anchor_no
      UNION
      SELECT yj.uid       AS anchor_no,
             yj.sign_time AS min_sign_dt
      FROM warehouse.ods_yujia_anchor_list yj
      WHERE platform = 'NOW'
        AND yj.sign_time <> '1970-01-01'
     ) t
GROUP BY t.anchor_no
;


-- 计算每月主播开播天数，开播时长，流水
-- DROP TABLE IF EXISTS stage.stage_now_month_anchor_live;
-- CREATE TABLE stage.stage_now_month_anchor_live
DELETE
FROM stage.stage_now_month_anchor_live
WHERE dt = '{month}';
INSERT INTO stage.stage_now_month_anchor_live
SELECT '{month}' as dt,
       t.platform_id,
       t.anchor_no,
       t.revenue, -- 注意人民币
       CASE
           WHEN t.revenue / 10000 >= 50 THEN '50+'
           WHEN t.revenue / 10000 >= 10 THEN '10-50'
           WHEN t.revenue / 10000 >= 3 THEN '3-10'
           WHEN t.revenue / 10000 > 0 THEN '0-3'
           ELSE '0' END     AS revenue_level,
       t.live_days,
       t.duration,
       CASE
           WHEN t.live_days >= 20 AND t.duration >= 60 * 60 * 60 THEN '活跃主播'
           ELSE '非活跃主播' END AS active_state
FROM (
         SELECT 
                al.platform_id,
                al.anchor_no,
                SUM(revenue_rmb)                                                   AS revenue,
                COUNT(DISTINCT CASE WHEN al.live_status = 1 THEN dt ELSE NULL END) AS live_days,
                SUM(al.duration)                                                   AS duration
         FROM warehouse.ods_now_day_anchor_live al
         WHERE dt >= '{month}'
           AND dt <= LAST_DAY('{month}')
         GROUP BY 
                  al.platform_id,
                  al.anchor_no) t
;


-- DROP TABLE IF EXISTS warehouse.dw_now_day_anchor_live;
-- CREATE TABLE warehouse.dw_now_day_anchor_live AS
DELETE
FROM warehouse.dw_now_day_anchor_live
WHERE dt >= '{month}'
  AND dt <= LAST_DAY('{month}');
INSERT INTO warehouse.dw_now_day_anchor_live
(
  `backend_account_id`,
  `anchor_uid`,
  `anchor_qq_no`,
  `anchor_no`,
  `anchor_nick_name`,
  `anchor_name`,
  `fans_cnt`,
  `fans_goup_cnt`,
  `live_status`,
  `duration_hour`,
  `duration`,
  `revenue`,
  `contract_sign_time`,
  `settle_method_code`,
  `settle_method_text`,
  `dt`,
  `city`,
  `min_live_dt`,
  `min_sign_dt`,
  `newold_state`,
  `month_duration`,
  `month_live_days`,
  `active_state`,
  `month_revenue`,
  `revenue_level`
)
SELECT 
    al.`backend_account_id`,
    al.`anchor_uid`,
    al.`anchor_qq_no`,
    al.`anchor_no`,
    al.`anchor_nick_name`,
    al.`anchor_name`,
    al.`fans_cnt`,
    al.`fans_goup_cnt`,
    al.`live_status`,
    al.`duration_hour`,
    al.`duration`,
    al.`revenue_rmb` AS `revenue`,
    al.`contract_sign_time`,
    al.`settle_method_code`,
    al.`settle_method_text`,
    al.`dt`,
       IFNULL(at.city, '未知')                                                  AS city,
       aml.min_live_dt,
       ams.min_sign_dt,
       -- 通过判断主播最小注册时间和最小开播时间，取两者之间最小的时间作为判断新老主播条件，两者为NULL则为‘未知’
       warehouse.ANCHOR_NEW_OLD(aml.min_live_dt, ams.min_sign_dt, al.dt, 180) AS newold_state,
       mal.duration                                                           AS month_duration,
       mal.live_days                                                          AS month_live_days,
       -- 开播天数大于等于20天且开播时长大于等于60小时（t-1月累计）
       mal.active_state,
       mal.revenue                                                            AS month_revenue,
       -- 主播流水分级（t-1月）
       mal.revenue_level
FROM warehouse.ods_now_day_anchor_live al
         LEFT JOIN stage.stage_now_anchor_min_live_dt aml ON al.anchor_no = aml.anchor_no
         LEFT JOIN stage.stage_now_anchor_min_sign_dt ams ON al.anchor_no = ams.anchor_no
         LEFT JOIN stage.stage_now_month_anchor_live mal
                   ON mal.dt ='{month}' AND
                      al.anchor_no = mal.anchor_no
         LEFT JOIN warehouse.ods_yj_anchor_team at ON al.anchor_no = at.anchor_no
-- 只取主播入驻公会后的直播数据
WHERE (aml.min_live_dt <= al.dt OR al.contract_sign_time <= al.dt)
  AND al.dt >= '{month}'
  AND al.dt <= LAST_DAY('{month}')
;


-- 刷新主播活跃及流水分档(按月)
-- UPDATE
--     warehouse.dw_now_day_anchor_live al, stage.stage_now_month_anchor_live mal
-- SET al.active_state    = mal.active_state,
--     al.month_duration  = mal.duration,
--     al.month_live_days = mal.live_days,
--     al.revenue_level   = mal.revenue_level,
--     al.month_revenue   = mal.revenue
-- WHERE al.anchor_no = mal.anchor_no
--   AND al.dt >= mal.dt
--   AND al.dt < mal.dt + INTERVAL 1 MONTH
--   AND mal.dt BETWEEN DATE_FORMAT('{start_date}', '%Y-%m-01') AND '{end_date}'
--   AND '{end_date}' = LAST_DAY('{end_date}')
-- ;


