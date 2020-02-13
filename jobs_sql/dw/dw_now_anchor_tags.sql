-- 主播最早开播时间（基于现有数据）
-- DROP TABLE IF EXISTS stage.stage_now_anchor_min_live_dt;
-- CREATE TABLE stage.stage_now_anchor_min_live_dt
INSERT IGNORE INTO stage.stage_now_anchor_min_live_dt
SELECT al.platform_id,
       al.anchor_no,
       MIN(dt) AS min_live_dt
FROM warehouse.ods_now_day_anchor_live al
WHERE al.live_status = 1
GROUP BY al.platform_id,
         al.anchor_no
;


-- 主播最早签约时间（基于现有数据）,后期结合公司现有主播的签约时间
-- DROP TABLE IF EXISTS stage.stage_now_anchor_min_sign_dt;
-- CREATE TABLE stage.stage_now_anchor_min_sign_dt
INSERT IGNORE INTO stage.stage_now_anchor_min_sign_dt
SELECT t0.platform_id,
       t0.anchor_no,
       CASE WHEN t1.sign_time IS NULL THEN t0.min_sign_dt ELSE t1.sign_time END AS min_sign_dt
FROM (
         SELECT al.platform_id,
                al.anchor_no,
                MIN(DATE(contract_sign_time)) AS min_sign_dt
         FROM warehouse.ods_now_day_anchor_live al
         WHERE al.contract_sign_time IS NOT NULL
         GROUP BY al.platform_id,
                  al.anchor_no) t0
         LEFT JOIN (SELECT * FROM warehouse.ods_yujia_anchor_list WHERE platform = 'NOW') t1 ON t0.anchor_no = t1.uid
;


-- 计算每月主播开播天数，开播时长，流水
-- DROP TABLE IF EXISTS stage.stage_now_month_anchor_live;
-- CREATE TABLE stage.stage_now_month_anchor_live
DELETE
FROM stage.stage_now_month_anchor_live
WHERE DATE_FORMAT(dt, '%Y-%m') BETWEEN DATE_FORMAT('{start_date}', '%Y-%m') AND DATE_FORMAT('{end_date}', '%Y-%m');
INSERT INTO stage.stage_now_month_anchor_live
SELECT CONCAT(DATE_FORMAT(al.dt, '%Y-%m'), '-01')                         AS dt,
       al.platform_id,
       al.anchor_no,
       SUM(revenue_rmb)                                                   AS revenue,
       COUNT(DISTINCT CASE WHEN al.live_status = 1 THEN dt ELSE NULL END) AS live_days,
       SUM(al.duration)                                                   AS duration
FROM warehouse.ods_now_day_anchor_live al
WHERE DATE_FORMAT(dt, '%Y-%m') BETWEEN DATE_FORMAT('{start_date}', '%Y-%m') AND DATE_FORMAT('{end_date}', '%Y-%m')
GROUP BY CONCAT(DATE_FORMAT(al.dt, '%Y-%m'), '-01'),
         al.platform_id,
         al.anchor_no
;