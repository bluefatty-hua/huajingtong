-- 主播最早开播时间（基于现有数据）
DROP TABLE IF EXISTS stage.stage_yy_anchor_min_live_dt;
CREATE TABLE stage.stage_yy_anchor_min_live_dt
SELECT al.platform_id,
       al.anchor_uid,
       MIN(dt) AS min_live_dt
FROM warehouse.ods_yy_day_anchor_live al
WHERE al.live_status = 1
#   AND al.comment = 'orig'
GROUP BY al.platform_id,
         al.anchor_uid
;


-- 主播最早签约时间（基于现有数据）,后期结合公司现有主播的签约时间
DROP TABLE IF EXISTS stage.stage_yy_anchor_min_sign_dt;
CREATE TABLE stage.stage_yy_anchor_min_sign_dt
SELECT al.platform_id,
       al.anchor_uid,
       MIN(contract_signtime) AS min_sign_dt
FROM warehouse.ods_yy_day_anchor_live al
WHERE al.contract_signtime IS NOT NULL
#   AND al.comment = 'orig'
GROUP BY al.platform_id,
         al.anchor_uid
;


-- 计算每月主播开播天数，开播时长，流水
DROP TABLE IF EXISTS stage.stage_yy_month_anchor_live;
CREATE TABLE stage.stage_yy_month_anchor_live
SELECT CONCAT(DATE_FORMAT(al.dt, '%Y-%m'), '-01')                         AS dt,
       al.platform_id,
       al.anchor_uid,
       SUM(bluediamond)                                                   AS revenue,
       COUNT(DISTINCT CASE WHEN al.live_status = 1 THEN dt ELSE NULL END) AS live_days,
       SUM(al.duration)                                                   AS duration
FROM warehouse.ods_yy_day_anchor_live al
WHERE dt < '2020-02-01'
#   AND comment = 'orig'
GROUP BY CONCAT(DATE_FORMAT(al.dt, '%Y-%m'), '-01'),
         al.platform_id,
         al.anchor_uid
;



