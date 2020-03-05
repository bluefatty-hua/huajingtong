-- 主播最早开播时间（基于现有数据）
-- DROP TABLE IF EXISTS stage.stage_dy_xjl_anchor_min_live_dt;
-- CREATE TABLE stage.stage_dy_xjl_anchor_min_live_dt
INSERT IGNORE INTO stage.stage_dy_xjl_anchor_min_live_dt
SELECT 1005 AS platform_id,
       t.anchor_uid,
       MIN(t.min_live_dt)
FROM (SELECT al.anchor_uid,
             MIN(al.dt) AS min_live_dt
      FROM warehouse.ods_dy_xjl_day_anchor_live al
      WHERE al.live_status = 1
      GROUP BY al.anchor_uid
      UNION
      SELECT ai.anchor_uid,
             yj.first_live_time AS min_live_time
      FROM warehouse.delete_ods_yujia_anchor_list yj
               INNER JOIN warehouse.ods_dy_xjl_day_anchor_info ai ON yj.uid = ai.anchor_no
      WHERE platform = '抖音'
        AND first_live_time != '0000-00-00') t
GROUP BY t.anchor_uid
;



-- 主播最早签约时间（基于现有数据）,后期结合公司现有主播的签约时间
-- DROP TABLE IF EXISTS stage.stage_dy_xjl_anchor_min_sign_dt;
-- CREATE TABLE stage.stage_dy_xjl_anchor_min_sign_dt
INSERT IGNORE INTO stage.stage_dy_xjl_anchor_min_sign_dt
SELECT 1005             AS platform_id,
       t.anchor_uid,
       MIN(min_sign_dt) AS min_sign_dt
FROM (SELECT al.anchor_uid,
             MIN(al.sign_time) AS min_sign_dt
      FROM warehouse.ods_dy_xjl_day_anchor_live al
      WHERE al.signing_time IS NOT NULL
        AND al.signing_time <> 0
      GROUP BY al.anchor_uid
      UNION
      SELECT yj.uid       AS anchor_no,
             yj.sign_time AS min_sign_dt
      FROM warehouse.delete_ods_yujia_anchor_list yj
               INNER JOIN warehouse.ods_dy_xjl_day_anchor_info ai ON yj.uid = ai.anchor_no
      WHERE platform = '抖音'
        AND yj.uid <> 0
        AND yj.sign_time <> '0000-00-00'
     ) t
GROUP BY t.anchor_uid
;


-- 计算每月主播开播天数，开播时长，流水
-- DROP TABLE IF EXISTS stage.stage_dy_xjl_month_anchor_live;
-- CREATE TABLE stage.stage_dy_xjl_month_anchor_live
DELETE
FROM stage.stage_dy_xjl_month_anchor_live
WHERE DATE_FORMAT(dt, '%Y-%m') BETWEEN DATE_FORMAT('{start_date}', '%Y-%m') AND DATE_FORMAT('{end_date}', '%Y-%m');
INSERT INTO stage.stage_dy_xjl_month_anchor_live
SELECT CONCAT(DATE_FORMAT(al.dt, '%Y-%m'), '-01')                         AS dt,
       al.platform_id,
       al.anchor_uid,
       SUM(revenue)                                                       AS revenue,
       COUNT(DISTINCT CASE WHEN al.live_status = 1 THEN dt ELSE NULL END) AS live_days,
       SUM(al.duration)                                                   AS duration
FROM warehouse.ods_dy_xjl_day_anchor_live al
WHERE DATE_FORMAT(dt, '%Y-%m') BETWEEN DATE_FORMAT('{start_date}', '%Y-%m') AND DATE_FORMAT('{end_date}', '%Y-%m')
GROUP BY CONCAT(DATE_FORMAT(al.dt, '%Y-%m'), '-01'),
         al.platform_id,
         al.anchor_uid
;