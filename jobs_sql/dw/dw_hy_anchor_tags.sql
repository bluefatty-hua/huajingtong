-- 主播最早开播时间（基于现有数据）
-- DROP TABLE IF EXISTS stage.stage_hy_anchor_min_live_dt;
-- CREATE TABLE stage.stage_hy_anchor_min_live_dt
INSERT IGNORE INTO stage.stage_hy_anchor_min_live_dt
SELECT t.anchor_no,
       MIN(t.min_live_dt)
FROM (SELECT ai.anchor_no,
             MIN(al.dt) AS min_live_dt
      FROM warehouse.ods_huya_day_anchor_live al
               INNER JOIN (SELECT DISTINCT anchor_uid,
                                           anchor_no
                           FROM warehouse.ods_huya_day_anchor_info) ai
                          ON al.anchor_uid = ai.anchor_uid
      WHERE al.live_status = 1
      GROUP BY ai.anchor_no
      UNION
      SELECT yj.uid             AS anchor_no,
             yj.first_live_time AS min_live_time
      FROM warehouse.ods_yujia_anchor_list yj
      WHERE platform = '虎牙'
        AND first_live_time <> '2019-01-01') t
GROUP BY anchor_no
;


-- 主播最早签约时间（基于现有数据）,后期结合公司现有主播的签约时间
-- DROP TABLE IF EXISTS stage.stage_hy_anchor_min_sign_dt;
-- CREATE TABLE stage.stage_hy_anchor_min_sign_dt
INSERT IGNORE INTO stage.stage_hy_anchor_min_sign_dt
SELECT t.anchor_no,
       MIN(min_sign_dt) AS min_sign_dt
FROM (SELECT al.anchor_no,
             MIN(from_unixtime(sign_time, '%Y-%m-%d')) AS min_sign_dt
      FROM warehouse.ods_huya_day_anchor_info al
      GROUP BY al.anchor_no
      UNION
      SELECT yj.uid       AS anchor_no,
             yj.sign_time AS min_sign_dt
      FROM warehouse.ods_yujia_anchor_list yj
      WHERE platform = '虎牙'
        AND yj.sign_time <> '2019-01-01') t
GROUP BY t.anchor_no
;


-- 计算每月主播开播天数，开播时长，流水
-- DROP TABLE IF EXISTS stage.stage_hy_month_anchor_live;
-- CREATE TABLE stage.stage_hy_month_anchor_live
DELETE
FROM stage.stage_hy_month_anchor_live
WHERE DATE_FORMAT(dt, '%Y-%m') BETWEEN DATE_FORMAT('{start_date}', '%Y-%m') AND DATE_FORMAT('{end_date}', '%Y-%m');
INSERT INTO stage.stage_hy_month_anchor_live
SELECT CONCAT(DATE_FORMAT(al.dt, '%Y-%m'), '-01')                         AS dt,
       al.anchor_uid,
       SUM(income)                                                        AS revenue,
       COUNT(DISTINCT CASE WHEN al.live_status = 1 THEN dt ELSE NULL END) AS live_days,
       SUM(al.duration)                                                   AS duration
FROM warehouse.ods_huya_day_anchor_live al
WHERE DATE_FORMAT(dt, '%Y-%m') BETWEEN DATE_FORMAT('{start_date}', '%Y-%m') AND DATE_FORMAT('{end_date}', '%Y-%m')
GROUP BY CONCAT(DATE_FORMAT(al.dt, '%Y-%m'), '-01'),
         al.anchor_uid
;