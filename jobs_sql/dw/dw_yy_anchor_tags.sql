-- 主播最早开播时间（基于现有数据）
-- DROP TABLE IF EXISTS stage.stage_yy_anchor_min_live_dt;
-- CREATE TABLE stage.stage_yy_anchor_min_live_dt
INSERT IGNORE INTO stage.stage_yy_anchor_min_live_dt
SELECT 1000 AS platform_id,
       t.anchor_uid,
       MIN(t.min_live_dt)
FROM (SELECT al.anchor_uid,
             MIN(dt) AS min_live_dt
      FROM warehouse.ods_yy_day_anchor_live al
      WHERE al.live_status = 1
      GROUP BY al.anchor_uid
      UNION
      SELECT yj.uid             AS anchor_no,
             yj.first_live_time AS min_live_time
      FROM warehouse.ods_yujia_anchor_list yj
      WHERE platform = 'YY'
        AND first_live_time != '0000-00-00') t
GROUP BY t.anchor_uid
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
        AND yj.sign_time <> '0000-00-00'
     ) t
GROUP BY t.anchor_no
;


-- 计算每月主播开播天数，开播时长，流水
-- DROP TABLE IF EXISTS stage.stage_yy_month_anchor_live;
-- CREATE TABLE stage.stage_yy_month_anchor_live
DELETE
FROM stage.stage_yy_month_anchor_live
WHERE dt BETWEEN DATE_FORMAT('{start_date}', '%Y-%m-01') AND DATE_FORMAT('{end_date}', '%Y-%m-01');
INSERT INTO stage.stage_yy_month_anchor_live
SELECT DATE_FORMAT(al.dt, '%Y-%m-01')                                     AS dt,
       al.platform_id,
       al.anchor_uid,
       SUM(bluediamond)                                                   AS revenue,
       COUNT(DISTINCT CASE WHEN al.live_status = 1 THEN dt ELSE NULL END) AS live_days,
       SUM(al.duration)                                                   AS duration
FROM warehouse.ods_yy_day_anchor_live al
WHERE DATE_FORMAT(dt, '%Y-%m-01') BETWEEN DATE_FORMAT('{start_date}', '%Y-%m-01') AND DATE_FORMAT('{end_date}', '%Y-%m-01')
  AND comment = 'orig'
GROUP BY DATE_FORMAT(al.dt, '%Y-%m-01'),
         al.platform_id,
         al.anchor_uid
;

