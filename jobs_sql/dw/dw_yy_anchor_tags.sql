-- 主播最早开播时间（基于现有数据）
-- DROP TABLE IF EXISTS stage.stage_yy_anchor_min_live_dt;
-- CREATE TABLE stage.stage_yy_anchor_min_live_dt
INSERT IGNORE INTO stage.stage_yy_anchor_min_live_dt
SELECT 1000 AS platform_id,
       t.anchor_no,
       MIN(t.min_live_dt)
FROM (SELECT al.anchor_no,
             MIN(dt) AS min_live_dt
      FROM warehouse.ods_yy_day_anchor_live al
      WHERE al.live_status = 1
      GROUP BY al.anchor_no
      UNION
      SELECT yj.uid             AS anchor_no,
             yj.first_live_time AS min_live_time
      FROM warehouse.ods_yujia_anchor_list yj
      WHERE platform = 'YY'
        AND first_live_time != '1970-01-01'
        AND uid = '') t
GROUP BY t.anchor_no
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
        AND yj.sign_time <> '1970-01-01'
     ) t
GROUP BY t.anchor_no
;


-- 计算每月主播开播天数，开播时长，流水
-- DROP TABLE IF EXISTS stage.stage_yy_month_anchor_live;
-- CREATE TABLE stage.stage_yy_month_anchor_live
DELETE
FROM stage.stage_yy_month_anchor_live
WHERE dt = '{month}';
INSERT INTO stage.stage_yy_month_anchor_live
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
           AND dt < '{month}' + INTERVAL 1 MONTH
         GROUP BY DATE_FORMAT(al.dt, '%Y-%m-01'),
                  al.platform_id,
                  al.anchor_uid) t
;

