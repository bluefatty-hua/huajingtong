-- 汇总维度 月-公会
-- 汇总指标 公会主播数 公会开播主播数 公会主播收入
-- DROP TABLE IF EXISTS warehouse.dw_now_month_guild_live;
-- CREATE TABLE warehouse.dw_now_month_guild_live AS
DELETE
FROM warehouse.dw_now_month_guild_live
WHERE DATE_FORMAT(dt, '%Y%m') BETWEEN DATE_FORMAT('{start_date}', '%Y%m') AND DATE_FORMAT('{end_date}', '%Y%m');
INSERT INTO warehouse.dw_now_month_guild_live
SELECT al.dt,
       al.platform_id,
       al.platform_name,
       al.backend_account_id,
       al.anchor_cnt,
       al.anchor_live_cnt,
       al.revenue_rmb,
       gc.anchor_cnt  AS anchor_cnt_true,
       gc.revenue_rmb AS revenue_rmb_true,
       gc.anchor_live_rate,
       gc.average_anchor_revenue_rmb
FROM (
         SELECT DATE_FORMAT(dt, '%Y-%m-01')       AS dt,
                t.platform_id,
                t.platform_name,
                t.backend_account_id,
                COUNT(DISTINCT t.anchor_no)                                                AS anchor_cnt,
                COUNT(DISTINCT CASE WHEN t.live_status = 1 THEN t.anchor_no ELSE NULL END) AS anchor_live_cnt,
                SUM(t.revenue_rmb)                                                         AS revenue_rmb
         FROM warehouse.ods_now_day_anchor_live t
         GROUP BY DATE_FORMAT(dt, '%Y-%m-01'),
                  t.platform_id,
                  t.platform_name,
                  t.backend_account_id) al
         LEFT JOIN warehouse.ods_now_month_guild_live gc
                   ON al.dt = gc.dt AND al.backend_account_id = gc.backend_account_id
WHERE DATE_FORMAT(al.dt, '%Y%m') BETWEEN DATE_FORMAT('{start_date}', '%Y%m') AND DATE_FORMAT('{end_date}', '%Y%m')
;



-- 汇总维度 月-公会-主播
-- 汇总指标 主播数，开播主播数，开播时长，流水
-- DROP TABLE IF EXISTS warehouse.dw_now_month_anchor_live;
-- CREATE TABLE warehouse.dw_now_month_anchor_live AS
DELETE
FROM warehouse.dw_now_month_anchor_live
WHERE DATE_FORMAT(dt, '%Y%m') BETWEEN DATE_FORMAT('{start_date}', '%Y%m') AND DATE_FORMAT('{end_date}', '%Y%m');
INSERT INTO warehouse.dw_now_month_anchor_live
SELECT DATE_FORMAT(dt, '%Y-%m-01') AS dt,
       t.platform_id,
       t.platform_name,
       t.backend_account_id,
       anchor_no,
       COUNT(DISTINCT CASE WHEN t.live_status = 1 THEN t.dt ELSE NULL END)  AS live_days,
       SUM(t.duration)                                                      AS duration,
       SUM(t.revenue_rmb)                                                   AS revenue_rmb
FROM warehouse.ods_now_day_anchor_live t
WHERE DATE_FORMAT(dt, '%Y%m') BETWEEN DATE_FORMAT('{start_date}', '%Y%m') AND DATE_FORMAT('{end_date}', '%Y%m')
GROUP BY DATE_FORMAT(dt, '%Y-%m-01'),
         t.platform_id,
         t.platform_name,
         t.backend_account_id,
         anchor_no
;

