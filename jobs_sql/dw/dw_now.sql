# DROP TABLE IF EXISTS warehouse.dw_now_day_anchor_live;
# CREATE TABLE warehouse.dw_now_day_anchor_live AS
DELETE
FROM warehouse.dw_now_day_anchor_live
WHERE dt BETWEEN '{start_date}' AND '{end_date}';
INSERT INTO warehouse.dw_now_day_anchor_live
SELECT *
FROM warehouse.ods_now_day_anchor_live
WHERE dt BETWEEN '{start_date}' AND '{end_date}'
;


-- 汇总维度 日-公会
-- 汇总指标 开播天数，开播时长，主播流水，公会流水，公会收入
-- DROP TABLE IF EXISTS warehouse.dw_now_day_guild_live;
-- CREATE TABLE warehouse.dw_now_day_guild_live AS
DELETE
FROM warehouse.dw_now_day_guild_live
WHERE dt BETWEEN '{start_date}' AND '{end_date}';
INSERT INTO warehouse.dw_now_day_guild_live
SELECT al.dt,
       al.platform_id,
       al.platform_name,
       al.backend_account_id,
       al.anchor_cnt,
       al.anchor_live_cnt,
       ac.anchor_live_cnt  AS anchor_live_cnt_true,
       al.duration,
       al.revenue_rmb,
       ac.revenue_rmb      AS guild_commission_rmb_true,
       ac.guild_income_rmb AS guild_income_rmb_true
FROM (SELECT t.dt,
             t.platform_id,
             t.platform_name,
             t.backend_account_id,
             COUNT(t.anchor_no)                                                         AS anchor_cnt,
             COUNT(DISTINCT CASE WHEN t.live_status = 1 THEN t.anchor_no ELSE NULL END) AS anchor_live_cnt,
             SUM(t.duration)                                                            AS duration,
             SUM(t.revenue_rmb)                                                         AS revenue_rmb
      FROM warehouse.ods_now_day_anchor_live t
      WHERE t.dt BETWEEN '{start_date}' AND '{end_date}'
      GROUP BY t.dt,
               t.platform_id,
               t.platform_name,
               t.backend_account_id) al
         LEFT JOIN warehouse.ods_now_day_guild_live ac
                   ON al.dt = ac.dt AND al.backend_account_id = ac.backend_account_id
;


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
         SELECT DATE_FORMAT(CONCAT(YEAR(t.dt), '-', MONTH(t.dt), '-01'), '%Y-%m-%d')       AS dt,
                t.platform_id,
                t.platform_name,
                t.backend_account_id,
                COUNT(DISTINCT t.anchor_no)                                                AS anchor_cnt,
                COUNT(DISTINCT CASE WHEN t.live_status = 1 THEN t.anchor_no ELSE NULL END) AS anchor_live_cnt,
                SUM(t.revenue_rmb)                                                         AS revenue_rmb
         FROM warehouse.ods_now_day_anchor_live t
         GROUP BY DATE_FORMAT(CONCAT(YEAR(t.dt), '-', MONTH(t.dt), '-01'), '%Y-%m-%d'),
                  t.platform_id,
                  t.platform_name,
                  t.backend_account_id) al
         LEFT JOIN warehouse.ods_now_month_guild gc
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
SELECT DATE_FORMAT(CONCAT(YEAR(t.dt), '-', MONTH(t.dt), '-01'), '%Y-%m-%d') AS dt,
       t.platform_id,
       t.platform_name,
       t.backend_account_id,
       anchor_no,
       COUNT(DISTINCT CASE WHEN t.live_status = 1 THEN t.dt ELSE NULL END)  AS live_days,
       SUM(t.duration)                                                      AS duration,
       SUM(t.revenue_rmb)                                                   AS revenue_rmb
FROM warehouse.ods_now_day_anchor_live t
# WHERE DATE_FORMAT(dt, '%Y%m') BETWEEN DATE_FORMAT('{start_date}', '%Y%m') AND DATE_FORMAT('{end_date}', '%Y%m')
GROUP BY DATE_FORMAT(CONCAT(YEAR(t.dt), '-', MONTH(t.dt), '-01'), '%Y-%m-%d'),
         t.platform_id,
         t.platform_name,
         t.backend_account_id,
         anchor_no
;

