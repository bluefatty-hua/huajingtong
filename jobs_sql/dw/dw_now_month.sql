-- 汇总维度 月-公会
-- 汇总指标 公会主播数 公会开播主播数 公会主播收入

# DROP TABLE IF EXISTS warehouse.dw_now_month_guild_live_true;
# CREATE TABLE warehouse.dw_now_month_guild_live_true AS
DELETE
FROM warehouse.dw_now_month_guild_live_true
WHERE dt BETWEEN DATE_FORMAT('{start_date}', '%Y-%m-01') AND DATE_FORMAT('{end_date}', '%Y-%m-01');
INSERT INTO warehouse.dw_now_month_guild_live_true
SELECT dt,
       platform_id,
       platform_name,
       backend_account_id,
       anchor_cnt  AS anchor_cnt_true,
       revenue_rmb AS revenue_rmb_true,
       average_anchor_revenue_rmb,
       anchor_live_rate
FROM warehouse.ods_now_month_guild_live
WHERE dt BETWEEN DATE_FORMAT('{start_date}', '%Y-%m-01') AND DATE_FORMAT('{end_date}', '%Y-%m-01')
;


-- DROP TABLE IF EXISTS warehouse.dw_now_month_guild_live;
-- CREATE TABLE warehouse.dw_now_month_guild_live AS
DELETE
FROM warehouse.dw_now_month_guild_live
WHERE dt BETWEEN DATE_FORMAT('{start_date}', '%Y-%m-01') AND DATE_FORMAT('{end_date}', '%Y-%m-01');
INSERT INTO warehouse.dw_now_month_guild_live
SELECT DATE_FORMAT(al.dt, '%Y-%m-01')                                               AS dt,
       al.platform_id,
       al.platform_name,
       al.backend_account_id,
       al.month_newold_state                                                        AS newold_state,
       active_state,
       revenue_level,
       COUNT(DISTINCT al.anchor_no)                                                 AS anchor_cnt,
       COUNT(DISTINCT CASE WHEN al.live_status = 1 THEN al.anchor_no ELSE NULL END) AS anchor_live_cnt,
       SUM(duration)                                                                AS duration,
       SUM(al.revenue_rmb)                                                          AS revenue_rmb
FROM (SELECT *,
             warehouse.ANCHOR_NEW_OLD(min_live_dt, min_sign_dt, CASE
                                                                    WHEN dt < DATE_FORMAT('{end_date}', '%Y-%m-01')
                                                                        THEN LAST_DAY(dt)
                                                                    ELSE '{end_date}' END, 180) AS month_newold_state
      FROM warehouse.dw_now_day_anchor_live
WHERE dt BETWEEN DATE_FORMAT('{start_date}', '%Y-%m-01') AND '{end_date}'
     ) al
GROUP BY DATE_FORMAT(dt, '%Y-%m-01'),
         al.platform_id,
         al.platform_name,
         al.backend_account_id,
         al.MONTH_newold_state,
         active_state,
         revenue_level
;



-- 汇总维度 月-公会-主播
-- 汇总指标 主播数，开播主播数，开播时长，流水
-- DROP TABLE IF EXISTS warehouse.dw_now_month_anchor_live;
-- CREATE TABLE warehouse.dw_now_month_anchor_live AS
DELETE
FROM warehouse.dw_now_month_anchor_live
WHERE DATE_FORMAT(dt, '%Y%m') BETWEEN DATE_FORMAT('{start_date}', '%Y%m') AND DATE_FORMAT('{end_date}', '%Y%m');
INSERT INTO warehouse.dw_now_month_anchor_live
SELECT DATE_FORMAT(dt, '%Y-%m-01')                                         AS dt,
       t.platform_id,
       t.platform_name,
       t.backend_account_id,
       anchor_no,
       COUNT(DISTINCT CASE WHEN t.live_status = 1 THEN t.dt ELSE NULL END) AS live_days,
       SUM(t.duration)                                                     AS duration,
       SUM(t.revenue_rmb)                                                  AS revenue_rmb
FROM warehouse.dw_now_day_anchor_live t
WHERE DATE_FORMAT(dt, '%Y%m') BETWEEN DATE_FORMAT('{start_date}', '%Y%m') AND DATE_FORMAT('{end_date}', '%Y%m')
GROUP BY DATE_FORMAT(dt, '%Y-%m-01'),
         t.platform_id,
         t.platform_name,
         t.backend_account_id,
         anchor_no
;

