-- 汇总维度 月-公会
-- 汇总指标 公会主播数 公会开播主播数 公会主播收入

# DROP TABLE IF EXISTS warehouse.dw_now_month_guild_live_true;
# CREATE TABLE warehouse.dw_now_month_guild_live_true AS
DELETE
FROM warehouse.dw_now_month_guild_live_true
WHERE dt = '{month}';
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
WHERE dt >= '{month}'
  AND dt < '{month}' + INTERVAL 1 MONTH
;


-- DROP TABLE IF EXISTS warehouse.dw_now_month_guild_live;
-- CREATE TABLE warehouse.dw_now_month_guild_live AS
DELETE
FROM warehouse.dw_now_month_guild_live
WHERE dt = '{month}';
INSERT INTO warehouse.dw_now_month_guild_live
SELECT DATE_FORMAT(al.dt, '%Y-%m-01')                                               AS dt,
       al.platform_id,
       al.platform_name,
       al.backend_account_id,
       al.city,
       al.month_newold_state                                                        AS newold_state,
       al.active_state,
       al.revenue_level,
       COUNT(DISTINCT al.anchor_no)                                                 AS anchor_cnt,
       COUNT(DISTINCT CASE WHEN al.live_status = 1 THEN al.anchor_no ELSE NULL END) AS anchor_live_cnt,
       SUM(duration)                                                                AS duration,
       SUM(al.revenue)                                                          AS revenue_rmb
FROM (SELECT *,
             warehouse.ANCHOR_NEW_OLD(min_live_dt, min_sign_dt, CASE
                                                                    WHEN dt < DATE_FORMAT('{cur_date}', '%Y-%m-01')
                                                                        THEN LAST_DAY(dt)
                                                                    ELSE '{cur_date}' END, 180) AS month_newold_state
      FROM warehouse.dw_now_day_anchor_live
      WHERE dt >= '{month}'
        AND dt < '{month}' + INTERVAL 1 MONTH
     ) al
GROUP BY DATE_FORMAT(dt, '%Y-%m-01'),
         al.platform_id,
         al.platform_name,
         al.backend_account_id,
         al.city,
         al.month_newold_state,
         al.active_state,
         al.revenue_level
;



-- 汇总维度 月-公会-主播
-- 汇总指标 主播数，开播主播数，开播时长，流水
-- DROP TABLE IF EXISTS warehouse.dw_now_month_anchor_live;
-- CREATE TABLE warehouse.dw_now_month_anchor_live AS
DELETE
FROM warehouse.dw_now_month_anchor_live
WHERE dt = '{month}';
INSERT INTO warehouse.dw_now_month_anchor_live
SELECT DATE_FORMAT(dt, '%Y-%m-01')                                         AS dt,
       t.platform_id,
       t.platform_name,
       t.backend_account_id,
       t.city,
       t.active_state,
       t.month_newold_state                                                AS newold_state,
       t.revenue_level,
       t.anchor_no,
       COUNT(DISTINCT CASE WHEN t.live_status = 1 THEN t.dt ELSE NULL END) AS live_days,
       SUM(t.duration)                                                     AS duration,
       SUM(t.revenue)                                                  AS revenue_rmb
-- cur_date: t-1
FROM (SELECT *,
             warehouse.ANCHOR_NEW_OLD(min_live_dt, min_sign_dt, CASE
                                                                    WHEN dt < DATE_FORMAT('{cur_date}', '%Y-%m-01')
                                                                        THEN LAST_DAY(dt)
                                                                    ELSE '{cur_date}' END, 180) AS month_newold_state
      FROM warehouse.dw_now_day_anchor_live
      WHERE dt >= '{month}'
        AND dt < '{month}' + INTERVAL 1 MONTH
     ) t
GROUP BY DATE_FORMAT(dt, '%Y-%m-01'),
         t.platform_id,
         t.platform_name,
         t.backend_account_id,
         t.city,
         t.active_state,
         t.month_newold_state,
         t.revenue_level,
         t.anchor_no
;
