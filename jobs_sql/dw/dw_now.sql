-- 数据源 warehouse.ods_now_anchor_live_detail_daily
-- ===============================================================
-- 汇总数据
-- 汇总维度 月-主播
-- 汇总指标 开播天数，开播时长，虚拟币收入

DROP TABLE IF EXISTS warehouse.dw_sum_now_pf_an_mon;
CREATE TABLE warehouse.dw_sum_now_pf_an_mon AS
SELECT YEAR(t.dt)                                                                      AS rpt_year,
       MONTH(t.dt)                                                                     AS rpt_month,
       t.platform_id,
       t.platform_name,
       t.anchor_no,
       COUNT(CASE WHEN t.live_status = 1 THEN t.dt ELSE NULL END)                      AS live_days,
       SUM(CASE WHEN t.duration IS NULL OR t.duration = '' THEN 0 ELSE t.duration END) AS sum_duration,
       SUM(CASE WHEN t.amt IS NULL OR t.amt = '' THEN 0 ELSE t.amt END)                AS sum_amt
FROM warehouse.ods_now_anchor_live_detail_daily t
GROUP BY YEAR(t.dt),
         MONTH(t.dt),
         t.backend_account_id,
         t.platform_id,
         t.platform_name,
         t.anchor_no
;
