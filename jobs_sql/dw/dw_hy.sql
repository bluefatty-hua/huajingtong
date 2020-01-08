-- 数据源 warehouse.ods_bb_anchor_live_detail_daily
-- ===============================================================
-- 汇总数据
-- 汇总维度 月-主播
-- 汇总指标 开播天数，开播时长，虚拟币收入
DROP TABLE IF EXISTS warehouse.dw_sum_hy_an_mon;
CREATE TABLE warehouse.dw_sum_hy_an_mon AS
SELECT YEAR(t.dt)                                                 AS rpt_year,
       MONTH(t.dt)                                                AS rpt_month,
       t.platform_id,
       t.platform_name,
       t.anchor_no,
       COUNT(CASE WHEN t.live_status = 1 THEN t.dt ELSE NULL END) AS live_days,
       SUM(t.duration)                                            AS sum_duration,
       ROUND(SUM(t.amt), 2)                                                 AS sum_amt
FROM warehouse.ods_hy_anchor_live_detail_daily t
WHERE t.dt < CURRENT_DATE
GROUP BY YEAR(t.dt),
         MONTH(t.dt),
         t.platform_id,
         t.platform_name,
         t.anchor_no
;


-- 汇总数据
-- 汇总维度 月
-- 汇总指标 主播数，开播主播数，主播收入
DROP TABLE IF EXISTS warehouse.dw_sum_hy_g_mon;
CREATE TABLE warehouse.dw_sum_hy_g_mon AS
SELECT YEAR(t.dt)                                                                 AS rpt_year,
       MONTH(t.dt)                                                                AS rpt_month,
       t.platform_id,
       t.platform_name,
       t.channel_id,
       COUNT(DISTINCT t.anchor_no)                                                AS an_cnt,
       COUNT(DISTINCT CASE WHEN t.live_status = 1 THEN t.anchor_no ELSE NULL END) AS an_live_cnt,
       ROUND(SUM(t.amt), 2)                                                       AS sum_amt
FROM warehouse.ods_hy_anchor_live_detail_daily t
WHERE t.dt < CURRENT_DATE
GROUP BY YEAR(t.dt),
         MONTH(t.dt),
         t.platform_id,
         t.platform_name,
         t.channel_id
;