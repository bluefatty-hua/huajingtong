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
FROM warehouse.ods_anchor_hy_live_detail_daily t
WHERE t.dt < CURRENT_DATE
GROUP BY YEAR(t.dt),
         MONTH(t.dt),
         t.platform_id,
         t.platform_name,
         t.anchor_no
;


-- 汇总维度 月-公会或频道
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
FROM warehouse.ods_anchor_hy_live_detail_daily t
WHERE t.dt < CURRENT_DATE
GROUP BY YEAR(t.dt),
         MONTH(t.dt),
         t.platform_id,
         t.platform_name,
         t.channel_id
;


-- ==========================================================================
-- 公会或频道
-- 汇总维度 月-公会或频道
-- 汇总指标 公会流水，公会收入，礼物|贵族|守护公会分成，统计天数
DROP TABLE IF EXISTS warehouse.dw_sum_hy_g_amt_mon;
CREATE TABLE warehouse.dw_sum_hy_g_amt_mon AS
SELECT t.platform_id,
       t.platform_name,
       YEAR(t.dt) AS rpt_year,
       MONTH(t.dt) AS rpt_month,
       t.channel_id,
       t.channel_num,
       SUM(total_amt) AS g_total_amt,
       SUM(t.g_gift_vir_coin + t.g_guard_vir_coin + t.g_nobel_vir_coin) / 1000 AS g_final_amt,
       SUM(t.g_gift_vir_coin) AS g_gift_vir_coin,
       SUM(t.g_guard_vir_coin) AS g_guard_vir_coin,
       SUM(t.g_nobel_vir_coin) AS g_nobel_vir_coin,
       COUNT(DISTINCT t.dt) AS dt_cnt
FROM warehouse.ods_guild_hy_amt_daily t
GROUP BY t.platform_id,
         t.platform_name,
         YEAR(t.dt),
         MONTH(t.dt),
         t.channel_id,
         t.channel_num
;
