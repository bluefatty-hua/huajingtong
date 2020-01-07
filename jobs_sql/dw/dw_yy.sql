-- 数据源 warehouse.ods_yy_anchor_live_detail_daily
-- ===============================================================
-- 汇总数据
-- 汇总维度 月-主播
-- 汇总指标 开播天数，开播时长，虚拟币收入，主播佣金，公会佣金
DROP TABLE IF EXISTS warehouse.dw_sum_yy_pf_an_mon;
CREATE TABLE warehouse.dw_sum_yy_pf_an_mon AS
SELECT YEAR(t.dt)                                                 AS rpt_year,
       MONTH(t.dt)                                                AS rpt_month,
       t.platform_id,
       t.platform_name,
       t.anchor_no,
       COUNT(CASE WHEN t.live_status = 1 THEN t.dt ELSE NULL END) AS live_days,
       SUM(t.duration)                               AS sum_duration,
       SUM(t.virtual_coin)                           AS total_vir_coin,
       SUM(t.anchor_commission)                      AS total_an_commission,
       SUM(t.guild_commission)                       AS total_g_commission
FROM warehouse.ods_yy_anchor_live_detail_daily t
WHERE t.dt < CURRENT_DATE
GROUP BY YEAR(t.dt),
         MONTH(t.dt),
         t.platform_id,
         t.platform_name,
         t.anchor_no
;
