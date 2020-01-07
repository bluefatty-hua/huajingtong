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
       SUM(t.duration)                                            AS sum_duration,
       SUM(t.virtual_coin)                                        AS total_vir_coin,
       SUM(t.anchor_commission)                                   AS total_an_commission,
       SUM(t.guild_commission)                                    AS total_g_commission
FROM warehouse.ods_yy_anchor_live_detail_daily t
WHERE t.dt < CURRENT_DATE
GROUP BY YEAR(t.dt),
         MONTH(t.dt),
         t.platform_id,
         t.platform_name,
         t.anchor_no
;


-- 汇总数据
-- 汇总维度 月
-- 汇总指标 主播数，开播主播数，主播虚拟币收入,主播佣金收入，公会佣金收入
DROP TABLE IF EXISTS warehouse.dw_sum_yy_mon;
CREATE TABLE warehouse.dw_sum_yy_mon AS
SELECT YEAR(t.dt)                                                                 AS rpt_year,
       MONTH(t.dt)                                                                AS rpt_month,
       t.platform_id,
       t.platform_name,
       COUNT(DISTINCT t.anchor_no)                                                AS an_cnt,
       COUNT(DISTINCT CASE WHEN t.live_status = 1 THEN t.anchor_no ELSE NULL END) AS an_live_cnt,
       ROUND(SUM(t.virtual_coin), 2)                                              AS total_vir_coin,
       SUM(t.anchor_commission)                                                   AS total_an_commission,
       SUM(t.guild_commission)                                                    AS total_g_commission
FROM warehouse.ods_yy_anchor_live_detail_daily t
WHERE t.dt < CURRENT_DATE
GROUP BY YEAR(t.dt),
         MONTH(t.dt),
         t.platform_id,
         t.platform_name
;

