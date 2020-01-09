-- 数据源 warehouse.ods_yy_anchor_live_detail_daily
-- ===============================================================
-- 汇总数据
-- 汇总维度 月-主播
-- 汇总指标 开播天数，开播时长，虚拟币收入，主播佣金，公会佣金
DROP TABLE IF EXISTS warehouse.dw_sum_yy_an_mon;
CREATE TABLE warehouse.dw_sum_yy_an_mon AS
SELECT DATE_FORMAT(CONCAT(YEAR(t.dt), '-', MONTH(t.dt), '-01'), '%Y-%m-%d')        AS rpt_month,
       t.platform_id,
       t.platform_name,
       t.anchor_no,
       COUNT(CASE WHEN t.live_status = 1 THEN t.dt ELSE NULL END)                  AS live_days,
       SUM(CASE WHEN t.duration >= 0 THEN t.duration ELSE 0 END)                   AS sum_duration,
       SUM(CASE WHEN t.virtual_coin >= 0 THEN t.virtual_coin ELSE 0 END)           AS total_vir_coin,
       SUM(CASE WHEN t.anchor_commission >= 0 THEN t.anchor_commission ELSE 0 END) AS total_an_commission,
       SUM(CASE WHEN t.guild_commission >= 0 THEN t.guild_commission ELSE 0 END)   AS total_g_commission,
       COUNT(DISTINCT t.dt) AS dt_cnt
FROM warehouse.ods_anchor_yy_live_detail_daily t
WHERE t.dt < CURRENT_DATE AND t.guild_commission > 0
GROUP BY DATE_FORMAT(CONCAT(YEAR(t.dt), '-', MONTH(t.dt), '-01'), '%Y-%m-%d'),
         t.platform_id,
         t.platform_name,
         t.anchor_no
;


-- 汇总数据
-- 汇总维度 月-公会
-- 汇总指标 主播数，开播主播数，主播虚拟币收入,主播佣金收入，公会佣金收入
DROP TABLE IF EXISTS warehouse.dw_sum_yy_g_mon;
CREATE TABLE warehouse.dw_sum_yy_g_mon AS
SELECT DATE_FORMAT(CONCAT(YEAR(t.dt), '-', MONTH(t.dt), '-01'), '%Y-%m-%d')        AS rpt_month,
       t.platform_id,
       t.platform_name,
       t.backend_account_id,
       t.channel_num,
       COUNT(DISTINCT t.anchor_no)                                                 AS an_cnt,
       COUNT(DISTINCT CASE WHEN t.live_status = 1 THEN t.anchor_no ELSE NULL END)  AS an_live_cnt,
       SUM(CASE WHEN t.virtual_coin >= 0 THEN t.virtual_coin ELSE 0 END)           AS total_vir_coin,
       SUM(CASE WHEN t.anchor_commission >= 0 THEN t.anchor_commission ELSE 0 END) AS total_an_commission,
       SUM(CASE WHEN t.guild_commission >= 0 THEN t.guild_commission ELSE 0 END)   AS total_g_commission,
       COUNT(DISTINCT t.dt)                                                        AS dt_cnt
FROM warehouse.ods_anchor_yy_live_detail_daily t
WHERE t.dt < CURRENT_DATE
GROUP BY DATE_FORMAT(CONCAT(YEAR(t.dt), '-', MONTH(t.dt), '-01'), '%Y-%m-%d'),
         t.platform_id,
         t.platform_name,
         t.backend_account_id,
         t.channel_num
;

