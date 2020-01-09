-- 数据源 warehouse.ods_bb_anchor_live_detail_daily
-- ===============================================================
-- 汇总数据
-- 汇总维度 月-主播
-- 汇总指标 开播天数，开播时长，虚拟币收入
DROP TABLE IF EXISTS warehouse.dw_sum_bb_an_mon;
CREATE TABLE warehouse.dw_sum_bb_an_mon AS
SELECT DATE_FORMAT(CONCAT(YEAR(t.dt), '-', MONTH(t.dt), '-01'), '%Y-%m-%d')                                                AS rpt_month,
       t.platform_id,
       t.platform_name,
       t.backend_account_id,
       t.anchor_no,
       COUNT(CASE WHEN t.live_status = 1 THEN t.dt ELSE NULL END) AS live_days,
       SUM(t.duration)                                            AS sum_duration,
       SUM(t.total_vir_coin)                                      AS total_vir_coin
FROM warehouse.ods_anchor_bb_live_detail_daily t
WHERE t.dt < CURRENT_DATE
GROUP BY DATE_FORMAT(CONCAT(YEAR(t.dt), '-', MONTH(t.dt), '-01'), '%Y-%m-%d'),
         t.platform_id,
         t.platform_name,
         t.backend_account_id,
         t.anchor_no
;

-- 汇总维度 月-公会
-- 汇总指标 主播数，开播主播数，虚拟币收入
DROP TABLE IF EXISTS warehouse.dw_sum_bb_g_mon;
CREATE TABLE warehouse.dw_sum_bb_g_mon AS
SELECT t.rpt_month,
       t.platform_id,
       t.platform_name,
       t.backend_account_id,
       t.an_cnt,
       t.an_live_cnt,
       t1.total_amt AS total_amt_g,
       t.total_vir_coin  AS total_vir_coin_sum,
       t1.total_vir_coin AS total_vir_coin_g
FROM (SELECT DATE_FORMAT(CONCAT(YEAR(t.dt), '-', MONTH(t.dt), '-01'), '%Y-%m-%d')                                                                AS rpt_month,
             t.platform_id,
             t.platform_name,
             t.backend_account_id,
             COUNT(DISTINCT t.anchor_no)                                                AS an_cnt,
             COUNT(DISTINCT CASE WHEN t.live_status = 1 THEN t.anchor_no ELSE NULL END) AS an_live_cnt,
             SUM(t.total_vir_coin)                                                      AS total_vir_coin
      FROM warehouse.ods_anchor_bb_live_detail_daily t
      WHERE t.dt < CURRENT_DATE
      GROUP BY DATE_FORMAT(CONCAT(YEAR(t.dt), '-', MONTH(t.dt), '-01'), '%Y-%m-%d'),
               t.platform_id,
               t.platform_name,
               t.backend_account_id) t
LEFT JOIN warehouse.ods_guild_bb_amt_mon t1 ON t.rpt_month = t1.rpt_month AND t.backend_account_id = t1.backend_account_id
WHERE t1.type rlike '公会总收益'
;


