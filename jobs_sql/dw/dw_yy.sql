-- 数据源 warehouse.ods_yy_anchor_live_detail_daily
--        warehouse.ods_guild_yy_virtual_coin_an_mon
-- ===============================================================
-- 公会


-- 汇总维度 月-主播
-- 汇总指标 开播天数，开播时长，虚拟币收入，主播佣金，公会佣金
DROP TABLE IF EXISTS warehouse.dw_sum_yy_an_mon;
CREATE TABLE warehouse.dw_sum_yy_an_mon AS
    # DELETE FROM warehouse.dw_sum_yy_an_mon WHERE rpt_month = CONCAT(YEAR('{start_date}'), '-', MONTH('{start_date}'), '-01');
# INSERT INTO warehouse.dw_sum_yy_an_mon
SELECT ad.rpt_month,
       ad.platform_id,
       ad.platform_name,
       ad.anchor_no,
       ad.live_days,
       ad.sum_duration,
       ad.sum_vir_coin,
       av.an_total_vir_coin,
       ad.sum_an_commission,
       ad.sum_g_commission,
       ad.dt_cnt
FROM (SELECT CONCAT(YEAR(dt), '-', MONTH(dt), '-01')                                 AS rpt_month,
             platform_id,
             platform_name,
             anchor_no,
             COUNT(CASE WHEN live_status = 1 THEN dt ELSE NULL END)                  AS live_days,
             SUM(CASE WHEN duration >= 0 THEN duration ELSE 0 END)                   AS sum_duration,
             SUM(CASE WHEN virtual_coin >= 0 THEN virtual_coin ELSE 0 END)           AS sum_vir_coin,
             SUM(CASE WHEN anchor_commission >= 0 THEN anchor_commission ELSE 0 END) AS sum_an_commission,
             SUM(CASE WHEN guild_commission >= 0 THEN guild_commission ELSE 0 END)   AS sum_g_commission,
             COUNT(DISTINCT dt)                                                      AS dt_cnt
      FROM warehouse.ods_anchor_yy_live_detail_daily
#       WHERE dt BETWEEN CONCAT(YEAR('{start_date}'), '-', MONTH('{start_date}'), '-01') AND '{end_date}'
      GROUP BY CONCAT(YEAR(dt), '-', MONTH(dt), '-01'),
               platform_id,
               platform_name,
               anchor_no) ad
         LEFT JOIN warehouse.ods_guild_yy_virtual_coin_an_mon av
                   ON ad.rpt_month = av.rpt_month AND ad.anchor_no = av.anchor_no
;



-- 汇总数据
-- 汇总维度 月-公会
-- 汇总指标 主播数，开播主播数，主播虚拟币收入,主播佣金收入，公会佣金收入
# DROP TABLE IF EXISTS warehouse.dw_sum_yy_g_mon;
# CREATE TABLE warehouse.dw_sum_yy_g_mon AS
DELETE
FROM warehouse.dw_sum_yy_g_mon
WHERE rpt_month = CONCAT(YEAR('{start_date}'), '-', MONTH('{start_date}'), '-01');
INSERT INTO warehouse.dw_sum_yy_g_mon
SELECT DATE_FORMAT(CONCAT(YEAR(t.dt), '-', MONTH(t.dt), '-01'), '%Y-%m-%d')        AS rpt_month,
       t.platform_id,
       t.platform_name,
       t.backend_account_id,
       COUNT(DISTINCT t.anchor_no)                                                 AS an_cnt,
       COUNT(DISTINCT CASE WHEN t.live_status = 1 THEN t.anchor_no ELSE NULL END)  AS an_live_cnt,
       SUM(CASE WHEN t.virtual_coin >= 0 THEN t.virtual_coin ELSE 0 END)           AS vir_coin,
       SUM(CASE WHEN t.anchor_commission >= 0 THEN t.anchor_commission ELSE 0 END) AS an_commission,
       SUM(CASE WHEN t.guild_commission >= 0 THEN t.guild_commission ELSE 0 END)   AS g_commission,
       COUNT(DISTINCT t.dt)                                                        AS dt_cnt
FROM warehouse.ods_anchor_yy_live_detail_daily t
# WHERE dt BETWEEN CONCAT(YEAR('{start_date}'), '-', MONTH('{start_date}'), '-01') AND '{end_date}'
GROUP BY DATE_FORMAT(CONCAT(YEAR(t.dt), '-', MONTH(t.dt), '-01'), '%Y-%m-%d'),
         t.platform_id,
         t.platform_name,
         t.backend_account_id
;

