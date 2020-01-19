-- 汇总维度 月-公会—主播
-- 汇总指标 开播天数，开播时长，虚拟币收入
-- DROP TABLE IF EXISTS warehouse.dw_month_bb_anchor_live;
-- CREATE TABLE warehouse.dw_month_bb_anchor_live AS
DELETE
FROM warehouse.dw_bb_month_anchor_live
WHERE DATE_FORMAT(dt, '%Y%m') BETWEEN DATE_FORMAT('{start_date}', '%Y%m') AND DATE_FORMAT('{end_date}', '%Y%m');
INSERT INTO warehouse.dw_bb_month_anchor_live
SELECT DATE_FORMAT(CONCAT(YEAR(t.dt), '-', MONTH(t.dt), '-01'), '%Y-%m-%d') AS dt,
       t.platform_id,
       t.platform_name,
       t.backend_account_id,
       t.anchor_no,
       COUNT(CASE WHEN t.live_status = 1 THEN t.dt ELSE NULL END)           AS live_days,
       SUM(t.duration)                                                      AS duration,
       SUM(t.anchor_total_coin)                                             AS anchor_virtual_coin
FROM warehouse.ods_bb_day_anchor_live t
WHERE (contract_status = 0
    OR contract_status = 1)
  AND DATE_FORMAT(dt, '%Y%m') BETWEEN DATE_FORMAT('{start_date}', '%Y%m') AND DATE_FORMAT('{end_date}', '%Y%m')
GROUP BY DATE_FORMAT(CONCAT(YEAR(t.dt), '-', MONTH(t.dt), '-01'), '%Y-%m-%d'),
         t.platform_id,
         t.platform_name,
         t.backend_account_id,
         t.anchor_no
;


-- 汇总维度 月-公会
-- 汇总指标 主播数，开播主播数，虚拟币收入
-- DROP TABLE IF EXISTS warehouse.dw_bb_month_guild_live;
-- CREATE TABLE warehouse.dw_bb_month_guild_live AS
DELETE
FROM warehouse.dw_bb_month_guild_live
WHERE DATE_FORMAT(dt, '%Y%m') BETWEEN DATE_FORMAT('{start_date}', '%Y%m') AND DATE_FORMAT('{end_date}', '%Y%m');
INSERT INTO warehouse.dw_bb_month_guild_live
SELECT t.dt,
       t.platform_id,
       t.platform_name,
       t.backend_account_id,
       t.anchor_cnt,
       t.anchor_live_cnt,
       t1.type,
       t1.guild_salary_rmb          AS guild_salart_rmb_ture,
       t1.guild_virtual_coin        AS guild_virtual_coin_ture,
       t.anchor_virtual_coin,
       t1.anchor_virtual_coin       AS anchor_vitual_coin_ture,
       t.anchor_income,
       t1.anchor_base_coin          AS anchor_base_coin,
       t1.guild_award_coin          AS guild_award_coin,
       t1.operate_award_punish_coin AS operate_award_punish_coin,
       t1.special_coin              AS special_virtual_coin,
       t1.anchor_change_vir_coin,
       t1.guild_change_coin
FROM (SELECT DATE_FORMAT(CONCAT(YEAR(t.dt), '-', MONTH(t.dt), '-01'), '%Y-%m-%d')       AS dt,
             t.platform_id,
             t.platform_name,
             t.backend_account_id,
             COUNT(DISTINCT t.anchor_no)                                                AS anchor_cnt,
             COUNT(DISTINCT CASE WHEN t.live_status = 1 THEN t.anchor_no ELSE NULL END) AS anchor_live_cnt,
             SUM(t.anchor_total_coin)                                                   AS anchor_virtual_coin,
             SUM(t.anchor_income_virtual_coin)                                          AS anchor_income
      FROM warehouse.ods_bb_day_anchor_live t
      WHERE contract_status = 0
         OR contract_status = 1
      GROUP BY DATE_FORMAT(CONCAT(YEAR(t.dt), '-', MONTH(t.dt), '-01'), '%Y-%m-%d'),
               t.platform_id,
               t.platform_name,
               t.backend_account_id) t
         LEFT JOIN (SELECT * FROM warehouse.ods_bb_month_guild_live WHERE type = '公会总收益') t1
                   ON t.dt = t1.dt AND t.backend_account_id = t1.backend_account_id
WHERE DATE_FORMAT(t.dt, '%Y%m') BETWEEN DATE_FORMAT('{start_date}', '%Y%m') AND DATE_FORMAT('{end_date}', '%Y%m')
;

