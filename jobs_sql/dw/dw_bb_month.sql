-- 汇总维度 月-公会—主播
-- 汇总指标 开播天数，开播时长，虚拟币收入
-- DROP TABLE IF EXISTS warehouse.dw_month_bb_anchor_live;
-- CREATE TABLE warehouse.dw_month_bb_anchor_live AS
DELETE
FROM warehouse.dw_bb_month_anchor_live
WHERE dt = '{month}';
INSERT INTO warehouse.dw_bb_month_anchor_live
SELECT DATE_FORMAT(al.dt, '%Y-%m-01')                               AS dt,
       al.platform_id,
       al.platform_name,
       al.backend_account_id,
       al.anchor_no,
       al.month_newold_state                                        AS newold_state,
       al.active_state,
       al.revenue_level,
       COUNT(CASE WHEN al.live_status = 1 THEN al.dt ELSE NULL END) AS live_days,
       SUM(al.duration)                                             AS duration,
       SUM(al.anchor_total_coin)                                    AS revenue
FROM (SELECT *,
             -- cur_date: t-1
             warehouse.ANCHOR_NEW_OLD(min_live_dt, min_sign_dt, CASE
                                                                    WHEN dt < DATE_FORMAT('{cur_date}', '%Y-%m-01')
                                                                        THEN LAST_DAY(dt)
                                                                    ELSE '{cur_date}' END, 180) AS month_newold_state
      FROM warehouse.dw_bb_day_anchor_live
      WHERE (contract_status <> 2 OR contract_status IS NULL)
        AND dt >= '{month}'
        AND dt < '{month}' + INTERVAL 1 MONTH
     ) al
GROUP BY DATE_FORMAT(al.dt, '%Y-%m-01'),
         al.platform_id,
         al.platform_name,
         al.backend_account_id,
         al.anchor_no,
         al.month_newold_state,
         al.active_state,
         al.revenue_level
;


-- 汇总维度 月-公会
-- 汇总指标 主播数，开播主播数，虚拟币收入
-- DROP TABLE IF EXISTS warehouse.dw_bb_month_guild_live_true;
-- CREATE TABLE warehouse.dw_bb_month_guild_live_true AS
DELETE
FROM warehouse.dw_bb_month_guild_live_true
WHERE dt = '{month}';
INSERT INTO warehouse.dw_bb_month_guild_live_true
SELECT dt,
       backend_account_id,
       guild_salart_rmb   AS guild_salart_rmb_true,
       guild_virtual_coin AS guild_virtual_coin_true,
       type,
       anchor_income,
       anchor_base_coin,
       guild_award_coin,
       operate_award_punish_coin,
       special_coin,
       guild_change_coin,
       anchor_change_coin,
       comment
FROM warehouse.ods_bb_month_guild_live
WHERE type = '公会总收益'
  AND dt = '{month}'
;


-- 注意backend_account_id = 3  数据中没有主播收入
-- DROP TABLE IF EXISTS warehouse.dw_bb_month_guild_live;
-- CREATE TABLE warehouse.dw_bb_month_guild_live AS
DELETE
FROM warehouse.dw_bb_month_guild_live
WHERE dt = '{month}';
INSERT INTO warehouse.dw_bb_month_guild_live
SELECT DATE_FORMAT(al.dt, '%Y-%m-01')                                               AS dt,
       al.platform_id,
       al.platform_name,
       al.backend_account_id,
       al.month_newold_state                                                        AS newold_state,
       al.active_state,
       al.revenue_level,
       COUNT(DISTINCT al.anchor_no)                                                 AS anchor_cnt,
       COUNT(DISTINCT CASE WHEN al.live_status = 1 THEN al.anchor_no ELSE NULL END) AS anchor_live_cnt,
       SUM(al.duration)                                                             AS duration,
       SUM(al.anchor_total_coin)                                                    AS revenue,
       SUM(al.anchor_income)                                                        AS anchor_income,
       SUM(al.send_coin)                                                            AS operate_award_punish_coin,
       SUM(al.anchor_base_coin)                                                     AS anchor_base_coin,
       SUM(al.special_coin)                                                         AS special_coin
FROM (SELECT *,
             -- cur_date: t-1
             warehouse.ANCHOR_NEW_OLD(min_live_dt, min_sign_dt, CASE
                                                                    WHEN dt < DATE_FORMAT('{cur_date}', '%Y-%m-01')
                                                                        THEN LAST_DAY(dt)
                                                                    ELSE '{cur_date}' END, 180) AS month_newold_state
      FROM warehouse.dw_bb_day_anchor_live
      WHERE (contract_status <> 2 OR contract_status IS NULL)
        AND dt >= '{month}'
        AND dt < '{month}' + INTERVAL 1 MONTH
     ) al
GROUP BY DATE_FORMAT(dt, '%Y-%m-01'),
         al.platform_id,
         al.platform_name,
         al.backend_account_id,
         al.month_newold_state,
         al.active_state,
         al.revenue_level
;
