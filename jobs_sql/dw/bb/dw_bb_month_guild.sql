
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
(
  `dt`,
  `platform_id`,
  `platform_name`,
  `backend_account_id`,
  `newold_state`,
  `active_state`,
  `revenue_level`,
  `anchor_cnt`,
  `new_anchor_cnt`,
  `anchor_live_cnt`,
  `duration`,
  `revenue`,
  `revenue_orig`,
  `anchor_income`,
  `anchor_base_coin`,
  `special_coin_true`,
  `special_coin`
)
SELECT DATE_FORMAT(al.dt, '%Y-%m-01')                                               AS dt,
       al.platform_id,
       al.platform_name,
       al.backend_account_id,
       al.newold_state                                                              AS newold_state,
       al.active_state,
       al.revenue_level,
       COUNT(DISTINCT if(contract_status!=2,anchor_uid,null))                       as anchor_cnt, 
       sum(if(add_loss_state='new',1,0))                                            as new_anchor_cnt,
       COUNT(DISTINCT CASE WHEN al.live_days > 0 THEN al.anchor_uid ELSE NULL END)  AS anchor_live_cnt,
       SUM(al.duration)                                                             AS duration,
       sum(al.revenue)                                                              AS revenue,
       SUM(al.revenue_orig)                                                         AS revenue_orig,
       0                                                                            AS anchor_income,
       0                                                                            AS operate_award_punish_coin,
       0                                                                            AS anchor_base_coin,
       0                                                                            AS special_coin
      FROM warehouse.dw_bb_month_anchor_live al
      WHERE
      dt >= '{month}'
      AND dt < '{month}' + INTERVAL 1 MONTH
GROUP BY DATE_FORMAT(dt, '%Y-%m-01'),
         al.platform_id,
         al.platform_name,
         al.backend_account_id,
         al.newold_state,
         al.active_state,
         al.revenue_level
;
