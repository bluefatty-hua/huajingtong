
-- 汇总维度 月-公会
-- 汇总指标 主播数，开播主播数，虚拟币收入
-- DROP TABLE IF EXISTS warehouse.dw_now_month_guild_live_true;
-- CREATE TABLE warehouse.dw_now_month_guild_live_true AS
-- DELETE
-- FROM warehouse.dw_now_month_guild_live_true
-- WHERE dt = '{month}';
-- INSERT INTO warehouse.dw_now_month_guild_live_true
-- SELECT dt,
--        backend_account_id,
--        guild_salart_rmb   AS guild_salart_rmb_true,
--        guild_virtual_coin AS guild_virtual_coin_true,
--        type,
--        anchor_income,
--        anchor_base_coin,
--        guild_award_coin,
--        operate_award_punish_coin,
--        special_coin,
--        guild_change_coin,
--        anchor_change_coin,
--        comment
-- FROM warehouse.ods_now_month_guild_live
-- WHERE type = '公会总收益'
--   AND dt = '{month}'
-- ;


DELETE
FROM warehouse.dw_now_month_guild_live
WHERE dt = '{month}';
INSERT INTO warehouse.dw_now_month_guild_live
(
  `dt`,
  `backend_account_id`,
  `city`,
  `newold_state`,
  `active_state`,
  `revenue_level`,
  `anchor_cnt`,
  `anchor_live_cnt`,
  `duration`,
  `revenue`,
  `new_anchor_cnt`
)

SELECT '{month}'                                               AS dt,
       al.backend_account_id,
       al.city,
       al.newold_state                                                        AS newold_state,
       al.active_state,
       al.revenue_level,
       COUNT(DISTINCT al.anchor_no)                                                 AS anchor_cnt,
       COUNT(DISTINCT CASE WHEN al.live_days > 0 THEN al.anchor_no ELSE NULL END)   AS anchor_live_cnt,
       SUM(al.duration)                                                             AS duration,
       SUM(al.revenue)                                                              AS revenue,
       sum(if(al.add_loss_state='new',1,0))                                         as new_anchor_cnt
FROM  warehouse.dw_now_month_anchor_live al
      WHERE  dt = '{month}'
GROUP BY 
         al.backend_account_id,
         al.city,
         al.newold_state,
         al.active_state,
         al.revenue_level
;

