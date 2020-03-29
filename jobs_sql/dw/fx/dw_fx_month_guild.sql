-- dw_fx_month_anchor_live
-- DROP TABLE IF EXISTS warehouse.dw_fx_month_guild_live;
-- CREATE TABLE warehouse.dw_fx_month_guild_live AS
DELETE
FROM warehouse.dw_fx_month_guild_live
WHERE dt = '{month}';
INSERT INTO warehouse.dw_fx_month_guild_live
(
  `dt`,
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
  `guild_income`
)
SELECT '{month}',
       al.backend_account_id,
       al.newold_state,
       al.active_state,
       al.revenue_level,
       COUNT(DISTINCT al.anchor_no)                                                 AS anchor_cnt,
       sum(if(add_loss_state='new',1,0))                                            as new_anchor_cnt,
       COUNT(DISTINCT CASE WHEN al.live_days >0 THEN al.anchor_no ELSE NULL END)    AS live_cnt,
       SUM(al.duration)                                                             AS duration,
       SUM(al.revenue)                                                              AS revenue,
       SUM(al.revenue_orig)                                                         AS revenue_orig,
       SUM(al.anchor_income)                                                        AS anchor_income,
       SUM(al.guild_income)                                                         AS guild_income
FROM warehouse.dw_fx_month_anchor_live al
WHERE al.dt = '{month}'
GROUP BY 
         al.backend_account_id,
         al.newold_state,
         al.active_state,
         al.revenue_level
;
