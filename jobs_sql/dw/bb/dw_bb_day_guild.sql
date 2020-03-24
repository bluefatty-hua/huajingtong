DELETE
FROM warehouse.dw_bb_day_guild_live
WHERE dt >= '{month}'
  AND dt <= LAST_DAY('{month}');
INSERT INTO warehouse.dw_bb_day_guild_live
(
  `dt`,
  `platform_id`,
  `platform_name`,
  `backend_account_id`,
  `newold_state`,
  `active_state`,
  `revenue_level`,
  `contract_status`,
  `anchor_cnt`,
  `live_cnt`,
  `duration`,
  `revenue`,
  `revenue_orig`,
  `special_coin`,
  `send_coin`,
  `anchor_base_coin`,
  `anchor_income`,
  `new_anchor_cnt`,
  `new_r30_cnt`,
  `new_r60_cnt`,
  `new_r90_cnt`,
  `new_r120_cnt`
)
SELECT t.dt,
       t.platform_id,
       t.platform_name,
       t.backend_account_id,
       t.newold_state,
       t.active_state,
       t.revenue_level,
       t.contract_status,
       COUNT(DISTINCT t.anchor_no)                                                AS anchor_cnt,
       COUNT(DISTINCT CASE WHEN t.live_status = 1 THEN t.anchor_no ELSE NULL END) AS live_cnt,
       SUM(t.duration)                                                            AS duration,
       ROUND(SUM(t.revenue), 2)                                                   AS revenue,
       SUM(t.revenue_orig)                                                        AS revenue_orig,
       SUM(t.special_coin)                                                        AS special_coin,
       SUM(t.send_coin)                                                           AS send_coin,
       SUM(t.anchor_base_coin)                                                    AS anchor_base_coin,
       SUM(t.anchor_income)                                                       AS anchor_income,
       sum(if(add_loss_state='new',1,0))                                          as new_anchor_cnt,
       sum(retention_r30)                                                            as new_r30_cnt,
       sum(retention_r60)                                                            as new_r60_cnt,
       sum(retention_r90)                                                            as new_r90_cnt,
       sum(retention_r120)                                                           as new_r120_cnt
FROM warehouse.dw_bb_day_anchor_live t
WHERE (t.contract_status <> 2 OR t.contract_status IS NULL)
and t.dt >= '{month}'
  AND t.dt <= LAST_DAY('{month}')
GROUP BY t.dt,
         t.platform_id,
         t.platform_name,
         t.backend_account_id,
         t.newold_state,
         t.active_state,
         t.revenue_level,
         t.contract_status
;