-- 维度 日-公会
-- 指标 主播数、开播主播数、主播流水、
-- DROP TABLE IF EXISTS warehouse.dw_yy_day_guild_live;
-- CREATE TABLE warehouse.dw_yy_day_guild_live AS
DELETE
FROM warehouse.dw_yy_day_guild_live
WHERE dt >= '{month}'
  AND dt <= LAST_DAY('{month}');;
INSERT INTO warehouse.dw_yy_day_guild_live
(
  `dt`,
  `platform_id`,
  `backend_account_id`,
  `channel_num`,
  `comment`,
  `newold_state`,
  `active_state`,
  `revenue_level`,
  `anchor_cnt`,
  `anchor_live_cnt`,
  `duration`,
  `bluediamond`,
  `anchor_income_bluediamond`,
  `guild_income_bluediamond`,
  `anchor_commission`,
  `guild_commission`,
  `new_anchor_cnt`,
  `new_r30_cnt`,
  `new_r60_cnt`,
  `new_r90_cnt`,
  `new_r120_cnt`
)
SELECT al.dt,
       al.platform_id,
       al.backend_account_id,
       al.channel_num,
       al.comment,
       al.newold_state,
       al.active_state,
       al.revenue_level,
       COUNT(DISTINCT al.anchor_uid)                                                 AS anchor_cnt,
       COUNT(DISTINCT CASE WHEN al.live_status = 1 THEN al.anchor_uid ELSE NULL END) AS anchor_live_cnt,
       SUM(IF(al.duration > 0, al.duration, 0))                                      AS duration,
       SUM(IF(al.bluediamond > 0, al.bluediamond, 0))                                AS bluediamond,
       SUM(IF(al.bluediamond > 0, al.bluediamond * al.anchor_settle_rate, 0))        AS anchor_income_bluediamond,
       SUM(IF(al.bluediamond > 0, al.bluediamond * (1 - al.anchor_settle_rate), 0))  AS guild_income_bluediamond,
       SUM(IF(al.anchor_commission > 0, al.anchor_commission, 0))                    AS anchor_commission,
       SUM(IF(al.guild_commission > 0, al.guild_commission, 0))                      AS guild_commission,
       sum(if(al.add_loss_state='new',1,0))                                          as new_anchor_cnt,
       sum(retention_r30)                                                            as new_r30_cnt,
       sum(retention_r60)                                                            as new_r60_cnt,
       sum(retention_r90)                                                            as new_r90_cnt,
       sum(retention_r120)                                                           as new_r120_cnt
FROM warehouse.dw_yy_day_anchor_live al
WHERE al.dt >= '{month}'
  AND al.dt <= LAST_DAY('{month}')
-- where comment <> 'from guild_anchor_sign_tran'
GROUP BY al.dt,
         al.platform_id,
         al.backend_account_id,
         al.channel_num,
         al.comment,
         al.newold_state,
         al.active_state,
         al.revenue_level
;
