-- 汇总维度 月-公会—主播
-- 汇总指标 开播天数，开播时长，虚拟币收入
-- DROP TABLE IF EXISTS warehouse.dw_month_bb_anchor_live;
-- CREATE TABLE warehouse.dw_month_bb_anchor_live AS
DELETE
FROM warehouse.dw_bb_month_anchor_live
WHERE dt = '{month}';
INSERT INTO warehouse.dw_bb_month_anchor_live
(
  `dt`,
  `platform_id`,
  `platform_name`,
  `backend_account_id`,
  `anchor_no`,
  `contract_status`,
  `newold_state`,
  `active_state`,
  `revenue_level`,
  `live_days`,
  `duration`,
  `revenue`,
  `revenue_orig`,
  `add_loss_state`,
  `retention_r30`,
  `retention_r60`,
  `retention_r90`,
  `retention_r120`
)
SELECT '{month}'                                                    AS dt,
       al.platform_id,
       al.platform_name,
       al.backend_account_id,
       al.anchor_no,
       if(sum(if(contract_status!=2,1,0))>0,0,2)                     as contract_status,   -- 月内有非2（非签约），都算在约
       al.month_newold_state                                        AS newold_state,
       al.active_state,
       al.revenue_level,
       COUNT(CASE WHEN al.live_status = 1 THEN al.dt ELSE NULL END) AS live_days,
       SUM(al.duration)                                             AS duration,
       SUM(al.revenue)                                         AS revenue,
       SUM(al.revenue_orig)                                         AS revenue_orig,
       if(sum(if(add_loss_state='new',1,0))>0,'new','old')                     as add_loss_state,
       if(sum(ifnull(retention_r30,0))>0,1,0)                                  as retention_r30,
       if(sum(ifnull(retention_r60,0))>0,1,0)                                  as retention_r60,
       if(sum(ifnull(retention_r90,0))>0,1,0)                                  as retention_r90,
       if(sum(ifnull(retention_r120,0))>0,1,0)                                 as retention_r120
FROM (SELECT *,
             -- cur_date: t-1
             warehouse.ANCHOR_NEW_OLD(min_live_dt, min_sign_dt, CASE
                                                                    WHEN dt < DATE_FORMAT('{cur_date}', '%Y-%m-01')
                                                                        THEN LAST_DAY(dt)
                                                                    ELSE '{cur_date}' END, 180) AS month_newold_state
      FROM warehouse.dw_bb_day_anchor_live
      WHERE  dt >= '{month}'
        AND dt < '{month}' + INTERVAL 1 MONTH
     ) al
GROUP BY al.platform_id,
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
  `new_r30_cnt`,
  `new_r60_cnt`,
  `new_r90_cnt`,
  `new_r120_cnt`,
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
       al.newold_state                                                        AS newold_state,
       al.active_state,
       al.revenue_level,
       sum(if(contract_status<>2,1,0)) as anchor_cnt, -- 只要有一天是有效签约都算到本月主播数
       sum(if(add_loss_state='new',1,0)) as new_anchor_cnt,
       sum(retention_r30) as new_r30_cnt,
       sum(retention_r60) as new_r60_cnt,
       sum(retention_r90) as new_r90_cnt,
       sum(retention_r120) as new_r120_cnt,
       COUNT(DISTINCT CASE WHEN al.live_days > 0 THEN al.anchor_no ELSE NULL END)   AS anchor_live_cnt,
       SUM(al.duration)                                                             AS duration,
       sum(al.revenue)                                                              AS revenue,
       SUM(al.revenue_orig)                                                         AS revenue_orig,
       0                                                                            AS anchor_income,
       0                                                                            AS operate_award_punish_coin,
       0                                                                            AS anchor_base_coin,
       0                                                                            AS special_coin
FROM warehouse.dw_bb_month_anchor_live al
WHERE contract_status <> 2
        AND dt >= '{month}'
        AND dt < '{month}' + INTERVAL 1 MONTH
-- (SELECT *,
--              -- cur_date: t-1
--              warehouse.ANCHOR_NEW_OLD(min_live_dt, min_sign_dt, CASE
--                                                                     WHEN dt < DATE_FORMAT('{cur_date}', '%Y-%m-01')
--                                                                         THEN LAST_DAY(dt)
--                                                                     ELSE '{cur_date}' END, 180) AS month_newold_state
--       FROM warehouse.dw_bb_day_guild_live
--       WHERE contract_status <> 2
--         AND dt >= '{month}'
--         AND dt < '{month}' + INTERVAL 1 MONTH
--      ) al
GROUP BY DATE_FORMAT(dt, '%Y-%m-01'),
         al.platform_id,
         al.platform_name,
         al.backend_account_id,
         al.newold_state,
         al.active_state,
         al.revenue_level
;
