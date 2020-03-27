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

