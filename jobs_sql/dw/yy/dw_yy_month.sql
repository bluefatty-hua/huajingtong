
DELETE
FROM warehouse.dw_yy_month_anchor_live_bluediamond
WHERE dt = '{month}';
INSERT INTO warehouse.dw_yy_month_anchor_live_bluediamond
SELECT dt,
       platform_id,
       platform_name,
       backend_account_id,
       anchor_no,
       SUM(anchor_bluediamond) AS anchor_bluediamond,
       SUM(guild_bluediamond)  AS guild_bluediamond
FROM warehouse.ods_yy_guild_live_bluediamond
WHERE dt >= '{month}'
  AND dt < '{month}' + INTERVAL 1 MONTH
GROUP BY dt,
         platform_id,
         platform_name,
         backend_account_id,
         anchor_no
;


-- 公会佣金收入
-- 汇总维度 月-公会-主播
-- 汇总指标 公会分成佣金
-- DROP TABLE IF EXISTS warehouse.dw_yy_month_anchor_live_commission;
-- CREATE TABLE warehouse.dw_yy_month_anchor_live_commission AS
DELETE
FROM warehouse.dw_yy_month_anchor_live_commission
WHERE dt = '{month}';
INSERT INTO warehouse.dw_yy_month_anchor_live_commission
SELECT dt,
       platform_id,
       platform_name,
       backend_account_id,
       channel_num,
       anchor_no,
       SUM(guild_commission) AS guild_commission
FROM warehouse.ods_yy_guild_live_commission
WHERE dt >= '{month}'
  AND dt < '{month}' + INTERVAL 1 MONTH
GROUP BY dt,
         platform_id,
         platform_name,
         backend_account_id,
         channel_num,
         anchor_no
;


-- 公会主播月收入
-- 汇总维度 月-公会—主播
-- 汇总指标 主播总蓝钻 公会分成蓝钻 公会分成佣金

--  DROP TABLE IF EXISTS warehouse.dw_yy_month_anchor_live_true;
--  CREATE TABLE warehouse.dw_yy_month_anchor_live_true AS
DELETE
FROM warehouse.dw_yy_month_anchor_live_true
WHERE dt = '{month}';
INSERT INTO warehouse.dw_yy_month_anchor_live_true
SELECT ab.dt,
       ab.platform_id,
       ab.backend_account_id,
       ac.channel_num,
       ab.anchor_no,
       ab.anchor_bluediamond AS anchor_bluediamond_true,
       ab.guild_bluediamond  AS guild_bluediamond_true,
       ac.guild_commission   AS guild_commission_true
FROM warehouse.dw_yy_month_anchor_live_bluediamond ab
         LEFT JOIN warehouse.dw_yy_month_anchor_live_commission ac
                   ON ab.dt = ac.dt AND ab.backend_account_id = ac.backend_account_id AND ab.anchor_no = ac.anchor_no
WHERE ab.dt = '{month}'
;


-- DROP TABLE IF EXISTS warehouse.dw_yy_month_anchor_live;
-- CREATE TABLE warehouse.dw_yy_month_anchor_live AS
DELETE
FROM warehouse.dw_yy_month_anchor_live
WHERE dt = '{month}';
INSERT INTO warehouse.dw_yy_month_anchor_live
(
  `dt`,
  `platform_id`,
  `platform_name`,
  `backend_account_id`,
  `channel_num`,
  `anchor_no`,
  `anchor_uid`,
  `revenue_level`,
  `newold_state`,
  `active_state`,
  `comment`,
  `live_days`,
  `duration`,
  `bluediamond`,
  `anchor_income_bluediamond`,
  `guild_income_bluediamond`,
  `anchor_commission`,
  `guild_commission`,
  `dt_cnt`,
  `add_loss_state`,
  `retention_r30`,
  `retention_r60`,
  `retention_r90`,
  `retention_r120`

)
SELECT '{month}'                                         AS dt,
       al.platform_id,
       al.platform_name,
       al.backend_account_id,
       al.channel_num,
       al.anchor_no,
       al.anchor_uid,
       al.revenue_level,
       al.month_newold_state                                                   AS newold_state,
       al.active_state,
       al.comment,
       COUNT(CASE WHEN live_status = 1 THEN dt ELSE NULL END)                  AS live_days,
       SUM(CASE WHEN duration >= 0 THEN duration ELSE 0 END)                   AS duration,
       SUM(CASE WHEN bluediamond >= 0 THEN bluediamond ELSE 0 END)             AS bluediamond,
       SUM(CASE
               WHEN bluediamond > 0 THEN bluediamond * anchor_settle_rate
               ELSE 0 END)                                                     AS anchor_income_bluediamond,
       SUM(CASE
               WHEN bluediamond > 0 THEN bluediamond * (1 - anchor_settle_rate)
               ELSE 0 END)                                                     AS guild_income_bluediamond,
       SUM(CASE WHEN anchor_commission >= 0 THEN anchor_commission ELSE 0 END) AS anchor_commission,
       SUM(CASE WHEN guild_commission >= 0 THEN guild_commission ELSE 0 END)   AS guild_commission,
       COUNT(DISTINCT dt)                                                      AS dt_cnt,
       if(sum(if(add_loss_state='new',1,0))>0,'new','old')                     as add_loss_state,
       if(sum(ifnull(retention_r30,0))>0,1,0)                                  as retention_r30,
       if(sum(ifnull(retention_r60,0))>0,1,0)                                  as retention_r60,
       if(sum(ifnull(retention_r90,0))>0,1,0)                                  as retention_r90,
       if(sum(ifnull(retention_r120,0))>0,1,0)                                  as retention_r120
FROM (SELECT *,
             warehouse.ANCHOR_NEW_OLD(min_live_dt, min_sign_dt, CASE
                                                                    WHEN dt < DATE_FORMAT('{cur_date}', '%Y-%m-01')
                                                                        THEN LAST_DAY(dt)
                                                                    ELSE '{cur_date}' END, 180) AS month_newold_state
      FROM warehouse.dw_yy_day_anchor_live
      WHERE dt >= '{month}'
        AND dt < '{month}' + INTERVAL 1 MONTH
     ) al
GROUP BY DATE_FORMAT(al.dt, '%Y-%m-01'),
         al.platform_id,
         al.platform_name,
         al.backend_account_id,
         al.channel_num,
         al.anchor_no,
         al.anchor_uid,
         al.revenue_level,
         al.month_newold_state,
         al.active_state,
         al.comment
;


-- 公会蓝钻月收入
-- 汇总维度 月-公会
-- 汇总指标 主播总蓝钻 公会分成蓝钻
-- DROP TABLE IF EXISTS warehouse.dw_yy_month_guild_live_bluediamond;
-- CREATE TABLE warehouse.dw_yy_month_guild_live_bluediamond AS
DELETE
FROM warehouse.dw_yy_month_guild_live_bluediamond
WHERE dt = '{month}';
INSERT INTO warehouse.dw_yy_month_guild_live_bluediamond
SELECT dt,
       platform_id,
       platform_name,
       backend_account_id,
       SUM(anchor_bluediamond) AS anchor_bluediamond,
       SUM(guild_bluediamond)  AS guild_bluediamond
FROM warehouse.ods_yy_guild_live_bluediamond
WHERE dt >= '{month}'
  AND dt < '{month}' + INTERVAL 1 MONTH
GROUP BY dt,
         platform_id,
         platform_name,
         backend_account_id
;


-- 公会佣金月收入
-- 汇总维度 月-公会
-- 汇总指标 公会分成佣金
-- DROP TABLE IF EXISTS warehouse.dw_yy_month_guild_live_commission;
-- CREATE TABLE warehouse.dw_yy_month_guild_live_commission AS
DELETE
FROM warehouse.dw_yy_month_guild_live_commission
WHERE dt = '{month}';
INSERT INTO warehouse.dw_yy_month_guild_live_commission
SELECT dt,
       platform_id,
       platform_name,
       backend_account_id,
       channel_num,
       SUM(guild_commission) AS guild_commission
FROM warehouse.ods_yy_guild_live_commission
WHERE dt >= '{month}'
  AND dt < '{month}' + INTERVAL 1 MONTH
GROUP BY dt,
         platform_id,
         platform_name,
         backend_account_id,
         channel_num
;


-- 公会蓝钻月收入
-- 汇总维度 月-公会
-- 汇总指标 主播总蓝钻 公会分成蓝钻 公会分成佣金
--  DROP TABLE IF EXISTS warehouse.dw_yy_month_guild_live_true;
--  CREATE TABLE warehouse.dw_yy_month_guild_live_true AS
DELETE
FROM warehouse.dw_yy_month_guild_live_true
WHERE dt = '{month}';
INSERT INTO warehouse.dw_yy_month_guild_live_true
SELECT ab.dt,
       ab.platform_id,
       ab.backend_account_id,
       cl.channel_num,
       IFNULL(ab.anchor_bluediamond, 0) AS anchor_bluediamond_true,
       IFNULL(ab.guild_bluediamond, 0)  AS guild_bluediamond_true,
       IFNULL(ac.guild_commission, 0)   AS guild_commission_true
FROM warehouse.dw_yy_month_guild_live_bluediamond ab
         LEFT OUTER JOIN warehouse.dw_yy_month_guild_live_commission ac
                         ON ab.dt = ac.dt AND ab.backend_account_id = ac.backend_account_id
         LEFT JOIN spider_yy_backend.channel_list cl ON ab.backend_account_id = cl.backend_account_id
WHERE ab.dt = '{month}';
;


-- DROP TABLE IF EXISTS warehouse.dw_yy_month_guild_live;
-- CREATE TABLE warehouse.dw_yy_month_guild_live AS
DELETE
FROM warehouse.dw_yy_month_guild_live
WHERE dt = '{month}';
INSERT INTO warehouse.dw_yy_month_guild_live
(
  `dt`,
  `platform_id`,
  `platform_name`,
  `backend_account_id`,
  `channel_num`,
  `comment`,
  `revenue_level`,
  `newold_state`,
  `active_state`,
  `anchor_cnt`,
  `new_anchor_cnt`,
  `new_r30_cnt`,
  `new_r60_cnt`,
  `new_r90_cnt`,
  `new_r120_cnt`,
  `anchor_live_cnt`,
  `duration`,
  `anchor_bluediamond`,
  `anchor_income_bluediamond`,
  `guild_income_bluediamond`,
  `anchor_commission`,
  `guild_commission`
)
SELECT '{month}'                                                                         AS dt,
       al.platform_id,
       al.platform_name,
       al.backend_account_id,
       al.channel_num,
       al.comment,
       al.revenue_level,
       al.newold_state                                                                   AS newold_state,
       al.active_state,
       COUNT(DISTINCT al.anchor_uid)                                                      AS anchor_cnt,
       sum(if(add_loss_state='new',1,0)) as new_anchor_cnt,
       sum(retention_r30) as new_r30_cnt,
       sum(retention_r60) as new_r60_cnt,
       sum(retention_r90) as new_r90_cnt,
       sum(retention_r120) as new_r120_cnt,
       COUNT(DISTINCT CASE WHEN al.live_days > 0 THEN al.anchor_uid ELSE NULL END)        AS anchor_live_cnt,
       SUM(al.duration)                                                                  AS duration,
       SUM(ifnull(al.bluediamond,0))                                                     AS anchor_bluediamond,
       0 AS anchor_income_bluediamond,
       0 AS guild_income_bluediamond,
       0 AS anchor_commission,
       0 AS guild_commission
FROM 
       warehouse.dw_yy_month_anchor_live al where dt = '{month}'
  

-- (SELECT *,
--              warehouse.ANCHOR_NEW_OLD(min_live_dt, min_sign_dt, CASE
--                                                                     WHEN dt < DATE_FORMAT('{cur_date}', '%Y-%m-01')
--                                                                         THEN LAST_DAY(dt)
--                                                                     ELSE '{cur_date}' END, 180
--                  ) AS month_newold_state
--       FROM warehouse.dw_yy_day_anchor_live
--       WHERE dt >= '{month}'
--         AND dt < '{month}' + INTERVAL 1 MONTH
--      ) al
GROUP BY 
         al.platform_id,
         al.platform_name,
         al.backend_account_id,
         al.channel_num,
         al.comment,
         al.revenue_level,
         al.newold_state,
         al.active_state
;

