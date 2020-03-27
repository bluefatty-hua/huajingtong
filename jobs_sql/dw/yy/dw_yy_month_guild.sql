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
       al.newold_state                                                                    AS newold_state,
       al.active_state,
       COUNT(DISTINCT al.anchor_uid)                                                      AS anchor_cnt,
       sum(if(add_loss_state='new',1,0))                                                  as new_anchor_cnt,
       COUNT(DISTINCT CASE WHEN al.live_days > 0 THEN al.anchor_uid ELSE NULL END)        AS anchor_live_cnt,
       SUM(al.duration)                                                                   AS duration,
       SUM(ifnull(al.bluediamond,0))                                                      AS anchor_bluediamond,
       SUM(anchor_income_bluediamond),
       SUM(guild_income_bluediamond),
       SUM(anchor_commission),
       SUM(guild_commission)
FROM 
       warehouse.dw_yy_month_anchor_live al where dt = '{month}'
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

