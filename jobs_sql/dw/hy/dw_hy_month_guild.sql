-- 月公会
-- DROP TABLE IF EXISTS stage.stage_huya_month_guild_live_revenue;
-- CREATE TABLE stage.stage_huya_month_guild_live_revenue AS
DELETE
FROM stage.stage_huya_month_guild_live_revenue
WHERE dt = '{month}';
INSERT INTO stage.stage_huya_month_guild_live_revenue
SELECT DATE_FORMAT(dt, '%Y-%m-01'),
       t1.channel_id,
       SUM(IFNULL(revenue, 0)) AS revenue
FROM warehouse.dw_huya_day_guild_live_true t1
WHERE t1.dt >= '{month}'
  AND t1.dt < '{month}' + INTERVAL 1 MONTH
GROUP BY DATE_FORMAT(dt, '%Y-%m-01'),
         t1.channel_id
;


-- DROP TABLE IF EXISTS stage.stage_huya_month_guild_live_gift_income;
-- CREATE TABLE stage.stage_huya_month_guild_live_gift_income AS
DELETE
FROM stage.stage_huya_month_guild_live_gift_income
WHERE dt = '{month}';
INSERT INTO stage.stage_huya_month_guild_live_gift_income
SELECT DATE_FORMAT(dt, '%Y-%m-01') AS dt,
       t1.channel_id,
       SUM(IFNULL(gift_income, 0)) AS gift_income
FROM warehouse.dw_huya_day_guild_live_true t1
WHERE dt >= '{month}'
  AND dt < '{month}' + INTERVAL 1 MONTH
GROUP BY DATE_FORMAT(dt, '%Y-%m-01'),
         t1.channel_id
;


-- DROP TABLE IF EXISTS stage.stage_huya_month_guild_live_guard_income;
-- CREATE TABLE stage.stage_huya_month_guild_live_guard_income AS
DELETE
FROM stage.stage_huya_month_guild_live_guard_income
WHERE dt = '{month}';
INSERT INTO stage.stage_huya_month_guild_live_guard_income
SELECT DATE_FORMAT(dt, '%Y-%m-01')  AS dt,
       t1.channel_id,
       SUM(ifnull(guard_income, 0)) AS guard_income
FROM warehouse.dw_huya_day_guild_live_true t1
WHERE dt >= '{month}'
  AND dt < '{month}' + INTERVAL 1 MONTH
GROUP BY DATE_FORMAT(dt, '%Y-%m-01'),
         t1.channel_id
;


-- 有几个频道缺失数据
-- DROP TABLE IF EXISTS stage.stage_huya_month_guild_live_noble_income;
-- CREATE TABLE stage.stage_huya_month_guild_live_noble_income AS
DELETE
FROM stage.stage_huya_month_guild_live_noble_income
WHERE dt = '{month}';
INSERT INTO stage.stage_huya_month_guild_live_noble_income
SELECT DATE_FORMAT(dt, '%Y-%m-01')  AS dt,
       t1.channel_id,
       SUM(IFNULL(noble_income, 0)) AS noble_income
FROM warehouse.dw_huya_day_guild_live_true t1
WHERE dt >= '{month}'
  AND dt < '{month}' + INTERVAL 1 MONTH
GROUP BY DATE_FORMAT(dt, '%Y-%m-01'),
         t1.channel_id
;


-- DROP TABLE IF EXISTS warehouse.dw_huya_month_guild_live_true;
-- CREATE TABLE warehouse.dw_huya_month_guild_live_true AS
DELETE
FROM warehouse.dw_huya_month_guild_live_true
WHERE dt = '{month}';
INSERT INTO warehouse.dw_huya_month_guild_live_true
SELECT t1.dt AS dt,
       t1.platform_id,
       t1.platform_name,
       aci.channel_type,
       t1.channel_id,
       t1.channel_num,
       t1.ow,
       t1.channel_name,
       t1.logo,
       t1.`desc`,
       t1.create_time,
       t1.is_platinum,
       t1.sign_count,
       t1.sign_limit,
       t2.revenue,
       t3.gift_income,
       t4.guard_income,
       t5.noble_income
FROM warehouse.dw_huya_month_guild_info t1
         LEFT JOIN
     stage.stage_huya_month_guild_live_revenue t2
     ON t2.channel_id = t1.channel_id AND t1.dt = t2.dt
         LEFT JOIN
     stage.stage_huya_month_guild_live_gift_income t3
     ON t3.channel_id = t1.channel_id AND t1.dt = t3.dt
         LEFT JOIN
     stage.stage_huya_month_guild_live_guard_income t4
     ON t1.channel_id = t4.channel_id AND t4.dt = t1.dt
         LEFT JOIN stage.stage_huya_month_guild_live_noble_income t5
                   ON t5.channel_id = t1.channel_id AND t5.dt = t1.dt
         LEFT JOIN warehouse.ods_hy_account_info aci ON t1.channel_id = aci.channel_id
WHERE t1.dt = '{month}'
;


-- dw_huya_month_guild_live
-- DROP TABLE IF EXISTS warehouse.dw_huya_month_guild_live;
-- CREATE TABLE warehouse.dw_huya_month_guild_live AS
DELETE
FROM warehouse.dw_huya_month_guild_live
WHERE dt = '{month}';
INSERT INTO warehouse.dw_huya_month_guild_live
(
  `dt`,
  `channel_type`,
  `channel_id`,
  `channel_num`,
  `revenue_level`,
  `newold_state`,
  `active_state`,
  `anchor_cnt`,
  `new_anchor_cnt`,
  `live_cnt`,
  `duration`,
  `revenue`
)
SELECT '{month}'    AS dt,
       al.channel_type,
       al.channel_id,
       al.channel_num,
       al.revenue_level,
       al.newold_state             AS newold_state,
       al.active_state,
       COUNT(DISTINCT al.anchor_uid)     AS anchor_cnt,
       sum(if(add_loss_state='new',1,0)) as new_anchor_cnt,
       sum(if(live_days>0,1,0))          AS live_cnt,
       SUM(IFNULL(al.duration, 0))       AS duration,
       SUM(IFNULL(al.revenue, 0))        AS revenue
FROM  warehouse.dw_huya_month_anchor_live al
      WHERE dt = '{month}'
GROUP BY al.platform_id,
         al.platform_name,
         al.channel_type,
         al.channel_id,
         al.channel_num,
         al.revenue_level,
         al.newold_state,
         al.active_state
;
