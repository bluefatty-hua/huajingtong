-- 数据源 warehouse.dw_month_huya_guild_info、dw_month_huya_anchor_info
-- ===============================================================
-- 汇总月guild、anchor info数据
-- DROP TABLE IF EXISTS stage.stage_month_huya_guild_info;
-- CREATE TABLE stage.stage_month_huya_guild_info AS
delete from stage.stage_month_huya_guild_info where dt >='{month.start}' AND dt<='{month.end}';
insert into stage.stage_month_huya_guild_info
SELECT channel_id,MAX(dt) AS dt
FROM warehouse.ods_day_huya_guild_info
WHERE dt >='{month.start}' AND dt<='{month.end}'
GROUP BY channel_id;

-- DROP TABLE IF EXISTS warehouse.dw_month_huya_guild_info;
-- CREATE TABLE warehouse.dw_month_huya_guild_info AS
delete from warehouse.dw_month_huya_guild_info where dt >='{month.start}' AND dt<='{month.end}';
insert into warehouse.dw_month_huya_guild_info
select `platform_id`,
  `platform_name`,
  t1.`channel_id`,
  `channel_num`,
  `ow`,
  `channel_name`,
  `logo`,
  `desc`,
  `create_time`,
  `is_platinum`,
  `sign_count`,
  `sign_limit`,
  '{month.start}' as `dt`
from `warehouse`.`ods_day_huya_guild_info`  t1
join stage.stage_month_huya_guild_info t2
on t1.dt= t2.dt and t1.channel_id = t2.channel_id
where t1.dt >='{month.start}' AND t1.dt<='{month.end}';


-- DROP TABLE IF EXISTS stage.stage_month_huya_anchor_info;
-- CREATE TABLE stage.stage_month_huya_anchor_info AS
delete from stage.stage_month_huya_anchor_info where dt >='{month.start}' AND dt<='{month.end}';
insert into stage.stage_month_huya_anchor_info
SELECT anchor_uid,MAX(dt) AS dt,channel_id
FROM warehouse.ods_day_huya_anchor_info
WHERE dt >='{month.start}' AND dt<='{month.end}'
GROUP BY anchor_uid,channel_id;


-- DROP TABLE IF EXISTS warehouse.dw_month_huya_anchor_info;
-- CREATE TABLE warehouse.dw_month_huya_anchor_info AS
delete from warehouse.dw_month_huya_anchor_info where dt >='{month.start}' AND dt<='{month.end}';
insert into warehouse.dw_month_huya_anchor_info
select  `platform_id`,
  `platform_name`,
  t1.`channel_id`,
  `channel_num`,
  t1.`anchor_uid`,
  `anchor_no`,
  `comment`,
  `nick`,
  `activity_days`,
  `months`,
  `ow_percent`,
  `sign_time`,
  `surplus_days`,
  '{month.start}' as `dt`,
  `avatar`,
  t2.dt as last_active_date
from `warehouse`.`ods_day_huya_anchor_info`  t1
join stage.stage_month_huya_anchor_info t2
on t1.dt= t2.dt and t1.anchor_uid = t2.anchor_uid and t1.channel_id= t2.channel_id
where t1.dt >='{month.start}' AND t1.dt<='{month.end}';



-- DROP TABLE IF EXISTS warehouse.dw_month_huya_anchor_live;
-- CREATE TABLE warehouse.dw_month_huya_anchor_live AS
delete from warehouse.dw_month_huya_anchor_live where dt ='{month.start}';
insert into warehouse.dw_month_huya_anchor_live
SELECT

  t2.platform_id,
  t2.platform_name,
  t1.channel_id,
  t2.channel_num,
  t1.anchor_uid,
  t2.anchor_no,
  t2.nick,
  t2.comment,
  SUM(IFNULL(t1.`duration`,0)) AS duration,
  SUM(IFNULL(t1.live_status,0)) AS live_cnt,
  SUM(IFNULL(t1.income,0)) AS income,
  AVG(IF(t1.peak_pcu>0,t1.peak_pcu,NULL)) AS peak_pcu_avg,
  MAX(t1.peak_pcu) AS peak_pcu_max,
  MIN(t1.peak_pcu) AS peak_pcu_min,
  '{month.start}' AS dt,
  t2.activity_days,
  t2.months,
  t2.ow_percent,
  t2.sign_time,
  t2.surplus_days,
  t2.`avatar` AS avatar,
  MAX(t1.vir_coin_name) AS vir_coin_name,
  MAX(t1.vir_coin_rate) AS vir_coin_rate,
  MAX(t1.include_pf_amt) AS include_pf_amt,
  MAX(t1.pf_amt_rate) AS pf_amt_rate
 
  
FROM `warehouse`.`ods_day_huya_anchor_live` t1
LEFT JOIN warehouse.dw_month_huya_anchor_info t2
        ON t1.channel_id = t2.channel_id AND t1.anchor_uid = t2.anchor_uid AND t2.dt = '{month.end}'
WHERE t1.dt BETWEEN '{month.start}' AND '{month.end}'
GROUP BY anchor_uid,channel_id;


-- DROP TABLE IF EXISTS stage.stage_month_huya_guild_live_revenue;
-- CREATE TABLE stage.stage_month_huya_guild_live_revenue AS
delete from stage.stage_month_huya_guild_live_revenue where dt ='{month.start}';
insert into stage.stage_month_huya_guild_live_revenue
SELECT
  t1.`channel_id`,
  SUM(ifnull(`revenue`,0)) AS revenue,
  '{month.start}' as dt
FROM `warehouse`.`ods_day_huya_guild_live` t1
WHERE t1.dt BETWEEN '{month.start}' AND '{month.end}'
GROUP BY t1.channel_id;

-- DROP TABLE IF EXISTS stage.stage_month_huya_guild_live_gift_income;
-- CREATE TABLE stage.stage_month_huya_guild_live_gift_income AS
delete from stage.stage_month_huya_guild_live_gift_income where dt ='{month.start}';
insert into stage.stage_month_huya_guild_live_gift_income
SELECT
  t1.`channel_id`,
  SUM(ifnull(`gift_income`,0)) AS gift_income,
  '{month.start}' as dt
FROM `warehouse`.`ods_day_huya_guild_live` t1
WHERE gift_calc_month=  '{month.start}' 
GROUP BY t1.channel_id;

-- DROP TABLE IF EXISTS stage.stage_month_huya_guild_live_guard_income;
-- CREATE TABLE stage.stage_month_huya_guild_live_guard_income AS
delete from stage.stage_month_huya_guild_live_guard_income where dt ='{month.start}';
insert into stage.stage_month_huya_guild_live_guard_income
SELECT
  t1.`channel_id`,
  SUM(ifnull(`guard_income`,0)) AS guard_income,
  '{month.start}' as dt
FROM `warehouse`.`ods_day_huya_guild_live` t1
WHERE guard_calc_month = '{month.start}' 
GROUP BY t1.channel_id;

-- DROP TABLE IF EXISTS stage.stage_month_huya_guild_live_noble_income;
-- CREATE TABLE stage.stage_month_huya_guild_live_noble_income AS
delete from stage.stage_month_huya_guild_live_noble_income where dt ='{month.start}';
insert into stage.stage_month_huya_guild_live_noble_income
SELECT
  t1.`channel_id`,
  SUM(ifnull(`noble_income`,0)) AS noble_income,
  '{month.start}' as dt
FROM `warehouse`.`ods_day_huya_guild_live` t1
WHERE noble_calc_month = '{month.start}' 
GROUP BY t1.channel_id;


-- DROP TABLE IF EXISTS warehouse.dw_month_huya_guild_live;
-- CREATE TABLE warehouse.dw_month_huya_guild_live AS
delete from warehouse.dw_month_huya_guild_live where dt ='{month.start}';
insert into warehouse.dw_month_huya_guild_live
SELECT
  t2.`platform_id`,
  t2.`platform_name`,
  t1.`channel_id`,
  t2.`channel_num`,
  t2.`ow`,
  t2.`channel_name`,
  t2.`logo`,
  t2.`desc`,
  t2.`create_time`,
  t2.`is_platinum`,
  t2.`sign_count`,
  t2.`sign_limit`,
  '{month.start}' AS dt,
  revenue,
  gift_income,
  guard_income,
  noble_income
FROM (SELECT channel_id FROM `warehouse`.`ods_day_huya_guild_live` WHERE dt BETWEEN '{month.start}' AND '{month.end}' GROUP BY channel_id) t1
join stage.stage_month_huya_guild_live_revenue t3
  on t3.channel_id = t1.channel_id and t3.dt = '{month.start}' 
join stage.stage_month_huya_guild_live_gift_income t4
  on t4.channel_id = t1.channel_id and t4.dt = '{month.start}'
join stage.stage_month_huya_guild_live_guard_income t5
  on t5.channel_id = t1.channel_id and t5.dt = '{month.start}'
join stage.stage_month_huya_guild_live_noble_income t6
  on t6.channel_id = t1.channel_id and t6.dt = '{month.start}'
LEFT OUTER JOIN `warehouse`.`dw_month_huya_guild_info` t2
	ON t1.channel_id= t2.channel_id AND t2.dt= '{month.start}'


