-- 数据源 warehouse.dw_huya_month_guild_info、dw_huya_month_anchor_info
-- ===============================================================
-- 汇总月guild、anchor info数据
-- DROP TABLE IF EXISTS stage.stage_huya_month_guild_info;
-- CREATE TABLE stage.stage_huya_month_guild_info AS
delete from stage.stage_huya_month_guild_info where dt >='{start_date}' AND dt<='{end_date}';
insert into stage.stage_huya_month_guild_info
SELECT channel_id,MAX(dt) AS dt
FROM warehouse.dw_huya_day_guild_info
WHERE dt >='{start_date}' AND dt<='{end_date}'
GROUP BY channel_id;

-- DROP TABLE IF EXISTS warehouse.dw_huya_month_guild_info;
-- CREATE TABLE warehouse.dw_huya_month_guild_info AS
delete from warehouse.dw_huya_month_guild_info where dt >='{start_date}' AND dt<='{end_date}';
insert into warehouse.dw_huya_month_guild_info
select 
  '{start_date}' as `dt`,
  t1.`channel_id`,
  `channel_num`,
  `platform_id`,
  `platform_name`,
  `ow`,
  `channel_name`,
  `logo`,
  `desc`,
  `create_time`,
  `is_platinum`,
  `sign_count`,
  `sign_limit`
  
from `warehouse`.`dw_huya_day_guild_info`  t1
join stage.stage_huya_month_guild_info t2
on t1.dt= t2.dt and t1.channel_id = t2.channel_id
where t1.dt >='{start_date}' AND t1.dt<='{end_date}';


-- DROP TABLE IF EXISTS stage.stage_huya_month_anchor_info;
-- CREATE TABLE stage.stage_huya_month_anchor_info AS
delete from stage.stage_huya_month_anchor_info where dt >='{start_date}' AND dt<='{end_date}';
insert into stage.stage_huya_month_anchor_info
SELECT anchor_uid,MAX(dt) AS dt,channel_id
FROM warehouse.dw_huya_day_anchor_info
WHERE dt >='{start_date}' AND dt<='{end_date}'
GROUP BY anchor_uid,channel_id;


-- DROP TABLE IF EXISTS warehouse.dw_huya_month_anchor_info;
-- CREATE TABLE warehouse.dw_huya_month_anchor_info AS
delete from warehouse.dw_huya_month_anchor_info where dt >='{start_date}' AND dt<='{end_date}';
insert into warehouse.dw_huya_month_anchor_info
select  
  '{start_date}' as `dt`,
  t1.`channel_id`,
  `channel_num`,
  t1.`anchor_uid`,
  `anchor_no`,
  `platform_id`,
  `platform_name`,
  `comment`,
  `nick`,
  `activity_days`,
  `months`,
  `ow_percent`,
  `sign_time`,
  `surplus_days`,
  `avatar`,
  t2.dt as last_active_date
from `warehouse`.`dw_huya_day_anchor_info`  t1
join stage.stage_huya_month_anchor_info t2
on t1.dt= t2.dt and t1.anchor_uid = t2.anchor_uid and t1.channel_id= t2.channel_id
where t1.dt >='{start_date}' AND t1.dt<='{end_date}';



-- DROP TABLE IF EXISTS warehouse.dw_huya_month_anchor_live;
-- CREATE TABLE warehouse.dw_huya_month_anchor_live AS
delete from warehouse.dw_huya_month_anchor_live where dt ='{start_date}';
insert into warehouse.dw_huya_month_anchor_live
SELECT
 '{start_date}' AS dt,
  
  t1.channel_id,
  t2.channel_num,
  t1.anchor_uid,
  t2.anchor_no,
  t2.nick,
  t2.comment,
  duration,
  live_cnt,
  income,
  peak_pcu_avg,
  peak_pcu_max,
  peak_pcu_min,
  t2.platform_id,
  t2.platform_name,
  t2.activity_days,
  t2.months,
  t2.ow_percent,
  t2.sign_time,
  t2.surplus_days,
  t2.`avatar` AS avatar,
  vir_coin_name,
  vir_coin_rate,
  include_pf_amt,
  pf_amt_rate
 
  
FROM (select anchor_uid,channel_id,
  SUM(IFNULL(`duration`,0)) AS duration,
  SUM(IFNULL(live_status,0)) AS live_cnt,
  SUM(IFNULL(income,0)) AS income,
  AVG(IF(peak_pcu>0,peak_pcu,NULL)) AS peak_pcu_avg,
  MAX(peak_pcu) AS peak_pcu_max,
  MIN(if(peak_pcu>0,peak_pcu,null)) AS peak_pcu_min,
  MAX(vir_coin_name) AS vir_coin_name,
  MAX(vir_coin_rate) AS vir_coin_rate,
  MAX(include_pf_amt) AS include_pf_amt,
  MAX(pf_amt_rate) AS pf_amt_rate
  from `warehouse`.`dw_huya_day_anchor_live` where dt BETWEEN '{start_date}' AND '{end_date}' GROUP BY anchor_uid,channel_id) t1
LEFT JOIN warehouse.dw_huya_month_anchor_info t2
        ON t1.channel_id = t2.channel_id AND t1.anchor_uid = t2.anchor_uid AND t2.dt = '{start_date}';


-- DROP TABLE IF EXISTS stage.stage_huya_month_guild_live_revenue;
-- CREATE TABLE stage.stage_huya_month_guild_live_revenue AS
delete from stage.stage_huya_month_guild_live_revenue where dt ='{start_date}';
insert into stage.stage_huya_month_guild_live_revenue
SELECT
  t1.`channel_id`,
  SUM(ifnull(`revenue`,0)) AS revenue,
  '{start_date}' as dt
FROM `warehouse`.`dw_huya_day_guild_live` t1
WHERE t1.dt BETWEEN '{start_date}' AND '{end_date}'
GROUP BY t1.channel_id;

-- DROP TABLE IF EXISTS stage.stage_huya_month_guild_live_gift_income;
-- CREATE TABLE stage.stage_huya_month_guild_live_gift_income AS
delete from stage.stage_huya_month_guild_live_gift_income where dt ='{start_date}';
insert into stage.stage_huya_month_guild_live_gift_income
SELECT
  t1.`channel_id`,
  SUM(ifnull(`gift_income`,0)) AS gift_income,
  '{start_date}' as dt
FROM `warehouse`.`dw_huya_day_guild_live` t1
WHERE gift_calc_month=  '{start_date}' 
GROUP BY t1.channel_id;

-- DROP TABLE IF EXISTS stage.stage_huya_month_guild_live_guard_income;
-- CREATE TABLE stage.stage_huya_month_guild_live_guard_income AS
delete from stage.stage_huya_month_guild_live_guard_income where dt ='{start_date}';
insert into stage.stage_huya_month_guild_live_guard_income
SELECT
  t1.`channel_id`,
  SUM(ifnull(`guard_income`,0)) AS guard_income,
  '{start_date}' as dt
FROM `warehouse`.`dw_huya_day_guild_live` t1
WHERE guard_calc_month = '{start_date}' 
GROUP BY t1.channel_id;

-- DROP TABLE IF EXISTS stage.stage_huya_month_guild_live_noble_income;
-- CREATE TABLE stage.stage_huya_month_guild_live_noble_income AS
delete from stage.stage_huya_month_guild_live_noble_income where dt ='{start_date}';
insert into stage.stage_huya_month_guild_live_noble_income
SELECT
  t1.`channel_id`,
  SUM(ifnull(`noble_income`,0)) AS noble_income,
  '{start_date}' as dt
FROM `warehouse`.`dw_huya_day_guild_live` t1
WHERE noble_calc_month = '{start_date}' 
GROUP BY t1.channel_id;


-- DROP TABLE IF EXISTS warehouse.dw_huya_month_guild_live;
-- CREATE TABLE warehouse.dw_huya_month_guild_live AS
delete from warehouse.dw_huya_month_guild_live where dt ='{start_date}';
insert into warehouse.dw_huya_month_guild_live
SELECT
  '{start_date}' AS dt,
  t1.`channel_id`,
  t2.`channel_num`,
  t2.`platform_id`,
  t2.`platform_name`,
  t2.`ow`,
  t2.`channel_name`,
  t2.`logo`,
  t2.`desc`,
  t2.`create_time`,
  t2.`is_platinum`,
  t2.`sign_count`,
  t2.`sign_limit`,
  
  revenue,
  gift_income,
  guard_income,
  noble_income
FROM (SELECT channel_id FROM `warehouse`.`dw_huya_day_guild_live` WHERE dt BETWEEN '{start_date}' AND '{end_date}' GROUP BY channel_id) t1
join stage.stage_huya_month_guild_live_revenue t3
  on t3.channel_id = t1.channel_id and t3.dt = '{start_date}' 
join stage.stage_huya_month_guild_live_gift_income t4
  on t4.channel_id = t1.channel_id and t4.dt = '{start_date}'
join stage.stage_huya_month_guild_live_guard_income t5
  on t5.channel_id = t1.channel_id and t5.dt = '{start_date}'
join stage.stage_huya_month_guild_live_noble_income t6
  on t6.channel_id = t1.channel_id and t6.dt = '{start_date}'
LEFT OUTER JOIN `warehouse`.`dw_huya_month_guild_info` t2
	ON t1.channel_id= t2.channel_id AND t2.dt= '{start_date}'


