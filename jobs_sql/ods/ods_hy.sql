-- 工会信息
-- DROP TABLE IF EXISTS warehouse.ods_huya_day_guild_info;
-- CREATE TABLE warehouse.ods_huya_day_guild_info AS
DELETE FROM  warehouse.ods_huya_day_guild_info  WHERE dt BETWEEN '{start_date}' AND '{end_date}';
INSERT INTO warehouse.ods_huya_day_guild_info
SELECT
  `dt`,
  1002 AS platform_id,
 '虎牙' AS platform_name,
`channel_id`,
  `channel_number` as channel_num,
  `uid` as ow,
  `name` as channel_name,
  `logo`,
  `desc`,
  `create_time`,
  `is_platinum`,
  `sign_count`,
  `sign_limit`,
  `timestamp`
FROM `spider_huya_backend`.`channel_detail` 
WHERE dt  BETWEEN '{start_date}' AND '{end_date}';

-- 主播信息
-- DROP TABLE IF EXISTS warehouse.ods_huya_day_anchor_info;
-- CREATE TABLE warehouse.ods_huya_day_anchor_info AS
DELETE FROM  warehouse.ods_huya_day_anchor_info  WHERE dt BETWEEN '{start_date}' AND '{end_date}';
INSERT INTO warehouse.ods_huya_day_anchor_info
SELECT
  ad.`dt`,
  l_uid AS `anchor_uid`,
  l_yy AS `anchor_no`,
  ad.`channel_id`,
  `s_nick` AS nick,
  `i_activity_days` AS activity_days,
  `i_months` AS months,
  `i_ow_percent` AS ow_percent,
  `i_sign_time` AS sign_time,
  `i_surplus_days` surplus_days,
   ad.`timestamp`,
  `s_avatar` AS avatar
FROM `spider_huya_backend`.`anchor_detail` ad
WHERE ad.dt  BETWEEN '{start_date}' AND '{end_date}';



-- 主播直播和直播收入
-- DROP TABLE IF EXISTS warehouse.ods_huya_day_anchor_live;
-- CREATE TABLE warehouse.ods_huya_day_anchor_live AS
DELETE FROM warehouse.ods_huya_day_anchor_live WHERE dt BETWEEN '{start_date}' AND '{end_date}';
INSERT INTO warehouse.ods_huya_day_anchor_live
SELECT 
        date AS dt,
        channel_id,
        uid as anchor_uid,
        nick,
        income,
        live_time AS duration,
        CASE WHEN live_time > 0 THEN 1 ELSE 0 END AS live_status,
        peak_pcu,
        `timestamp`   
FROM spider_huya_backend.anchor_live_detail_day 
WHERE `date` BETWEEN '{start_date}' AND '{end_date}';



-- ===================================================================
-- 公会收入
-- DROP TABLE IF EXISTS warehouse.ods_huya_day_guild_live_revenue;
-- CREATE TABLE warehouse.ods_huya_day_guild_live_revenue AS
DELETE FROM warehouse.ods_huya_day_guild_live_revenue WHERE dt BETWEEN '{start_date}' AND '{end_date}';
INSERT INTO warehouse.ods_huya_day_guild_live_revenue
SELECT channel_id,
      dt,
      concat(year(dt),'-',month(dt),'-01') as calc_month,
      d_daily_income as revenue,
      i_live_profile_cnt as live_cnt,
      `timestamp`
FROM spider_huya_backend.channel_revenue_day 
WHERE dt BETWEEN '{start_date}' AND '{end_date}';


-- DROP TABLE IF EXISTS warehouse.ods_huya_day_guild_live_income_gift;
-- CREATE TABLE warehouse.ods_huya_day_guild_live_income_gift AS
DELETE FROM warehouse.ods_huya_day_guild_live_income_gift WHERE dt BETWEEN '{start_date}' AND '{end_date}';
INSERT INTO warehouse.ods_huya_day_guild_live_income_gift
SELECT channel_id,
      dt,
      month as calc_month,
      income_amt as gift_income,
      `timestamp`
FROM spider_huya_backend.channel_income_gift_day 
WHERE dt BETWEEN '{start_date}' AND '{end_date}';


-- DROP TABLE IF EXISTS warehouse.ods_huya_day_guild_live_income_guard;
-- CREATE TABLE warehouse.ods_huya_day_guild_live_income_guard AS
DELETE FROM warehouse.ods_huya_day_guild_live_income_guard WHERE dt BETWEEN '{start_date}' AND '{end_date}';
INSERT INTO warehouse.ods_huya_day_guild_live_income_guard
SELECT channel_id,
      income_date as dt,
      month as calc_month,
      income_amt as guard_income,
      `timestamp`
FROM spider_huya_backend.channel_income_guard_day 
WHERE income_date BETWEEN '{start_date}' AND '{end_date}';


-- DROP TABLE IF EXISTS warehouse.ods_huya_day_guild_live_income_noble;
-- CREATE TABLE warehouse.ods_huya_day_guild_live_income_noble AS
DELETE FROM warehouse.ods_huya_day_guild_live_income_noble WHERE dt BETWEEN '{start_date}' AND '{end_date}';
INSERT INTO warehouse.ods_huya_day_guild_live_income_noble
SELECT channel_id,
      dt as dt,
      month as calc_month,
      income_amt as noble_income,
      `timestamp`
FROM spider_huya_backend.channel_income_noble_day 
WHERE dt BETWEEN '{start_date}' AND '{end_date}';



