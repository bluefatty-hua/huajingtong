-- 工会信息
-- DROP TABLE IF EXISTS warehouse.ods_day_huya_guild_info;
-- CREATE TABLE warehouse.ods_day_huya_guild_info AS
DELETE FROM  warehouse.ods_day_huya_guild_info  WHERE dt BETWEEN '{start_date}' AND '{end_date}';
INSERT INTO warehouse.ods_day_huya_guild_info
SELECT
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
  `dt`,
  `timestamp`
FROM `spider_huya_backend`.`channel_detail` 
WHERE dt  BETWEEN '{start_date}' AND '{end_date}';

-- 主播信息
-- DROP TABLE IF EXISTS stage.stage_day_huya_anchor_info;
-- CREATE TABLE stage.stage_day_huya_anchor_info AS
DELETE FROM  stage.stage_day_huya_anchor_info  WHERE dt BETWEEN '{start_date}' AND '{end_date}';
INSERT INTO stage.stage_day_huya_anchor_info
SELECT
  l_uid AS `anchor_uid`,
  l_yy AS `anchor_no`,
  ad.`channel_id`,
  'orig' AS `comment`,
  `s_nick` AS nick,
  `i_activity_days` AS activity_days,
  `i_months` AS months,
  `i_ow_percent` AS ow_percent,
  `i_sign_time` AS sign_time,
  `i_surplus_days` surplus_days,
   ad.`dt`,
   ad.`timestamp`,
  `s_avatar` AS avatar
FROM `spider_huya_backend`.`anchor_detail` ad
WHERE ad.dt  BETWEEN '{start_date}' AND '{end_date}';

INSERT IGNORE INTO stage.stage_day_huya_anchor_info
(anchor_uid,channel_id, `comment` ,dt)
SELECT uid,channel_id,'from anchor_live_detail_day',`date` AS dt 
FROM `spider_huya_backend`.`anchor_live_detail_day`
WHERE `date` BETWEEN '{start_date}' AND '{end_date}'


-- DROP TABLE IF EXISTS warehouse.ods_day_huya_anchor_info;
-- REATE TABLE warehouse.ods_day_huya_anchor_info AS
DELETE FROM  warehouse.ods_day_huya_anchor_info  WHERE dt BETWEEN '{start_date}' AND '{end_date}';
INSERT INTO warehouse.ods_day_huya_anchor_info
SELECT
  platform_id,
  platform_name,
  ad.`channel_id`,
  channel_num AS channel_num,
  anchor_uid AS `anchor_uid`,
  anchor_no AS `anchor_no`,
  `comment`,
  `nick` AS nick,
  `activity_days` AS activity_days,
  `months` AS months,
  `ow_percent` AS ow_percent,
  `sign_time` AS sign_time,
  `surplus_days` surplus_days,
   ad.`dt`,
  `avatar` AS avatar
FROM `stage`.`stage_day_huya_anchor_info` ad
LEFT JOIN warehouse.`ods_day_huya_guild_info` ch ON  ad.`channel_id` = ch.`channel_id` AND ad.dt = ch.dt
WHERE ad.dt  BETWEEN '{start_date}' AND '{end_date}';


-- 主播直播和直播收入
-- DROP TABLE IF EXISTS warehouse.dw_day_huya_anchor_live;
-- CREATE TABLE warehouse.dw_day_huya_anchor_live AS
DELETE FROM warehouse.dw_day_huya_anchor_live WHERE dt BETWEEN '{start_date}' AND '{end_date}';
INSERT INTO warehouse.dw_day_huya_anchor_live
SELECT ai.platform_id,
        ai.platform_name,
        ai.channel_id,
        ai.channel_num,
        ai.anchor_uid,
        ai.anchor_no,
        ai.nick,
        ai.comment,
        al.live_time AS duration,
        CASE WHEN al.live_time > 0 THEN 1 ELSE 0 END AS live_status,
        al.income AS income,
        al.peak_pcu,
	    al.date AS dt,
        ai.activity_days,
        ai.months,
        ai.ow_percent,
        ai.sign_time,
        ai.surplus_days,
        ai.`avatar` AS avatar,
        pf.vir_coin_name,
        pf.vir_coin_rate,
        pf.include_pf_amt,
        pf.pf_amt_rate
FROM warehouse.ods_day_huya_anchor_info ai
JOIN spider_huya_backend.anchor_live_detail_day al 
    ON ai.channel_id = al.channel_id AND ai.anchor_uid = al.uid AND ai.dt = al.date
LEFT JOIN warehouse.platform pf ON ai.platform_id = pf.id
WHERE ai.dt BETWEEN '{start_date}' AND '{end_date}'




-- ===================================================================
-- 公会收入
-- DROP TABLE IF EXISTS warehouse.ods_day_huya_guild_live;
-- CREATE TABLE warehouse.ods_day_huya_guild_live AS
DELETE FROM warehouse.ods_day_huya_guild_live WHERE dt BETWEEN '{start_date}' AND '{end_date}';
INSERT INTO warehouse.ods_day_huya_guild_live
SELECT cd.platform_id,
       cd.platform_name,
       cd.channel_id,
       cd.channel_number AS channel_num,
       cd.ow AS ow,
       cd.channel_name AS channel_name,
       cd.is_platinum,
       cd.sign_count,
       cd.sign_limit,
       cr.i_live_profile_cnt AS active_anchor_cnt,
       CASE WHEN cr.d_daily_income IS NULL THEN 0 ELSE cr.d_daily_income END AS revenue,
       CASE WHEN cgi.income_amt IS NULL THEN 0 ELSE cgi.income_amt END AS gift_income,
       CASE WHEN cgu.income_amt IS NULL THEN 0 ELSE cgu.income_amt END AS guard_income,
       CASE WHEN cn.income_amt IS NULL THEN 0 ELSE cn.income_amt END AS noble_income,
       cd.logo,
       cd.desc,
       cd.create_time,
       cd.dt,
       cgi.month as gift_calc_month,
       cgu.month as guard_calc_month,
       cn.month as noble_calc_month

FROM warehouse.ods_day_huya_guild_info cd
LEFT JOIN spider_huya_backend.channel_revenue_day cr ON cd.dt = cr.dt AND cd.channel_id = cr.channel_id
LEFT JOIN spider_huya_backend.channel_income_gift_day cgi ON cd.dt = cgi.dt AND cd.channel_id = cgi.channel_id
LEFT JOIN spider_huya_backend.channel_income_guard_day cgu ON cd.dt = cgu.income_date AND cd.channel_id = cgu.channel_id
LEFT JOIN spider_huya_backend.channel_income_noble_day cn ON cd.dt = cn.dt AND cd.channel_id = cn.channel_id
WHERE cd.dt BETWEEN '{start_date}' AND '{end_date}';


