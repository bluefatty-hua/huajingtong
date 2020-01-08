-- 主播信息
-- DROP TABLE IF EXISTS warehouse.ods_anchor_hy_info;
-- REATE TABLE warehouse.ods_anchor_hy_info AS
DELETE FROM warehouse.ods_anchor_hy_info WHERE dt BETWEEN '{start_date}' AND '{end_date}';
INSERT INTO warehouse.ods_anchor_hy_info
SELECT 1002 AS platform_id,
       '虎牙' AS platform_name,
       ad.channel_id,
       ad.l_uid AS anchor_uid,
       ad.l_yy AS anchor_no,
       ad.s_nick AS anchor_nick_name,
       DATE_FORMAT(FROM_UNIXTIME(ad.i_sign_time), '%Y-%m-%d %T') AS contract_signtime,
       DATE_ADD(DATE_FORMAT(FROM_UNIXTIME(ad.i_sign_time), '%Y-%m-%d %T'), INTERVAL ad.i_months MONTH) AS contract_endtime,
       1 AS settle_method_code,
       '对私分成' AS settle_method_text,
	   (100 - ad.i_ow_percent) / 100 AS anchor_settle_rate,
       ad.s_avatar AS logo,
       ad.dt,
       ad.timestamp
FROM spider_huya_backend.anchor_detail ad
WHERE ad.dt BETWEEN '{start_date}' AND '{end_date}'
;


-- 主播直播和直播收入
-- DROP TABLE IF EXISTS warehouse.ods_anchor_hy_live_amt;
-- CREATE TABLE warehouse.ods_anchor_hy_live_amt AS
DELETE FROM warehouse.ods_anchor_hy_live_amt WHERE dt BETWEEN '{start_date}' AND '{end_date}';
INSERT INTO warehouse.ods_anchor_hy_live_amt
SELECT ai.platform_id,
       ai.platform_name,
       ai.channel_id,
       ai.anchor_uid,
       ai.anchor_no,
       ai.anchor_nick_name,
       al.live_time AS duration,
       CASE WHEN al.live_time > 0 THEN 1 ELSE 0 END AS live_status,
       al.income AS amt,
       ai.settle_method_code,
       ai.settle_method_text,
       ai.anchor_settle_rate,
       al.peak_pcu,
	   al.date AS dt,
       al.timestamp
FROM warehouse.ods_anchor_hy_info ai
LEFT JOIN spider_huya_backend.anchor_live_detail_day al ON ai.channel_id = al.channel_id AND ai.anchor_uid = al.uid AND ai.dt = al.date
WHERE ai.dt BETWEEN '{start_date}' AND '{end_date}'
;


-- Merge
-- DROP TABLE IF EXISTS warehouse.ods_anchor_hy_live_detail_daily;
-- CREATE TABLE warehouse.ods_anchor_hy_live_detail_daily AS
DELETE FROM warehouse.ods_anchor_hy_live_detail_daily WHERE dt BETWEEN '{start_date}' AND '{end_date}';
INSERT INTO warehouse.ods_anchor_hy_live_detail_daily
SELECT ai.platform_id,
       ai.platform_name,
       ai.channel_id,
       ai.anchor_uid,
       ai.anchor_no,
       ai.anchor_nick_name,
       al.live_status,
       al.duration,
       al.amt,
       al.peak_pcu,
       ai.settle_method_code,
       ai.settle_method_text,
       ai.anchor_settle_rate,
       pf.vir_coin_name,
       pf.vir_coin_rate,
       pf.include_pf_amt,
       pf.pf_amt_rate,
       ai.contract_signtime,
       ai.contract_endtime,
       ai.logo,
       ai.dt
FROM warehouse.ods_anchor_hy_info ai
LEFT JOIN warehouse.ods_anchor_hy_live_amt al ON ai.channel_id = al.channel_id AND ai.anchor_uid = al.anchor_uid AND ai.dt = al.dt
LEFT JOIN warehouse.platform pf ON ai.platform_id = pf.id
WHERE ai.dt BETWEEN '{start_date}' AND '{end_date}'
;


-- ===================================================================
-- 公会收入
-- DROP TABLE IF EXISTS warehouse.ods_guild_hy_amt_daily;
-- CREATE TABLE warehouse.ods_guild_hy_amt_daily AS
DELETE FROM warehouse.ods_guild_hy_amt_daily WHERE dt BETWEEN '{start_date}' AND '{end_date}';
INSERT INTO warehouse.ods_guild_hy_amt_daily
SELECT  1002 AS platform_id,
       '虎牙' AS platform_name,
       cd.channel_id,
       cd.channel_number AS channel_num,
       cd.uid AS channel_uid,
       cd.name AS channel_name,
       cd.is_platinum,
       cd.sign_count AS sign_an_cnt,
       cd.sign_limit AS sign_onlive_an_cnt,
       cr.i_live_profile_cnt AS active_an_cnt,
       CASE WHEN cr.d_daily_income IS NULL THEN 0 ELSE cr.d_daily_income END AS g_total_amt,
       CASE WHEN cgi.income_amt IS NULL THEN 0 ELSE cgi.income_amt END AS g_gift_vir_coin,
       CASE WHEN cgu.income_amt IS NULL THEN 0 ELSE cgu.income_amt END AS g_guard_vir_coin,
       CASE WHEN cn.income_amt IS NULL THEN 0 ELSE cn.income_amt END AS g_nobel_vir_coin,
       cd.logo,
       cd.desc,
       cd.create_time,
       cd.dt
FROM spider_huya_backend.channel_detail cd
LEFT JOIN spider_huya_backend.channel_revenue_day cr ON cd.dt = cr.dt AND cd.channel_id = cr.channel_id
LEFT JOIN spider_huya_backend.channel_income_gift_day cgi ON cd.dt = cgi.dt AND cd.channel_id = cgi.channel_id
LEFT JOIN spider_huya_backend.channel_income_guard_day cgu ON cd.dt = cgu.income_date AND cd.channel_id = cgu.channel_id
LEFT JOIN spider_huya_backend.channel_income_noble_day cn ON cd.dt = cn.dt AND cd.channel_id = cn.channel_id
WHERE cd.dt BETWEEN '{start_date}' AND '{end_date}'
;
