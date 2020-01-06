-- 主播信息
-- DROP TABLE IF EXISTS warehouse.ods_anchor_bb_info;
-- CREATE TABLE warehouse.ods_anchor_bb_info AS
DELETE FROM warehouse.ods_anchor_bb_info WHERE dt BETWEEN '{start_date}' AND '{end_date}';
INSERT INTO warehouse.ods_anchor_bb_info
SELECT 1001 AS platform_id,
       'B站' AS platform_name,
       an.backend_account_id,
       an.id AS anchor_uid,
       an.uid AS anchor_no,
       an.uname AS anchor_nick_name,
       an.type AS anchor_status,
       an.type_text AS anchor_status_text,
       an.status AS contract_status,
       an.status_text AS contract_status_text,
	   DATE_FORMAT(an.start_date, '%Y-%m-%d %T') AS contract_signtime,
       DATE_FORMAT(an.end_date, '%Y-%m-%d %T') AS contract_endtime,
       an.dt
FROM spider_bb_backend.normal_list an
WHERE an.dt BETWEEN '{start_date}' AND '{end_date}'
;


-- 主播直播
--  DROP TABLE IF EXISTS warehouse.ods_anchor_bb_live;
--  CREATE TABLE warehouse.ods_anchor_bb_live AS
DELETE FROM warehouse.ods_anchor_bb_live WHERE dt BETWEEN '{start_date}' AND '{end_date}';
INSERT INTO warehouse.ods_anchor_bb_live
SELECT ai.platform_id,
       ai.platform_name,
       ai.backend_account_id,
       ad.g_id AS guild_id,
       ad.g_name AS guild_name,
       ad.guild_type AS guild_type,
       ai.anchor_uid,
       ai.anchor_no,
       ai.anchor_nick_name,
       ai.anchor_status,
       ai.anchor_status_text,
       ad.live_day AS live_status,
       ad.valid_live_day AS valid_live_status,
       ad.live_hour,
       ad.live_hour * 60 * 60 AS duration,
       ad.valid_live_hour,
       ad.valid_live_hour * 60 * 60 AS valid_duration,
       ai.dt,
       ad.timestamp
FROM warehouse.ods_anchor_bb_info ai
LEFT JOIN spider_bb_backend.anchor_detail ad ON ai.backend_account_id = ad.backend_account_id AND ai.anchor_no = ad.uid AND ai.dt = ad.dt
;

-- 主播收入
-- DROP TABLE IF EXISTS warehouse.ods_anchor_bb_virtual_coin;
-- CREATE TABLE warehouse.ods_anchor_bb_virtual_coin AS
DELETE FROM warehouse.ods_anchor_bb_virtual_coin WHERE dt BETWEEN '{start_date}' AND '{end_date}';
INSERT INTO warehouse.ods_anchor_bb_virtual_coin
SELECT ai.platform_id,
       ai.platform_name,
       ai.backend_account_id,
       ad.g_id AS guild_id,
       ad.g_name AS guild_name,
       ad.guild_type AS guild_type,
       ai.anchor_uid,
       ai.anchor_no,
       ai.anchor_nick_name,
       ai.anchor_status,
       ai.anchor_status_text,
	   ad.ios_coin,
       ad.android_coin,
       ad.pc_coin,
       ad.total_income AS total_vir_coin,
       ad.special_coin,
       ad.send_coin,
       ad.DAU,
       ad.max_ppl,
       ad.fc,
       ai.dt,
       ad.timestamp
FROM warehouse.ods_anchor_bb_info ai
LEFT JOIN spider_bb_backend.anchor_detail ad ON ai.backend_account_id = ad.backend_account_id AND ai.anchor_no = ad.uid AND ai.dt = ad.dt
;


-- Merge
-- DROP TABLE IF EXISTS warehouse.ods_bb_anchor_live_detail_daily;
-- CREATE TABLE warehouse.ods_bb_anchor_live_detail_daily AS
DELETE FROM warehouse.ods_bb_anchor_live_detail_daily WHERE dt BETWEEN '{start_date}' AND '{end_date}';
INSERT INTO warehouse.ods_bb_anchor_live_detail_daily
SELECT ai.platform_id,
       ai.platform_name,
       ai.backend_account_id,
       ai.anchor_uid,
       ai.anchor_no,
       ai.anchor_nick_name,
       ai.anchor_status,
       ai.anchor_status_text,
       al.guild_id,
       al.guild_name,
       al.live_status,
       al.valid_live_status,
       al.live_hour,
       al.valid_live_hour,
       al.duration,
       al.valid_duration,
       av.ios_coin,
       av.android_coin,
       av.pc_coin,
       av.total_vir_coin,
       av.special_coin,
       av.send_coin,
       pf.vir_coin_name,
       pf.vir_coin_rate,
       pf.include_pf_amt,
       pf.pf_amt_rate,
       av.DAU,
       av.max_ppl,
       av.fc,
	   ai.contract_status,
       ai.contract_status_text,
       ai.contract_signtime,
       ai.contract_endtime,
       ai.dt
FROM warehouse.ods_anchor_bb_info ai 
LEFT JOIN warehouse.ods_anchor_bb_live al ON ai.backend_account_id = al.backend_account_id AND ai.anchor_uid = al.anchor_uid AND ai.dt = al.dt
LEFT JOIN warehouse.ods_anchor_bb_virtual_coin av ON ai.backend_account_id = av.backend_account_id AND ai.anchor_uid = av.anchor_uid AND ai.dt = av.dt
LEFT JOIN warehouse.platform pf ON ai.platform_id = pf.id
;

