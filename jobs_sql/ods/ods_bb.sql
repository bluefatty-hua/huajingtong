-- 主播信息
DROP TABLE IF EXISTS warehouse.ods_anchor_bb_info;
CREATE TABLE warehouse.ods_anchor_bb_info AS
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
DROP TABLE IF EXISTS warehouse.ods_anchor_bb_live;
CREATE TABLE warehouse.ods_anchor_bb_live AS
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
       ad.valid_live_hour,
       ai.dt,
       ad.timestamp
FROM warehouse.ods_anchor_bb_info ai
LEFT JOIN spider_bb_backend.anchor_detail ad ON ai.backend_account_id = ad.backend_account_id AND ai.anchor_no = ad.uid AND ai.dt = ad.dt
;

-- 主播收入
DROP TABLE IF EXISTS warehouse.ods_anchor_bb_virtual_coin;
CREATE TABLE warehouse.ods_anchor_bb_virtual_coin AS
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
       ad.total_income,
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
