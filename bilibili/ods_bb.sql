-- 主播信息
-- 从normal_detail中抽取主播最新信息补充到ods_anchor_bb_info
DROP TABLE IF EXISTS tmp.anchor_detail_recent_day;
CREATE TABLE tmp.anchor_detail_recent_day AS
SELECT ad.g_id,
       ad.uid,
       MAX(ad.dt) AS recent_date
FROM spider_bb_backend.anchor_detail ad
GROUP BY ad.g_id,
         ad.uid
;


DROP TABLE IF EXISTS warehouse.ods_anchor_bb_info;
CREATE TABLE warehouse.ods_anchor_bb_info AS
SELECT 1001 AS platform_id,
       'B站' AS platform_name,
       ad.g_id AS guild_id,
       ad.g_name AS guild_name,
       ad.guild_type,
       an.id AS anchor_uid,
       an.uid AS anchor_no,
       ad.uname AS anchor_nick_name,
       an.type AS anchor_type,
       ad.roomid AS live_room_id,
       an.type AS contract_type,
       an.type_text AS contract_type_name,
       an.status AS contract_status,
       an.status_text AS contract_status_name,
       an.start_date AS contract_signtime,
       an.end_date AS contract_endtime,
       an.entry_time,
       an.ctime,
       rec.recent_date
FROM spider_bb_backend.normal_list an
LEFT JOIN tmp.anchor_detail_recent_day rec ON an.uid = rec.uid
LEFT JOIN spider_bb_backend.anchor_detail ad ON rec.g_id = ad.g_id AND rec.uid = ad.uid AND rec.recent_date = ad.dt
;

-- 主播直播
DROP TABLE IF EXISTS warehouse.ods_anchor_bb_live;
CREATE TABLE warehouse.ods_anchor_bb_live AS
SELECT ai.platform_id,
       ai.platform_name,
       ai.guild_id,
       ai.guild_name,
       ai.guild_type,
       ai.anchor_uid,
       ai.anchor_no,
       ai.anchor_nick_name,
       ai.anchor_type,
       ad.live_day AS live_status,
       ad.valid_live_day AS valid_live_status,
       ad.live_hour,
       ad.valid_live_hour,
       ad.dt,
       ad.timestamp
FROM warehouse.ods_anchor_bb_info ai
LEFT JOIN spider_bb_backend.anchor_detail ad ON ai.guild_id = ad.g_id AND ai.anchor_no = ad.uid
;

-- 主播收入
DROP TABLE IF EXISTS warehouse.ods_anchor_bb_virtual_coin;
CREATE TABLE warehouse.ods_anchor_bb_virtual_coin AS 
SELECT ai.platform_id,
       ai.platform_name,
       ai.guild_id,
       ai.guild_name,
       ai.guild_type,
       ai.anchor_uid,
       ai.anchor_no,
       ai.anchor_nick_name,
       ai.anchor_type,
	   ad.ios_coin,
       ad.android_coin,
       ad.pc_coin,
       ad.total_income,
       ad.special_coin,
       ad.send_coin,
       ad.DAU,
       ad.max_ppl,
       ad.fc,
       ad.dt,
       ad.timestamp
FROM warehouse.ods_anchor_bb_info ai
LEFT JOIN spider_bb_backend.anchor_detail ad ON ai.guild_id = ad.g_id AND ai.anchor_no = ad.uid
;
