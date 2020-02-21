-- DROP TABLE IF EXISTS warehouse.ods_dy_day_anchor_info;
-- CREATE TABLE warehouse.ods_dy_day_anchor_info AS
DELETE
FROM warehouse.ods_dy_day_anchor_info
WHERE dt BETWEEN '{start_date}' AND '{end_date}';
INSERT INTO warehouse.ods_dy_day_anchor_info
SELECT ad.dt,
       1005              AS platform_id,
       'DouYin'          AS platform_name,
       ad.backend_account_id,
       ad.faction_name   AS guild_name,
       ad.uid            AS anchor_uid,
       ad.short_id       AS anchor_short_id,
       ad.unique_id      AS anchor_no,
       ad.nick_name      AS anchor_nick_name,
       ad.real_name,
       ad.telephone,
       ad.last_live_time,
       ad.follower_count,
       ad.total_diamond,
       ad.signing_type,
       ad.signing_time,
       ad.income_percent AS anchor_settle_rate,
       ad.gender,
       ad.avatar         AS logo,
       ad.agent_id,
       ad.agent_name,
       ad.notes,
       ad.display_room_id,
       ad.aweme_display_id,
       ad.signature,
       ad.punished,
       ad.timestamp
FROM spider_dy_backend.anchor_detail ad
WHERE dt BETWEEN '{start_date}' AND '{end_date}'
;


-- DROP TABLE IF EXISTS warehouse.ods_dy_day_anchor_live_duration;
-- CREATE TABLE warehouse.ods_dy_day_anchor_live_duration AS
DELETE
FROM warehouse.ods_dy_day_anchor_live_duration
WHERE dt BETWEEN '{start_date}' AND '{end_date}';
INSERT INTO warehouse.ods_dy_day_anchor_live_duration
SELECT ad.dt,
       1005                           AS platform_id,
       'DouYin'                       AS platform_name,
       ad.backend_account_id,
       ad.uid                         AS anchor_uid,
       ad.nick_name                   AS anchor_nick_name,
       ad.live_duration               AS duration,
       ad.live_earnings               AS live_revenue,
       ad.prop_earnings               AS prop_revenue,
       ad.activity_earnings           AS act_revenue,
       ad.total_earnings              AS revenue,
       ad.fan_rise,
       ad.timestamp
FROM spider_dy_backend.anchor_data ad
;


-- DROP TABLE IF EXISTS warehouse.ods_dy_day_anchor_live;
-- CREATE TABLE warehouse.ods_dy_day_anchor_live AS
DELETE
FROM warehouse.ods_dy_day_anchor_live
WHERE dt BETWEEN '{start_date}' AND '{end_date}';
INSERT INTO warehouse.ods_dy_day_anchor_live
SELECT ai.dt,
       ai.platform_id,
       ai.platform_name,
       ai.backend_account_id,
       ai.guild_name,
       ai.anchor_uid,
       ai.anchor_short_id,
       ai.anchor_no,
       ai.anchor_nick_name,
       ai.last_live_time,
       ai.follower_count,
       ai.total_diamond,
       IF(al.duration > 0, 1, 0) AS live_status,
       al.duration,
       al.revenue,
       al.live_revenue,
       al.prop_revenue,
       al.act_revenue,
       al.fan_rise,
       ai.signing_type,
       ai.signing_time,
       IF(ai.signing_time=0, '', FROM_UNIXTIME(ai.signing_time, '%Y-%m-%d')) AS sign_time,
       ai.anchor_settle_rate,
       ai.gender,
       ai.agent_id,
       ai.agent_name,
       ai.logo
FROM warehouse.ods_dy_day_anchor_info ai
         LEFT JOIN warehouse.ods_dy_day_anchor_live_duration al
                   ON ai.dt = al.dt AND ai.backend_account_id = al.backend_account_id AND ai.anchor_uid = al.anchor_uid
WHERE ai.dt BETWEEN '{start_date}' AND '{end_date}'
;

