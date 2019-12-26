-- 汇总佣金，保证粒度一致
DROP TABLE IF EXISTS warehouse.dw_anchor_yy_commission_daily;
CREATE TABLE warehouse.dw_anchor_yy_commission_daily AS
SELECT ac.platform_id,
       ac.guild_id,
       ac.anchor_uid,
       ac.anchor_no,
       DATE(ac.dtime) AS dt,
       SUM(ac.anchor_commission) AS anchor_commission,
       SUM(ac.guild_commission) AS guild_commission
FROM warehouse.ods_anchor_yy_commission ac
GROUP BY ac.platform_id,
         ac.guild_id,
         ac.anchor_uid,
         ac.anchor_no,
         DATE(ac.dtime)
;


DROP TABLE IF EXISTS warehouse.dw_anchor_yy_day;
CREATE TABLE warehouse.dw_anchor_yy_day AS
SELECT ai.platform_id,
       ai.platform_name,
       ai.guild_id,
       ai.anchor_uid,
       ai.anchor_no,
       ai.anchor_nick_name,
       ai.anchor_type,
       ai.live_room_id,
       ai.roomid,
       ai.contract_id,
       ai.contract_type,
       ai.contract_signtime,
       ai.contract_endtime,
       ai.settle_method_code,
       ai.settle_method_name,
       ai.anchor_settle_rate,
       al.channel_id,
       al.duration,
       al.duration_sec,
       al.pcduration,
       al.pcduration_sec,
       al.mobduration,
       al.mobduration_sec,
       ab.diamond,
       acd.anchor_commission,
       acd.guild_commission,
       ai.logo,
       al.dt
FROM warehouse.ods_anchor_yy_info ai
LEFT JOIN warehouse.ods_anchor_yy_live al ON ai.platform_id = al.platform_id AND ai.guild_id = al.guild_id AND ai.anchor_uid = al.anchor_uid AND ai.anchor_no = al.anchor_no
LEFT JOIN warehouse.ods_anchor_yy_bluediamond ab ON ai.platform_id = ab.platform_id AND ai.guild_id = ab.guild_id AND ai.anchor_uid = ab.anchor_uid AND ai.anchor_no = ab.anchor_no AND al.dt = ab.dt
LEFT JOIN tmp.dw_anchor_yy_commission_daily acd ON ai.platform_id = acd.platform_id AND ai.guild_id = acd.guild_id AND ai.anchor_uid = acd.anchor_uid AND ai.anchor_no = acd.anchor_no AND al.dt = acd.dt
;


