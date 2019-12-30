-- 直播时长转秒 DURATION_CH
DELIMITER $$
DROP FUNCTION IF EXISTS DURATION_CH$$
CREATE FUNCTION DURATION_CH(duration varchar(20))
RETURNS INT(10)
BEGIN
    DECLARE sec INT(10) DEFAULT 0;
    SET sec = (CASE WHEN duration RLIKE '小时' THEN SUBSTRING_INDEX(duration, '小时', 1) + 0 ELSE 0 END) * 60 * 60 + (CASE WHEN duration RLIKE '分' THEN SUBSTRING_INDEX(SUBSTRING_INDEX(duration, '小时', -1), '分', 1) + 0 ELSE 0 END) * 60 + (SUBSTRING_INDEX(SUBSTRING_INDEX(duration, '分', -1), '秒', 1) + 0);
    RETURN sec;
END $$
DELIMITER ;


-- 主播信息
DROP TABLE IF EXISTS tmp.ods_anchor_info_tmp;
CREATE TABLE tmp.ods_anchor_info_tmp AS
SELECT 1000 AS platform_id,
       'YY' AS platform_name,
       backend_account_id as guild_id,
       uid AS anchor_uid,
       yynum AS anchor_no,
       nick AS anchor_nick_name,
       anchortype AS anchor_type,
       roomaid AS live_room_id,
       roomid,
       conId AS contract_id,
       contype AS contract_type,
       signtime AS contract_signtime,
       endtime AS contract_endtime,
       contype AS settle_method_code,
       CASE WHEN contype = 1 THEN '对公分成' 
            WHEN contype = 2 then '对私分成' END AS settle_method_name,
       anchorRate / 100 AS anchor_settle_rate,
       logo AS logo
FROM spider_yy_backend.guild_anchor ga
;


-- 主播直播
DROP TABLE IF EXISTS tmp.ods_anchor_live_tmp;
CREATE TABLE tmp.ods_anchor_live_tmp AS 
SELECT ai.platform_id,
	   ai.platform_name,
       ai.guild_id,
       ai.anchor_uid,
       ai.anchor_no,
       ai.anchor_nick_name,
       ai.anchor_type,
       ad.chaid AS channel_id,
       ad.duration,
       DURATION_CH(ad.duration) AS duration_sec,
       ad.pcduration,
       DURATION_CH(ad.pcduration) AS pcduration_sec,
       ad.mobduration,
       DURATION_CH(ad.mobduration) AS mobduration_sec,
       ad.dt,
       ad.timestamp
FROM tmp.ods_anchor_info_tmp ai
LEFT JOIN spider_yy_backend.anchor_duration ad ON ai.guild_id = ad.backend_account_id AND ai.anchor_uid = ad.uid AND ai.anchor_no = ad.yynum
;


-- 主播收入（佣金）
DROP TABLE IF EXISTS tmp.ods_anchor_commission_tmp;
CREATE TABLE tmp.ods_anchor_commission_tmp AS 
SELECT ai.platform_id,
       ai.platform_name,
       ai.guild_id,
       ai.anchor_uid,
       ai.anchor_no,
       ai.anchor_nick_name,
       ai.anchor_type,
       ai.settle_method_code,
       ai.settle_method_name,
       ai.anchor_settle_rate,
       ROUND(ac.usrMoney / 1000, 2) AS anchor_commission,
       ROUND(ac.owMoney / 1000, 2) AS guild_commission,
       ac.inType,
       ac.frmYY AS from_visitor_no,
       ac.frmNick AS from_visitor_name,
       ac.dtime
-- SELECT *
FROM tmp.ods_anchor_info_tmp ai
LEFT JOIN spider_yy_backend.anchor_commission ac ON ai.anchor_uid = ac.uid AND ai.anchor_no = ac.yynum
LEFT JOIN warehouse.platform pf ON ai.platform_id = pf.id
;


-- 主播收入（蓝钻）
DROP TABLE IF EXISTS tmp.ods_anchor_bluediamond_tmp;
CREATE TABLE tmp.ods_anchor_bluediamond_tmp AS 
SELECT ai.platform_id,
       ai.platform_name,
       ai.guild_id,
       ai.anchor_uid,
       ai.anchor_no,
       ai.anchor_nick_name,
       ai.anchor_type,
       ai.settle_method_code,
       ai.settle_method_name,
       ai.anchor_settle_rate,
       ab.diamond,
       ab.timestamp,
       ab.dt
FROM tmp.ods_anchor_info_tmp ai
LEFT JOIN spider_yy_backend.anchor_bluediamond ab ON ab.backend_account_id = ai.guild_id AND ab.uid = ai.anchor_uid AND ab.yynum = ai.anchor_no
;



