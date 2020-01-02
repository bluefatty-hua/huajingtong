DROP TABLE IF EXISTS stage.hy_anchor_list_recent_day;
CREATE TABLE stage.hy_anchor_list_recent_day AS
SELECT al.*
FROM (SELECT channel_id,
			 uid, 
             yy_id, 
			 MAX(dt) AS recent_date 
	  FROM spider_huya_backend.anchor_list
	  GROUP BY channel_id,
               uid, 
               yy_id) alrec
LEFT JOIN spider_huya_backend.anchor_list al ON alrec.channel_id = al.channel_id AND alrec.uid = al.uid AND alrec.yy_id = al.yy_id AND alrec.recent_date = al.dt
;


DROP TABLE IF EXISTS stage.hy_profile_detail_rencent_day;
CREATE TABLE stage.hy_profile_detail_rencent_day AS 
SELECT pd.*
FROM (SELECT channel_id,
	         l_uid, l_yy,
             MAX(dt) AS recent_date 
      FROM spider_huya_backend.profile_detail
      GROUP BY channel_id,
               l_uid,
               l_yy) pdrec
LEFT JOIN spider_huya_backend.profile_detail pd ON pdrec.channel_id  = pd.channel_id AND pdrec.l_uid = pd.l_uid AND pdrec.l_yy = pd.l_yy AND pdrec.recent_date = pd.dt
;


DROP TABLE IF EXISTS stage.hy_channel_list_recent_day;
CREATE TABLE stage.hy_channel_list_recent_day AS 
SELECT cl.*
FROM (SELECT account_id,
			 channel_id,
			 channel_number,
             MAX(dt) AS recent_date
	  FROM spider_huya_backend.channel_list
	  GROUP BY account_id, 
			   channel_id, 
               channel_number) clrec
LEFT JOIN spider_huya_backend.channel_list cl ON clrec.account_id = cl.account_id AND clrec.channel_id = cl.channel_id AND clrec.channel_number = cl.channel_number AND clrec.recent_date = cl.dt
;

-- 主播信息
DROP TABLE IF EXISTS warehouse.ods_anchor_hy_info;
CREATE TABLE warehouse.ods_anchor_hy_info AS
SELECT 1002 AS platform_id,
       '虎牙' AS platform_name,
       cl.account_id AS backend_account_id,
       cl.channel_number AS guild_id,
       cl.name AS guild_name,
       al.channel_id,
       al.uid AS anchor_uid,
       al.yy_id AS anchor_no,
       al.nick AS anchor_nick_name,
       al.isOfficialSign AS  contract_type,  -- 是否官签
       CASE WHEN al.isOfficialSign = 0 THEN '非官签'
            WHEN al.isOfficialSign = 1 THEN '官签'
	   ELSE '' END AS contract_type_text,
       DATE_FORMAT(FROM_UNIXTIME(al.sign_time), '%Y-%m-%d %T') AS contract_signtime,
       DATE_ADD(DATE_FORMAT(FROM_UNIXTIME(sign_time), '%Y-%m-%d %T'), INTERVAL months MONTH) AS contract_endtime,
       1 AS settle_method_code,
       '对私分成' AS settle_method_text,
	   al.percent / 100 AS anchor_settle_rate,
       al.avatar AS logo,
       al.dt,
       al.timestamp
-- SELECT *
FROM stage.hy_anchor_list_recent_day al
LEFT JOIN stage.hy_profile_detail_rencent_day pd ON al.channel_id = pd.channel_id AND al.uid = pd.l_uid
LEFT JOIN stage.hy_channel_list_recent_day cl ON al.channel_id = cl.channel_id
-- WHERE al.yy_id = 222433
;


-- 主播直播和直播收入
SELECT ai.platform_id,
       ai.platform_name,
       ai.backend_account_id,
       ai.guild_id,
       ai.guild_name,
       ai.channel_id,
       ai.anchor_uid,
       ai.anchor_no,
       ai.anchor_nick_name,
       pdl.live_time AS duration,
       CASE WHEN pdl.live_time > 0 THEN 1 ELSE 0 END AS live_status,
       pdl.income AS amt,
       ai.settle_method_code,
       ai.settle_method_text,
       ai.anchor_settle_rate,
       pdl.peak_pcu,
	   pdl.date AS dt,
       pdl.timestamp
FROM warehouse.ods_anchor_hy_info ai
LEFT JOIN spider_huya_backend.profile_daily_live_detail pdl ON ai.channel_id = pdl.channel_id AND ai.anchor_uid = pdl.uid
WHERE pld.date BETWEEN '{start_date}' AND '{end_date}'
;

