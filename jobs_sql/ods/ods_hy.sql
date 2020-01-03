-- 主播信息
DROP TABLE IF EXISTS warehouse.ods_anchor_hy_info;
CREATE TABLE warehouse.ods_anchor_hy_info AS
SELECT 1002 AS platform_id,
       '虎牙' AS platform_name,
       cl.account_id AS backend_account_id,
       cl.channel_number AS guild_id,
       cl.name AS guild_name,
       pd.channel_id,
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
	   (100 - al.percent) / 100 AS anchor_settle_rate,
       al.avatar AS logo,
       al.dt,
       al.timestamp
FROM spider_huya_backend.anchor_list al
LEFT JOIN spider_huya_backend.profile_detail pd ON al.channel_id = pd.channel_id AND al.uid = pd.l_uid AND al.dt = pd.dt
LEFT JOIN spider_huya_backend.channel_list cl ON al.channel_id = cl.channel_id AND al.dt = cl.dt
WHERE ai.dt BETWEEN '{start_date}' AND '{end_date}'
;


-- 主播直播和直播收入
DROP TABLE IF EXISTS warehouse.ods_anchor_hy_live_amt;
CREATE TABLE warehouse.ods_anchor_hy_live_amt AS
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
LEFT JOIN spider_huya_backend.profile_daily_live_detail pdl ON ai.channel_id = pdl.channel_id AND ai.anchor_uid = pdl.uid AND ai.dt = pdl.date
WHERE ai.dt BETWEEN '{start_date}' AND '{end_date}'
;

