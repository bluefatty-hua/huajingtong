-- 主播信息
DROP TABLE IF EXISTS warehouse.ods_anchor_hy_info;
CREATE TABLE warehouse.ods_anchor_hy_info AS
SELECT 1002 AS platform_id,
       '虎牙' AS platform_name,
       cl.account_id AS backend_account_id,
       cl.channel_number AS guild_id,
       cl.name AS guild_name,
       ad.channel_id,
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
LEFT JOIN spider_huya_backend.anchor_detail ad ON al.channel_id = ad.channel_id AND al.uid = ad.l_uid AND al.dt = ad.dt
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
       alg.game_id AS live_game_id,
       alg.game_name AS live_game_name,
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
LEFT JOIN spider_huya_backend.anchor_live_detail_game_list_day alg ON ai.channel_id = al.channel_id AND ai.anchor_uid = alg.uid AND ai.dt = alg.date
;


-- Merge
DROP TABLE IF EXISTS warehouse.ods_yy_anchor_live_detail_daily;
CREATE TABLE warehouse.ods_yy_anchor_live_detail_daily AS
SELECT ai.platform_id,
       ai.platform_name,
       ai.backend_account_id,
       ai.guild_id,
       ai.guild_name,
       ai.channel_id,
       ai.anchor_uid,
       ai.anchor_no,
       ai.anchor_nick_name,
       al.live_game_id,
       al.live_game_name,
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
       ai.contract_type,
       ai.contract_type_text,
       ai.contract_signtime,
       ai.contract_endtime,
       ai.logo,
       ai.dt
FROM warehouse.ods_anchor_hy_info ai
LEFT JOIN warehouse.ods_anchor_hy_live_amt al ON ai.backend_account_id = al.backend_account_id AND ai.anchor_uid = al.anchor_uid AND ai.dt = al.dt
LEFT JOIN warehouse.platform pf ON ai.platform_id = pf.id
;

