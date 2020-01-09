-- 主播信息
-- DROP TABLE IF EXISTS warehouse.ods_anchor_yy_info;
-- CREATE TABLE warehouse.ods_anchor_yy_info AS
DELETE FROM warehouse.ods_anchor_yy_info WHERE dt BETWEEN '{start_date}' AND '{end_date}';
INSERT INTO warehouse.ods_anchor_yy_info
SELECT 1000 AS platform_id,
       'YY' AS platform_name,
       ga.backend_account_id,
       cl.channel_num,
       ga.uid AS anchor_uid,
       ga.yynum AS anchor_no,
       ga.nick AS anchor_nick_name,
       ga.anchortype AS anchor_type,
       CASE WHEN ga.anchortype=1 THEN '普通艺人'
            WHEN ga.anchortype=2 or ga.anchortype=3 THEN '金牌艺人'
            ELSE '' END AS anchor_type_text,
       ga.roomaid AS live_room_id,
       ga.roomid,
       ga.conId AS contract_id,
       ga.signtime AS contract_signtime,
       ga.endtime AS contract_endtime,
       ga.contype AS settle_method_code,
       CASE WHEN ga.contype = 1 THEN '对公分成'
            WHEN ga.contype = 2 then '对私分成' END AS settle_method_text,
       ga.anchorRate / 100 AS anchor_settle_rate,
       ga.logo AS logo,
       ga.dt
FROM spider_yy_backend.guild_anchor ga
LEFT JOIN spider_yy_backend.channel_list cl ON ga.backend_account_id = cl.backend_account_id
WHERE ga.dt BETWEEN '{start_date}' AND '{end_date}'
;


SELECT * FROM spider_yy_backend.channel_list;

-- 主播直播
-- DROP TABLE IF EXISTS stage.union_yy_anchor_duration;
-- CREATE TABLE stage.union_yy_anchor_duration AS
DELETE FROM stage.union_yy_anchor_duration WHERE dt BETWEEN '{start_date}' AND '{end_date}';
INSERT INTO stage.union_yy_anchor_duration
SELECT *
FROM spider_yy_backend.anchor_duration
WHERE dt BETWEEN '{start_date}' AND '{end_date}'
UNION ALL
SELECT *
FROM spider_yy_backend.anchor_duration_history
WHERE dt BETWEEN '{start_date}' AND '{end_date}'
;


-- DROP TABLE IF EXISTS stage.union_yy_anchor_duration_max_time;
-- CREATE TABLE stage.union_yy_anchor_duration_max_time AS
DELETE FROM stage.union_yy_anchor_duration_max_time WHERE dt BETWEEN '{start_date}' AND '{end_date}';
INSERT INTO stage.union_yy_anchor_duration_max_time
SELECT dt, uid, MAX(timestamp) AS max_timestamp
FROM stage.union_yy_anchor_duration
WHERE dt BETWEEN '{start_date}' AND '{end_date}'
group by dt, uid
;


-- DROP TABLE IF EXISTS stage.yy_anchor_duration_distinct;
-- CREATE TABLE stage.yy_anchor_duration_distinct AS
DELETE FROM stage.yy_anchor_duration_distinct WHERE dt BETWEEN '{start_date}' AND '{end_date}';
INSERT INTO stage.yy_anchor_duration_distinct
SELECT uad.*
FROM stage.union_yy_anchor_duration_max_time mt
LEFT JOIN stage.union_yy_anchor_duration uad ON mt.dt = uad.dt AND mt.uid = uad.uid AND mt.max_timestamp = uad.timestamp
WHERE mt.dt BETWEEN '{start_date}' AND '{end_date}'
;


-- DROP TABLE IF EXISTS warehouse.ods_anchor_yy_live;
-- CREATE TABLE warehouse.ods_anchor_yy_live AS
DELETE FROM warehouse.ods_anchor_yy_live WHERE dt BETWEEN '{start_date}' AND '{end_date}';
INSERT INTO warehouse.ods_anchor_yy_live
SELECT ai.platform_id,
	   ai.platform_name,
       ai.backend_account_id,
       ai.anchor_uid,
       ai.anchor_no,
       ai.anchor_nick_name,
       ai.anchor_type,
       ad.chaid AS channel_id,
       ad.duration,
       warehouse.DURATION_CH(ad.duration) AS duration_sec,
       ad.pcduration,
       warehouse.DURATION_CH(ad.pcduration) AS pcduration_sec,
       ad.mobduration,
       warehouse.DURATION_CH(ad.mobduration) AS mobduration_sec,
       ai.dt,
       ad.timestamp
FROM warehouse.ods_anchor_yy_info ai
LEFT JOIN stage.yy_anchor_duration_distinct ad ON ai.backend_account_id = ad.backend_account_id AND ai.anchor_uid = ad.uid AND ai.dt = ad.dt
WHERE ai.dt BETWEEN '{start_date}' AND '{end_date}'
;


-- 主播收入（佣金）
-- DROP TABLE IF EXISTS warehouse.ods_anchor_yy_commission;
-- CREATE TABLE warehouse.ods_anchor_yy_commission AS
DELETE FROM warehouse.ods_anchor_yy_commission WHERE dt BETWEEN '{start_date}' AND '{end_date}';
INSERT INTO warehouse.ods_anchor_yy_commission
SELECT ai.platform_id,
       ai.platform_name,
       ai.backend_account_id,
       ai.anchor_uid,
       ai.anchor_no,
       ai.anchor_nick_name,
       ai.anchor_type,
       ai.settle_method_code,
       ai.settle_method_text,
       ai.anchor_settle_rate,
       ac.usrMoney AS anchor_commission,
       ac.owMoney AS guild_commission,
       ac.inType,
       ac.frmYY AS from_visitor_no,
       ac.frmNick AS from_visitor_name,
       ac.dtime,
       ai.dt
FROM warehouse.ods_anchor_yy_info ai
LEFT JOIN spider_yy_backend.anchor_commission ac ON ai.backend_account_id = ac.backend_account_id AND ai.anchor_uid = ac.uid AND ai.dt = DATE(ac.dtime)
LEFT JOIN warehouse.platform pf ON ai.platform_id = pf.id
WHERE ai.dt BETWEEN '{start_date}' AND '{end_date}'
;


-- 按日汇总主播佣金收入及工会分成（佣金）
DROP TABLE IF EXISTS warehouse.ods_anchor_yy_commission_daily;
CREATE TABLE warehouse.ods_anchor_yy_commission_daily AS
-- DELETE FROM warehouse.ods_anchor_yy_commission_daily WHERE dt BETWEEN '{start_date}' AND '{end_date}';
-- INSERT INTO warehouse.ods_anchor_yy_commission_daily
SELECT ac.platform_id,
       ac.backend_account_id,
       ac.anchor_uid,
       ac.anchor_no,
       ac.dt,
       ac.anchor_commission,
       ROUND(SUM(ac.anchor_commission / 1000), 2) AS anchor_commission_rmb,
       ac.guild_commission,
       ROUND(SUM(ac.guild_commission / 1000), 2) AS guild_commission_rmb
FROM warehouse.ods_anchor_yy_commission ac
WHERE ac.dt BETWEEN '{start_date}' AND '{end_date}'
GROUP BY ac.platform_id,
         ac.backend_account_id,
         ac.anchor_uid,
         ac.anchor_no,
         ac.dt
;


-- 主播收入（蓝钻）
-- DROP TABLE IF EXISTS warehouse.ods_anchor_yy_virtual_coin;
-- CREATE TABLE warehouse.ods_anchor_yy_virtual_coin AS
DELETE FROM warehouse.ods_anchor_yy_virtual_coin WHERE dt BETWEEN '{start_date}' AND '{end_date}';
INSERT INTO warehouse.ods_anchor_yy_virtual_coin
SELECT ai.platform_id,
       ai.platform_name,
       ai.backend_account_id,
       ai.anchor_uid,
       ai.anchor_no,
       ai.anchor_nick_name,
       ai.anchor_type,
       ai.settle_method_code,
       ai.settle_method_text,
       ai.anchor_settle_rate,
       ab.diamond AS virtual_coin,
       ab.timestamp,
       ai.dt
FROM warehouse.ods_anchor_yy_info ai
LEFT JOIN spider_yy_backend.anchor_bluediamond ab ON ab.backend_account_id = ai.backend_account_id AND ab.uid = ai.anchor_uid AND ai.dt = ab.dt
WHERE ai.dt BETWEEN '{start_date}' AND '{end_date}'
;


-- Merge
-- DROP TABLE IF EXISTS warehouse.ods_anchor_yy_live_detail_daily;
-- CREATE TABLE warehouse.ods_anchor_yy_live_detail_daily AS
DELETE FROM warehouse.ods_anchor_yy_live_detail_daily WHERE dt BETWEEN '{start_date}' AND '{end_date}';
INSERT INTO warehouse.ods_anchor_yy_live_detail_daily
SELECT ai.platform_id,
       ai.platform_name,
       ai.backend_account_id,
       ai.channel_num,
       ai.anchor_uid,
       ai.anchor_no,
       ai.anchor_nick_name,
       ai.anchor_type,
       ai.anchor_type_text,
       ai.live_room_id,
       al.channel_id,
       al.duration_sec AS duration,
       al.mobduration_sec AS mob_duration,
       al.pcduration_sec AS pc_duration,
       CASE WHEN al.duration_sec > 0 THEN 1 ELSE 0 END AS live_status,
       av.virtual_coin,
       ac.anchor_commission,
       ac.anchor_commission_rmb,
       ac.guild_commission,
       ac.guild_commission_rmb,
       pf.vir_coin_name,
       pf.vir_coin_rate,
       pf.include_pf_amt,
       pf.pf_amt_rate,
       ai.contract_id,
       ai.contract_signtime,
       ai.contract_endtime,
       ai.settle_method_code,
       ai.settle_method_text,
       ai.anchor_settle_rate,
       ai.logo,
       ai.dt
FROM warehouse.ods_anchor_yy_info ai
LEFT JOIN warehouse.ods_anchor_yy_live al ON ai.backend_account_id = al.backend_account_id AND ai.anchor_uid = al.anchor_uid AND ai.dt = al.dt
LEFT JOIN warehouse.ods_anchor_yy_virtual_coin av ON ai.backend_account_id = av.backend_account_id AND ai.anchor_uid = av.anchor_uid AND ai.dt = av.dt
LEFT JOIN warehouse.ods_anchor_yy_commission_daily ac ON ai.backend_account_id = ac.backend_account_id AND ai.anchor_uid = ac.anchor_uid AND ai.dt = ac.dt
LEFT JOIN warehouse.platform pf ON ai.platform_id = pf.id
WHERE ai.dt BETWEEN '{start_date}' AND '{end_date}'
;



-- =====================================================================
-- 公会收支明细
-- 公会蓝钻
-- DROP TABLE IF EXISTS warehouse.ods_guild_yy_virtual_coin_an_mon;
-- CREATE TABLE warehouse.ods_guild_yy_virtual_coin_an_mon AS
DELETE FROM warehouse.ods_guild_yy_virtual_coin_an_mon WHERE dt BETWEEN '{start_date}' AND '{end_date}';
INSERT INTO warehouse.ods_guild_yy_virtual_coin_an_mon
SELECT 1000 AS platform_id,
       'YY' AS platform_name,
       cl.backend_account_id,
       cl.channel_num,
       gb.yynum AS anchor_no,
       gb.nick AS anchor_nick_name,
       gb.totalDiamond AS total_vir_coin,
       gb.settType AS settle_method_code,
       CASE WHEN gb.settType = 1 THEN '对公分成'
            WHEN gb.settType = 2 then '对私分成' END AS settle_method_text,
       gb.totalDiamond AS an_total_vir_coin,
       gb.money AS g_settle_vir_coin,
       gb.year AS rpt_year,
       gb.month AS rpt_month,
       DATE(gb.payTime) AS dt
FROM spider_yy_backend.channel_list cl
LEFT JOIN spider_yy_backend.guild_bluediamond gb ON cl.backend_account_id = gb.backend_account_id
WHERE DATE(gb.payTime) BETWEEN '{start_date}' AND '{end_date}'
;


-- 公会佣金收入
-- DROP TABLE IF EXISTS warehouse.ods_guild_yy_commission_an_mon;
-- CREATE TABLE warehouse.ods_guild_yy_commission_an_mon AS
DELETE FROM warehouse.ods_guild_yy_commission_an_mon WHERE dt BETWEEN '{start_date}' AND '{end_date}';
INSERT INTO warehouse.ods_guild_yy_commission_an_mon
SELECT 1000 AS platform_id,
       'YY' AS platform_name,
       cl.backend_account_id,
       cl.channel_num,
       gc.yynum AS anchor_no,
       gc.nick AS anchor_nick_name,
       gc.owMoney AS g_settle_commission,
       gc.year AS rpt_year,
       gc.month AS rpt_month,
       DATE(gc.time) AS dt
FROM spider_yy_backend.channel_list cl
LEFT JOIN spider_yy_backend.guild_commission gc ON cl.backend_account_id = gc.backend_account_id
WHERE DATE(gc.time) BETWEEN '{start_date}' AND '{end_date}'
;

