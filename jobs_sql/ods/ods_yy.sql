-- 主播信息
-- DROP TABLE IF EXISTS warehouse.ods_yy_anchor_info;
-- CREATE TABLE warehouse.ods_yy_anchor_info AS
DELETE FROM warehouse.ods_yy_anchor_info WHERE dt BETWEEN '{start_date}' AND '{end_date}';
INSERT INTO warehouse.ods_yy_anchor_info
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
       '' AS comment,
       ga.dt
FROM spider_yy_backend.guild_anchor ga
LEFT JOIN spider_yy_backend.channel_list cl ON ga.backend_account_id = cl.backend_account_id
WHERE ga.dt BETWEEN '{start_date}' AND '{end_date}'
;

-- 补充spider_yy_backend.guild_anchor中缺失主播
INSERT IGNORE INTO warehouse.ods_yy_anchor_info (platform_id, platform_name, backend_account_id, anchor_uid, anchor_no, anchor_nick_name, comment, dt)
SELECT 1000 AS platform_id,
       'YY' AS platform_name,
       backend_account_id,
       uid,
       yynum,
       nick,
       '修复主播缺失插入' AS comment,
       DATE(dtime) AS dt
FROM spider_yy_backend.anchor_commission
WHERE DATE(dtime) BETWEEN '{start_date}' AND '{end_date}'
UNION
SELECT 1000 AS platform_id,
       'YY' AS platform_name,
       backend_account_id,
       uid,
       yynum,
       nick,
       '修复主播缺失插入' AS comment,
       dt
FROM spider_yy_backend.anchor_duration
WHERE dt BETWEEN '{start_date}' AND '{end_date}'
UNION
SELECT 1000 AS platform_id,
       'YY' AS platform_name,
       backend_account_id,
       uid,
       yynum,
       nick,
       '修复主播缺失插入' AS comment,
       dt
FROM spider_yy_backend.anchor_duration_history
WHERE dt BETWEEN '{start_date}' AND '{end_date}'
;


INSERT IGNORE INTO warehouse.ods_yy_anchor_info (platform_id, platform_name, backend_account_id, anchor_uid, anchor_no, anchor_nick_name, comment, dt)
SELECT 1000 AS platform_id,
       'YY' AS platform_name,
       backend_account_id,
       uid,
       yynum,
       '' AS nick,
       '修复主播缺失插入' AS comment,
       dt
FROM spider_yy_backend.anchor_bluediamond
WHERE dt BETWEEN '{start_date}' AND '{end_date}'
;


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
SELECT backend_account_id, dt, uid, MAX(timestamp) AS max_timestamp
FROM stage.union_yy_anchor_duration
WHERE dt BETWEEN '{start_date}' AND '{end_date}'
group by backend_account_id, dt, uid
;


-- DROP TABLE IF EXISTS stage.distinct_yy_anchor_duration;
-- CREATE TABLE stage.distinct_yy_anchor_duration AS
DELETE FROM stage.distinct_yy_anchor_duration WHERE dt BETWEEN '{start_date}' AND '{end_date}';
INSERT INTO stage.distinct_yy_anchor_duration
SELECT uad.*
FROM stage.union_yy_anchor_duration_max_time mt
LEFT JOIN stage.union_yy_anchor_duration uad ON mt.backend_account_id = uad.backend_account_id AND mt.dt = uad.dt AND mt.uid = uad.uid AND mt.max_timestamp = uad.timestamp
WHERE mt.dt BETWEEN '{start_date}' AND '{end_date}'
;


-- DROP TABLE IF EXISTS warehouse.ods_yy_anchor_live;
-- CREATE TABLE warehouse.ods_yy_anchor_live AS
DELETE FROM warehouse.ods_yy_anchor_live WHERE dt BETWEEN '{start_date}' AND '{end_date}';
INSERT INTO warehouse.ods_yy_anchor_live
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
FROM warehouse.ods_yy_anchor_info ai
LEFT JOIN stage.distinct_yy_anchor_duration ad ON ai.backend_account_id = ad.backend_account_id AND ai.anchor_uid = ad.uid AND ai.dt = ad.dt
WHERE ai.dt BETWEEN '{start_date}' AND '{end_date}'
;


-- 主播收入（佣金）
-- DROP TABLE IF EXISTS warehouse.ods_yy_anchor_commission;
-- CREATE TABLE warehouse.ods_yy_anchor_commission AS
DELETE FROM warehouse.ods_yy_anchor_commission WHERE dt BETWEEN '{start_date}' AND '{end_date}';
INSERT INTO warehouse.ods_yy_anchor_commission
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
FROM warehouse.ods_yy_anchor_info ai
LEFT JOIN spider_yy_backend.anchor_commission ac ON ai.backend_account_id = ac.backend_account_id AND ai.anchor_uid = ac.uid AND ai.dt = DATE(ac.dtime)
LEFT JOIN warehouse.platform pf ON ai.platform_id = pf.id
WHERE ai.dt BETWEEN '{start_date}' AND '{end_date}'
;


-- 按日汇总主播佣金收入及工会分成（佣金）
-- DROP TABLE IF EXISTS warehouse.ods_day_yy_anchor_commission;
-- CREATE TABLE warehouse.ods_day_yy_anchor_commission AS
DELETE FROM warehouse.ods_day_yy_anchor_commission WHERE dt BETWEEN '{start_date}' AND '{end_date}';
INSERT INTO warehouse.ods_day_yy_anchor_commission
SELECT ac.platform_id,
       ac.backend_account_id,
       ac.anchor_uid,
       ac.anchor_no,
       ac.dt,
       SUM(ac.anchor_commission) AS anchor_commission,
       SUM(ac.guild_commission) AS guild_commission
FROM warehouse.ods_yy_anchor_commission ac
WHERE ac.dt BETWEEN '{start_date}' AND '{end_date}'
GROUP BY ac.platform_id,
         ac.backend_account_id,
         ac.anchor_uid,
         ac.anchor_no,
         ac.dt
;


-- 主播收入（蓝钻）
-- DROP TABLE IF EXISTS warehouse.ods_yy_anchor_virtual_coin;
-- CREATE TABLE warehouse.ods_yy_anchor_virtual_coin AS
DELETE FROM warehouse.ods_yy_anchor_virtual_coin WHERE dt BETWEEN '{start_date}' AND '{end_date}';
INSERT INTO warehouse.ods_yy_anchor_virtual_coin
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
FROM warehouse.ods_yy_anchor_info ai
LEFT JOIN spider_yy_backend.anchor_bluediamond ab ON ab.backend_account_id = ai.backend_account_id AND ab.uid = ai.anchor_uid AND ai.dt = ab.dt
WHERE ai.dt BETWEEN '{start_date}' AND '{end_date}'
;


-- Merge
-- DROP TABLE IF EXISTS warehouse.ods_yy_anchor_live_detail;
-- CREATE TABLE warehouse.ods_yy_anchor_live_detail AS
DELETE FROM warehouse.ods_yy_anchor_live_detail WHERE dt BETWEEN '{start_date}' AND '{end_date}';
INSERT INTO warehouse.ods_yy_anchor_live_detail
SELECT ai.platform_id,
       ai.platform_name,
       ai.backend_account_id,
       cl.channel_num,
       ai.anchor_uid,
       ai.anchor_no,
       ai.anchor_nick_name,
       ai.anchor_type,
       ai.anchor_type_text,
       ai.live_room_id,
       al.channel_id,
       CASE WHEN al.duration_sec >= 0 THEN al.duration_sec ELSE 0 END AS duration,
       CASE WHEN al.mobduration_sec >= 0 THEN al.mobduration_sec ELSE 0 END AS mob_duration,
       CASE WHEN al.pcduration_sec >= 0 THEN al.pcduration_sec ELSE 0 END AS pc_duration,
       CASE WHEN al.duration_sec > 0 THEN 1 ELSE 0 END AS live_status,
       CASE WHEN av.virtual_coin >= 0 THEN av.virtual_coin ELSE 0 END AS virtual_coin,
       CASE WHEN ac.anchor_commission >= 0 THEN ac.anchor_commission ELSE 0 END AS anchor_commission,
       CASE WHEN ac.guild_commission >= 0 THEN ac.guild_commission ELSE 0 END AS guild_commission,
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
FROM warehouse.ods_yy_anchor_info ai
LEFT JOIN warehouse.ods_yy_anchor_live al ON ai.backend_account_id = al.backend_account_id AND ai.anchor_uid = al.anchor_uid AND ai.dt = al.dt
LEFT JOIN warehouse.ods_yy_anchor_virtual_coin av ON ai.backend_account_id = av.backend_account_id AND ai.anchor_uid = av.anchor_uid AND ai.dt = av.dt
LEFT JOIN warehouse.ods_day_yy_anchor_commission ac ON ai.backend_account_id = ac.backend_account_id AND ai.anchor_uid = ac.anchor_uid AND ai.dt = ac.dt
LEFT JOIN warehouse.platform pf ON ai.platform_id = pf.id
LEFT JOIN spider_yy_backend.channel_list cl ON ai.backend_account_id = cl.backend_account_id
WHERE ai.dt BETWEEN '{start_date}' AND '{end_date}'
;



-- =====================================================================
-- 公会收支明细
-- 公会每月获得各主播分成蓝钻
-- DROP TABLE IF EXISTS warehouse.ods_yy_guild_virtual_coin_detail;
-- CREATE TABLE warehouse.ods_yy_guild_virtual_coin_detail AS
DELETE FROM warehouse.ods_yy_guild_virtual_coin_detail WHERE dt BETWEEN '{start_date}' AND '{end_date}';
INSERT INTO warehouse.ods_yy_guild_virtual_coin_detail
SELECT 1000 AS platform_id,
       'YY' AS platform_name,
       cl.backend_account_id,
       cl.channel_num,
       gb.yynum AS anchor_no,
       gb.nick AS anchor_nick_name,
       gb.totalDiamond AS anchor_virtual_coin,
       gb.settType AS settle_method_code,
       CASE WHEN gb.settType = 1 THEN '对公分成'
            WHEN gb.settType = 2 then '对私分成' END AS settle_method_text,
       gb.money AS guild_vir_coin,
       gb.payTime AS pay_time,
       CONCAT(gb.year, '-', gb.month, '-01') AS dt
FROM spider_yy_backend.channel_list cl
LEFT JOIN spider_yy_backend.guild_bluediamond gb ON cl.backend_account_id = gb.backend_account_id
WHERE DATE(gb.payTime) BETWEEN '{start_date}' AND '{end_date}'
;


-- 公会每月获得各主播分成佣金
-- DROP TABLE IF EXISTS warehouse.ods_yy_guild_commission_detail;
-- CREATE TABLE warehouse.ods_yy_guild_commission_detail AS
DELETE FROM warehouse.ods_yy_guild_commission_detail WHERE dt BETWEEN '{start_date}' AND '{end_date}';
INSERT INTO warehouse.ods_yy_guild_commission_detail
SELECT 1000 AS platform_id,
       'YY' AS platform_name,
       cl.backend_account_id,
       cl.channel_num,
       gc.yynum AS anchor_no,
       gc.nick AS anchor_nick_name,
       gc.owMoney AS guild_commission,
       gc.time AS get_commission_time,
       CONCAT(gc.year, '-', gc.month, '-01') AS dt
FROM spider_yy_backend.channel_list cl
LEFT JOIN spider_yy_backend.guild_commission gc ON cl.backend_account_id = gc.backend_account_id
WHERE DATE(gc.time) BETWEEN '{start_date}' AND '{end_date}'
;


