-- table list
-- warehouse.ods_yy_day_anchor_info
-- warehouse.ods_yy_anchor_live_commission
-- warehouse.ods_yy_guild_live_bluediamond
-- warehouse.ods_yy_guild_live_commission
-- warehouse.dw_yy_day_anchor_live_duration
-- warehouse.dw_yy_day_anchor_live_commission
-- warehouse.dw_yy_day_anchor_live_bluediamond
-- warehouse.dw_yy_day_anchor_live


-- 主播信息
-- DROP TABLE IF EXISTS warehouse.ods_yy_day_anchor_info;
-- CREATE TABLE warehouse.ods_yy_day_anchor_info AS
DELETE
FROM warehouse.ods_yy_day_anchor_info
WHERE dt BETWEEN '{start_date}' AND '{end_date}';
INSERT INTO warehouse.ods_yy_day_anchor_info
SELECT ga.dt,
       1000                                    AS platform_id,
       'YY'                                    AS platform_name,
       ga.backend_account_id,
       cl.channel_num,
       ga.uid                                  AS anchor_uid,
       ga.yynum                                AS anchor_no,
       ga.nick                                 AS anchor_nick_name,
       ga.anchortype                           AS anchor_type,
       CASE
           WHEN ga.anchortype = 1 THEN '普通艺人'
           WHEN ga.anchortype = 2 or ga.anchortype = 3 THEN '金牌艺人'
           ELSE '' END                         AS anchor_type_text,
       ga.roomaid                              AS live_room_id,
       ga.roomid,
       ga.conId                                AS contract_id,
       ga.signtime                             AS contract_signtime,
       ga.endtime                              AS contract_endtime,
       ga.contype                              AS settle_method_code,
       CASE
           WHEN ga.contype = 1 THEN '对公分成'
           WHEN ga.contype = 2 then '对私分成' END AS settle_method_text,
       ga.anchorRate / 100                     AS anchor_settle_rate,
       ga.logo                                 AS logo,
       ''                                      AS comment
FROM spider_yy_backend.guild_anchor ga
         LEFT JOIN spider_yy_backend.channel_list cl ON ga.backend_account_id = cl.backend_account_id
WHERE ga.dt BETWEEN '{start_date}' AND '{end_date}'
;


-- 补充spider_yy_backend.guild_anchor中缺失主播
INSERT IGNORE INTO warehouse.ods_yy_day_anchor_info (platform_id, platform_name, backend_account_id, channel_num,
                                                     anchor_uid,
                                                     anchor_no, anchor_nick_name, comment, dt)
SELECT 1000                     AS platform_id,
       'YY'                     AS platform_name,
       ac.backend_account_id,
       cl.channel_num,
       ac.uid,
       ac.yynum,
       ac.nick,
       'from anchor_commission' AS comment,
       DATE(dtime)              AS dt
FROM spider_yy_backend.anchor_commission ac
         LEFT JOIN spider_yy_backend.channel_list cl ON ac.backend_account_id = cl.backend_account_id
WHERE DATE(dtime) BETWEEN '{start_date}' AND '{end_date}'
UNION
SELECT 1000                   AS platform_id,
       'YY'                   AS platform_name,
       ad.backend_account_id,
       cl.channel_num,
       ad.uid,
       ad.yynum,
       nick,
       'from anchor_duration' AS comment,
       ad.dt
FROM spider_yy_backend.anchor_duration ad
         LEFT JOIN spider_yy_backend.channel_list cl ON ad.backend_account_id = cl.backend_account_id
WHERE dt BETWEEN '{start_date}' AND '{end_date}'
UNION
SELECT 1000                           AS platform_id,
       'YY'                           AS platform_name,
       ad.backend_account_id,
       cl.channel_num,
       ad.uid,
       ad.yynum,
       nick,
       'from anchor_duration_history' AS comment,
       ad.dt
FROM spider_yy_backend.anchor_duration_history ad
         LEFT JOIN spider_yy_backend.channel_list cl ON ad.backend_account_id = cl.backend_account_id
WHERE dt BETWEEN '{start_date}' AND '{end_date}'
;


INSERT IGNORE INTO warehouse.ods_yy_day_anchor_info (platform_id, platform_name, backend_account_id, channel_num,
                                                     anchor_uid, anchor_no, anchor_nick_name, comment, dt)
SELECT 1000                      AS platform_id,
       'YY'                      AS platform_name,
       ab.backend_account_id,
       cl.channel_num,
       ab.uid,
       ab.yynum,
       ''                        AS nick,
       'from anchor_bluediamond' AS comment,
       ab.dt
FROM spider_yy_backend.anchor_bluediamond ab
         LEFT JOIN spider_yy_backend.channel_list cl ON ab.backend_account_id = cl.backend_account_id
WHERE dt BETWEEN '{start_date}' AND '{end_date}'
;


-- 主播直播
-- DROP TABLE IF EXISTS stage.union_yy_anchor_duration;
-- CREATE TABLE stage.union_yy_anchor_duration AS
DELETE
FROM stage.union_yy_anchor_duration
WHERE dt BETWEEN '{start_date}' AND '{end_date}';
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
DELETE
FROM stage.union_yy_anchor_duration_max_time
WHERE dt BETWEEN '{start_date}' AND '{end_date}';
INSERT INTO stage.union_yy_anchor_duration_max_time
SELECT backend_account_id, dt, uid, MAX(timestamp) AS max_timestamp
FROM stage.union_yy_anchor_duration
WHERE dt BETWEEN '{start_date}' AND '{end_date}'
group by backend_account_id, dt, uid
;


-- DROP TABLE IF EXISTS stage.distinct_yy_anchor_duration;
-- CREATE TABLE stage.distinct_yy_anchor_duration AS
DELETE
FROM stage.distinct_yy_anchor_duration
WHERE dt BETWEEN '{start_date}' AND '{end_date}';
INSERT INTO stage.distinct_yy_anchor_duration
SELECT uad.*
FROM stage.union_yy_anchor_duration_max_time mt
         LEFT JOIN stage.union_yy_anchor_duration uad
                   ON mt.backend_account_id = uad.backend_account_id AND mt.dt = uad.dt AND mt.uid = uad.uid AND
                      mt.max_timestamp = uad.timestamp
WHERE mt.dt BETWEEN '{start_date}' AND '{end_date}'
;

-- DROP TABLE IF EXISTS warehouse.dw_yy_day_anchor_live_duration;
-- CREATE TABLE warehouse.dw_yy_day_anchor_live_duration AS
DELETE
FROM warehouse.dw_yy_day_anchor_live_duration
WHERE dt BETWEEN '{start_date}' AND '{end_date}';
INSERT INTO warehouse.dw_yy_day_anchor_live_duration
SELECT ai.dt,
       ai.platform_id,
       ai.platform_name,
       ai.backend_account_id,
       ai.anchor_uid,
       ai.anchor_no,
       ai.anchor_nick_name,
       ai.anchor_type,
       ad.chaid                              AS channel_id,
       ad.duration,
       warehouse.DURATION_CH(ad.duration)    AS duration_sec,
       ad.pcduration,
       warehouse.DURATION_CH(ad.pcduration)  AS pcduration_sec,
       ad.mobduration,
       warehouse.DURATION_CH(ad.mobduration) AS mobduration_sec,
       ad.timestamp
FROM warehouse.ods_yy_day_anchor_info ai
         LEFT JOIN stage.distinct_yy_anchor_duration ad
                   ON ai.backend_account_id = ad.backend_account_id AND ai.anchor_uid = ad.uid AND ai.dt = ad.dt
WHERE ai.dt BETWEEN '{start_date}' AND '{end_date}'
;


-- 主播收入（佣金）
-- DROP TABLE IF EXISTS warehouse.ods_yy_anchor_live_commission;
-- CREATE TABLE warehouse.ods_yy_anchor_live_commission AS
DELETE
FROM warehouse.ods_yy_anchor_live_commission
WHERE dt BETWEEN '{start_date}' AND '2019-12-31';
INSERT INTO warehouse.ods_yy_anchor_live_commission
SELECT ai.dt,
       ai.platform_id,
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
       ac.owMoney  AS guild_commission,
       ac.inType   as in_type,
       ac.frmYY    AS from_visitor_no,
       ac.frmNick  AS from_visitor_name,
       ac.dtime
FROM warehouse.ods_yy_day_anchor_info ai
         LEFT JOIN spider_yy_backend.anchor_commission ac
                   ON ai.backend_account_id = ac.backend_account_id AND ai.anchor_uid = ac.uid AND
                      ai.dt = DATE(ac.dtime)
WHERE ai.dt BETWEEN '{start_date}' AND '{end_date}'
;


-- 按日汇总主播佣金收入及工会分成（佣金）
-- DROP TABLE IF EXISTS warehouse.dw_yy_day_anchor_live_commission;
-- CREATE TABLE warehouse.dw_yy_day_anchor_live_commission AS
DELETE
FROM warehouse.dw_yy_day_anchor_live_commission
WHERE dt BETWEEN '{start_date}' AND '{end_date}';
INSERT INTO warehouse.dw_yy_day_anchor_live_commission
SELECT dt,
       platform_id,
       backend_account_id,
       anchor_uid,
       max(platform_name)                   as platform_name,
       max(anchor_nick_name)                as anchor_nick_name,
       max(anchor_type)                     as anchor_type,
       max(settle_method_code)              as settle_method_code,
       max(settle_method_text)              as settle_method_text,
       max(anchor_settle_rate)              as anchor_settle_rate,
       SUM(ifnull(ac.anchor_commission, 0)) AS anchor_commission,
       SUM(ifnull(ac.guild_commission, 0))  AS guild_commission
FROM warehouse.ods_yy_anchor_live_commission ac
WHERE ac.dt BETWEEN '{start_date}' AND '{end_date}'
GROUP BY ac.platform_id,
         ac.backend_account_id,
         ac.anchor_uid,
         ac.dt
;


-- 主播收入（蓝钻）
-- DROP TABLE IF EXISTS warehouse.dw_yy_day_anchor_live_bluediamond;
-- CREATE TABLE warehouse.dw_yy_day_anchor_live_bluediamond AS
DELETE
FROM warehouse.dw_yy_day_anchor_live_bluediamond
WHERE dt BETWEEN '{start_date}' AND '{end_date}';
INSERT INTO warehouse.dw_yy_day_anchor_live_bluediamond
SELECT ai.dt,
       ai.platform_id,
       ai.platform_name,
       ai.backend_account_id,
       ai.anchor_uid,
       ai.anchor_no,
       ai.anchor_nick_name,
       ai.anchor_type,
       ai.settle_method_code,
       ai.settle_method_text,
       ai.anchor_settle_rate,
       ab.diamond AS bluediamond,
       ab.timestamp
FROM warehouse.ods_yy_day_anchor_info ai
         LEFT JOIN spider_yy_backend.anchor_bluediamond ab
                   ON ab.backend_account_id = ai.backend_account_id AND ab.uid = ai.anchor_uid AND ai.dt = ab.dt
WHERE ai.dt BETWEEN '{start_date}' AND '{end_date}'
  and diamond > 0
;


-- DROP TABLE IF EXISTS warehouse.dw_yy_day_anchor_live;
-- CREATE TABLE warehouse.dw_yy_day_anchor_live AS
DELETE
FROM warehouse.dw_yy_day_anchor_live
WHERE dt BETWEEN '{start_date}' AND '{end_date}';
INSERT INTO warehouse.dw_yy_day_anchor_live
SELECT ai.dt,
       ai.platform_id,
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
       CASE WHEN al.duration_sec >= 0 THEN al.duration_sec ELSE 0 END           AS duration,
       CASE WHEN al.mobduration_sec >= 0 THEN al.mobduration_sec ELSE 0 END     AS mob_duration,
       CASE WHEN al.pcduration_sec >= 0 THEN al.pcduration_sec ELSE 0 END       AS pc_duration,
       CASE WHEN al.duration_sec > 0 THEN 1 ELSE 0 END                          AS live_status,
       CASE WHEN ab.bluediamond >= 0 THEN ab.bluediamond ELSE 0 END             AS bluediamond,
       CASE WHEN ac.anchor_commission >= 0 THEN ac.anchor_commission ELSE 0 END AS anchor_commission,
       CASE WHEN ac.guild_commission >= 0 THEN ac.guild_commission ELSE 0 END   AS guild_commission,
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
       ai.logo
FROM warehouse.ods_yy_day_anchor_info ai
         LEFT JOIN warehouse.dw_yy_day_anchor_live_duration al
                   ON ai.backend_account_id = al.backend_account_id AND ai.anchor_uid = al.anchor_uid AND ai.dt = al.dt
         LEFT JOIN warehouse.dw_yy_day_anchor_live_bluediamond ab
                   ON ai.backend_account_id = ab.backend_account_id AND ai.anchor_uid = ab.anchor_uid AND ai.dt = ab.dt
         LEFT JOIN warehouse.dw_yy_day_anchor_live_commission ac
                   ON ai.backend_account_id = ac.backend_account_id AND ai.anchor_uid = ac.anchor_uid AND ai.dt = ac.dt
         LEFT JOIN warehouse.platform pf ON ai.platform_id = pf.id
WHERE ai.dt BETWEEN '{start_date}' AND '{end_date}'
;


-- =====================================================================
-- 公会收支明细
-- 公会每月获得各主播分成蓝钻
-- DROP TABLE IF EXISTS warehouse.ods_yy_guild_live_bluediamond;
-- CREATE TABLE warehouse.ods_yy_guild_live_bluediamond AS
DELETE
FROM warehouse.ods_yy_guild_live_bluediamond
WHERE DATE_FORMAT(dt, '%Y%m') BETWEEN DATE_FORMAT('{start_date}', '%Y%m') AND DATE_FORMAT('{end_date}', '%Y%m');
INSERT INTO warehouse.ods_yy_guild_live_bluediamond
SELECT CONCAT(gb.year, '-', gb.month, '-01')    AS dt,
       1000                                     AS platform_id,
       'YY'                                     AS platform_name,
       cl.backend_account_id,
       cl.channel_num,
       gb.yynum                                 AS anchor_no,
       gb.nick                                  AS anchor_nick_name,
       gb.totalDiamond                          AS anchor_bluediamond,
       gb.settType                              AS settle_method_code,
       CASE
           WHEN gb.settType = 1 THEN '对公分成'
           WHEN gb.settType = 2 then '对私分成' END AS settle_method_text,
       gb.money                                 AS guild_bluediamond,
       gb.payTime                               AS pay_time
FROM spider_yy_backend.channel_list cl
         LEFT JOIN spider_yy_backend.guild_bluediamond gb ON cl.backend_account_id = gb.backend_account_id
WHERE CONCAT(gb.year, gb.month) BETWEEN DATE_FORMAT('{start_date}', '%Y%m') AND DATE_FORMAT('{end_date}', '%Y%m')
;


-- 公会每月获得各主播分成佣金
-- DROP TABLE IF EXISTS warehouse.ods_yy_guild_live_commission;
-- CREATE TABLE warehouse.ods_yy_guild_live_commission AS
DELETE
FROM warehouse.ods_yy_guild_live_commission
WHERE DATE_FORMAT(dt, '%Y%m') BETWEEN DATE_FORMAT('{start_date}', '%Y%m') AND DATE_FORMAT('{end_date}', '%Y%m');
INSERT INTO warehouse.ods_yy_guild_live_commission
SELECT CONCAT(gc.year, '-', gc.month, '-01') AS dt,
       1000                                  AS platform_id,
       'YY'                                  AS platform_name,
       cl.backend_account_id,
       cl.channel_num,
       gc.yynum                              AS anchor_no,
       gc.nick                               AS anchor_nick_name,
       gc.owMoney                            AS guild_commission,
       gc.time                               AS get_commission_time
FROM spider_yy_backend.channel_list cl
         LEFT JOIN spider_yy_backend.guild_commission gc ON cl.backend_account_id = gc.backend_account_id
WHERE CONCAT(gc.year, gc.month) BETWEEN DATE_FORMAT('{start_date}', '%Y%m') AND DATE_FORMAT('{end_date}', '%Y%m')
;
