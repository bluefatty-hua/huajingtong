-- 工会信息
-- DROP TABLE IF EXISTS warehouse.dw_huya_day_guild_info;
-- CREATE TABLE warehouse.dw_huya_day_guild_info AS
DELETE
FROM warehouse.dw_huya_day_guild_info
WHERE dt BETWEEN '{start_date}' AND '{end_date}';
INSERT INTO warehouse.dw_huya_day_guild_info
SELECT dt,
       platform_id,
       platform_name,
       channel_id,
       channel_num,
       ow,
       channel_name,
       logo,
       `desc`,
       create_time,
       is_platinum,
       sign_count,
       sign_limit,
       timestamp
FROM warehouse.ods_huya_day_guild_info
WHERE dt BETWEEN '{start_date}' AND '{end_date}'
;


-- 主播信息
-- DROP TABLE IF EXISTS stage.stage_huya_day_anchor_info;
-- CREATE TABLE stage.stage_huya_day_anchor_info AS
DELETE
FROM stage.stage_huya_day_anchor_info
WHERE dt BETWEEN '{start_date}' AND '{end_date}';
INSERT INTO stage.stage_huya_day_anchor_info
SELECT dt,
       anchor_uid,
       anchor_no,
       channel_id,
       'orig' AS comment,
       nick,
       activity_days,
       months,
       ow_percent,
       sign_time,
       surplus_days,
       avatar
FROM warehouse.ods_huya_day_anchor_info
WHERE dt BETWEEN '{start_date}' AND '{end_date}'
;


INSERT IGNORE INTO stage.stage_huya_day_anchor_info (anchor_uid, channel_id, comment, dt)
SELECT anchor_uid, channel_id, 'from anchor_live_detail_day' AS comment, dt
FROM warehouse.ods_huya_day_anchor_live
WHERE dt BETWEEN '{start_date}' AND '{end_date}'
;


-- DROP TABLE IF EXISTS warehouse.dw_huya_day_anchor_info;
-- CREATE TABLE warehouse.dw_huya_day_anchor_info AS
DELETE
FROM warehouse.dw_huya_day_anchor_info
WHERE dt BETWEEN '{start_date}' AND '{end_date}';
INSERT INTO warehouse.dw_huya_day_anchor_info
SELECT ad.dt,
       platform_id,
       platform_name,
       ad.channel_id,
       channel_num   AS channel_num,
       anchor_uid    AS anchor_uid,
       anchor_no     AS anchor_no,
       comment,
       nick          AS nick,
       activity_days AS activity_days,
       months        AS months,
       ow_percent    AS ow_percent,
       sign_time     AS sign_time,
       surplus_days     surplus_days,
       avatar        AS avatar
FROM stage.stage_huya_day_anchor_info ad
         LEFT JOIN warehouse.dw_huya_day_guild_info ch ON ad.channel_id = ch.channel_id AND ad.dt = ch.dt
WHERE ad.dt BETWEEN '{start_date}' AND '{end_date}'
;


-- 主播直播和直播收入
-- DROP TABLE IF EXISTS warehouse.dw_huya_day_anchor_live;
-- CREATE TABLE warehouse.dw_huya_day_anchor_live AS
DELETE
FROM warehouse.dw_huya_day_anchor_live
WHERE dt BETWEEN '{start_date}' AND '{end_date}';
INSERT INTO warehouse.dw_huya_day_anchor_live
SELECT ai.dt     AS dt,
       ai.platform_id,
       ai.platform_name,
       ai.channel_id,
       ai.channel_num,
       ai.anchor_uid,
       ai.anchor_no,
       ai.nick,
       ai.comment,
       al.duration,
       al.live_status,
       al.income AS income,
       al.peak_pcu,
       ai.activity_days,
       ai.months,
       ai.ow_percent,
       ai.sign_time,
       ai.surplus_days,
       ai.avatar AS avatar,
       pf.vir_coin_name,
       pf.vir_coin_rate,
       pf.include_pf_amt,
       pf.pf_amt_rate
FROM warehouse.dw_huya_day_anchor_info ai
         LEFT JOIN warehouse.ods_huya_day_anchor_live al
         -- 现只有2019-12至今的数据
                   ON ai.channel_id = al.channel_id AND ai.anchor_uid = al.anchor_uid AND ai.dt = al.dt
         LEFT JOIN warehouse.platform pf ON ai.platform_id = pf.id
WHERE ai.dt BETWEEN '{start_date}' AND '{end_date}'
;


-- ===================================================================
-- 公会收入
-- DROP TABLE IF EXISTS warehouse.dw_huya_day_guild_live;
-- CREATE TABLE warehouse.dw_huya_day_guild_live AS
DELETE
FROM warehouse.dw_huya_day_guild_live
WHERE dt BETWEEN '{start_date}' AND '{end_date}';
INSERT INTO warehouse.dw_huya_day_guild_live
SELECT cd.dt,
       cd.platform_id,
       cd.platform_name,
       cd.channel_id,
       cd.channel_num              AS channel_num,
       cd.ow                       AS ow,
       cd.channel_name             AS channel_name,
       cd.is_platinum,
       cd.sign_count,
       cd.sign_limit,
       cr.live_cnt,
       ifnull(cr.revenue, 0)       AS revenue,
       ifnull(cgi.gift_income, 0)  AS gift_income,
       ifnull(cgu.guard_income, 0) AS guard_income,
       ifnull(cn.noble_income, 0)  AS noble_income,
       cd.logo,
       cd.desc,
       cd.create_time,
       cgi.calc_month              as gift_calc_month,
       cgu.calc_month              as guard_calc_month,
       cn.calc_month               as noble_calc_month
FROM warehouse.dw_huya_day_guild_info cd
         LEFT JOIN warehouse.ods_huya_day_guild_live_revenue cr ON cd.dt = cr.dt AND cd.channel_id = cr.channel_id
         LEFT JOIN warehouse.ods_huya_day_guild_live_income_gift cgi
                   ON cd.dt = cgi.dt AND cd.channel_id = cgi.channel_id
         LEFT JOIN warehouse.ods_huya_day_guild_live_income_guard cgu
                   ON cd.dt = cgu.dt AND cd.channel_id = cgu.channel_id
         LEFT JOIN warehouse.ods_huya_day_guild_live_income_noble cn ON cd.dt = cn.dt AND cd.channel_id = cn.channel_id
WHERE cd.dt BETWEEN '{start_date}' AND '{end_date}'
;

