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


INSERT IGNORE INTO stage.stage_huya_day_anchor_info (anchor_uid, anchor_no, channel_id, comment, dt)
SELECT al.anchor_uid, ai.anchor_no, al.channel_id, 'from anchor_live_detail_day' AS comment, al.dt
FROM warehouse.ods_huya_day_anchor_live al
LEFT JOIN (SELECT DISTINCT anchor_uid, anchor_no FROM stage.stage_huya_day_anchor_info) ai ON al.anchor_uid = ai.anchor_uid
WHERE al.dt BETWEEN '{start_date}' AND '{end_date}'
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
SELECT ai.dt                                                                  AS dt,
       ai.platform_id,
       ai.platform_name,
       aci.channel_type,
       ai.channel_id,
       ai.channel_num,
       ai.anchor_uid,
       ai.anchor_no,
       ai.nick,
       ai.comment,
       al.duration,
       al.live_status,
       al.income                                                              AS revenue,
       al.peak_pcu,
       ai.activity_days,
       ai.months,
       ai.ow_percent,
       ai.sign_time,
       FROM_UNIXTIME(ai.sign_time, '%Y-%m-%d')                                AS sign_date,
       ai.surplus_days,
       ai.avatar                                                              AS avatar,
       aml.min_live_dt,
       ams.min_sign_dt,
       -- 通过判断主播最小注册时间和最小开播时间，取两者之间最小的时间作为判断新老主播条件，两者为NULL则为‘未知’
       warehouse.ANCHOR_NEW_OLD(aml.min_live_dt, ams.min_sign_dt, al.dt, 180) AS newold_state,
       mal.duration                                                           AS month_duration,
       mal.live_days                                                          AS month_live_days,
       -- 开播天数大于等于20天且开播时长大于等于60小时（t-1月累计）
       CASE
           WHEN mal.live_days >= 20 AND mal.duration >= 60 * 60 * 60 THEN '活跃主播'
           ELSE '非活跃主播' END                                                   AS active_state,
       mal.revenue                                                            AS month_revenue,
       -- 主播流水分级（t-1月）
       CASE
           WHEN mal.revenue / 10000 >= 50 THEN '50+'
           WHEN mal.revenue / 10000 >= 10 THEN '10-50'
           WHEN mal.revenue / 10000 >= 3 THEN '3-10'
           WHEN mal.revenue / 10000 > 0 THEN '0-3'
           ELSE '0' END                                                       AS revenue_level,
       pf.vir_coin_name,
       pf.vir_coin_rate,
       pf.include_pf_amt,
       pf.pf_amt_rate
FROM warehouse.dw_huya_day_anchor_info ai
         LEFT JOIN warehouse.ods_huya_day_anchor_live al
    -- 现只有2019-12至今的数据
                   ON ai.channel_id = al.channel_id AND ai.anchor_uid = al.anchor_uid AND ai.dt = al.dt
         LEFT JOIN stage.stage_hy_anchor_min_live_dt aml ON ai.anchor_no = aml.anchor_no
         LEFT JOIN stage.stage_hy_anchor_min_sign_dt ams ON ai.anchor_no = ams.anchor_no
         LEFT JOIN stage.stage_hy_month_anchor_live mal
                   ON mal.dt = DATE_FORMAT(al.dt, '%Y-%m-01') AND
                      ai.anchor_uid = mal.anchor_uid
         LEFT JOIN warehouse.platform pf ON ai.platform_id = pf.id
         LEFT JOIN warehouse.ods_hy_account_info aci ON ai.channel_id = aci.channel_id
WHERE ai.dt BETWEEN '{start_date}' AND '{end_date}'
;


-- ===================================================================
-- 公会收入
-- DROP TABLE IF EXISTS warehouse.dw_huya_day_guild_live_true;
-- CREATE TABLE warehouse.dw_huya_day_guild_live_true AS
DELETE
FROM warehouse.dw_huya_day_guild_live_true
WHERE dt BETWEEN '{start_date}' AND '{end_date}';
INSERT INTO warehouse.dw_huya_day_guild_live_true
SELECT gi.dt,
       gi.platform_id,
       gi.platform_name,
       ai.channel_type,
       gi.channel_id,
       gi.channel_num              AS channel_num,
       gi.ow                       AS ow,
       gi.channel_name             AS channel_name,
       gi.is_platinum,
       gi.sign_count,
       gi.sign_limit,
       cr.live_cnt,
       IFNULL(cr.revenue, 0)       AS revenue,
       IFNULL(cgi.gift_income, 0)  AS gift_income,
       IFNULL(cgu.guard_income, 0) AS guard_income,
       IFNULL(cn.noble_income, 0)  AS noble_income,
       gi.logo,
       gi.desc,
       gi.create_time,
       cgi.calc_month              AS gift_calc_month,
       cgu.calc_month              AS guard_calc_month,
       cn.calc_month               AS noble_calc_month
FROM warehouse.dw_huya_day_guild_info gi
         LEFT JOIN warehouse.ods_huya_day_guild_live_revenue cr ON gi.dt = cr.dt AND gi.channel_id = cr.channel_id
         LEFT JOIN warehouse.ods_huya_day_guild_live_income_gift cgi
                   ON gi.dt = cgi.dt AND gi.channel_id = cgi.channel_id
         LEFT JOIN warehouse.ods_huya_day_guild_live_income_guard cgu
                   ON gi.dt = cgu.dt AND gi.channel_id = cgu.channel_id
         LEFT JOIN warehouse.ods_huya_day_guild_live_income_noble cn ON gi.dt = cn.dt AND gi.channel_id = cn.channel_id
         LEFT JOIN warehouse.ods_hy_account_info ai ON gi.channel_id = ai.channel_id
    WHERE gi.dt BETWEEN '{start_date}' AND '{end_date}'
;


DELETE
FROM warehouse.dw_huya_day_guild_live
WHERE dt BETWEEN '{start_date}' AND '{end_date}';
INSERT INTO warehouse.dw_huya_day_guild_live
SELECT al.dt,
       al.platform_id,
       al.platform_name,
       al.channel_type,
       al.channel_id,
       ai.channel_no AS channel_num,
       al.newold_state,
       al.active_state,
       al.revenue_level,
       COUNT(DISTINCT al.anchor_no) AS anchor_cnt,
       COUNT(DISTINCT CASE WHEN al.live_status = 1 THEN al.anchor_no ELSE NULL END) AS live_cnt,
       SUM(IFNULL(al.revenue, 0))       AS revenue
FROM warehouse.dw_huya_day_anchor_live al
LEFT JOIN warehouse.ods_hy_account_info ai ON al.channel_id = ai.channel_id
    WHERE al.dt BETWEEN '{start_date}' AND '{end_date}'
GROUP BY al.dt,
       al.platform_id,
       al.platform_name,
       al.channel_type,
       al.channel_id,
       al.channel_num,
       al.newold_state,
       al.active_state,
       al.revenue_level
;

