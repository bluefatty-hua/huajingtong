-- 工会信息
-- DROP TABLE IF EXISTS warehouse.dw_huya_day_guild_info;
-- CREATE TABLE warehouse.dw_huya_day_guild_info AS
DELETE
FROM warehouse.dw_huya_day_guild_info
WHERE dt >= '{month}'
  AND dt < '{month}' + INTERVAL 1 MONTH;
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
WHERE dt >= '{month}'
  AND dt < '{month}' + INTERVAL 1 MONTH
;


-- 主播信息
-- 主播列表数据中，同一天可能会出现多条记录（同一主播出现在多个公会，通过时间戳取最新记录）
-- DROP TABLE IF EXISTS stage.stage_huya_day_anchor_info;
-- CREATE TABLE stage.stage_huya_day_anchor_info AS
DELETE
FROM stage.stage_huya_day_anchor_info
WHERE dt >= '{month}'
  AND dt < '{month}' + INTERVAL 1 MONTH;
INSERT INTO stage.stage_huya_day_anchor_info
SELECT ai.dt,
       ai.anchor_uid,
       ai.anchor_no,
       ai.channel_id,
       'orig' AS comment,
       ai.nick,
       ai.activity_days,
       ai.months,
       ai.ow_percent,
       ai.sign_time,
       ai.surplus_days,
       ai.avatar
FROM warehouse.ods_huya_day_anchor_info ai
         INNER JOIN (SELECT dt,
                            anchor_uid,
                            anchor_no,
                            MAX(timestamp) AS max_timestamp
                     FROM warehouse.ods_huya_day_anchor_info
                     WHERE dt >= '{month}'
                       AND dt < '{month}' + INTERVAL 1 MONTH
                     GROUP BY dt,
                              anchor_uid,
                              anchor_no
) mai ON ai.dt = mai.dt AND ai.anchor_uid = mai.anchor_uid AND ai.timestamp = mai.max_timestamp
WHERE ai.dt >= '{month}'
  AND ai.dt < '{month}' + INTERVAL 1 MONTH
;


-- 主播转签问题，当主播发生转签时: 1、一主播列表为准；2、补充数据时以主播同一天旧记录为准（时间戳较小）
DELETE
FROM stage.stage_huya_day_anchor_live
WHERE dt >= '{month}'
  AND dt < '{month}' + INTERVAL 1 MONTH;
INSERT INTO stage.stage_huya_day_anchor_live
SELECT al.*
FROM warehouse.ods_huya_day_anchor_live al
         INNER JOIN (SELECT dt, anchor_uid, MIN(timestamp) AS min_timestamp
                     FROM warehouse.ods_huya_day_anchor_live
                     WHERE dt >= '{month}'
                       AND dt < '{month}' + INTERVAL 1 MONTH
                     GROUP BY dt, anchor_uid
) mal
                    ON al.dt = mal.dt AND al.anchor_uid = mal.anchor_uid AND al.timestamp = mal.min_timestamp
;


-- 补充开播主播到主播列表
INSERT IGNORE INTO stage.stage_huya_day_anchor_info (anchor_uid, anchor_no, channel_id, nick, comment, dt)
SELECT al.anchor_uid,
       ai.anchor_no,
       al.channel_id,
       al.nick,
       'from anchor_live_detail_day' AS comment,
       al.dt
FROM stage.stage_huya_day_anchor_live al
         LEFT JOIN (SELECT DISTINCT anchor_uid, anchor_no FROM warehouse.ods_huya_day_anchor_info) ai
                   ON al.anchor_uid = ai.anchor_uid
WHERE al.dt >= '{month}'
  AND al.dt < '{month}' + INTERVAL 1 MONTH
;


-- DROP TABLE IF EXISTS warehouse.dw_huya_day_anchor_info;
-- CREATE TABLE warehouse.dw_huya_day_anchor_info AS
DELETE
FROM warehouse.dw_huya_day_anchor_info
WHERE dt >= '{month}'
  AND dt < '{month}' + INTERVAL 1 MONTH;
INSERT INTO warehouse.dw_huya_day_anchor_info
SELECT ad.dt,
       platform_id,
       platform_name,
       ad.channel_id,
       channel_num,
       anchor_uid,
       anchor_no,
       comment,
       nick,
       activity_days,
       months,
       ow_percent,
       sign_time,
       surplus_days,
       avatar
FROM stage.stage_huya_day_anchor_info ad
         LEFT JOIN warehouse.dw_huya_day_guild_info ch ON ad.channel_id = ch.channel_id AND ad.dt = ch.dt
WHERE ad.dt >= '{month}'
  AND ad.dt < '{month}' + INTERVAL 1 MONTH
;


-- 主播最早开播时间（基于现有数据）
-- DROP TABLE IF EXISTS stage.stage_hy_anchor_min_live_dt;
-- CREATE TABLE stage.stage_hy_anchor_min_live_dt
INSERT IGNORE INTO stage.stage_hy_anchor_min_live_dt
SELECT t.anchor_no,
       MIN(t.min_live_dt)
FROM (SELECT ai.anchor_no,
             MIN(al.dt) AS min_live_dt
      FROM stage.stage_huya_day_anchor_live al
               INNER JOIN (SELECT DISTINCT anchor_uid,
                                           anchor_no
                           FROM warehouse.ods_huya_day_anchor_info) ai
                          ON al.anchor_uid = ai.anchor_uid
      WHERE al.live_status = 1
      GROUP BY ai.anchor_no
      UNION
      SELECT yj.uid             AS anchor_no,
             yj.first_live_time AS min_live_time
      FROM warehouse.ods_yujia_anchor_list yj
      WHERE platform = '虎牙'
        AND first_live_time <> '1970-01-01') t
GROUP BY anchor_no
;


-- 主播最早签约时间（基于现有数据）,后期结合公司现有主播的签约时间
-- DROP TABLE IF EXISTS stage.stage_hy_anchor_min_sign_dt;
-- CREATE TABLE stage.stage_hy_anchor_min_sign_dt
INSERT IGNORE INTO stage.stage_hy_anchor_min_sign_dt
SELECT t.anchor_no,
       MIN(min_sign_dt) AS min_sign_dt
FROM (SELECT al.anchor_no,
             MIN(from_unixtime(sign_time, '%Y-%m-%d')) AS min_sign_dt
      FROM warehouse.dw_huya_day_anchor_info al
      GROUP BY al.anchor_no
      UNION
      SELECT yj.uid       AS anchor_no,
             yj.sign_time AS min_sign_dt
      FROM warehouse.ods_yujia_anchor_list yj
      WHERE platform = '虎牙'
        AND yj.sign_time <> '1970-01-01') t
GROUP BY t.anchor_no
;


-- 计算每月主播开播天数，开播时长，流水
-- DROP TABLE IF EXISTS stage.stage_hy_month_anchor_live;
-- CREATE TABLE stage.stage_hy_month_anchor_live
DELETE
FROM stage.stage_hy_month_anchor_live
WHERE dt = '{month}';
INSERT INTO stage.stage_hy_month_anchor_live
SELECT t.dt,
       1002                 AS platform_id,
       t.anchor_uid,
       t.revenue,
       -- revenue、income是rmb
       CASE
           WHEN t.revenue / 10000 >= 50 THEN '50+'
           WHEN t.revenue / 10000 >= 10 THEN '10-50'
           WHEN t.revenue / 10000 >= 3 THEN '3-10'
           WHEN t.revenue / 10000 > 0 THEN '0-3'
           ELSE '0' END     AS revenue_level,
       t.live_days,
       t.duration,
       CASE
           WHEN t.live_days >= 20 AND t.duration >= 60 * 60 * 60 THEN '活跃主播'
           ELSE '非活跃主播' END AS active_state
FROM (
         SELECT DATE_FORMAT(al.dt, '%Y-%m-01')                                     AS dt,
                al.anchor_uid,
                SUM(income)                                                        AS revenue,
                COUNT(DISTINCT CASE WHEN al.live_status = 1 THEN dt ELSE NULL END) AS live_days,
                SUM(al.duration)                                                   AS duration
         FROM stage.stage_huya_day_anchor_live al
         WHERE dt >= '{month}'
           AND dt < '{month}' + INTERVAL 1 MONTH
         GROUP BY DATE_FORMAT(al.dt, '%Y-%m-01'),
                  al.anchor_uid) t
;


-- 主播直播和直播收入
-- DROP TABLE IF EXISTS warehouse.dw_huya_day_anchor_live;
-- CREATE TABLE warehouse.dw_huya_day_anchor_live AS
DELETE
FROM warehouse.dw_huya_day_anchor_live
WHERE dt >= '{month}'
  AND dt < '{month}' + INTERVAL 1 MONTH;
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
       IFNULL(mal.duration, 0)                                                AS month_duration,
       IFNULL(mal.live_days, 0)                                               AS month_live_days,
       -- 开播天数大于等于20天且开播时长大于等于60小时（t月累计）
       IFNULL(mal.active_state, '非活跃主播')                                      AS active_state,
       IFNULL(mal.revenue, 0)                                                 AS month_revenue,
       -- 主播流水分级（t月）
       IFNULL(mal.revenue_level, 0)                                           AS revenue_level,
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
WHERE ai.dt >= '{month}'
  AND ai.dt < '{month}' + INTERVAL 1 MONTH
;


-- UPDATE
--     warehouse.dw_huya_day_anchor_live al, stage.stage_hy_month_anchor_live mal
-- SET al.active_state    = mal.active_state,
--     al.month_duration  = mal.duration,
--     al.month_live_days = mal.live_days,
--     al.revenue_level   = mal.revenue_level,
--     al.month_revenue   = mal.revenue
-- WHERE al.anchor_uid = mal.anchor_uid
--   AND al.dt >= mal.dt
--   AND al.dt < mal.dt + INTERVAL 1 MONTH
--   AND mal.dt = '{month}'
--   AND al.dt >= '{month}'
--   And al.dt < '{month}' + INTERVAL 1 MONTH
-- ;


-- ===================================================================
-- 公会收入
-- DROP TABLE IF EXISTS warehouse.dw_huya_day_guild_live_true;
-- CREATE TABLE warehouse.dw_huya_day_guild_live_true AS
DELETE
FROM warehouse.dw_huya_day_guild_live_true
WHERE dt >= '{month}'
  AND dt < '{month}' + INTERVAL 1 MONTH;
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
WHERE gi.dt >= '{month}'
  AND gi.dt < '{month}' + INTERVAL 1 MONTH
;


DELETE
FROM warehouse.dw_huya_day_guild_live
WHERE dt >= '{month}'
  AND dt < '{month}' + INTERVAL 1 MONTH;
INSERT INTO warehouse.dw_huya_day_guild_live
SELECT al.dt,
       al.platform_id,
       al.platform_name,
       al.channel_type,
       al.channel_id,
       ai.channel_no                                                                AS channel_num,
       al.newold_state,
       al.active_state,
       al.revenue_level,
       COUNT(DISTINCT al.anchor_no)                                                 AS anchor_cnt,
       COUNT(DISTINCT CASE WHEN al.live_status = 1 THEN al.anchor_no ELSE NULL END) AS live_cnt,
       SUM(IFNULL(al.revenue, 0))                                                   AS revenue
FROM warehouse.dw_huya_day_anchor_live al
         LEFT JOIN warehouse.ods_hy_account_info ai ON al.channel_id = ai.channel_id
WHERE al.dt >= '{month}'
  AND al.dt < '{month}' + INTERVAL 1 MONTH
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

