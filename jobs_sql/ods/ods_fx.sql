-- spider_fx_backend.star_list 1月以前的数据的数据
-- 125星豆 = 1元
REPLACE INTO stage.stage_fx_star_list
SELECT *
FROM spider_fx_backend.star_list
WHERE dt >= 20200209
  AND dt BETWEEN '{start_date}' AND '{end_date}'
;

-- DROP TABLE IF EXISTS warehouse.ods_fx_day_anchor_info;
-- CREATE TABLE warehouse.ods_fx_day_anchor_info AS
DELETE
FROM warehouse.ods_fx_day_anchor_info
WHERE dt BETWEEN '{start_date}' AND '{end_date}';
INSERT INTO warehouse.ods_fx_day_anchor_info
SELECT sl.dt,
       1004             AS platform_id,
       'FanXing'        AS platform_name,
       sl.backend_account_id,
       sl.user_id       AS anchor_no,
       sl.nick_name     AS anchor_nick_name,
       sl.qq            AS anchor_qq_no,
       sl.gender,
       sl.level,
       sl.star_level,
       sl.group_name,
       sl.exclusive_str AS contract_sign_type,
       sl.sign_time,
       sl.latest_time,
       sl.avatar_url,
       sl.timestamp
FROM stage.stage_fx_star_list sl
WHERE sl.dt BETWEEN '{start_date}' AND '{end_date}'
;


-- 主播开播
-- DROP TABLE IF EXISTS warehouse.ods_fx_day_anchor_live_duration;
-- CREATE TABLE warehouse.ods_fx_day_anchor_live_duration AS
DELETE
FROM warehouse.ods_fx_day_anchor_live_duration
WHERE dt BETWEEN '{start_date}' AND '{end_date}';
INSERT INTO warehouse.ods_fx_day_anchor_live_duration
SELECT sd.dt,
       1004                                         AS platform_id,
       'FanXing'                                    AS platform_name,
       sd.backend_account_id,
       sd.user_id                                   AS anchor_no,
       sd.nick_name                                 AS anchor_nick_name,
       sd.total_live_time,
       warehouse.DURATION_CH(sd.total_live_time)    AS duration,
       sd.pc_live_time,
       warehouse.DURATION_CH(sd.pc_live_time)       AS pc_duration,
       sd.mobile_live_time,
       warehouse.DURATION_CH(sd.mobile_live_time)   AS mob_duration,
       sd.voice_live_time,
       warehouse.DURATION_CH(sd.voice_live_time)    AS voi_duration,
       sd.game_live_time,
       warehouse.DURATION_CH(sd.game_live_time)     AS game_duration,
       sd.dual_cam_live_time,
       warehouse.DURATION_CH(sd.dual_cam_live_time) AS dual_duration,
       sd.avatar_url,
       sd.timestamp
FROM spider_fx_backend.star_statistics_detail sd
WHERE sd.dt BETWEEN '{start_date}' AND '{end_date}'
;


-- 主播日收入、流水
-- DROP TABLE IF EXISTS warehouse.ods_fx_day_anchor_live_revenue;
-- CREATE TABLE warehouse.ods_fx_day_anchor_live_revenue AS
DELETE
FROM warehouse.ods_fx_day_anchor_live_revenue
WHERE dt BETWEEN '{start_date}' AND '{end_date}';
INSERT INTO warehouse.ods_fx_day_anchor_live_revenue
SELECT si.dt,
       1004         AS platform_id,
       'FanXing'    AS platform_name,
       si.backend_account_id,
       si.user_id   AS anchor_no,
       si.nick_name AS anchor_nick_name,
       si.bean_num  AS anchor_income,
       si.bean_to_coin_num,
       si.bean_to_rmb,
       si.clan_share_bean_num,
       si.avatar_url,
       si.timestamp
FROM spider_fx_backend.star_income_detail si
WHERE si.dt BETWEEN '{start_date}' AND '{end_date}'
;


# DROP TABLE IF EXISTS warehouse.ods_fx_day_anchor_live;
# CREATE TABLE warehouse.ods_fx_day_anchor_live AS
DELETE
FROM warehouse.ods_fx_day_anchor_live
WHERE dt BETWEEN '{start_date}' AND '{end_date}';
INSERT INTO warehouse.ods_fx_day_anchor_live
SELECT ai.dt,
       ai.platform_id,
       ai.platform_name,
       ai.backend_account_id,
       ai.anchor_no,
       ai.anchor_nick_name,
       ai.gender,
       ai.level,
       ai.star_level,
       ai.group_name,
       ai.contract_sign_type,
       ai.sign_time,
       ai.latest_time,
       CASE WHEN ald.duration > 0 THEN 1 ELSE 0 END AS live_status,
       IFNULL(ald.total_live_time, '0秒')            AS total_live_time,
       IFNULL(ald.duration, 0)                      AS duration,
       IFNULL(ald.pc_live_time, '0秒')               AS pc_live_time,
       IFNULL(ald.pc_duration, 0)                   AS pc_duration,
       IFNULL(ald.mobile_live_time, '0秒')           AS mobile_live_time,
       IFNULL(ald.mob_duration, 0)                  AS mob_duration,
       IFNULL(ald.voice_live_time, '0秒')            AS voice_live_time,
       IFNULL(ald.voi_duration, 0)                  AS voi_duration,
       IFNULL(ald.game_live_time, '0秒')             AS game_live_time,
       IFNULL(ald.game_duration, 0)                 AS game_duration,
       IFNULL(ald.dual_cam_live_time, '0秒')         AS dual_cam_live_time,
       IFNULL(ald.dual_duration, 0)                 AS dual_duration,
       IFNULL(alr.anchor_income, 0)                 AS anchor_income,
       IFNULL(alr.bean_to_coin_num, 0)              AS bean_to_coin_num,
       IFNULL(alr.bean_to_rmb, 0)                   AS bean_to_rmb,
       IFNULL(alr.clan_share_bean_num, 0)           AS clan_share_bean_num,
       ai.avatar_url
FROM warehouse.ods_fx_day_anchor_info ai
         LEFT JOIN warehouse.ods_fx_day_anchor_live_duration ald
                   ON ai.dt = ald.dt AND ai.backend_account_id = ald.backend_account_id AND ai.anchor_no = ald.anchor_no
         LEFT JOIN warehouse.ods_fx_day_anchor_live_revenue alr
                   ON ai.dt = alr.dt AND ai.backend_account_id = alr.backend_account_id AND ai.anchor_no = alr.anchor_no
WHERE ai.dt BETWEEN '{start_date}' AND '{end_date}'
;


-- 公会-月（结算）
-- DROP TABLE IF EXISTS warehouse.ods_fx_month_guild_live_revenue;
-- CREATE TABLE warehouse.ods_fx_month_guild_live_revenue AS
DELETE
FROM warehouse.ods_fx_month_guild_live_revenue
WHERE dt BETWEEN DATE_FORMAT('{start_date}', '%Y-%m-01') AND DATE_FORMAT('{end_date}', '%Y-%m-01');
INSERT INTO warehouse.ods_fx_month_guild_live_revenue
SELECT CONCAT(r.month, '-01') AS dt,
       1004                   AS platform_id,
       'FanXing'              AS platform_name,
       r.backend_account_id,
       r.adjustmentAmount,
       r.bonus,
       r.clanBedge,
       r.commission,
       r.deposit,
       r.lastSalary,
       r.payAmount,
       r.realDeductAmount,
       r.starShare,
       r.starBeans            AS anchor_income,
       r.amount               AS guild_income,
       r.statusDesc
FROM spider_fx_backend.revenue r
WHERE CONCAT(r.month, '-01') BETWEEN DATE_FORMAT('{start_date}', '%Y-%m-01') AND DATE_FORMAT('{end_date}', '%Y-%m-01')
;

