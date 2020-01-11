-- 主播信息
DROP TABLE IF EXISTS warehouse.ods_now_anchor_info;
CREATE TABLE warehouse.ods_now_anchor_info AS
-- DELETE FROM warehouse.ods_anchor_now_info WHERE dt BETWEEN '{start_date}' AND '{end_date}';
-- INSERT INTO warehouse.ods_anchor_now_info
SELECT 1003 AS platform_id,
       'NOW' AS platform_name,
       ad.backend_account_id,
       ad.uid AS anchor_uid,
       ad.uin AS anchor_qq_no,
       ad.nowid AS anchor_no,
       ad.nickname AS anchor_nick_name,
       ad.name AS anchor_name,
       ad.level,
       ad.fans_num,
       ad.fans_group_num,
       DATE_FORMAT(FROM_UNIXTIME(ad.enter_time), '%Y-%m-%d %T') AS contract_sign_time,
       CASE WHEN ad.income_status_msg = '对公' THEN 1
            WHEN ad.income_status_msg = '对私' THEN 2
            ELSE '' END AS settle_method_code,
       CASE WHEN ad.income_status_msg = '对公' THEN '对公分成'
            WHEN ad.income_status_msg = '对私' THEN '对私分成'
            ELSE '' END AS settle_method_text,
       ad.dt,
       ad.timestamp
FROM
    spider_now_backend.anchor_detail ad
-- WHERE ad.dt BETWEEN '{start_date}' AND '{end_date}'
;


-- Merge
DROP TABLE IF EXISTS warehouse.ods_now_anchor_live_detail;
CREATE TABLE warehouse.ods_now_anchor_live_detail AS
-- DELETE FROM warehouse.ods_now_anchor_live_detail WHERE dt BETWEEN '{start_date}' AND '{end_date}';
-- INSERT INTO warehouse.ods_now_anchor_live_detail
SELECT ai.platform_id,
       ai.platform_name,
       ai.backend_account_id,
       ai.anchor_uid,
       ai.anchor_qq_no,
       ai.anchor_no,
       ai.anchor_nick_name,
       ai.anchor_name,
       ai.fans_num AS fans_cnt,
       ai.fans_group_num AS fans_goup_cnt,
       CASE WHEN ain.live_time > 0 THEN 1 ELSE 0 END AS live_status,
       CASE WHEN ain.live_time >= 0 THEN ain.live_time ELSE 0 END AS duration_hour,
       CASE WHEN ain.live_time >= 0 THEN ain.live_time * 60 * 60 ELSE 0 END AS duration,
       ROUND(CASE WHEN ain.origin_money >= 0 THEN ain.origin_money ELSE 0 END, 2) AS anchor_commission_rmb,
       ai.contract_sign_time,
       ai.settle_method_code,
       ai.settle_method_text,
       ai.dt
FROM warehouse.ods_now_anchor_info ai
LEFT JOIN spider_now_backend.anchor_income ain ON ai.backend_account_id = ain.backend_account_id AND ai.dt = DATE_FORMAT(ain.date, '%Y-%m-%d') AND ai.anchor_qq_no = ain.uin
-- WHERE ai.dt BETWEEN '{start_date}' AND '{end_date}'
;


-- ===================================================================
-- 公会每日流水、收入、每日开播主播数
-- DROP TABLE IF EXISTS warehouse.ods_day_now_guild_commission;
-- CREATE TABLE warehouse.ods_day_now_guild_commission AS
DELETE FROM warehouse.ods_day_now_guild_commission WHERE dt BETWEEN '{start_date}' AND '{end_date}';
INSERT INTO warehouse.ods_day_now_guild_commission
SELECT 1003 AS platform_id,
       'NOW' AS platform_name,
       ui.backend_account_id,
       ui.on_live AS anchor_live_cnt,
       ut.origin_money AS guild_commission_rmb,
       ut.income AS guild_salary_rmb,
       DATE_FORMAT(ui.day, '%Y-%m-%d') AS dt
FROM spider_now_backend.union_stat_info_by_day ui
LEFT JOIN spider_now_backend.union_total_income ut ON ui.backend_account_id = ut.backend_account_id AND ui.day = ut.date
WHERE DATE_FORMAT(ui.day, '%Y-%m-%d') BETWEEN '{start_date}' AND '{end_date}'
;


-- 公会每月流水，平均主播流水
-- DROP TABLE IF EXISTS warehouse.ods_month_now_guild_commission;
-- CREATE TABLE warehouse.ods_month_now_guild_commission AS
DELETE FROM warehouse.ods_month_now_guild_commission WHERE dt BETWEEN '{start_date}' AND '{end_date}';
INSERT INTO warehouse.ods_month_now_guild_commission
SELECT 1003 AS platform_id,
       'NOW' AS platform_name,
       backend_account_id,
       DATE_FORMAT(ui.date, '%Y-%m-%d') AS dt,
       ui.anchor_num AS anchor_cnt,
       ui.cur_month_total_journal AS guild_commission_rmb,
       ui.average_journal AS average_anchor_commission_rmb,
       ui.living_rate AS anchor_live_rate
FROM spider_now_backend.union_stat_info_by_month ui
WHERE DATE_FORMAT(ui.date, '%Y-%m-%d') BETWEEN '{start_date}' AND '{end_date}'
;
