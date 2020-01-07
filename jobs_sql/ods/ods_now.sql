-- 主播信息
-- DROP TABLE IF EXISTS warehouse.ods_anchor_now_info;
-- CREATE TABLE warehouse.ods_anchor_now_info AS
DELETE FROM warehouse.ods_anchor_now_info WHERE dt BETWEEN '{start_date}' AND '{end_date}';
INSERT INTO warehouse.ods_anchor_now_info
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
WHERE ad.dt BETWEEN '{start_date}' AND '{end_date}'
;


-- 主播直播和收入
-- DROP TABLE IF EXISTS warehouse.ods_anchor_now_live_amt;
-- CREATE TABLE warehouse.ods_anchor_now_live_amt AS
DELETE FROM warehouse.ods_anchor_now_live_amt WHERE dt BETWEEN '{start_date}' AND '{end_date}';
INSERT INTO warehouse.ods_anchor_now_live_amt
SELECT ani.platform_id,
       ani.platform_name,
       ani.backend_account_id,
       ani.anchor_uid,
       ani.anchor_qq_no,
       ani.anchor_no,
       ani.anchor_nick_name,
       ani.anchor_name,
       ai.live_time AS duratiion_hours,
       ROUND(ai.live_time * 60 * 60, 2) AS duration,
       ai.origin_money AS amt,
       ani.settle_method_code,
       ani.settle_method_text,
       DATE_FORMAT(ai.date, '%Y-%m-%d') AS dt,
       ai.timestamp
FROM warehouse.ods_anchor_now_info ani
LEFT JOIN spider_now_backend.anchor_income ai ON ani.dt = DATE_FORMAT(ai.date, '%Y-%m-%d') AND ani.backend_account_id = ai.backend_account_id AND ani.anchor_no = ai.nowid
WHERE ani.dt BETWEEN '{start_date}' AND '{end_date}'
;


-- Merge
-- DROP TABLE IF EXISTS warehouse.ods_now_anchor_live_detail_daily;
-- CREATE TABLE warehouse.ods_now_anchor_live_detail_daily AS
DELETE FROM warehouse.ods_now_anchor_live_detail_daily WHERE dt BETWEEN '{start_date}' AND '{end_date}';
INSERT INTO warehouse.ods_now_anchor_live_detail_daily
SELECT ai.platform_id,
       ai.platform_name,
       ai.backend_account_id,
       ai.anchor_uid,
       ai.anchor_qq_no,
       ai.anchor_no,
       ai.anchor_nick_name,
       ai.anchor_name,
       ai.fans_num,
       ai.fans_group_num,
       CASE WHEN al.duration > 0 THEN 1 ELSE 0 END AS live_status,
       al.duration,
       al.amt,
       pf.vir_coin_name,
       pf.vir_coin_rate,
       pf.include_pf_amt,
       pf.pf_amt_rate,
       ai.contract_sign_time,
       ai.settle_method_code,
       ai.settle_method_text,
       ai.dt
FROM warehouse.ods_anchor_now_info ai
LEFT JOIN warehouse.ods_anchor_now_live_amt al ON ai.backend_account_id = al.backend_account_id AND ai.dt = al.dt AND ai.anchor_no = al.anchor_no
LEFT JOIN warehouse.platform pf ON ai.platform_id = pf.id
WHERE ai.dt BETWEEN '{start_date}' AND '{end_date}'
;

