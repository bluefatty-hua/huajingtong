-- 主播信息
DROP TABLE IF EXISTS warehouse.ods_anchor_now_info;
CREATE TABLE warehouse.ods_anchor_now_info AS
SELECT 1003 AS platform_id,
       'NOW' AS platform_name,
       ad.backend_account_id,
       ad.uid AS anchor_uid,
       ad.uin AS anchor_qq_no,
       ad.nowid AS anchor_no,
       ad.nickname AS anchor_nick_name,
       ad.name AS anchor_name,
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
DROP TABLE IF EXISTS warehouse.ods_anchor_now_live_amt;
CREATE TABLE warehouse.ods_anchor_now_live_amt AS
SELECT ani.platform_id,
       ani.platform_name,
       ani.backend_account_id,
       ani.anchor_uid,
       ani.anchor_qq_no,
       ani.anchor_no,
       ani.anchor_nick_name,
       ani.anchor_name,
       ai.live_time AS duratiion_hours,
       ROUND(ai.live_time * 60 * 60, 2) AS duratiion,
       ai.origin_money AS amt,
       ani.settle_method_code,
       ani.settle_method_text,
       DATE_FORMAT(ai.date, '%Y-%m-%d') AS dt,
       ai.timestamp
FROM warehouse.ods_anchor_now_info ani
LEFT JOIN spider_now_backend.anchor_income ai ON ani.backend_account_id = ai.backend_account_id AND ani.anchor_no = ai.nowid AND ani.dt = DATE_FORMAT(ai.date, '%Y-%m-%d')
;

