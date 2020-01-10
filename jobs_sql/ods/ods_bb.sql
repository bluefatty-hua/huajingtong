-- 主播信息
# DROP TABLE IF EXISTS stage.bb_guild_anchor_dt;
# CREATE TABLE stage.bb_guild_anchor_dt AS
DELETE
FROM stage.bb_guild_anchor_dt
WHERE dt BETWEEN '{start_date}' AND '{end_date}';
INSERT INTO stage.bb_guild_anchor_dt
SELECT backend_account_id,
       uid,
       dt
FROM spider_bb_backend.anchor_detail
WHERE dt BETWEEN '{start_date}' AND '{end_date}'
UNION
SELECT backend_account_id,
       uid,
       dt
FROM spider_bb_backend.normal_list
WHERE dt BETWEEN '{start_date}' AND '{end_date}'
;


# DROP TABLE IF EXISTS stage.bb_anchor_detail;
# CREATE TABLE stage.bb_anchor_detail AS
DELETE
FROM stage.bb_anchor_detail
WHERE dt BETWEEN '{start_date}' AND '{end_date}';
INSERT INTO stage.bb_anchor_detail
SELECT *
FROM spider_bb_backend.anchor_detail
WHERE dt BETWEEN '{start_date}' AND '{end_date}'
;


# DROP TABLE IF EXISTS stage.bb_normal_list;
# CREATE TABLE stage.bb_normal_list AS
DELETE
FROM stage.bb_normal_list
WHERE dt BETWEEN '{start_date}' AND '{end_date}';
INSERT INTO stage.bb_normal_list
SELECT *
FROM spider_bb_backend.normal_list
WHERE dt BETWEEN '{start_date}' AND '{end_date}'
;


# DROP TABLE IF EXISTS warehouse.ods_day_bb_anchor_live_detail;
# CREATE TABLE warehouse.ods_bb_anchor_live_detail AS
DELETE
FROM warehouse.ods_bb_anchor_live_detail
WHERE dt BETWEEN '{start_date}' AND '{end_date}';
INSERT INTO warehouse.ods_bb_anchor_live_detail
SELECT 1001                                                                    AS platform_id,
       'B站'                                                                    AS platform_name,
       ad.backend_account_id,
       ad.g_id                                                                 AS guild_id,
       ad.g_name                                                               AS guild_name,
       ad.guild_type                                                           AS guild_type,
       ad.uid                                                                  AS anchor_uid,
       ad.uid                                                                  AS anchor_no,
       ad.uname                                                                AS anchor_nick_name,
       nl.type                                                                 AS anchor_status,
       nl.type_text                                                            AS anchor_status_text,
       ad.live_day                                                             AS live_status,
       ad.valid_live_day                                                       AS valid_live_status,
       CASE WHEN ad.live_hour >= 0 THEN ad.live_hour ELSE 0 END                AS live_hour,
       CASE WHEN ad.live_hour >= 0 THEN ad.live_hour * 60 * 60 ELSE 0 END      AS duration,
       CASE WHEN ad.valid_live_hour >= 0 THEN ad.valid_live_hour ELSE 0 END    AS valid_live_hour,
       CASE WHEN ad.valid_live_hour >= 0 THEN ad.valid_live_hour * 60 * 60 END AS valid_duration,
       CASE WHEN ad.ios_coin >= 0 THEN ad.ios_coin ELSE 0 END                  AS ios_coin,
       CASE WHEN ad.android_coin >= 0 THEN ad.android_coin ELSE 0 END          AS android_coin,
       CASE WHEN ad.pc_coin >= 0 THEN ad.pc_coin ELSE 0 END                    AS pc_coin,
       (CASE WHEN ad.ios_coin >= 0 THEN ad.ios_coin ELSE 0 END +
        CASE WHEN ad.android_coin >= 0 THEN ad.android_coin ELSE 0 END +
        CASE WHEN ad.pc_coin >= 0 THEN ad.pc_coin ELSE 0 END)                  AS anchor_total_coin,
       CASE WHEN ad.special_coin >= 0 THEN ad.special_coin ELSE 0 END          AS special_coin,
       CASE WHEN ad.send_coin >= 0 THEN ad.send_coin ELSE 0 END                AS send_coin,
       pf.vir_coin_name,
       pf.vir_coin_rate,
       pf.include_pf_amt,
       pf.pf_amt_rate,
       ad.DAU,
       ad.max_ppl,
       ad.fc,
       nl.status                                                               AS contract_status,
       nl.status_text                                                          AS contract_status_text,
       DATE_FORMAT(nl.start_date, '%Y-%m-%d %T')                               AS contract_signtime,
       DATE_FORMAT(nl.end_date, '%Y-%m-%d %T')                                 AS contract_endtime,
       gat.dt
FROM stage.bb_guild_anchor_dt gat
         LEFT JOIN stage.bb_anchor_detail ad
                   ON gat.uid = ad.uid AND gat.dt = ad.dt AND gat.backend_account_id = ad.backend_account_id
         LEFT JOIN stage.bb_normal_list nl
                   ON gat.uid = nl.uid AND gat.dt = nl.dt AND ad.backend_account_id = nl.backend_account_id
         LEFT JOIN warehouse.platform pf
                   ON 1001 = pf.id
# WHERE gat.dt BETWEEN '{start_date}' AND '{end_date}'
;



-- =====================================================================================================================
-- -- DROP TABLE IF EXISTS warehouse.ods_anchor_bb_info;
-- -- CREATE TABLE warehouse.ods_anchor_bb_info AS
-- DELETE FROM warehouse.ods_anchor_bb_info WHERE dt BETWEEN '{start_date}' AND '{end_date}';
-- INSERT INTO warehouse.ods_anchor_bb_info
-- SELECT 1001 AS platform_id,
--        'B站' AS platform_name,
--        an.backend_account_id,
--        an.uid AS anchor_uid,
--        an.uid AS anchor_no,
--        an.uname AS anchor_nick_name,
--        an.type AS anchor_status,
--        an.type_text AS anchor_status_text,
--        an.status AS contract_status,
--        an.status_text AS contract_status_text,
-- 	   DATE_FORMAT(an.start_date, '%Y-%m-%d %T') AS contract_signtime,
--        DATE_FORMAT(an.end_date, '%Y-%m-%d %T') AS contract_endtime,
--        an.dt
-- FROM spider_bb_backend.normal_list an
-- WHERE an.dt BETWEEN '{start_date}' AND '{end_date}'
-- ;
--
--
-- -- 主播直播和主播收入
-- -- DROP TABLE IF EXISTS warehouse.ods_anchor_bb_live_amt;
-- -- CREATE TABLE warehouse.ods_anchor_bb_live_amt AS
-- DELETE FROM warehouse.ods_anchor_bb_live_amt WHERE dt BETWEEN '{start_date}' AND '{end_date}';
-- INSERT INTO warehouse.ods_anchor_bb_live_amt
-- SELECT ai.platform_id,
--        ai.platform_name,
--        ai.backend_account_id,
--        ad.g_id AS guild_id,
--        ad.g_name AS guild_name,
--        ad.guild_type AS guild_type,
--        ai.anchor_uid,
--        ai.anchor_no,
--        ai.anchor_nick_name,
--        ai.anchor_status,
--        ai.anchor_status_text,
--        ad.live_day AS live_status,
--        ad.valid_live_day AS valid_live_status,
--        ad.live_hour,
--        ad.live_hour * 60 * 60 AS duration,
--        ad.valid_live_hour,
--        ad.valid_live_hour * 60 * 60 AS valid_duration,
--        ad.ios_coin,
--        ad.android_coin,
--        ad.pc_coin,
--        (ad.ios_coin + ad.android_coin + ad.pc_coin) AS total_vir_coin,
--        ad.special_coin,
--        ad.send_coin,
--        ad.DAU,
--        ad.max_ppl,
--        ad.fc,
--        ai.dt,
--        ad.timestamp
-- FROM warehouse.ods_anchor_bb_info ai
-- LEFT JOIN spider_bb_backend.anchor_detail ad ON ai.backend_account_id = ad.backend_account_id AND ai.anchor_uid = ad.uid AND ai.dt = ad.dt
-- WHERE ai.dt BETWEEN '{start_date}' AND '{end_date}'
-- ;
--
--
-- -- Merge
-- -- DROP TABLE IF EXISTS warehouse.ods_day_bb_anchor_live_detail;
-- -- CREATE TABLE warehouse.ods_day_bb_anchor_live_detail AS
-- DELETE FROM warehouse.ods_day_bb_anchor_live_detail WHERE dt BETWEEN '{start_date}' AND '{end_date}';
-- INSERT INTO warehouse.ods_day_bb_anchor_live_detail
-- SELECT ai.platform_id,
--        ai.platform_name,
--        ai.backend_account_id,
--        ai.anchor_uid,
--        ai.anchor_no,
--        ai.anchor_nick_name,
--        ai.anchor_status,
--        ai.anchor_status_text,
--        al.guild_id,
--        al.guild_name,
--        al.live_status,
--        al.valid_live_status,
--        al.live_hour,
--        al.valid_live_hour,
--        al.duration,
--        al.valid_duration,
--        al.ios_coin,
--        al.android_coin,
--        al.pc_coin,
--        al.total_vir_coin,
--        al.special_coin,
--        al.send_coin,
--        pf.vir_coin_name,
--        pf.vir_coin_rate,
--        pf.include_pf_amt,
--        pf.pf_amt_rate,
--        al.DAU,
--        al.max_ppl,
--        al.fc,
-- 	   ai.contract_status,
--        ai.contract_status_text,
--        ai.contract_signtime,
--        ai.contract_endtime,
--        ai.dt
-- FROM warehouse.ods_anchor_bb_info ai
-- LEFT JOIN warehouse.ods_anchor_bb_live_amt al ON ai.backend_account_id = al.backend_account_id AND ai.anchor_uid = al.anchor_uid AND ai.dt = al.dt
-- LEFT JOIN warehouse.platform pf ON ai.platform_id = pf.id
-- WHERE ai.dt BETWEEN '{start_date}' AND '{end_date}'
-- ;


-- ================================================================================
-- 公会月收入
-- DROP TABLE IF EXISTS warehouse.ods_guild_bb_amt_mon;
-- CREATE TABLE warehouse.ods_guild_bb_amt_mon AS
DELETE
FROM warehouse.ods_guild_bb_amt_mon
WHERE month BETWEEN DATE_FORMAT('{start_date}', '%Y%m') AND DATE_FORMAT('{end_date}', '%Y%m');
INSERT INTO warehouse.ods_guild_bb_amt_mon
SELECT gs.backend_account_id,
       gs.month,
       LEFT(gs.month, 4)                AS rpt_year,
       RIGHT(gs.month, 2)               AS rpt_month,
       gs.status,
       gs.status_text,
       gs.total                         AS total_amt,
       ROUND(gd.income + gd.base + gd.award + gd.send_money + gd.special_income + gd.admin_change +
             gd.anchor_admin_change, 2) AS total_vir_coin,
       gd.type,
       gd.income                        AS an_income_vir_coin,
       gd.base                          AS an_base_vir_coin,
       gd.award                         AS g_award_vir_coin,
       gd.send_money                    AS operate_award_punish,
       gd.special_income                AS special_income_vir_coin,
       gd.admin_change                  AS g_ch_vir_coin,
       gd.anchor_admin_change           AS an_ch_vir_coin,
       gd.admin_note                    AS comment,
       gs.timestamp
FROM spider_bb_backend.guild_salary gs
         LEFT JOIN spider_bb_backend.guild_salary_detail gd
                   ON gs.month = gd.month AND gs.backend_account_id = gd.backend_account_id
WHERE gs.month BETWEEN DATE_FORMAT('{start_date}', '%Y%m') AND DATE_FORMAT('{end_date}', '%Y%m')
;

