-- 公会列表
DELETE
FROM warehouse.ods_bb_account_info
WHERE 1;
INSERT INTO warehouse.ods_bb_account_info
SELECT *
FROM spider_bb_backend.account_info
;


-- 主播信息
-- DROP TABLE IF EXISTS stage.bb_guild_anchor_dt;
-- CREATE TABLE stage.bb_guild_anchor_dt AS
DELETE
FROM stage.bb_guild_anchor_dt
WHERE dt BETWEEN '{start_date}' AND '{end_date}';
INSERT INTO stage.bb_guild_anchor_dt
SELECT backend_account_id,
       uid,
       dt
FROM spider_bb_backend.normal_list
WHERE dt BETWEEN '{start_date}' AND '{end_date}'
UNION
SELECT backend_account_id,
       uid,
       dt
FROM spider_bb_backend.anchor_detail
WHERE dt BETWEEN '{start_date}' AND '{end_date}'
;


-- DROP TABLE IF EXISTS warehouse.ods_bb_day_anchor_live;
-- CREATE TABLE warehouse.ods_bb_day_anchor_live AS
DELETE
FROM warehouse.ods_bb_day_anchor_live
WHERE dt BETWEEN '{start_date}' AND '{end_date}';
INSERT INTO warehouse.ods_bb_day_anchor_live
SELECT 1001                                                                    AS platform_id,
       'bilibili'                                                              AS platform_name,
       gat.dt,
       gat.backend_account_id,
       gat.uid                                                                 AS anchor_uid,
       gat.uid                                                                 AS anchor_no,
       IFNULL(ad.uname, nl.uname)                                              AS anchor_nick_name,
       nl.type                                                                 AS anchor_status,
       nl.type_text                                                            AS anchor_status_text,
       ad.g_id                                                                 AS guild_id,
       ad.g_name                                                               AS guild_name,
       ad.guild_type                                                           AS guild_type,
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
       CASE WHEN ad.total_income > 0 THEN ad.total_income ELSE 0 END           AS anchor_income_virtual_coin,
       CASE WHEN ad.special_coin >= 0 THEN ad.special_coin ELSE 0 END          AS special_coin,
       CASE WHEN ad.send_coin >= 0 THEN ad.send_coin ELSE 0 END                AS send_coin,
       IF(ad.base_salary > 0, ad.base_salary, 0)                               AS banchor_ase_coin,
       ad.DAU,
       ad.max_ppl,
       ad.fc,
       nl.status                                                               AS contract_status,
       nl.status_text                                                          AS contract_status_text,
       DATE_FORMAT(nl.start_date, '%Y-%m-%d %T')                               AS contract_signtime,
       DATE_FORMAT(nl.end_date, '%Y-%m-%d %T')                                 AS contract_endtime
FROM stage.bb_guild_anchor_dt gat
         LEFT JOIN spider_bb_backend.anchor_detail ad
                   ON gat.uid = ad.uid AND gat.dt = ad.dt AND gat.backend_account_id = ad.backend_account_id
         LEFT JOIN spider_bb_backend.normal_list nl
                   ON gat.uid = nl.uid AND gat.dt = nl.dt AND gat.backend_account_id = nl.backend_account_id
WHERE gat.dt BETWEEN '{start_date}' AND '{end_date}'
;


-- ================================================================================
-- 公会月收入
-- DROP TABLE IF EXISTS warehouse.ods_bbmonth_guild_live;
-- CREATE TABLE warehouse.ods_bbmonth_guild_live AS
DELETE
FROM warehouse.ods_bb_month_guild_live
WHERE DATE_FORMAT(dt, '%Y-%m-01') BETWEEN DATE_FORMAT('{start_date}', '%Y-%m-01') AND DATE_FORMAT('{end_date}', '%Y-%m-01');
INSERT INTO warehouse.ods_bb_month_guild_live
SELECT CONCAT(LEFT(gs.month, 4), '-', RIGHT(gs.month, 2), '-01') AS dt,
       gs.backend_account_id,
       gs.status,
       gs.status_text,
       gs.total                                                  AS guild_salary_rmb,
       ROUND(gd.income + gd.base + gd.award + gd.send_money + gd.special_income + gd.admin_change +
             gd.anchor_admin_change, 2)                          AS guild_virtual_coin,
       gd.type,
       gd.income                                                 AS anchor_income,
       gd.base                                                   AS anchor_base_coin,
       gd.award                                                  AS guild_award_coin,
       gd.send_money                                             AS operate_award_punish_coin,
       gd.special_income                                         AS special_coin,
       gd.admin_change                                           AS guild_change_coin,
       gd.anchor_admin_change                                    AS anchor_change_coin,
       gd.admin_note                                             AS comment,
       gs.timestamp
FROM spider_bb_backend.guild_salary gs
         LEFT JOIN spider_bb_backend.guild_salary_detail gd
                   ON gs.month = gd.month AND gs.backend_account_id = gd.backend_account_id
WHERE gs.month BETWEEN DATE_FORMAT('{start_date}', '%Y%m') AND DATE_FORMAT('{end_date}', '%Y%m')
;
