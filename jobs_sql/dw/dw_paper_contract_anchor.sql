-- 法务提供纸质约主播
-- 注意数据中id_card不止是身份证
DELETE
FROM warehouse.dw_radar_anchor
WHERE 1;
INSERT INTO warehouse.dw_radar_anchor
SELECT id,
       source,
       online_type,
       scout_name,
       union_name,
       channel_id,
       real_name,
       nick_name,
       gender,
       contract_phone,
       wx_no,
       open_id,
       cert_type,
       LTRIM(id_card) AS id_card,
       email,
       address,
       sign_status,
       sign_sub_status,
       sign_wish,
       contract_status,
       has_live_experience,
       talent_skill_ext,
       intention_platform_ext,
       expected_base_salary,
       expected_bottom_guard,
       expected_signing_bonus,
       expected_other,
       device_own,
       device_other,
       artist_dev_intention,
       publish_time,
       create_type,
       version,
       dflag,
       dflag_no,
       tenant_id,
       create_time,
       update_time,
       create_by
FROM warehouse.ods_radar_anchor a
WHERE a.create_type = 3
  AND a.dflag = 0
  AND a.id_card IS NOT NULL
  AND a.id_card <> ''
-- AND LENGTH(a.id_card) >= 16
;


-- 纸质约主播的合同档案信息
-- 同一主播有可能多份合同
-- 同一份合同no的重复数据
-- 取出纸质约主播的合同档案信息时间最大的
-- 1、取出每个主播合约结束时间最大的数据
-- 2、主播可能同时会有多份合约， 取id较大的那条
DELETE
FROM warehouse.dw_radar_anchor_contract_archive
WHERE 1;
INSERT INTO warehouse.dw_radar_anchor_contract_archive
SELECT t4.id,
       t4.upload_id,
       t4.create_type,
       t4.valid_status,
       t4.valid_msg,
       t4.sheet_id,
       t4.contract_type,
       t4.contract_class,
       t4.anchor_id,
       t4.contract_no1,
       t4.contract_no2,
       t4.contract_no,
       t4.real_name,
       t4.nick_name,
       t4.cert_type,
       REPLACE(LTRIM(t4.id_card), ' ', '') AS id_card,
       t4.live_id,
       t4.yy_channel,
       t4.yy_account,
       t4.corp_subject,
       t4.contract_name,
       t4.sign_date,
       t4.contract_start_date,
       t4.contract_end_date,
       t4.pages,
       t4.contract_pos,
       t4.contract_link,
       t4.platform,
       t4.cost_type,
       t4.cost,
       t4.process_id,
       t4.remark,
       t4.tenant_id,
       t4.dflag,
       t4.dflag_no,
       t4.create_time
FROM (SELECT t1.id_card, t1.max_contract_end_date, MAX(t2.id) AS id
      FROM (SELECT id_card, MAX(contract_end_date) AS max_contract_end_date
            FROM warehouse.ods_radar_anchor_contract_archive
            WHERE dflag = 0
            GROUP BY id_card) t1
               INNER JOIN warehouse.ods_radar_anchor_contract_archive t2
                          ON t1.id_card = t2.id_card AND t1.max_contract_end_date = t2.contract_end_date
      GROUP BY t1.id_card, t1.max_contract_end_date) t3
         INNER JOIN warehouse.ods_radar_anchor_contract_archive t4
                    ON t3.id_card = t4.id_card AND t3.max_contract_end_date = t4.contract_end_date AND t3.id = t4.id
;


-- B站（财务提供）
-- 重复主播（身份证，主播号重复）只取其中一条
DELETE
FROM warehouse.dw_radar_t_anchor_all
WHERE plat = 'B站';
REPLACE INTO warehouse.dw_radar_t_anchor_all
SELECT plat,
       uid,
       room_id,
       nick_name,
       real_name,
       id_card
FROM warehouse.ods_radar_t_anchor_b
WHERE id_card IS NOT NULL
  AND id_card <> ''
;


-- NOW（财务提供）
-- 重复主播（身份证，主播号重复）只取其中一条
DELETE
FROM warehouse.dw_radar_t_anchor_all
WHERE plat = 'NOW';
REPLACE INTO warehouse.dw_radar_t_anchor_all
SELECT plat,
       uid,
       room_id,
       nick_name,
       real_name,
       id_card
FROM warehouse.ods_radar_t_anchor_now
WHERE uid NOT rlike ('流|吃|游')
  AND id_card IS NOT NULL
  AND id_card <> ''
;


-- 合同档案中部分主播信息
DELETE
FROM warehouse.dw_radar_t_anchor_all
WHERE plat = 'YY';
REPLACE INTO warehouse.dw_radar_t_anchor_all
SELECT 'YY'       AS plat,
       yy_account AS uid,
       ''         AS room_id,
       nick_name,
       real_name,
       id_card
FROM warehouse.dw_radar_anchor_contract_archive
WHERE dflag = 0
  AND contract_type = 'JP'
;


-- 纸质约主播匹配平台及主播平台ID
-- 一平台一主播一条记录
DELETE
FROM warehouse.dw_paper_contract_anchor
WHERE 1;
INSERT INTO warehouse.dw_paper_contract_anchor
SELECT a.id,
       a.id_card,
       IFNULL(ab.plat, '未知')          AS platform_name,
       IFNULL(ab.uid, '未知')           AS anchor_no,
       a.nick_name,
       a.real_name,
       ab.room_id,
       IFNULL(ac.contract_no, '未知')   AS contract_no,
       IFNULL(ac.contract_type, '未知') AS contract_type,
       a.contract_status,
       ac.contract_start_date,
       ac.contract_end_date,
       ''                             AS comment
FROM warehouse.dw_radar_anchor a
         LEFT JOIN warehouse.dw_radar_t_anchor_all ab
                   ON a.id_card = ab.id_card AND a.id_card <> '' AND ab.id_card <> ''
         LEFT JOIN warehouse.dw_radar_anchor_contract_archive ac
                   ON a.id_card = ac.id_card AND a.id_card <> '' AND ac.id_card <> ''
-- WHERE a.id_card = 110105199205110012
;


-- =====================================================================================================================
-- 各平台主播开播数据
-- B站
DELETE
FROM stage.stage_all_month_live
WHERE platform_name = 'B站';
INSERT INTO stage.stage_all_month_live
SELECT 'B站'                AS platform_name,
       anchor_no,
       dt,
       SUM(live_days)      AS live_days,
       SUM(duration)       AS duration,
       SUM(revenue) / 1000 AS revenue
FROM warehouse.dw_bb_month_anchor_live
GROUP BY anchor_no, dt
;

-- NOW
DELETE
FROM stage.stage_all_month_live
WHERE platform_name = 'NOW';
INSERT INTO stage.stage_all_month_live
SELECT 'NOW'            AS platform_name,
       anchor_no,
       dt,
       SUM(live_days)   AS live_days,
       SUM(duration)    AS duration,
       SUM(revenue_rmb) AS revenue
FROM warehouse.dw_now_month_anchor_live
GROUP BY anchor_no, dt
;

-- YY
DELETE
FROM stage.stage_all_month_live
WHERE platform_name = 'YY';
INSERT INTO stage.stage_all_month_live
SELECT 'YY'                                                    AS platform_name,
       anchor_no,
       dt,
       SUM(live_days)                                          AS live_days,
       SUM(duration)                                           AS duration,
       SUM((anchor_bluediamond + guild_commission)) * 2 / 1000 AS revenue
FROM warehouse.dw_yy_month_anchor_live
GROUP BY anchor_no,
         dt
;

-- HY
DELETE
FROM stage.stage_all_month_live
WHERE platform_name = '虎牙';
INSERT INTO stage.stage_all_month_live
SELECT '虎牙'           AS platform_name,
       anchor_no,
       dt,
       SUM(live_days) AS live_days,
       SUM(duration)  AS duration,
       SUM(revenue)   AS revenue
FROM warehouse.dw_huya_month_anchor_live
GROUP BY anchor_no,
         dt
;

-- =====================================================================================================================
DELETE
FROM warehouse.dw_paper_contract_anchor_month_live
WHERE 1;
INSERT INTO warehouse.dw_paper_contract_anchor_month_live
SELECT ca.platform_name,
       ca.anchor_no,
       ca.id_card,
       ca.nick_name,
       ca.real_name,
       ca.contract_no,
       ca.contract_status,
       ca.contract_type,
       ca.contract_start_date,
       ca.contract_end_date,
       IFNULL(al.dt, '1970-01-01')            AS dt,
       IFNULL(al.live_days, 0)                AS live_days,
       IFNULL(al.duration, 0)                 AS duration,
       IFNULL(al.revenue, 0)                  AS revenue,
       IF(al.dt IS NULL, '无开播记录', ca.comment) AS comment
FROM warehouse.dw_paper_contract_anchor ca
         LEFT JOIN stage.stage_all_month_live al
                   ON ca.platform_name = al.platform_name AND ca.anchor_no = al.anchor_no
--                           AND DATE_FORMAT(ca.contract_start_date, '%Y-%m-01') <= al.dt
;


