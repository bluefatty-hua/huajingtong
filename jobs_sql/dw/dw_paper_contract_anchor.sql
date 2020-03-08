-- 法务提供纸质约主播
-- 注意数据中id_card不止是身份证
REPLACE INTO warehouse.dw_radar_anchor
SELECT *
FROM warehouse.ods_radar_anchor a
WHERE a.create_type = 3
  AND a.dflag = 0
-- AND LENGTH(a.id_card) >= 16
;

-- 纸质约主播的合同档案信息
-- 同一主播有可能多份合同
-- 同一份合同no的重复数据取其中一条
REPLACE INTO warehouse.dw_radar_anchor_contract_archive
SELECT *
FROM warehouse.ods_radar_anchor_contract_archive t
WHERE t.dflag = 0
  AND t.contract_start_date IS NOT NULL
;

-- 取出纸质约主播的合同档案信息时间最大的
-- 1、取出每个主播合约结束时间最大的数据
-- 2、主播可能同时会有多份合约， 取id较大的那条
INSERT INTO stage.stage_radar_anchor_contract_archive
SELECT t4.*
FROM (SELECT t1.id_card, t1.max_contract_end_date, MAX(t2.id) AS id
      FROM (SELECT id_card, MAX(contract_end_date) AS max_contract_end_date
            FROM warehouse.dw_radar_anchor_contract_archive
            GROUP BY id_card) t1
               INNER JOIN warehouse.dw_radar_anchor_contract_archive t2
                          ON t1.id_card = t2.id_card AND t1.max_contract_end_date = t2.contract_end_date
      GROUP BY t1.id_card, t1.max_contract_end_date) t3
         INNER JOIN warehouse.dw_radar_anchor_contract_archive t4
                    ON t3.id_card = t4.id_card AND t3.max_contract_end_date = t4.contract_end_date AND t3.id = t4.id
;



-- B站（财务提供）
-- 重复主播（身份证，主播号重复）只取其中一条
REPLACE INTO warehouse.dw_radar_t_anchor_b
SELECT *
FROM warehouse.ods_radar_t_anchor_b
;


-- NOW（财务提供）
-- 重复主播（身份证，主播号重复）只取其中一条
REPLACE INTO warehouse.dw_radar_t_anchor_now
SELECT *
FROM warehouse.ods_radar_t_anchor_now
WHERE uid NOT rlike ('流|吃|游')
;


-- 纸质约主播匹配平台及主播平台ID
-- 一平台一主播一合约一条记录
-- B站
INSERT INTO warehouse.dw_paper_contract_anchor
SELECT a.id,
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
         INNER JOIN warehouse.dw_radar_t_anchor_b ab ON a.id_card = ab.id_card AND a.id_card <> '' AND ab.id_card <> ''
         INNER JOIN stage.stage_radar_anchor_contract_archive ac
                    ON ab.id_card = ac.id_card AND ab.id_card <> '' AND ac.id_card <> ''
;

-- NOW
REPLACE INTO warehouse.dw_paper_contract_anchor
SELECT a.id,
       IFNULL(an.plat, '未知')        AS platform_name,
       IFNULL(an.uid, '未知')         AS anchor_no,
       a.nick_name,
       a.real_name,
       an.room_id,
       IFNULL(ac.contract_no, '未知') AS contract_no,
       a.contract_status,
       ac.contract_type,
       ac.contract_start_date,
       ac.contract_end_date,
       ''                           AS comment
FROM warehouse.dw_radar_anchor a
         INNER JOIN warehouse.dw_radar_t_anchor_now an
                    ON a.id_card = an.id_card AND a.id_card <> '' AND an.id_card <> ''
         INNER JOIN stage.stage_radar_anchor_contract_archive ac
                    ON an.id_card = ac.id_card AND an.id_card <> '' AND ac.id_card <> '' AND ac.dflag = 0
;

-- YY
REPLACE INTO warehouse.dw_paper_contract_anchor
SELECT a.id,
       'YY'                         AS platform_name,
       ay.yy_account                AS anchor_no,
       a.nick_name,
       a.real_name,
       ''                           AS room_id,
       IFNULL(ay.contract_no, '未知') AS contract_no,
       a.contract_status,
       ay.contract_type,
       ay.contract_start_date,
       ay.contract_end_date,
       ''                           AS comment
FROM warehouse.dw_radar_anchor a
         INNER JOIN stage.stage_radar_anchor_contract_archive ay
                    ON a.id_card = ay.id_card AND a.id_card <> '' AND ay.id_card <> '' AND ay.dflag = 0
WHERE ay.yy_account IS NOT NULL
  AND ay.yy_account <> ''
;

-- =====================================================================================================================
REPLACE INTO warehouse.dw_paper_contract_anchor_month_live
SELECT ca.platform_name,
       ca.anchor_no,
       ca.nick_name,
       ca.real_name,
       ca.contract_no,
       ca.contract_status,
       ca.contract_type,
       ca.contract_start_date,
       ca.contract_end_date,
       al.dt,
       al.live_days,
       al.duration,
       al.revenue / 1000 AS revenue_rmb,
       ca.comment
FROM warehouse.dw_paper_contract_anchor ca
         LEFT JOIN warehouse.dw_bb_month_anchor_live al
                   ON ca.anchor_no = al.anchor_no AND DATE_FORMAT(ca.contract_start_date, '%Y-%m-01') <= al.dt
WHERE ca.platform_name = 'B站'
;


REPLACE INTO warehouse.dw_paper_contract_anchor_month_live
SELECT ca.platform_name,
       ca.anchor_no,
       ca.nick_name,
       ca.real_name,
       ca.contract_no,
       ca.contract_status,
       ca.contract_type,
       ca.contract_start_date,
       ca.contract_end_date,
       al.dt,
       al.live_days,
       al.duration,
       al.revenue_rmb,
       ca.comment
FROM warehouse.dw_paper_contract_anchor ca
         LEFT JOIN warehouse.dw_now_month_anchor_live al
                   ON ca.anchor_no = al.anchor_no AND DATE_FORMAT(ca.contract_start_date, '%Y-%m-01') <= al.dt
WHERE ca.platform_name = 'NOW'
;


REPLACE INTO warehouse.dw_paper_contract_anchor_month_live
SELECT ca.platform_name,
       ca.anchor_no,
       ca.nick_name,
       ca.real_name,
       ca.contract_no,
       ca.contract_status,
       ca.contract_type,
       ca.contract_start_date,
       ca.contract_end_date,
       IFNULL(al.dt, '1970-01-01')            AS dt,
       al.live_days,
       al.duration,
       al.revenue / 1000 * 2                  AS revenue_rmb,
       IF(al.dt IS NULL, '无开播记录', ca.comment) AS comment
FROM warehouse.dw_paper_contract_anchor ca
         LEFT JOIN (SELECT dt,
                           anchor_no,
                           SUM(live_days)          AS live_days,
                           SUM(duration)           AS duration,
                           SUM(anchor_bluediamond) AS revenue
                    FROM warehouse.dw_yy_month_anchor_live
                    GROUP BY dt, anchor_no) al
                   ON ca.anchor_no = al.anchor_no AND DATE_FORMAT(ca.contract_start_date, '%Y-%m-01') <= al.dt
WHERE ca.platform_name = 'YY'
  AND ca.anchor_no NOT RLIKE '无'
;


-- =====================================================================================================================
REPLACE INTO bireport.rpt_paper_contract_anchor
SELECT '{month}' AS dt,
       al.platform_name,
       al.anchor_no,
       al.real_name,
       al.contract_start_date,
       al.contract_end_date,
       al1.dt          AS dt_t3,
       al1.live_days   AS live_days_t3,
       al1.revenue_rmb AS revenue_t3,
       al2.dt          AS dt_t2,
       al2.live_days   AS live_days_t2,
       al2.revenue_rmb AS revenue_rmb_t2,
       al3.dt          AS dt_t1,
       al3.live_days   AS live_days_t1,
       al3.revenue_rmb AS revenue_rmb_t1,
       al4.dt          AS dt_t,
       al4.live_days   AS live_days_t,
       al4.revenue_rmb AS revenue_rmb_t
FROM (SELECT DISTINCT al.platform_name, al.anchor_no, al.real_name, al.contract_start_date, al.contract_end_date
      FROM warehouse.dw_paper_contract_anchor_month_live al) al
         LEFT JOIN warehouse.dw_paper_contract_anchor_month_live al1
                   ON al.platform_name = al1.platform_name AND al.anchor_no = al1.anchor_no AND
                      al1.dt = '{month}' - INTERVAL 3 MONTH
         LEFT JOIN warehouse.dw_paper_contract_anchor_month_live al2
                   ON al.platform_name = al2.platform_name AND al.anchor_no = al2.anchor_no AND
                      al2.dt = '{month}' - INTERVAL 2 MONTH
         LEFT JOIN warehouse.dw_paper_contract_anchor_month_live al3
                   ON al.platform_name = al3.platform_name AND al.anchor_no = al3.anchor_no AND
                      al3.dt = '{month}' - INTERVAL 1 MONTH
         LEFT JOIN warehouse.dw_paper_contract_anchor_month_live al4
                   ON al.platform_name = al4.platform_name AND al.anchor_no = al4.anchor_no AND
                      al4.dt = '{month}'
;















