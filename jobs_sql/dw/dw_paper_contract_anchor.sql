-- 法务提供纸质约主播
-- 注意数据中id_card不止是身份证
REPLACE INTO warehouse.dw_radar_anchor
SELECT *
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
INSERT INTO warehouse.dw_radar_anchor_contract_archive
SELECT t4.*
FROM (SELECT t1.id_card, t1.max_contract_end_date, MAX(t2.id) AS id
      FROM (SELECT id_card, MAX(contract_end_date) AS max_contract_end_date
            FROM warehouse.ods_radar_anchor_contract_archive
            WHERE dflag = 0
#               AND id_card = 440104198603281913
            GROUP BY id_card) t1
               INNER JOIN warehouse.ods_radar_anchor_contract_archive t2
                          ON t1.id_card = t2.id_card AND t1.max_contract_end_date = t2.contract_end_date
      GROUP BY t1.id_card, t1.max_contract_end_date) t3
         INNER JOIN warehouse.ods_radar_anchor_contract_archive t4
                    ON t3.id_card = t4.id_card AND t3.max_contract_end_date = t4.contract_end_date AND t3.id = t4.id
;


-- B站（财务提供）
-- 重复主播（身份证，主播号重复）只取其中一条
REPLACE INTO warehouse.dw_radar_t_anchor_all
SELECT *
FROM warehouse.ods_radar_t_anchor_b
WHERE id_card IS NOT NULL
  AND id_card <> ''
;


-- NOW（财务提供）
-- 重复主播（身份证，主播号重复）只取其中一条
REPLACE INTO warehouse.dw_radar_t_anchor_all
SELECT *
FROM warehouse.ods_radar_t_anchor_now
WHERE uid NOT rlike ('流|吃|游')
  AND id_card IS NOT NULL
  AND id_card <> ''
;


-- 合同档案中部分主播信息
REPLACE INTO warehouse.dw_radar_t_anchor_all
SELECT *
FROM warehouse.ods_radar_t_anchor_now
WHERE uid NOT rlike ('流|吃|游')
  AND id_card IS NOT NULL
  AND id_card <> ''
;


-- 纸质约主播匹配平台及主播平台ID
-- 一平台一主播一条记录
-- B站、NOW
REPLACE INTO warehouse.dw_paper_contract_anchor
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
         INNER JOIN warehouse.dw_radar_t_anchor_all ab
                    ON a.id_card = ab.id_card AND a.id_card <> '' AND ab.id_card <> ''
         INNER JOIN warehouse.dw_radar_anchor_contract_archive ac
                    ON ab.id_card = ac.id_card AND ab.id_card <> '' AND ac.id_card <> ''
;


-- YY
REPLACE INTO warehouse.dw_paper_contract_anchor
SELECT a.id,
       a.id_card,
       'YY'                         AS platform_name,
       ay.yy_account                AS anchor_no,
       a.nick_name,
       a.real_name,
       ''                           AS room_id,
       IFNULL(ay.contract_no, '未知') AS contract_no,
       ay.contract_type,
       a.contract_status,
       ay.contract_start_date,
       ay.contract_end_date,
       ''                           AS comment
FROM warehouse.dw_radar_anchor a
         INNER JOIN warehouse.dw_radar_anchor_contract_archive ay
                    ON a.id_card = ay.id_card AND a.id_card <> '' AND ay.id_card <> '' AND ay.dflag = 0
WHERE
      ay.yy_account IS NOT NULL
  AND ay.yy_account <> ''
  AND ay.yy_account NOT RLIKE '无'
  AND ay
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
       IFNULL(al.dt, '1970-01-01')            AS dt,
       al.live_days,
       al.duration,
       al.revenue / 1000                      AS revenue_rmb,
       IF(al.dt IS NULL, '无开播记录', ca.comment) AS comment
FROM warehouse.dw_paper_contract_anchor ca
         INNER JOIN warehouse.dw_bb_month_anchor_live al
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
       IF(al.dt IS NULL, '无开播记录', ca.comment) AS comment
FROM warehouse.dw_paper_contract_anchor ca
         INNER JOIN warehouse.dw_now_month_anchor_live al
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
       al.dt,
       al.live_days,
       al.duration,
       al.revenue / 1000 * 2                  AS revenue_rmb,
       IF(al.dt IS NULL, '无开播记录', ca.comment) AS comment
FROM warehouse.dw_paper_contract_anchor ca
         INNER JOIN (SELECT dt,
                            anchor_no,
                            SUM(live_days)          AS live_days,
                            SUM(duration)           AS duration,
                            SUM(anchor_bluediamond) AS revenue
                     FROM warehouse.dw_yy_month_anchor_live
                     GROUP BY dt, anchor_no) al
                    ON ca.anchor_no = al.anchor_no AND DATE_FORMAT(ca.contract_start_date, '%Y-%m-01') <= al.dt
WHERE ca.platform_name = 'YY'
;

