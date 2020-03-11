-- 计算主播累计开播月数，累计流水
DELETE
FROM stage.stage_paper_contract_anchor_month_live
WHERE dt = '{month}';
INSERT INTO stage.stage_paper_contract_anchor_month_live
SELECT '{month}'                               AS dt,
       platform_name,
       id_card,
       anchor_no,
       COUNT(DISTINCT DATE_FORMAT(dt, '%Y%m')) AS month_cnt,
       SUM(revenue_rmb)                        AS revenue
FROM warehouse.dw_paper_contract_anchor_month_live
WHERE dt <= '{month}'
GROUP BY platform_name,
         id_card,
         anchor_no
;


DELETE
FROM bireport.rpt_paper_contract_anchor
WHERE dt = '{month}';
INSERT INTO bireport.rpt_paper_contract_anchor
-- EXPLAIN
SELECT '{month}'                                                                 AS dt,
       al.platform_name,
       al.id_card,
       IF((al.anchor_no = '' OR al.anchor_no = '无'), '未知', al.anchor_no)         AS anchor_no,
       al.real_name,
       al.contract_start_date,
       al.contract_end_date,
       al1.dt                                                                    AS dt_t3,
       al1.live_days                                                             AS live_days_t3,
       al1.revenue_rmb                                                           AS revenue_t3,
       DATEDIFF(LAST_DAY(al1.dt), al1.dt) + 1 - al1.live_days                    AS unlive_days_t3,

       al2.dt                                                                    AS dt_t2,
       al2.live_days                                                             AS live_days_t2,
       al2.revenue_rmb                                                           AS revenue_rmb_t2,
       DATEDIFF(LAST_DAY(al2.dt), al2.dt) + 1 - al2.live_days                    AS unlive_days_t2,

       al3.dt                                                                    AS dt_t1,
       al3.live_days                                                             AS live_days_t1,
       al3.revenue_rmb                                                           AS revenue_rmb_t1,
       DATEDIFF(LAST_DAY(al3.dt), al3.dt) + 1 - al3.live_days                    AS unlive_days_t1,

       al4.dt                                                                    AS dt_t,
       al4.live_days                                                             AS live_days_t,
       al4.revenue_rmb                                                           AS revenue_rmb_t,
       DATEDIFF(CASE
                    WHEN DATE_FORMAT('{cur_date}', '%Y-%m-01') = '{month}' THEN '{cur_date}' -- 判断是否当前月，当月数据以t-1计算
                    ELSE LAST_DAY('{month}') END, '{month}') + 1 - al4.live_days AS unlive_days_t,
       CASE
           WHEN (DATEDIFF(CASE
                              WHEN DATE_FORMAT('{cur_date}', '%Y-%m-01') = '{month}' THEN '{cur_date}'
                              ELSE LAST_DAY('{month}') END, '{month}') + 1 - al4.live_days) >= 10 THEN '开播异常'
           ELSE '' END                                                           AS live_comment,
       aml.month_cnt                                                             AS month_cnt_t,
       ROUND(aml.revenue, 0)                                                     AS revenue_t
# SELECT *
FROM (SELECT DISTINCT al.platform_name,
                      al.id_card,
                      al.anchor_no,
                      al.real_name,
                      al.contract_start_date,
                      al.contract_end_date
      FROM warehouse.dw_paper_contract_anchor_month_live al
--       WHERE al.dt <> '1970-01-01'
--         AND al.contract_start_date >= al.dt
--         AND al.contract_start_date < '{month}' + INTERVAL 1 MONTH
     ) al
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
         LEFT JOIN stage.stage_paper_contract_anchor_month_live aml
                   ON al4.dt = aml.dt AND al4.platform_name = aml.platform_name AND aml.id_card = al.id_card AND
                      aml.anchor_no = al.anchor_no
;

