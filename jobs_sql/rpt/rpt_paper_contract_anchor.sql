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
WHERE al.contract_start_date >= '{month}'
;



