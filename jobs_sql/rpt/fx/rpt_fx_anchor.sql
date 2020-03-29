-- 主播数据 ---
DELETE
FROM bireport.rpt_day_fx_anchor
WHERE dt >= '{month}'
  AND dt <= LAST_DAY('{month}');
INSERT INTO bireport.rpt_day_fx_anchor
SELECT al.dt,
       al.backend_account_id,
       al.min_live_dt                   AS first_live_date,
       al.min_sign_dt                   AS sign_date,
       al.newold_state,
       al1.duration / 3600              AS duration_lastmonth,
       al1.live_days                    AS live_days_lastmonth,
       al.active_state,
       al1.revenue                      AS revenue_lastmonth,
       al.revenue_level,
       al.anchor_no,
       al.anchor_nick_name,
       al.duration / 3600               AS duration,
       IF(al.live_status = 1, '是', '否') AS live_status,
       al.anchor_income / 125 / 0.4     AS revenue
FROM warehouse.dw_fx_day_anchor_live al
         LEFT JOIN warehouse.dw_fx_month_anchor_live al1
                   ON al1.dt = DATE_FORMAT(al.dt - INTERVAL 1 MONTH, '%Y-%m-01') AND
                      al.backend_account_id = al1.backend_account_id AND
                      al.anchor_no = al1.anchor_no
WHERE al.dt >= '{month}'
  AND al.dt <= LAST_DAY('{month}')
;
