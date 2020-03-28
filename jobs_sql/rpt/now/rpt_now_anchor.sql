
-- 主播数据 ---
DELETE
FROM bireport.rpt_day_now_anchor
WHERE dt BETWEEN '{start_date}' AND '{end_date}';
INSERT INTO bireport.rpt_day_now_anchor
SELECT al.dt,
       al.backend_account_id,
       al.city,
       al.min_live_dt                   AS first_live_date,
       al.min_sign_dt                   AS sign_date,
       al.newold_state,
       al1.month_duration / 3600        AS duration_lastmonth,
       al1.month_live_days              AS live_days_lastmonth,
       al.active_state,
       al1.month_revenue                AS revenue_lastmonth,
       al.revenue_level,
       al.anchor_no                     AS anchor_uid,
       al.anchor_no,
       al.fans_cnt,
       al.fans_goup_cnt,
       al.anchor_nick_name,
       al.duration / 3600               AS duration,
       IF(al.live_status = 1, '是', '否') AS live_status,
       al.revenue_rmb                   AS revenue
FROM warehouse.dw_now_day_anchor_live al
         LEFT JOIN warehouse.dw_now_day_anchor_live al1
                   ON al1.dt = DATE_FORMAT(al.dt - INTERVAL 1 MONTH, '%Y-%m-01') AND
                      al.backend_account_id = al1.backend_account_id AND
                      al.anchor_no = al1.anchor_no
WHERE al.dt BETWEEN '{start_date}' AND '{end_date}'
;