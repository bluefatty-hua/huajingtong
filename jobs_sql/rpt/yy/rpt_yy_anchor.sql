-- 主播数据 --- 
DELETE
FROM bireport.rpt_day_yy_anchor
WHERE dt BETWEEN '{month}' AND LAST_DAY('{month}');
INSERT INTO bireport.rpt_day_yy_anchor
SELECT al.dt,
       al.channel_num,
       al.min_live_dt                   AS first_live_date,
       al.min_sign_dt                   AS sign_date,
       al.newold_state,
       al1.month_duration / 3600        AS duration_lastmonth,
       al1.month_live_days              AS live_days_lastmonth,
       al.active_state,
       al1.month_revenue * 2 / 1000     AS revenue_lastmonth,
       al.revenue_level,
       al.anchor_uid,
       al.anchor_no,
       al.anchor_nick_name,
       al.anchor_type_text,
       al.duration / 3600               AS duration,
       IF(al.live_status = 1, '是', '否') AS live_status,
       al.bluediamond * 2 / 1000        AS revenue
FROM warehouse.dw_yy_day_anchor_live al
         LEFT JOIN warehouse.dw_yy_day_anchor_live al1
                   ON al1.dt = DATE_FORMAT(al.dt - INTERVAL 1 MONTH, '%Y-%m-01') AND
                      al.channel_num = al1.channel_num AND
                      al.anchor_no = al1.anchor_no
WHERE al.dt BETWEEN '{month}' AND LAST_DAY('{month}')