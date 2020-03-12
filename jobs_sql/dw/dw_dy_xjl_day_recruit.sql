use warehouse;
delete
from dw_dy_xjl_day_anchor_live_recruit
where dt BETWEEN '{start_date}' AND '{end_date}';
insert into dw_dy_xjl_day_anchor_live_recruit
SELECT dt,
       record_date,
       first_live_date,
       recruit_team,
       cat,
       level,
       salary,
       salary_duration,
       commission_rate,
       recruit_staff,
       opertion_staff,
       backend_account_id,
       guild_name,
       t1.anchor_uid,
       anchor_short_id,
       anchor_no,
       anchor_nick_name,
       last_live_time,
       follower_count,
       total_diamond,
       live_status,
       duration,
       revenue,
       aweme_cnt,
       live_revenue,
       prop_revenue,
       act_revenue,
       fan_rise,
       signing_type,
       signing_time,
       sign_time,
       anchor_settle_rate,
       gender,
       agent_id,
       agent_name,
       logo,
       notes,
       anchor_income,
       guild_income,
       min_live_dt,
       min_sign_dt,
       newold_state,
       month_duration,
       month_live_days,
       active_state,
       month_revenue,
       revenue_level
FROM dw_dy_xjl_day_anchor_live t1

         LEFT OUTER JOIN
     ods_xjl_anchor_ref t2
     ON t1.anchor_no = t2.unique_id

         LEFT OUTER JOIN
     (SELECT anchor_uid, MIN(dt) AS first_live_date
      FROM dw_dy_xjl_day_anchor_live
      WHERE duration > 3600
      GROUP BY anchor_uid) AS first_live
     ON t1.anchor_uid = first_live.anchor_uid

         LEFT OUTER JOIN
     (SELECT author_id, DATE(create_time) AS aweme_create_date, COUNT(*) AS aweme_cnt
      FROM aweme.aweme
      where create_time BETWEEN '{start_date}' AND '{end_date} 23:59:59'
      GROUP BY author_id, DATE(create_time)) AS aweme
     ON t1.anchor_uid = aweme.author_id
         AND t1.dt = aweme.aweme_create_date
where t1.dt BETWEEN '{start_date}' AND '{end_date}'