SET NAMES utf8;
USE bireport;
DELETE
FROM rpt_day_dy_xjl_recruit;
INSERT INTO rpt_day_dy_xjl_recruit
SELECT MIN(dt)     AS dt,
       xjl_week,
       recruit_team,
       CONCAT('<br><img title="\\u7d2f\\u8ba1\\u4e3b\\u64ad\\u6570" style="height:22px" src="/images/181548.svg" />:', MAX(anchor_cnt),
              '<br><img title="\\u603b\\u65b0\\u5f00\\u64ad" style="height:22px" src="/images/new_black.svg" />:', SUM(new_live_cnt),
              '<br><img title="\\u65e5\\u5747\\u5f00\\u64ad" style="height:22px" src="/images/181537.svg"  />:', ROUND(AVG(live_cnt), 0),
              '<br><img title="\\u603b\\u77ed\\u89c6\\u9891" style="height:22px" src="/images/181538.svg"  />:', SUM(aweme_cnt),
              '<br><img title="\\u603b\\u6d41\\u6c34" style="height:22px" src="/images/181569.svg"  />:', ROUND(SUM(revenue) / 10, 0))
                   AS total,
       MAX(IF(xjl_day_of_week = 0,
              CONCAT(RIGHT(dt, 5), '<br>', '<img title="\\u7d2f\\u8ba1\\u4e3b\\u64ad\\u6570" style="height:22px" src="/images/181548.svg"  />:',
                     anchor_cnt, '<br><img title="\\u65b0\\u5f00\\u64ad" style="height:22px" src="/images/new_black.svg" />:', new_live_cnt,
                     '<br><img title="\\u5f00\\u64ad\\u6570" style="height:22px" src="/images/181537.svg"  />:', live_cnt,
                     '<br><img title="\\u77ed\\u89c6\\u9891" style="height:22px" src="/images/181538.svg"  />:', aweme_cnt,
                     '<br><img title="\\u6d41\\u6c34" style="height:22px" src="/images/181569.svg"  />:', ROUND(revenue / 10, 0)),
              '')) AS day1,
       MAX(IF(xjl_day_of_week = 1,
              CONCAT(RIGHT(dt, 5), '<br>', '<img title="\\u7d2f\\u8ba1\\u4e3b\\u64ad\\u6570" style="height:22px" src="/images/181548.svg"  />:',
                     anchor_cnt, '<br><img title="\\u65b0\\u5f00\\u64ad" style="height:22px" src="/images/new_black.svg"  />:', new_live_cnt,
                     '<br><img title="\\u5f00\\u64ad\\u6570" style="height:22px" src="/images/181537.svg"  />:', live_cnt,
                     '<br><img title="\\u77ed\\u89c6\\u9891" style="height:22px" src="/images/181538.svg"  />:', aweme_cnt,
                     '<br><img title="\\u6d41\\u6c34" style="height:22px" src="/images/181569.svg"  />:', ROUND(revenue / 10, 0)),
              '')) AS day2,
       MAX(IF(xjl_day_of_week = 2,
              CONCAT(RIGHT(dt, 5), '<br>', '<img title="\\u7d2f\\u8ba1\\u4e3b\\u64ad\\u6570" style="height:22px" src="/images/181548.svg"  />:',
                     anchor_cnt, '<br><img title="\\u65b0\\u5f00\\u64ad" style="height:22px" src="/images/new_black.svg"  />:', new_live_cnt,
                     '<br><img title="\\u5f00\\u64ad\\u6570" style="height:22px" src="/images/181537.svg"  />:', live_cnt,
                     '<br><img title="\\u77ed\\u89c6\\u9891" style="height:22px" src="/images/181538.svg"  />:', aweme_cnt,
                     '<br><img title="\\u6d41\\u6c34" style="height:22px" src="/images/181569.svg"  />:', ROUND(revenue / 10, 0)),
              '')) AS day3,
       MAX(IF(xjl_day_of_week = 3,
              CONCAT(RIGHT(dt, 5), '<br>', '<img title="\\u7d2f\\u8ba1\\u4e3b\\u64ad\\u6570" style="height:22px" src="/images/181548.svg"  />:',
                     anchor_cnt, '<br><img title="\\u65b0\\u5f00\\u64ad" style="height:22px" src="/images/new_black.svg"  />:', new_live_cnt,
                     '<br><img title="\\u5f00\\u64ad\\u6570" style="height:22px" src="/images/181537.svg"  />:', live_cnt,
                     '<br><img title="\\u77ed\\u89c6\\u9891" style="height:22px" src="/images/181538.svg"  />:', aweme_cnt,
                     '<br><img title="\\u6d41\\u6c34" style="height:22px" src="/images/181569.svg"  />:', ROUND(revenue / 10, 0)),
              '')) AS day4,
       MAX(IF(xjl_day_of_week = 4,
              CONCAT(RIGHT(dt, 5), '<br>', '<img title="\\u7d2f\\u8ba1\\u4e3b\\u64ad\\u6570" style="height:22px" src="/images/181548.svg"  />:',
                     anchor_cnt, '<br><img title="\\u65b0\\u5f00\\u64ad" style="height:22px" src="/images/new_black.svg"  />:', new_live_cnt,
                     '<br><img title="\\u5f00\\u64ad\\u6570" style="height:22px" src="/images/181537.svg"  />:', live_cnt,
                     '<br><img title="\\u77ed\\u89c6\\u9891" style="height:22px" src="/images/181538.svg"  />:', aweme_cnt,
                     '<br><img title="\\u6d41\\u6c34" style="height:22px" src="/images/181569.svg"  />:', ROUND(revenue / 10, 0)),
              '')) AS day5,
       MAX(IF(xjl_day_of_week = 5,
              CONCAT(RIGHT(dt, 5), '<br>', '<img title="\\u7d2f\\u8ba1\\u4e3b\\u64ad\\u6570" style="height:22px" src="/images/181548.svg"  />:',
                     anchor_cnt, '<br><img title="\\u65b0\\u5f00\\u64ad" style="height:22px" src="/images/new_black.svg"  />:', new_live_cnt,
                     '<br><img title="\\u5f00\\u64ad\\u6570" style="height:22px" src="/images/181537.svg"  />:', live_cnt,
                     '<br><img title="\\u77ed\\u89c6\\u9891" style="height:22px" src="/images/181538.svg"  />:', aweme_cnt,
                     '<br><img title="\\u6d41\\u6c34" style="height:22px" src="/images/181569.svg"  />:', ROUND(revenue / 10, 0)),
              '')) AS day6,
       MAX(IF(xjl_day_of_week = 6,
              CONCAT(RIGHT(dt, 5), '<br>', '<img title="\\u7d2f\\u8ba1\\u4e3b\\u64ad\\u6570" style="height:22px" src="/images/181548.svg"  />:',
                     anchor_cnt, '<br><img title="\\u65b0\\u5f00\\u64ad" style="height:22px" src="/images/new_black.svg"  />:', new_live_cnt,
                     '<br><img title="\\u5f00\\u64ad\\u6570" style="height:22px" src="/images/181537.svg"  />:', live_cnt,
                     '<br><img title="" style="height:22px" src="/images/181538.svg"  />:', aweme_cnt,
                     '<br><img title="\\u6d41\\u6c34" style="height:22px" src="/images/181569.svg"  />:', ROUND(revenue / 10, 0)),
              '')) AS day7
FROM (SELECT dt,
             WEEK(dt - INTERVAL 5 DAY, 0)        AS xjl_week,
             WEEKDAY(dt - INTERVAL 4 DAY)        AS xjl_day_of_week,
             IFNULL(recruit_team, '未知')          AS recruit_team,
             COUNT(anchor_uid)                   AS anchor_cnt,
             SUM(IF(duration > 3600, 1, 0))      AS live_cnt,
             SUM(IF(first_live_date = dt, 1, 0)) AS new_live_cnt,
             SUM(IFNULL(aweme_cnt, 0))           AS aweme_cnt,
             SUM(IFNULL(revenue, 0))             AS revenue
      FROM warehouse.dw_dy_xjl_day_anchor_live_recruit
      GROUP BY recruit_team, dt) t
GROUP BY xjl_week, recruit_team;


DELETE
FROM rpt_day_dy_xjl_recruit_compare;

INSERT INTO rpt_day_dy_xjl_recruit_compare
SELECT dt,
       '\\u65b0\\u5f00\\u64ad'                                           AS idx,
       SUM(IF(recruit_team = '招募1组', new_live_cnt, 0)) AS team1,
       SUM(IF(recruit_team = '招募2组', new_live_cnt, 0)) AS team2,
       SUM(IF(recruit_team = '招募3组', new_live_cnt, 0)) AS team3,
       SUM(IF(recruit_team = '招募4组', new_live_cnt, 0)) AS team4,
       SUM(IF(recruit_team = '招募5组', new_live_cnt, 0)) AS team5,
       SUM(IF(recruit_team = '招募6组', new_live_cnt, 0)) AS team6,
       SUM(IF(recruit_team = '招募7组', new_live_cnt, 0)) AS team7,
       SUM(IF(recruit_team = '未知', new_live_cnt, 0))   AS unknow
FROM (SELECT dt,
             IFNULL(recruit_team, '未知')          AS recruit_team,
             COUNT(anchor_uid)                   AS anchor_cnt,
             SUM(IF(duration > 3600, 1, 0))      AS live_cnt,
             SUM(IF(first_live_date = dt, 1, 0)) AS new_live_cnt,
             SUM(IFNULL(aweme_cnt, 0))           AS aweme_cnt,
             SUM(IFNULL(revenue, 0))             AS revenue
      FROM warehouse.dw_dy_xjl_day_anchor_live_recruit
      GROUP BY recruit_team, dt) t
GROUP BY dt;

INSERT INTO rpt_day_dy_xjl_recruit_compare
SELECT dt,
       '\\u6d41\\u6c34'                                                      AS idx,
       SUM(IF(recruit_team = '招募1组', ROUND(revenue / 10, 0), 0)) AS team1,
       SUM(IF(recruit_team = '招募2组', ROUND(revenue / 10, 0), 0)) AS team2,
       SUM(IF(recruit_team = '招募3组', ROUND(revenue / 10, 0), 0)) AS team3,
       SUM(IF(recruit_team = '招募4组', ROUND(revenue / 10, 0), 0)) AS team4,
       SUM(IF(recruit_team = '招募5组', ROUND(revenue / 10, 0), 0)) AS team5,
       SUM(IF(recruit_team = '招募6组', ROUND(revenue / 10, 0), 0)) AS team6,
       SUM(IF(recruit_team = '招募7组', ROUND(revenue / 10, 0), 0)) AS team7,
       SUM(IF(recruit_team = '未知', ROUND(revenue / 10, 0), 0))   AS unknow
FROM (SELECT dt,
             IFNULL(recruit_team, '未知')          AS recruit_team,
             COUNT(anchor_uid)                   AS anchor_cnt,
             SUM(IF(duration > 3600, 1, 0))      AS live_cnt,
             SUM(IF(first_live_date = dt, 1, 0)) AS new_live_cnt,
             SUM(IFNULL(aweme_cnt, 0))           AS aweme_cnt,
             SUM(IFNULL(revenue, 0))             AS revenue
      FROM warehouse.dw_dy_xjl_day_anchor_live_recruit
      GROUP BY recruit_team, dt) t
GROUP BY dt;

INSERT INTO rpt_day_dy_xjl_recruit_compare
SELECT dt,
       '\\u77ed\\u89c6\\u9891数'                                       AS idx,
       SUM(IF(recruit_team = '招募1组', aweme_cnt, 0)) AS team1,
       SUM(IF(recruit_team = '招募2组', aweme_cnt, 0)) AS team2,
       SUM(IF(recruit_team = '招募3组', aweme_cnt, 0)) AS team3,
       SUM(IF(recruit_team = '招募4组', aweme_cnt, 0)) AS team4,
       SUM(IF(recruit_team = '招募5组', aweme_cnt, 0)) AS team5,
       SUM(IF(recruit_team = '招募6组', aweme_cnt, 0)) AS team6,
       SUM(IF(recruit_team = '招募7组', aweme_cnt, 0)) AS team7,
       SUM(IF(recruit_team = '未知', aweme_cnt, 0))   AS unknow
FROM (SELECT dt,
             IFNULL(recruit_team, '未知')          AS recruit_team,
             COUNT(anchor_uid)                   AS anchor_cnt,
             SUM(IF(duration > 3600, 1, 0))      AS live_cnt,
             SUM(IF(first_live_date = dt, 1, 0)) AS new_live_cnt,
             SUM(IFNULL(aweme_cnt, 0))           AS aweme_cnt,
             SUM(IFNULL(revenue, 0))             AS revenue
      FROM warehouse.dw_dy_xjl_day_anchor_live_recruit
      GROUP BY recruit_team, dt) t
GROUP BY dt;



DELETE
FROM bireport.rpt_day_dy_xjl_recruit_detail;
INSERT INTO bireport.rpt_day_dy_xjl_recruit_detail
SELECT dt,
       record_date,
       first_live_date,
       IFNULL(recruit_team, '未知') AS recruit_team,
       cat,
       level,
       salary,
       salary_duration,
       commission_rate,
       recruit_staff,
       opertion_staff,
       backend_account_id,
       guild_name,
       anchor_uid,
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
FROM warehouse.dw_dy_xjl_day_anchor_live_recruit;