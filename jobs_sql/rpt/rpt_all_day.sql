REPLACE INTO bireport.rpt_day_all_new (dt, platform, revenue_level, newold_state, active_state, anchor_cnt, live_cnt,
                                       duration, revenue, guild_income, anchor_income)
SELECT *
FROM (SELECT dt,
             IFNULL(platform, 'all')      AS platform,
             IFNULL(revenue_level, 'all') AS revenue_level,
             IFNULL(newold_state, 'all')  AS newold_state,
             IFNULL(active_state, 'all')  AS active_state,
             SUM(anchor_cnt)              AS anchor_cnt,
             SUM(live_cnt)                AS live_cnt,
             SUM(duration)                AS duration,
             SUM(revenue)                 AS revenue,
             SUM(guild_income)            AS guild_income,
             SUM(anchor_income)           AS anchor_income
      FROM bireport.rpt_day_all_new
      WHERE (revenue_level != 'all' OR newold_state != 'all' OR active_state != 'all')
        AND dt BETWEEN '{start_date}' AND '{end_date}'
      GROUP BY dt, platform, revenue_level, newold_state, active_state
      WITH ROLLUP

      UNION

      SELECT dt,
             IFNULL(platform, 'all')      AS platform,
             IFNULL(revenue_level, 'all') AS revenue_level,
             IFNULL(newold_state, 'all')  AS newold_state,
             IFNULL(active_state, 'all')  AS active_state,
             SUM(anchor_cnt)              AS anchor_cnt,
             SUM(live_cnt)                AS live_cnt,
             SUM(duration)                AS duration,
             SUM(revenue)                 AS revenue,
             SUM(guild_income)            AS guild_income,
             SUM(anchor_income)           AS anchor_income
      FROM bireport.rpt_day_all_new
      WHERE (revenue_level != 'all' OR newold_state != 'all' OR active_state != 'all')
        AND dt BETWEEN '{start_date}' AND '{end_date}'
      GROUP BY dt, revenue_level, newold_state, active_state, platform
      WITH ROLLUP

      UNION

      SELECT dt,
             IFNULL(platform, 'all')      AS platform,
             IFNULL(revenue_level, 'all') AS revenue_level,
             IFNULL(newold_state, 'all')  AS newold_state,
             IFNULL(active_state, 'all')  AS active_state,
             SUM(anchor_cnt)              AS anchor_cnt,
             SUM(live_cnt)                AS live_cnt,
             SUM(duration)                AS duration,
             SUM(revenue)                 AS revenue,
             SUM(guild_income)            AS guild_income,
             SUM(anchor_income)           AS anchor_income
      FROM bireport.rpt_day_all_new
      WHERE (revenue_level != 'all' OR newold_state != 'all' OR active_state != 'all')
        AND dt BETWEEN '{start_date}' AND '{end_date}'
      GROUP BY dt, newold_state, active_state, platform, revenue_level
      WITH ROLLUP

      UNION

      SELECT dt,
             IFNULL(platform, 'all')      AS platform,
             IFNULL(revenue_level, 'all') AS revenue_level,
             IFNULL(newold_state, 'all')  AS newold_state,
             IFNULL(active_state, 'all')  AS active_state,
             SUM(anchor_cnt)              AS anchor_cnt,
             SUM(live_cnt)                AS live_cnt,
             SUM(duration)                AS duration,
             SUM(revenue)                 AS revenue,
             SUM(guild_income)            AS guild_income,
             SUM(anchor_income)           AS anchor_income
      FROM bireport.rpt_day_all_new
      WHERE (revenue_level != 'all' OR newold_state != 'all' OR active_state != 'all')
        AND dt BETWEEN '{start_date}' AND '{end_date}'
      GROUP BY dt, newold_state, active_state, platform, revenue_level
      WITH ROLLUP

      UNION

      SELECT dt,
             IFNULL(platform, 'all')      AS platform,
             IFNULL(revenue_level, 'all') AS revenue_level,
             IFNULL(newold_state, 'all')  AS newold_state,
             IFNULL(active_state, 'all')  AS active_state,
             SUM(anchor_cnt)              AS anchor_cnt,
             SUM(live_cnt)                AS live_cnt,
             SUM(duration)                AS duration,
             SUM(revenue)                 AS revenue,
             SUM(guild_income)            AS guild_income,
             SUM(anchor_income)           AS anchor_income
      FROM bireport.rpt_day_all_new
      WHERE (revenue_level != 'all' OR newold_state != 'all' OR active_state != 'all')
        AND dt BETWEEN '{start_date}' AND '{end_date}'
      GROUP BY dt, newold_state, platform, revenue_level, platform, active_state
      WITH ROLLUP
     ) t
WHERE t.dt IS NOT NULL
;


-- 报表用，计算上周、上月同期数据---
REPLACE INTO bireport.rpt_day_all_new_view
SELECT t1.dt,
       t1.platform,
       t1.revenue_level,
       t1.newold_state,
       t1.active_state,
       t1.anchor_cnt,
       t2.anchor_cnt                                                   AS anchor_cnt_lastweek,
       t3.anchor_cnt                                                   AS anchor_cnt_lastmonth,
       t1.live_cnt,
       t2.live_cnt                                                     AS live_cnt_lastweek,
       t3.live_cnt                                                     AS live_cnt_lastmonth,
       IF(t1.anchor_cnt > 0, ROUND(t1.live_cnt / t1.anchor_cnt, 3), 0) AS live_ratio,
       IF(t2.anchor_cnt > 0, ROUND(t2.live_cnt / t2.anchor_cnt, 3), 0) AS live_ratio_lastweek,
       IF(t3.anchor_cnt > 0, ROUND(t3.live_cnt / t3.anchor_cnt, 3), 0) AS live_ratio_lastmonth,
       ROUND(t1.duration / 3600, 1)                                    AS duration,
       ROUND(t2.duration / 3600, 1)                                    AS duration_lastweek,
       ROUND(t3.duration / 3600, 1)                                    AS duration_lastmonth,
       t1.revenue,
       t2.revenue                                                      AS revenue_lastweek,
       t3.revenue                                                      AS revenue_lastmonth,
       IF(t1.live_cnt > 0, ROUND(t1.revenue / t1.live_cnt, 0), 0)      AS revenue_per_live,
       IF(t2.live_cnt > 0, ROUND(t2.revenue / t2.live_cnt, 0), 0)      AS revenue_per_live_lastweek,
       IF(t3.live_cnt > 0, ROUND(t3.revenue / t3.live_cnt, 0), 0)      AS revenue_per_live_lastmonth
FROM bireport.rpt_day_all_new t1
         LEFT JOIN bireport.rpt_day_all_new t2
                   ON t1.dt - INTERVAL 7 DAY = t2.dt
                       AND t1.platform = t2.platform
                       AND t1.revenue_level = t2.revenue_level
                       AND t1.newold_state = t2.newold_state
                       AND t1.active_state = t2.active_state
         LEFT JOIN bireport.rpt_day_all_new t3
                   ON t1.dt - INTERVAL 1 MONTH = t3.dt
                       AND t1.platform = t3.platform
                       AND t1.revenue_level = t3.revenue_level
                       AND t1.newold_state = t3.newold_state
                       AND t1.active_state = t3.active_state
WHERE t1.dt BETWEEN '{start_date}' AND '{end_date}'
;

