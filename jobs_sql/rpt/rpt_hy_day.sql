-- DELETE
-- FROM bireport.rpt_day_all
-- WHERE platform = '虎牙'
--   AND dt BETWEEN '{start_date}' AND '{end_date}';
-- INSERT INTO bireport.rpt_day_all
-- SELECT t.dt,
--        t.platform,
--        CASE WHEN t.dt <= '2019-11-31' THEN t.anchor_cnt ELSE al.anchor_cnt END AS anchor_cnt,
--        CASE WHEN t.dt <= '2019-11-31' THEN t.live_cnt ELSE al.live_cnt END     AS live_cnt,
--        t.revenue,
--        t.guild_income,
--        t.anchor_income
-- FROM (
--          SELECT t.dt,
--                 t.platform_name                                                           AS platform,
--                 SUM(t.sign_count)                                                         AS anchor_cnt,
--                 SUM(t.live_cnt)                                                           AS live_cnt,
--                 SUM(t.revenue)                                                            AS revenue,
--                 SUM(t.gift_income + t.guard_income + t.noble_income) / 1000               AS guild_income,
--                 SUM(t.gift_income + t.guard_income + t.noble_income) * 0.7 / (0.3 * 1000) AS anchor_income
--          FROM warehouse.dw_huya_day_guild_live_true t
--          WHERE dt BETWEEN '{start_date}' AND '{end_date}'
--          GROUP BY t.dt,
--                   t.platform_name) t
--          LEFT JOIN (SELECT dt,
--                            COUNT(DISTINCT t.anchor_uid)                                       AS anchor_cnt,
--                            COUNT(DISTINCT
--                                  CASE WHEN t.live_status = 1 THEN t.anchor_uid ELSE NULL END) AS live_cnt,
--                            SUM(t.revenue)                                                     AS anchor_income
--                     FROM warehouse.dw_huya_day_anchor_live t
--                     WHERE dt BETWEEN '{start_date}' AND '{end_date}'
--                     GROUP BY dt) al ON al.dt = t.dt
-- ;
-- 
-- 
-- DELETE
-- FROM bireport.rpt_day_hy_guild
-- WHERE dt BETWEEN '{start_date}' AND '{end_date}';
-- INSERT INTO bireport.rpt_day_hy_guild
-- SELECT t.dt,
--        t.platform_id,
--        t.platform_name                                                         AS platform,
--        t.channel_num,
--        CASE WHEN t.dt <= '2019-11-31' THEN t.sign_count ELSE al.anchor_cnt END AS anchor_cnt,
--        CASE WHEN t.dt <= '2019-11-31' THEN t.live_cnt ELSE al.live_cnt END     AS live_cnt,
--        t.revenue,
--        t.revenue                                                               AS revenue_orig,
--        (t.gift_income + t.guard_income + t.noble_income) / 1000                AS guild_income,
--        t.gift_income + t.guard_income + t.noble_income                         AS guild_income_orig,
--        (t.gift_income + t.guard_income + t.noble_income) * 0.7 / (0.3 * 1000)  AS anchor_income,
--        t.gift_income + t.guard_income + t.noble_income                         AS anchor_incom_orig
-- FROM warehouse.dw_huya_day_guild_live_true t
--          LEFT JOIN (SELECT dt,
--                            t.channel_id,
--                            COUNT(DISTINCT t.anchor_uid)                                       AS anchor_cnt,
--                            COUNT(DISTINCT
--                                  CASE WHEN t.live_status = 1 THEN t.anchor_uid ELSE NULL END) AS live_cnt,
--                            SUM(t.revenue)                                                     AS revenue
--                     FROM warehouse.dw_huya_day_anchor_live t
--                     WHERE dt BETWEEN '{start_date}' AND '{end_date}'
--                     GROUP BY dt,
--                              t.channel_id) al ON al.dt = t.dt AND al.channel_id = t.channel_id
-- WHERE t.dt BETWEEN '{start_date}' AND '{end_date}'
-- ;
-- 
-- 
-- -- 补充汇总数据
-- REPLACE INTO bireport.rpt_day_hy_guild
-- (dt, channel_num, anchor_cnt, live_cnt, revenue, guild_income, anchor_income)
-- SELECT dt,
--        'all' AS channel_num,
--        anchor_cnt,
--        live_cnt,
--        revenue,
--        guild_income,
--        anchor_income
-- FROM bireport.rpt_day_all
-- WHERE platform = '虎牙'
--   AND dt BETWEEN '{start_date}' AND '{end_date}'
-- ;


-- rpt_day_hy_guild_new
DELETE
FROM bireport.rpt_day_hy_guild
WHERE dt BETWEEN '{start_date}' AND '{end_date}';
INSERT INTO bireport.rpt_day_hy_guild
SELECT dt,
       channel_type,
       channel_num,
       revenue_level,
       newold_state,
       active_state,
       COUNT(DISTINCT anchor_uid)                                              AS anchor_cnt,
       COUNT(DISTINCT CASE WHEN live_status = 1 THEN anchor_uid ELSE NULL END) AS live_cnt,
       SUM(IFNULL(duration, 0))                                                AS duration,
       SUM(IFNULL(revenue, 0))                                                 AS revenue
FROM warehouse.dw_huya_day_anchor_live
WHERE dt BETWEEN '{start_date}' AND '{end_date}'
GROUP BY dt,
         channel_type,
         channel_num,
         revenue_level,
         newold_state,
         active_state
;



REPLACE INTO bireport.rpt_day_hy_guild (dt, channel_type, channel_num, revenue_level,
                                        newold_state, active_state, anchor_cnt, live_cnt, duration, revenue)
SELECT *
FROM (SELECT dt,
             IFNULL(channel_type, 'all')  AS channel_type,
             IFNULL(channel_num, 'all')   AS channel_num,
             IFNULL(revenue_level, 'all') AS revenue_level,
             IFNULL(newold_state, 'all')  AS newold_state,
             IFNULL(active_state, 'all')  AS active_state,
             SUM(anchor_cnt)              AS anchor_cnt,
             SUM(live_cnt)                AS live_cnt,
             SUM(duration)                AS duration,
             SUM(revenue)                 AS revenue
      FROM bireport.rpt_day_hy_guild
      WHERE channel_type != 'all' AND channel_num != 'all' AND revenue_level != 'all' AND newold_state != 'all'
         AND active_state != 'all'
          AND dt BETWEEN '{start_date}' AND '{end_date}'
      GROUP BY dt, channel_type, channel_num, revenue_level, newold_state, active_state
      WITH ROLLUP

      UNION

      SELECT dt,
             IFNULL(channel_type, 'all')  AS channel_type,
             IFNULL(channel_num, 'all')   AS channel_num,
             IFNULL(revenue_level, 'all') AS revenue_level,
             IFNULL(newold_state, 'all')  AS newold_state,
             IFNULL(active_state, 'all')  AS active_state,
             SUM(anchor_cnt)              AS anchor_cnt,
             SUM(live_cnt)                AS live_cnt,
             SUM(duration)                AS duration,
             SUM(revenue)                 AS revenue
      FROM bireport.rpt_day_hy_guild
      WHERE channel_type != 'all' AND channel_num != 'all' AND revenue_level != 'all' AND newold_state != 'all'
         AND active_state != 'all'
          AND dt BETWEEN '{start_date}' AND '{end_date}'
      GROUP BY dt, channel_num, revenue_level, newold_state, active_state, channel_type
      WITH ROLLUP

      UNION

      SELECT dt,
             IFNULL(channel_type, 'all')  AS channel_type,
             IFNULL(channel_num, 'all')   AS channel_num,
             IFNULL(revenue_level, 'all') AS revenue_level,
             IFNULL(newold_state, 'all')  AS newold_state,
             IFNULL(active_state, 'all')  AS active_state,
             SUM(anchor_cnt)              AS anchor_cnt,
             SUM(live_cnt)                AS live_cnt,
             SUM(duration)                AS duration,
             SUM(revenue)                 AS revenue
      FROM bireport.rpt_day_hy_guild
      WHERE channel_type != 'all' AND channel_num != 'all' AND revenue_level != 'all' AND newold_state != 'all'
         AND active_state != 'all'
          AND dt BETWEEN '{start_date}' AND '{end_date}'
      GROUP BY dt, revenue_level, newold_state, active_state, channel_type, channel_num
      WITH ROLLUP

      UNION

      SELECT dt,
             IFNULL(channel_type, 'all')  AS channel_type,
             IFNULL(channel_num, 'all')   AS channel_num,
             IFNULL(revenue_level, 'all') AS revenue_level,
             IFNULL(newold_state, 'all')  AS newold_state,
             IFNULL(active_state, 'all')  AS active_state,
             SUM(anchor_cnt)              AS anchor_cnt,
             SUM(live_cnt)                AS live_cnt,
             SUM(duration)                AS duration,
             SUM(revenue)                 AS revenue
      FROM bireport.rpt_day_hy_guild
      WHERE channel_type != 'all' AND channel_num != 'all' AND revenue_level != 'all' AND newold_state != 'all'
         AND active_state != 'all'
          AND dt BETWEEN '{start_date}' AND '{end_date}'
      GROUP BY dt, newold_state, active_state, channel_type, channel_num, revenue_level
      WITH ROLLUP

      UNION

      SELECT dt,
             IFNULL(channel_type, 'all')  AS channel_type,
             IFNULL(channel_num, 'all')   AS channel_num,
             IFNULL(revenue_level, 'all') AS revenue_level,
             IFNULL(newold_state, 'all')  AS newold_state,
             IFNULL(active_state, 'all')  AS active_state,
             SUM(anchor_cnt)              AS anchor_cnt,
             SUM(live_cnt)                AS live_cnt,
             SUM(duration)                AS duration,
             SUM(revenue)                 AS revenue
      FROM bireport.rpt_day_hy_guild
      WHERE channel_type != 'all' AND channel_num != 'all' AND revenue_level != 'all' AND newold_state != 'all'
         AND active_state != 'all'
          AND dt BETWEEN '{start_date}' AND '{end_date}'
      GROUP BY dt, active_state, channel_type, channel_num, revenue_level, newold_state
      WITH ROLLUP

      UNION

      SELECT dt,
             IFNULL(channel_type, 'all')  AS channel_type,
             IFNULL(channel_num, 'all')   AS channel_num,
             IFNULL(revenue_level, 'all') AS revenue_level,
             IFNULL(newold_state, 'all')  AS newold_state,
             IFNULL(active_state, 'all')  AS active_state,
             SUM(anchor_cnt)              AS anchor_cnt,
             SUM(live_cnt)                AS live_cnt,
             SUM(duration)                AS duration,
             SUM(revenue)                 AS revenue
      FROM bireport.rpt_day_hy_guild
      WHERE channel_type != 'all' AND channel_num != 'all' AND revenue_level != 'all' AND newold_state != 'all'
         AND active_state != 'all'
          AND dt BETWEEN '{start_date}' AND '{end_date}'
      GROUP BY dt, channel_num, channel_type, active_state, revenue_level, newold_state
      WITH ROLLUP

      UNION

      SELECT dt,
             IFNULL(channel_type, 'all')  AS channel_type,
             IFNULL(channel_num, 'all')   AS channel_num,
             IFNULL(revenue_level, 'all') AS revenue_level,
             IFNULL(newold_state, 'all')  AS newold_state,
             IFNULL(active_state, 'all')  AS active_state,
             SUM(anchor_cnt)              AS anchor_cnt,
             SUM(live_cnt)                AS live_cnt,
             SUM(duration)                AS duration,
             SUM(revenue)                 AS revenue
      FROM bireport.rpt_day_hy_guild
      WHERE channel_type != 'all' AND channel_num != 'all' AND revenue_level != 'all' AND newold_state != 'all'
         AND active_state != 'all'
          AND dt BETWEEN '{start_date}' AND '{end_date}'
      GROUP BY dt, newold_state, channel_type, channel_num, revenue_level, active_state
      WITH ROLLUP

      UNION

      SELECT dt,
             IFNULL(channel_type, 'all')  AS channel_type,
             IFNULL(channel_num, 'all')   AS channel_num,
             IFNULL(revenue_level, 'all') AS revenue_level,
             IFNULL(newold_state, 'all')  AS newold_state,
             IFNULL(active_state, 'all')  AS active_state,
             SUM(anchor_cnt)              AS anchor_cnt,
             SUM(live_cnt)                AS live_cnt,
             SUM(duration)                AS duration,
             SUM(revenue)                 AS revenue
      FROM bireport.rpt_day_hy_guild
      WHERE channel_type != 'all' AND channel_num != 'all' AND revenue_level != 'all' AND newold_state != 'all'
         AND active_state != 'all'
          AND dt BETWEEN '{start_date}' AND '{end_date}'
      GROUP BY dt, active_state, channel_type, newold_state, revenue_level, channel_num
      WITH ROLLUP
     ) t
WHERE dt IS NOT NULL
;


-- 报表用，计算上周、上月同期数据---
DELETE
FROM bireport.rpt_day_hy_guild_view
WHERE dt BETWEEN '{start_date}' AND '{end_date}';
INSERT INTO bireport.rpt_day_hy_guild_view
SELECT t1.dt,
       t1.channel_type,
       t1.channel_num,
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
FROM bireport.rpt_day_hy_guild t1
         LEFT JOIN bireport.rpt_day_hy_guild t2
                   ON t1.dt - INTERVAL 7 DAY = t2.dt
                       AND t1.channel_type = t2.channel_type
                       AND t1.channel_num = t2.channel_num
                       AND t1.revenue_level = t2.revenue_level
                       AND t1.newold_state = t2.newold_state
                       AND t1.active_state = t2.active_state
         LEFT JOIN bireport.rpt_day_hy_guild t3
                   ON t1.dt - INTERVAL 1 MONTH = t3.dt
                       AND t1.channel_type = t3.channel_type
                       AND t1.channel_num = t3.channel_num
                       AND t1.revenue_level = t3.revenue_level
                       AND t1.newold_state = t3.newold_state
                       AND t1.active_state = t3.active_state
WHERE t1.dt BETWEEN '{start_date}' AND '{end_date}';


-- 报表用，计算指标占比---
DELETE
FROM bireport.rpt_day_hy_guild_view_compare
WHERE dt BETWEEN '{start_date}' AND '{end_date}';
INSERT INTO bireport.rpt_day_hy_guild_view_compare
SELECT *
FROM (SELECT dt,
             channel_num,
             revenue_level,
             newold_state,
             active_state,
             '主播数'      AS idx,
             anchor_cnt AS val
      FROM bireport.rpt_day_hy_guild
      where revenue_level != 'all'
        AND channel_type = 'all'
        AND dt BETWEEN '{start_date}' AND '{end_date}'
      UNION
      SELECT dt,
             channel_num,
             revenue_level,
             newold_state,
             active_state,
             '开播数'    AS idx,
             live_cnt AS val
      FROM bireport.rpt_day_hy_guild
      WHERE revenue_level != 'all'
        AND channel_type = 'all'
        AND dt BETWEEN '{start_date}' AND '{end_date}'
      UNION
      SELECT dt,
             channel_num,
             revenue_level,
             newold_state,
             active_state,
             '流水'    AS idx,
             revenue AS val
      FROM bireport.rpt_day_hy_guild
      WHERE revenue_level != 'all'
        AND channel_type = 'all'
        AND dt BETWEEN '{start_date}' AND '{end_date}'
      UNION
      SELECT dt,
             channel_num,
             revenue_level,
             newold_state,
             active_state,
             '开播人均流水'                     AS idx,
             round(revenue / live_cnt, 0) AS val
      FROM bireport.rpt_day_hy_guild
      WHERE revenue_level != 'all'
        AND channel_type = 'all'
        AND dt BETWEEN '{start_date}' AND '{end_date}'
        AND live_cnt > 0) t
;


-- 主播数据 --- 
DELETE
FROM bireport.rpt_day_hy_anchor
WHERE dt BETWEEN '{start_date}' AND '{end_date}';
INSERT INTO bireport.rpt_day_hy_anchor
SELECT al.dt,
       al.channel_type,
       al.channel_num,
       al.min_live_dt                   AS first_live_date,
       al.min_sign_dt                   AS sign_date,
       al.newold_state,
       al1.month_duration / 3600        AS duration_lastmonth,
       al1.month_live_days              AS live_days_lastmonth,
       al.active_state,
       al1.month_revenue                AS revenue_lastmonth,
       al.revenue_level,
       al.anchor_uid,
       al.anchor_no,
       al.nick                          AS anchor_nick_name,
       al.duration / 3600               AS duration,
       IF(al.live_status = 1, '是', '否') AS live_status,
       al.revenue
FROM warehouse.dw_huya_day_anchor_live al
         LEFT JOIN warehouse.dw_huya_day_anchor_live al1
                   ON al1.dt = DATE_FORMAT(al.dt - INTERVAL 1 MONTH, '%Y-%m-01') AND
                      al.channel_id = al1.channel_id AND
                      al.anchor_no = al1.anchor_no
WHERE al.dt BETWEEN '{start_date}' AND '{end_date}'
;
