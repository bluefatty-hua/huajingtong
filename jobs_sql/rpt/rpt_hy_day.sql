DELETE
FROM bireport.rpt_day_all
WHERE platform = '虎牙'
  AND dt BETWEEN '{start_date}' AND '{end_date}';
INSERT INTO bireport.rpt_day_all
SELECT t.dt,
       t.platform,
       CASE WHEN t.dt <= '2019-11-31' THEN t.anchor_cnt ELSE al.anchor_cnt END AS anchor_cnt,
       CASE WHEN t.dt <= '2019-11-31' THEN t.live_cnt ELSE al.live_cnt END     AS live_cnt,
       t.revenue,
       t.guild_income,
       t.anchor_income
FROM (
         SELECT t.dt,
                t.platform_name                                                           AS platform,
                SUM(t.sign_count)                                                         AS anchor_cnt,
                SUM(t.live_cnt)                                                           AS live_cnt,
                SUM(t.revenue)                                                            AS revenue,
                SUM(t.gift_income + t.guard_income + t.noble_income) / 1000               AS guild_income,
                SUM(t.gift_income + t.guard_income + t.noble_income) * 0.7 / (0.3 * 1000) AS anchor_income
         FROM warehouse.dw_huya_day_guild_live t
         WHERE dt BETWEEN '{start_date}' AND '{end_date}'
         GROUP BY t.dt,
                  t.platform_name) t
         LEFT JOIN (SELECT dt,
                           COUNT(DISTINCT t.anchor_uid)                                       AS anchor_cnt,
                           COUNT(DISTINCT
                                 CASE WHEN t.live_status = 1 THEN t.anchor_uid ELSE NULL END) AS live_cnt,
                           SUM(t.revenue)                                                     AS anchor_income
                    FROM warehouse.dw_huya_day_anchor_live t
                    WHERE dt BETWEEN '{start_date}' AND '{end_date}'
                    GROUP BY dt) al ON al.dt = t.dt
;


DELETE
FROM bireport.rpt_day_hy_guild
WHERE dt BETWEEN '{start_date}' AND '{end_date}';
INSERT INTO bireport.rpt_day_hy_guild
SELECT t.dt,
       t.platform_id,
       t.platform_name                                                         AS platform,
       t.channel_num,
       CASE WHEN t.dt <= '2019-11-31' THEN t.sign_count ELSE al.anchor_cnt END AS anchor_cnt,
       CASE WHEN t.dt <= '2019-11-31' THEN t.live_cnt ELSE al.live_cnt END     AS live_cnt,
       t.revenue,
       t.revenue                                                               AS revenue_orig,
       (t.gift_income + t.guard_income + t.noble_income) / 1000                AS guild_income,
       t.gift_income + t.guard_income + t.noble_income                         AS guild_income_orig,
       (t.gift_income + t.guard_income + t.noble_income) * 0.7 / (0.3 * 1000)  AS anchor_income,
       t.gift_income + t.guard_income + t.noble_income                         AS anchor_incom_orig
FROM warehouse.dw_huya_day_guild_live t
         LEFT JOIN (SELECT dt,
                           t.channel_id,
                           COUNT(DISTINCT t.anchor_uid)                                       AS anchor_cnt,
                           COUNT(DISTINCT
                                 CASE WHEN t.live_status = 1 THEN t.anchor_uid ELSE NULL END) AS live_cnt,
                           SUM(t.revenue)                                                     AS revenue
                    FROM warehouse.dw_huya_day_anchor_live t
                    WHERE dt BETWEEN '{start_date}' AND '{end_date}'
                    GROUP BY dt,
                             t.channel_id) al ON al.dt = t.dt AND al.channel_id = t.channel_id
WHERE t.dt BETWEEN '{start_date}' AND '{end_date}'
;


-- 补充汇总数据
REPLACE INTO bireport.rpt_day_hy_guild
(dt, channel_num, anchor_cnt, live_cnt, revenue, guild_income, anchor_income)
SELECT dt,
       'all' AS channel_num,
       anchor_cnt,
       live_cnt,
       revenue,
       guild_income,
       anchor_income
FROM bireport.rpt_day_all
WHERE platform = '虎牙'
  AND dt BETWEEN '{start_date}' AND '{end_date}'
;


-- rpt_day_hy_guild_new
DELETE
FROM bireport.rpt_day_hy_guild_new
WHERE dt BETWEEN '{start_date}' AND '{end_date}';
INSERT INTO bireport.rpt_day_hy_guild_new
select dt,
       platform_id,
       platform_name                                                           AS platform,
       channel_id,
       revenue_level,
       newold_state,
       active_state,
       COUNT(DISTINCT anchor_uid)                                              AS anchor_cnt,
       COUNT(DISTINCT CASE WHEN live_status = 1 THEN anchor_uid ELSE NULL END) AS live_cnt,
       SUM(IFNULL(duration, 0))                                                AS duration,
       SUM(IFNULL(revenue, 0))                                                 AS revenue
from warehouse.dw_huya_day_anchor_live
# WHERE dt BETWEEN '{start_date}' AND '{end_date}'
GROUP BY dt,
         platform_id,
         platform_name,
         channel_id,
         revenue_level,
         newold_state,
         active_state
;


REPLACE INTO bireport.rpt_day_hy_guild_new (dt, platform_id, platform, channel_id, revenue_level,
                                            newold_state, active_state, anchor_cnt, live_cnt, duration, revenue)
SELECT *
FROM (SELECT dt,
             MAX(platform_id)             AS platform_id,
             MAX(platform)                AS platform,
             IFNULL(channel_id, 'all')    AS channel_id,
             IFNULL(revenue_level, 'all') AS revenue_level,
             IFNULL(newold_state, 'all')  AS newold_state,
             IFNULL(active_state, 'all')  AS active_state,
             SUM(anchor_cnt)              AS anchor_cnt,
             SUM(live_cnt)                AS live_cnt,
             SUM(duration)                AS duration,
             SUM(revenue)                 AS revenue
      FROM bireport.rpt_day_hy_guild_new
      WHERE (channel_id != 'all' OR revenue_level != 'all' OR newold_state != 'all' OR active_state != 'all')
        AND dt BETWEEN '{start_date}' AND '{end_date}'
      GROUP BY dt, channel_id, revenue_level, newold_state, active_state
      WITH ROLLUP

      UNION

      SELECT dt,
             MAX(platform_id)             AS platform_id,
             MAX(platform)                AS platform,
             IFNULL(channel_id, 'all')    AS channel_id,
             IFNULL(revenue_level, 'all') AS revenue_level,
             IFNULL(newold_state, 'all')  AS newold_state,
             IFNULL(active_state, 'all')  AS active_state,
             SUM(anchor_cnt)              AS anchor_cnt,
             SUM(live_cnt)                AS live_cnt,
             SUM(duration)                AS duration,
             SUM(revenue)                 AS revenue
      FROM bireport.rpt_day_hy_guild_new
      WHERE (channel_id != 'all' OR revenue_level != 'all' OR newold_state != 'all' OR active_state != 'all')
        AND dt BETWEEN '{start_date}' AND '{end_date}'
      GROUP BY dt, revenue_level, newold_state, active_state, channel_id
      WITH ROLLUP

      UNION

      SELECT dt,
             MAX(platform_id)             AS platform_id,
             MAX(platform)                AS platform,
             IFNULL(channel_id, 'all')    AS channel_id,
             IFNULL(revenue_level, 'all') AS revenue_level,
             IFNULL(newold_state, 'all')  AS newold_state,
             IFNULL(active_state, 'all')  AS active_state,
             SUM(anchor_cnt)              AS anchor_cnt,
             SUM(live_cnt)                AS live_cnt,
             SUM(duration)                AS duration,
             SUM(revenue)                 AS revenue
      FROM bireport.rpt_day_hy_guild_new
      WHERE (channel_id != 'all' OR revenue_level != 'all' OR newold_state != 'all' OR active_state != 'all')
        AND dt BETWEEN '{start_date}' AND '{end_date}'
      GROUP BY dt, newold_state, active_state, channel_id, revenue_level
      WITH ROLLUP

      UNION

      SELECT dt,
             MAX(platform_id)             AS platform_id,
             MAX(platform)                AS platform,
             IFNULL(channel_id, 'all')    AS channel_id,
             IFNULL(revenue_level, 'all') AS revenue_level,
             IFNULL(newold_state, 'all')  AS newold_state,
             IFNULL(active_state, 'all')  AS active_state,
             SUM(anchor_cnt)              AS anchor_cnt,
             SUM(live_cnt)                AS live_cnt,
             SUM(duration)                AS duration,
             SUM(revenue)                 AS revenue
      FROM bireport.rpt_day_hy_guild_new
      WHERE (channel_id != 'all' OR revenue_level != 'all' OR newold_state != 'all' OR active_state != 'all')
        AND dt BETWEEN '{start_date}' AND '{end_date}'
      GROUP BY dt, active_state, channel_id, revenue_level, newold_state
      WITH ROLLUP

      UNION ALL
      SELECT dt,
             MAX(platform_id)             AS platform_id,
             MAX(platform)                AS platform,
             IFNULL(channel_id, 'all')    AS channel_id,
             IFNULL(revenue_level, 'all') AS revenue_level,
             IFNULL(newold_state, 'all')  AS newold_state,
             IFNULL(active_state, 'all')  AS active_state,
             SUM(anchor_cnt)              AS anchor_cnt,
             SUM(live_cnt)                AS live_cnt,
             SUM(duration)                AS duration,
             SUM(revenue)                 AS revenue
      FROM bireport.rpt_day_hy_guild_new
      WHERE (channel_id != 'all' OR revenue_level != 'all' OR newold_state != 'all' OR active_state != 'all')
        AND dt BETWEEN '{start_date}' AND '{end_date}'
      GROUP BY dt, active_state, revenue_level, channel_id, newold_state
      WITH ROLLUP
     ) t
WHERE dt IS NOT NULL
;


-- - rpt_day_all_new ----

REPLACE INTO bireport.rpt_day_all_new
(dt,
 platform,
 revenue_level,
 newold_state,
 active_state,
 anchor_cnt,
 live_cnt,
 duration,
 revenue,
 guild_income,
 anchor_income)
SELECT dt,
       platform,
       revenue_level,
       newold_state,
       active_state,
       anchor_cnt,
       live_cnt,
       duration,
       revenue,
       0 AS guild_income,
       0 AS anchor_income
FROM bireport.rpt_day_hy_guild_new
WHERE channel_id = 'all'
  AND dt BETWEEN '{start_date}' AND '{end_date}'
;
