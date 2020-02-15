-- 公会每月流水、公会收入、主播收入
-- DROP TABLE IF EXISTS bireport.rpt_month_hy_guild;
-- CREATE TABLE bireport.rpt_month_hy_guild AS
DROP TABLE IF EXISTS stage.hy_guild_income_rate;
CREATE TABLE stage.hy_guild_income_rate AS
SELECT channel_num, AVG(guild_income / revenue) avg_rate
FROM bireport.rpt_month_hy_guild
WHERE revenue > 0
  AND guild_income / revenue > 0.09
GROUP BY channel_num
;


DELETE
FROM bireport.rpt_month_hy_guild
WHERE DATE_FORMAT(dt, '%Y-%m') BETWEEN DATE_FORMAT('{start_date}', '%Y-%m') AND DATE_FORMAT('{end_date}', '%Y-%m');
INSERT INTO bireport.rpt_month_hy_guild
SELECT t0.dt,
       t0.platform_id,
       pf.platform_name                                                          AS platform,
       t0.channel_num,
       t0.sign_count                                                             AS anchor_cnt,
       t1.anchor_live_cnt                                                        AS live_cnt,
       t0.revenue,
       t0.revenue                                                                AS revenue_orig,
       (t0.gift_income + t0.guard_income + t0.noble_income) / 1000               AS guild_income,
       t0.gift_income + t0.guard_income + t0.noble_income                        AS guild_income_orig,
       (t0.gift_income + t0.guard_income + t0.noble_income) * 0.7 / (0.3 * 1000) AS anchor_income,
       t0.gift_income + t0.guard_income + t0.noble_income                        AS anchor_incom_orig
FROM warehouse.dw_huya_month_guild_live t0
         LEFT JOIN (SELECT CONCAT(DATE_FORMAT(t.dt, '%Y-%m'), '-01')                          AS dt,
                           t.channel_id,
                           COUNT(DISTINCT t.anchor_uid)                                       AS anchor_cnt,
                           COUNT(DISTINCT
                                 CASE WHEN t.live_status = 1 THEN t.anchor_uid ELSE NULL END) AS anchor_live_cnt,
                           SUM(t.income)                                                      AS anchor_income
                    FROM warehouse.ods_huya_day_anchor_live t
                    GROUP BY CONCAT(DATE_FORMAT(t.dt, '%Y-%m'), '-01'),
                             t.channel_id) t1 ON t0.dt = t1.dt AND t0.channel_id = t1.channel_id
         lEFT JOIN warehouse.platform pf ON pf.id = t0.platform_id
WHERE DATE_FORMAT(t0.dt, '%Y-%m') BETWEEN DATE_FORMAT('{start_date}', '%Y-%m') AND DATE_FORMAT('{end_date}', '%Y-%m')
  AND DATE_FORMAT(t0.dt, '%Y-%m') <> DATE_FORMAT('{end_date}', '%Y-%m')
UNION ALL
SELECT t0.dt,
       t0.platform_id,
       pf.platform_name                     AS platform,
       t0.channel_num,
       t0.sign_count                        AS anchor_cnt,
       t1.anchor_live_cnt                   AS live_cnt,
       t0.revenue,
       t0.revenue                           AS revenue_orig,
       t0.revenue * ig.avg_rate             AS guild_income,
       t0.revenue * ig.avg_rate             AS guild_income_orig,
       t0.revenue * ig.avg_rate * 0.7 / 0.3 AS anchor_income,
       t0.revenue * ig.avg_rate             AS anchor_incom_orig
FROM warehouse.dw_huya_month_guild_live t0
         LEFT JOIN (SELECT CONCAT(DATE_FORMAT(t.dt, '%Y-%m'), '-01')                          AS dt,
                           t.channel_id,
                           COUNT(DISTINCT t.anchor_uid)                                       AS anchor_cnt,
                           COUNT(DISTINCT
                                 CASE WHEN t.live_status = 1 THEN t.anchor_uid ELSE NULL END) AS anchor_live_cnt,
                           SUM(t.revenue)                                                     AS revenue
                    FROM warehouse.dw_huya_day_anchor_live t
                    GROUP BY CONCAT(DATE_FORMAT(t.dt, '%Y-%m'), '-01'),
                             t.channel_id) t1 ON t0.dt = t1.dt AND t0.channel_id = t1.channel_id
         LEFT JOIN stage.hy_guild_income_rate ig ON t0.channel_num = ig.channel_num
         lEFT JOIN warehouse.platform pf ON pf.id = t0.platform_id
WHERE DATE_FORMAT(t0.dt, '%Y-%m') = DATE_FORMAT('{end_date}', '%Y-%m')
;


DELETE
FROM bireport.rpt_month_all_guild
WHERE platform_id = 1002
  AND DATE_FORMAT(dt, '%Y-%m') BETWEEN DATE_FORMAT('{start_date}', '%Y-%m') AND DATE_FORMAT('{end_date}', '%Y-%m');
INSERT INTO bireport.rpt_month_all_guild
SELECT dt,
       platform_id,
       platform,
       channel_num,
       CASE WHEN anchor_cnt >= 0 THEN anchor_cnt ELSE 0 END                 AS anchor_cnt,
       CASE WHEN live_cnt >= 0 THEN live_cnt ELSE 0 END                     AS live_cnt,
       CASE WHEN revenue >= 0 THEN revenue ELSE 0 END                       AS revenue,
       CASE WHEN revenue_orig >= 0 THEN revenue_orig ELSE 0 END             AS revenue_orig,
       CASE WHEN guild_income >= 0 THEN guild_income ELSE 0 END             AS guild_income,
       CASE WHEN guild_income_orig >= 0 THEN guild_income_orig ELSE 0 END   AS guild_income_orig,
       CASE WHEN anchor_income >= 0 THEN anchor_income ELSE 0 END           AS anchor_income,
       CASE WHEN anchor_income_orig >= 0 THEN anchor_income_orig ELSE 0 END AS anchor_income_orig
FROM (
-- HuYa
         SELECT dt,
                platform_id,
                platform,
                channel_num,
                anchor_cnt,
                live_cnt,
                revenue,
                revenue_orig,
                guild_income,
                guild_income_orig,
                anchor_income,
                anchor_income_orig
         FROM bireport.rpt_month_hy_guild
     ) t
WHERE DATE_FORMAT(dt, '%Y-%m') BETWEEN DATE_FORMAT('{start_date}', '%Y-%m') AND DATE_FORMAT('{end_date}', '%Y-%m')
;


-- rpt_month_now_guild_new
DELETE
FROM bireport.rpt_month_hy_guild_new
WHERE dt BETWEEN DATE_FORMAT('{start_date}', '%Y-%m-01') AND DATE_FORMAT('{end_date}', '%Y-%m-01');
INSERT INTO bireport.rpt_month_hy_guild_new
SELECT DATE_FORMAT(al.dt, '%Y-%m-01')    AS dt,
       al.platform_id,
       al.platform_name                  AS platform,
       al.channel_num,
       al.revenue_level,
       al.month_newold_state             AS newold_state,
       al.active_state,
       COUNT(DISTINCT al.anchor_uid)     AS anchor_cnt,
       COUNT(DISTINCT CASE
                          WHEN al.live_status = 1 THEN al.anchor_uid
                          ELSE NULL END) AS live_cnt,
       SUM(IFNULL(al.duration, 0))       AS duration,
       SUM(IFNULL(al.revenue, 0))        AS revenue,
       SUM(IFNULL(al.revenue, 0))        AS revenune_orig,
       SUM(0)                            AS guild_income,
       SUM(0)                            AS guild_income_orig,
       SUM(0)                            AS anchor_income,
       SUM(0)                            AS anchor_income_orig
FROM (SELECT *,
             warehouse.ANCHOR_NEW_OLD(min_live_dt, min_sign_dt, CASE
                                                                    WHEN dt < DATE_FORMAT('{end_date}', '%Y-%m-01')
                                                                        THEN LAST_DAY(dt)
                                                                    ELSE dt END, 180) AS month_newold_state
      FROM warehouse.dw_huya_day_anchor_live
      WHERE dt BETWEEN DATE_FORMAT('{start_date}', '%Y-%m-01') AND DATE_FORMAT('{end_date}', '%Y-%m-01')
     ) al
GROUP BY DATE_FORMAT(al.dt, '%Y-%m-01'),
         al.platform_id,
         al.platform_name,
         al.channel_num,
         al.revenue_level,
         al.month_newold_state,
         al.active_state
;



REPLACE INTO bireport.rpt_month_hy_guild_new (dt, platform_id, platform, channel_num, revenue_level, newold_state,
                                              active_state, anchor_cnt, live_cnt, duration, revenue, revenue_orig,
                                              guild_income, guild_income_orig, anchor_income, anchor_income_orig)
SELECT t.dt,
       t.platform_id,
       t.platform,
       t.channel_num,
       t.revenue_level,
       t.newold_state,
       t.active_state,
       t.anchor_cnt,
       t.live_cnt,
       t.duration,
       t.revenue,
       t.revenue_orig,
       t.guild_income,
       t.guild_income_orig,
       t.anchor_income,
       t.anchor_income_orig
FROM (
         SELECT dt,
                MAX(platform_id)             AS platform_id,
                MAX(platform)                AS platform,
                IFNULL(channel_num, 'all')   AS channel_num,
                IFNULL(revenue_level, 'all') AS revenue_level,
                IFNULL(newold_state, 'all')  AS newold_state,
                IFNULL(active_state, 'all')  AS active_state,
                SUM(anchor_cnt)              AS anchor_cnt,
                SUM(live_cnt)                AS live_cnt,
                SUM(duration)                AS duration,
                SUM(revenue)                 AS revenue,
                SUM(revenue_orig)            AS revenue_orig,
                SUM(guild_income)            AS guild_income,
                SUM(guild_income_orig)       AS guild_income_orig,
                SUM(anchor_income)           AS anchor_income,
                SUM(anchor_income_orig)      AS anchor_income_orig
         FROM bireport.rpt_month_hy_guild_new
         WHERE (channel_num != 'all' OR revenue_level != 'all' OR newold_state != 'all' OR
                active_state != 'all')
           AND dt BETWEEN DATE_FORMAT('{start_date}', '%Y-%m-01') AND DATE_FORMAT('{end_date}', '%Y-%m-01')
         GROUP BY dt, channel_num, revenue_level, newold_state, active_state
         WITH ROLLUP

         UNION

         SELECT dt,
                MAX(platform_id)             AS platform_id,
                MAX(platform)                AS platform,
                IFNULL(channel_num, 'all')   AS channel_num,
                IFNULL(revenue_level, 'all') AS revenue_level,
                IFNULL(newold_state, 'all')  AS newold_state,
                IFNULL(active_state, 'all')  AS active_state,
                SUM(anchor_cnt)              AS anchor_cnt,
                SUM(live_cnt)                AS live_cnt,
                SUM(duration)                AS duration,
                SUM(revenue)                 AS revenue,
                SUM(revenue_orig)            AS revenue_orig,
                SUM(guild_income)            AS guild_income,
                SUM(guild_income_orig)       AS guild_income_orig,
                SUM(anchor_income)           AS anchor_income,
                SUM(anchor_income_orig)      AS anchor_income_orig
         FROM bireport.rpt_month_hy_guild_new
         WHERE (channel_num != 'all' OR revenue_level != 'all' OR newold_state != 'all' OR
                active_state != 'all')
           AND dt BETWEEN DATE_FORMAT('{start_date}', '%Y-%m-01') AND DATE_FORMAT('{end_date}', '%Y-%m-01')
         GROUP BY dt, revenue_level, newold_state, active_state, channel_num
         WITH ROLLUP

         UNION

         SELECT dt,
                MAX(platform_id)             AS platform_id,
                MAX(platform)                AS platform,
                IFNULL(channel_num, 'all')   AS channel_num,
                IFNULL(revenue_level, 'all') AS revenue_level,
                IFNULL(newold_state, 'all')  AS newold_state,
                IFNULL(active_state, 'all')  AS active_state,
                SUM(anchor_cnt)              AS anchor_cnt,
                SUM(live_cnt)                AS live_cnt,
                SUM(duration)                AS duration,
                SUM(revenue)                 AS revenue,
                SUM(revenue_orig)            AS revenue_orig,
                SUM(guild_income)            AS guild_income,
                SUM(guild_income_orig)       AS guild_income_orig,
                SUM(anchor_income)           AS anchor_income,
                SUM(anchor_income_orig)      AS anchor_income_orig
         FROM bireport.rpt_month_hy_guild_new
         WHERE (channel_num != 'all' OR revenue_level != 'all' OR newold_state != 'all' OR
                active_state != 'all')
           AND dt BETWEEN DATE_FORMAT('{start_date}', '%Y-%m-01') AND DATE_FORMAT('{end_date}', '%Y-%m-01')
         GROUP BY dt, newold_state, active_state, channel_num, revenue_level
         WITH ROLLUP

         UNION

         SELECT dt,
                MAX(platform_id)             AS platform_id,
                MAX(platform)                AS platform,
                IFNULL(channel_num, 'all')   AS channel_num,
                IFNULL(revenue_level, 'all') AS revenue_level,
                IFNULL(newold_state, 'all')  AS newold_state,
                IFNULL(active_state, 'all')  AS active_state,
                SUM(anchor_cnt)              AS anchor_cnt,
                SUM(live_cnt)                AS live_cnt,
                SUM(duration)                AS duration,
                SUM(revenue)                 AS revenue,
                SUM(revenue_orig)            AS revenue_orig,
                SUM(guild_income)            AS guild_income,
                SUM(guild_income_orig)       AS guild_income_orig,
                SUM(anchor_income)           AS anchor_income,
                SUM(anchor_income_orig)      AS anchor_income_orig
         FROM bireport.rpt_month_hy_guild_new
         WHERE (channel_num != 'all' OR revenue_level != 'all' OR newold_state != 'all' OR
                active_state != 'all')
           AND dt BETWEEN DATE_FORMAT('{start_date}', '%Y-%m-01') AND DATE_FORMAT('{end_date}', '%Y-%m-01')
         GROUP BY dt, active_state, channel_num, revenue_level, newold_state
         WITH ROLLUP
         UNION

         SELECT dt,
                MAX(platform_id)             AS platform_id,
                MAX(platform)                AS platform,
                IFNULL(channel_num, 'all')   AS channel_num,
                IFNULL(revenue_level, 'all') AS revenue_level,
                IFNULL(newold_state, 'all')  AS newold_state,
                IFNULL(active_state, 'all')  AS active_state,
                SUM(anchor_cnt)              AS anchor_cnt,
                SUM(live_cnt)                AS live_cnt,
                SUM(duration)                AS duration,
                SUM(revenue)                 AS revenue,
                SUM(revenue_orig)            AS revenue_orig,
                SUM(guild_income)            AS guild_income,
                SUM(guild_income_orig)       AS guild_income_orig,
                SUM(anchor_income)           AS anchor_income,
                SUM(anchor_income_orig)      AS anchor_income_orig
         FROM bireport.rpt_month_hy_guild_new
         WHERE (channel_num != 'all' OR revenue_level != 'all' OR newold_state != 'all' OR
                active_state != 'all')
           AND dt BETWEEN DATE_FORMAT('{start_date}', '%Y-%m-01') AND DATE_FORMAT('{end_date}', '%Y-%m-01')
         GROUP BY dt, newold_state, revenue_level, channel_num, active_state
         WITH ROLLUP) t
WHERE dt IS NOT NULL
;


REPLACE INTO bireport.rpt_month_all_new
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
       guild_income,
       anchor_income
FROM bireport.rpt_month_hy_guild_new
WHERE channel_num = 'all'
  AND dt BETWEEN DATE_FORMAT('{start_date}', '%Y-%m-01') AND DATE_FORMAT('{end_date}', '%Y-%m-01')
;

