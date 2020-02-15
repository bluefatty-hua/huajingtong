DELETE
FROM bireport.rpt_day_all
WHERE platform = 'YY'
  AND dt BETWEEN '{start_date}' AND '{end_date}';
INSERT INTO bireport.rpt_day_all
SELECT gl.dt,
       pf.platform_name                                                       AS platform,
       gl.anchor_cnt,
       gl.anchor_live_cnt                                                     AS live_cnt,
       -- 平台流水
       ROUND((gl.bluediamond + gl.guild_commission) * 2 / 1000, 2)            AS revenue,
       -- 公会收入
       ROUND((gl.guild_income_bluediamond + gl.guild_commission) / 1000, 2)   AS guild_income,
       -- 主播收入
       ROUND((gl.anchor_income_bluediamond + gl.anchor_commission) / 1000, 2) AS anchor_income
FROM (SELECT al.dt,
             al.platform_id,
             COUNT(DISTINCT al.anchor_uid)                                                 AS anchor_cnt,
             COUNT(DISTINCT CASE WHEN al.live_status = 1 THEN al.anchor_uid ELSE NULL END) AS anchor_live_cnt,
             SUM(IF(al.duration > 0, al.duration, 0))                                      AS duration,
             SUM(IF(al.bluediamond > 0, al.bluediamond, 0))                                AS bluediamond,
             SUM(IF(al.bluediamond > 0, al.bluediamond * al.anchor_settle_rate, 0))        AS anchor_income_bluediamond,
             SUM(IF(al.bluediamond > 0, al.bluediamond * (1 - al.anchor_settle_rate), 0))  AS guild_income_bluediamond,
             SUM(IF(al.anchor_commission > 0, al.anchor_commission, 0))                    AS anchor_commission,
             SUM(IF(al.guild_commission > 0, al.guild_commission, 0))                      AS guild_commission
      FROM warehouse.ods_yy_day_anchor_live al
      WHERE comment = 'orig'
        AND al.dt BETWEEN '{start_date}' AND '{end_date}'
      GROUP BY al.dt,
               al.platform_id) gl
         LEFT JOIN warehouse.platform pf ON gl.platform_id = pf.id
;


DELETE
FROM bireport.rpt_day_yy_guild
WHERE dt BETWEEN '{start_date}' AND '{end_date}';
INSERT INTO bireport.rpt_day_yy_guild
SELECT gl.dt,
       gl.platform_id,
       pf.platform_name                                                          AS platform_name,
       gl.channel_num,
       SUM(gl.anchor_cnt)                                                        AS anchor_cnt,
       SUM(gl.anchor_live_cnt)                                                   AS live_cnt,
       -- 平台流水
       SUM(gl.bluediamond)                                                       AS anchor_bluediamond_revenue,
       ROUND(SUM(gl.guild_commission) / 1000, 2)                                 AS guild_commssion_revenue,
       ROUND(SUM(gl.bluediamond + gl.guild_commission) * 2 / 1000, 2)            AS revenue,
       SUM(gl.bluediamond + gl.guild_commission)                                 AS revenue_orig,
       -- 公会收入
       SUM(gl.guild_income_bluediamond),
       ROUND(SUM(gl.guild_income_bluediamond + gl.guild_commission) / 1000, 2)   AS guild_income,
       SUM(gl.guild_income_bluediamond + gl.guild_commission)                    AS guild_income_orig,
       -- 主播收入
       ROUND(SUM(gl.anchor_income_bluediamond + gl.anchor_commission) / 1000, 2) AS anchor_income,
       SUM(gl.anchor_income_bluediamond + gl.anchor_commission)                  AS anchor_income_orig
FROM warehouse.dw_yy_day_guild_live gl
         LEFT JOIN warehouse.platform pf ON gl.platform_id = pf.id
WHERE comment = 'orig'
  AND gl.dt BETWEEN '{start_date}' AND '{end_date}'
GROUP BY gl.dt,
         gl.platform_id,
         pf.platform_name,
         gl.channel_num
;


-- 补充汇总数据
REPLACE INTO bireport.rpt_day_yy_guild
(dt, channel_num, anchor_cnt, live_cnt, revenue, guild_income, anchor_income)
SELECT dt,
       'all' AS channel_num,
       anchor_cnt,
       live_cnt,
       revenue,
       guild_income,
       anchor_income
FROM bireport.rpt_day_all
WHERE platform = 'YY'
  AND dt BETWEEN '{start_date}' AND '{end_date}';



-- -------- new code -------------

-- rpt_day_yy_guild_new
DELETE
FROM bireport.rpt_day_yy_guild_new
WHERE dt BETWEEN '{start_date}' AND '{end_date}';
INSERT INTO bireport.rpt_day_yy_guild_new
SELECT gl.dt,
       gl.platform_id,
       pf.platform_name                                                       AS platform_name,
       gl.channel_num,
       gl.revenue_level,
       gl.newold_state,
       gl.active_state,
       gl.anchor_cnt,
       gl.anchor_live_cnt                                                     AS live_cnt,
       gl.duration,
       -- 平台流水
       gl.bluediamond                                                         AS anchor_bluediamond_revenue,
       ROUND(gl.guild_commission / 1000, 2)                                   AS guild_commssion_revenue,
       ROUND((gl.bluediamond + gl.guild_commission) * 2 / 1000, 2)            AS revenue,
       gl.bluediamond + gl.guild_commission                                   AS revenue_orig,
       -- 公会收入
       gl.guild_income_bluediamond,
       ROUND((gl.guild_income_bluediamond + gl.guild_commission) / 1000, 2)   AS guild_income,
       gl.guild_income_bluediamond + gl.guild_commission                      AS guild_income_orig,
       -- 主播收入
       ROUND((gl.anchor_income_bluediamond + gl.anchor_commission) / 1000, 2) AS anchor_income,
       gl.anchor_income_bluediamond + gl.anchor_commission                    AS anchor_income_orig
FROM warehouse.dw_yy_day_guild_live gl
         LEFT JOIN warehouse.platform pf ON gl.platform_id = pf.id
WHERE comment = 'orig'
  AND gl.dt BETWEEN '{start_date}' AND '{end_date}'
;


-- 补充汇总数据
REPLACE INTO bireport.rpt_day_yy_guild_new
(dt,
 platform_id,
 platform,
 channel_num,
 revenue_level,
 newold_state,
 active_state,
 anchor_cnt,
 live_cnt,
 duration,
 anchor_bluediamond_revenue,
 guild_commission_revenue,
 revenue,
 revenue_orig,
 guild_income_bluediamond,
 guild_income,
 guild_income_orig,
 anchor_income,
 anchor_income_orig)
SELECT *
FROM (SELECT dt,
             MAX(platform_id)                AS platform_id,
             MAX(platform)                   AS platform,
             IFNULL(channel_num, 'all')      AS channel_num,
             IFNULL(revenue_level, 'all')    AS revenue_level,
             IFNULL(newold_state, 'all')     AS newold_state,
             IFNULL(active_state, 'all')     AS active_state,
             SUM(anchor_cnt)                 AS anchor_cnt,
             SUM(live_cnt)                   AS live_cnt,
             SUM(duration)                   AS duration,
             SUM(anchor_bluediamond_revenue) AS anchor_bluediamond_revenue,
             SUM(guild_commission_revenue)   AS guild_commission_revenue,
             SUM(revenue)                    AS revenue,
             SUM(revenue_orig)               AS revenue_orig,
             SUM(guild_income_bluediamond)   AS guild_income_bluediamond,
             SUM(guild_income)               AS guild_income,
             SUM(guild_income_orig)          AS guild_income_orig,
             SUM(anchor_income)              AS anchor_income,
             SUM(anchor_income_orig)         AS anchor_income_orig
      FROM bireport.rpt_day_yy_guild_new
      WHERE dt BETWEEN '{start_date}' AND '{end_date}'
        AND (channel_num != 'all' OR revenue_level != 'all' OR newold_state != 'all' OR active_state != 'all')
      GROUP BY dt, channel_num, revenue_level, newold_state, active_state
      WITH ROLLUP

      UNION

      SELECT dt,
             MAX(platform_id)                AS platform_id,
             MAX(platform)                   AS platform,
             IFNULL(channel_num, 'all')      AS channel_num,
             IFNULL(revenue_level, 'all')    AS revenue_level,
             IFNULL(newold_state, 'all')     AS newold_state,
             IFNULL(active_state, 'all')     AS active_state,
             SUM(anchor_cnt)                 AS anchor_cnt,
             SUM(live_cnt)                   AS live_cnt,
             SUM(duration)                   AS duration,
             SUM(anchor_bluediamond_revenue) AS anchor_bluediamond_revenue,
             SUM(guild_commission_revenue)   AS guild_commission_revenue,
             SUM(revenue)                    AS revenue,
             SUM(revenue_orig)               AS revenue_orig,
             SUM(guild_income_bluediamond)   AS guild_income_bluediamond,
             SUM(guild_income)               AS guild_income,
             SUM(guild_income_orig)          AS guild_income_orig,
             SUM(anchor_income)              AS anchor_income,
             SUM(anchor_income_orig)         AS anchor_income_orig
      FROM bireport.rpt_day_yy_guild_new
      WHERE dt BETWEEN '{start_date}' AND '{end_date}'
        AND (channel_num != 'all' OR revenue_level != 'all' OR newold_state != 'all' OR active_state != 'all')
      GROUP BY dt, revenue_level, newold_state, active_state, channel_num
      WITH ROLLUP

      UNION

      SELECT dt,
             MAX(platform_id)                AS platform_id,
             MAX(platform)                   AS platform,
             IFNULL(channel_num, 'all')      AS channel_num,
             IFNULL(revenue_level, 'all')    AS revenue_level,
             IFNULL(newold_state, 'all')     AS newold_state,
             IFNULL(active_state, 'all')     AS active_state,
             SUM(anchor_cnt)                 AS anchor_cnt,
             SUM(live_cnt)                   AS live_cnt,
             SUM(duration)                   AS duration,
             SUM(anchor_bluediamond_revenue) AS anchor_bluediamond_revenue,
             SUM(guild_commission_revenue)   AS guild_commission_revenue,
             SUM(revenue)                    AS revenue,
             SUM(revenue_orig)               AS revenue_orig,
             SUM(guild_income_bluediamond)   AS guild_income_bluediamond,
             SUM(guild_income)               AS guild_income,
             SUM(guild_income_orig)          AS guild_income_orig,
             SUM(anchor_income)              AS anchor_income,
             SUM(anchor_income_orig)         AS anchor_income_orig
      FROM bireport.rpt_day_yy_guild_new
      WHERE dt BETWEEN '{start_date}' AND '{end_date}'
        AND (channel_num != 'all' OR revenue_level != 'all' OR newold_state != 'all' OR active_state != 'all')
      GROUP BY dt, newold_state, active_state, channel_num, revenue_level
      WITH ROLLUP

      UNION

      SELECT dt,
             MAX(platform_id)                AS platform_id,
             MAX(platform)                   AS platform,
             IFNULL(channel_num, 'all')      AS channel_num,
             IFNULL(revenue_level, 'all')    AS revenue_level,
             IFNULL(newold_state, 'all')     AS newold_state,
             IFNULL(active_state, 'all')     AS active_state,
             SUM(anchor_cnt)                 AS anchor_cnt,
             SUM(live_cnt)                   AS live_cnt,
             SUM(duration)                   AS duration,
             SUM(anchor_bluediamond_revenue) AS anchor_bluediamond_revenue,
             SUM(guild_commission_revenue)   AS guild_commission_revenue,
             SUM(revenue)                    AS revenue,
             SUM(revenue_orig)               AS revenue_orig,
             SUM(guild_income_bluediamond)   AS guild_income_bluediamond,
             SUM(guild_income)               AS guild_income,
             SUM(guild_income_orig)          AS guild_income_orig,
             SUM(anchor_income)              AS anchor_income,
             SUM(anchor_income_orig)         AS anchor_income_orig
      FROM bireport.rpt_day_yy_guild_new
      WHERE dt BETWEEN '{start_date}' AND '{end_date}'
        AND (channel_num != 'all' OR revenue_level != 'all' OR newold_state != 'all' OR active_state != 'all')
      GROUP BY dt, active_state, channel_num, revenue_level, newold_state
      WITH ROLLUP

      UNION

      SELECT dt,
             MAX(platform_id)                AS platform_id,
             MAX(platform)                   AS platform,
             IFNULL(channel_num, 'all')      AS channel_num,
             IFNULL(revenue_level, 'all')    AS revenue_level,
             IFNULL(newold_state, 'all')     AS newold_state,
             IFNULL(active_state, 'all')     AS active_state,
             SUM(anchor_cnt)                 AS anchor_cnt,
             SUM(live_cnt)                   AS live_cnt,
             SUM(duration)                   AS duration,
             SUM(anchor_bluediamond_revenue) AS anchor_bluediamond_revenue,
             SUM(guild_commission_revenue)   AS guild_commission_revenue,
             SUM(revenue)                    AS revenue,
             SUM(revenue_orig)               AS revenue_orig,
             SUM(guild_income_bluediamond)   AS guild_income_bluediamond,
             SUM(guild_income)               AS guild_income,
             SUM(guild_income_orig)          AS guild_income_orig,
             SUM(anchor_income)              AS anchor_income,
             SUM(anchor_income_orig)         AS anchor_income_orig
      FROM bireport.rpt_day_yy_guild_new
      WHERE dt BETWEEN '{start_date}' AND '{end_date}'
        AND (channel_num != 'all' OR revenue_level != 'all' OR newold_state != 'all' OR active_state != 'all')
      GROUP BY dt, active_state, revenue_level, channel_num, newold_state
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
       'YY' AS platform,
       revenue_level,
       newold_state,
       active_state,
       anchor_cnt,
       live_cnt,
       duration,
       revenue,
       guild_income,
       anchor_income
FROM bireport.rpt_day_yy_guild_new
WHERE dt BETWEEN '{start_date}' AND '{end_date}'
  AND channel_num = 'all'
;

-- 报表用，计算上周、上月同期数据---
REPLACE INTO bireport.rpt_day_yy_guild_new_view
SELECT 
	t1.dt,
	t1.channel_num,
	t1.revenue_level,
	t1.newold_state,
	t1.active_state,
	t1.anchor_cnt,
	t2.anchor_cnt AS anchor_cnt_lastweek,
	t3.anchor_cnt AS anchor_cnt_lastmonth,
	t1.live_cnt,
	t2.live_cnt AS live_cnt_lastweek,
	t3.live_cnt AS live_cnt_lastmonth,
	IF(t1.anchor_cnt>0,ROUND(t1.live_cnt/t1.anchor_cnt,3),0) AS live_ratio,
	IF(t2.anchor_cnt>0,ROUND(t2.live_cnt/t2.anchor_cnt,3),0) AS live_ratio_lastweek,
	IF(t3.anchor_cnt>0,ROUND(t3.live_cnt/t3.anchor_cnt,3),0) AS live_ratio_lastmonth,
	ROUND(t1.duration/3600,1) AS duration,
	ROUND(t2.duration/3600,1) AS duration_lastweek,
	ROUND(t3.duration/3600,1) AS duration_lastmonth,
	t1.revenue,
	t2.revenue AS revenue_lastweek,
	t3.revenue AS revenue_lastmonth,
	IF(t1.live_cnt>0,ROUND(t1.`revenue`/t1.live_cnt,0),0) AS revenue_per_live,
	IF(t2.live_cnt>0,ROUND(t2.`revenue`/t2.live_cnt,0),0) AS revenue_per_live_lastweek,
	IF(t3.live_cnt>0,ROUND(t3.`revenue`/t3.live_cnt,0),0) AS revenue_per_live_lastmonth,
	t1.`guild_income`,
	t1.`anchor_income` 
FROM bireport.rpt_day_yy_guild_new t1
LEFT JOIN bireport.rpt_day_yy_guild_new t2
	ON t1.dt - INTERVAL 7 DAY = t2.dt
	AND t1.channel_num = t2.channel_num
	AND t1.revenue_level = t2.revenue_level
	AND t1.newold_state = t2.newold_state
	AND t1.active_state = t2.active_state
LEFT JOIN bireport.rpt_day_yy_guild_new t3
	ON t1.dt - INTERVAL 1 MONTH = t3.dt
	AND t1.channel_num = t3.channel_num
	AND t1.revenue_level = t3.revenue_level
	AND t1.newold_state = t3.newold_state
	AND t1.active_state = t3.active_state
  WHERE t1.dt BETWEEN '{start_date}' AND '{end_date}';