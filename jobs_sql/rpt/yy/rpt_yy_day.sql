DELETE
FROM bireport.rpt_day_yy_guild
WHERE dt  >='{month}' and dt <= LAST_DAY('{month}') ;
INSERT INTO bireport.rpt_day_yy_guild
(
  `dt`,
  `channel_num`,
  `revenue_level`,
  `newold_state`,
  `active_state`,
  `anchor_cnt`,
  `new_anchor_cnt`,
  `new_r30_cnt`,
  `new_r60_cnt`,
  `new_r90_cnt`,
  `new_r120_cnt`,
  `live_cnt`,
  `duration`,
  `anchor_bluediamond_revenue`,
  `guild_commission_revenue`,
  `revenue`,
  `revenue_orig`,
  `guild_income_bluediamond`,
  `guild_income`,
  `guild_income_orig`,
  `anchor_income`,
  `anchor_income_orig`
)
SELECT gl.dt,
       gl.channel_num,
       gl.revenue_level,
       gl.newold_state,
       gl.active_state,
       gl.anchor_cnt,
       gl.new_anchor_cnt as new_anchor_cnt,
       gl.new_r30_cnt as new_r30_cnt,
       gl.new_r60_cnt as new_r60_cnt,
       gl.new_r90_cnt as new_r90_cnt,
       gl.new_r120_cnt as new_r120_cnt, 
       gl.anchor_live_cnt                                  AS live_cnt,
       gl.duration,
       -- 平台流水
       gl.bluediamond                                      AS anchor_bluediamond_revenue,
       gl.guild_commission / 1000                          AS guild_commssion_revenue,
       (gl.bluediamond + gl.guild_commission) * 2 / 1000   AS revenue,
       gl.bluediamond + gl.guild_commission                AS revenue_orig,
       -- 公会收入
       gl.guild_income_bluediamond,
       0                                                   as guild_income,
       -- ROUND((gl.guild_income_bluediamond + gl.guild_commission) / 1000, 2)   AS guild_income,
       gl.guild_income_bluediamond + gl.guild_commission   AS guild_income_orig,
       -- 主播收入
       0                                                   as anchor_income,
       -- ROUND((gl.anchor_income_bluediamond + gl.anchor_commission) / 1000, 2) AS anchor_income,
       gl.anchor_income_bluediamond + gl.anchor_commission AS anchor_income_orig
FROM warehouse.dw_yy_day_guild_live gl
         LEFT JOIN warehouse.platform pf ON gl.platform_id = pf.id
WHERE  dt  >='{month}' and dt <= LAST_DAY('{month}') ;
;


-- 补充汇总数据
Replace INTO bireport.rpt_day_yy_guild
(dt,
 platform_id,
 platform,
 channel_num,
 revenue_level,
 newold_state,
 active_state,
 anchor_cnt,
 new_anchor_cnt,
 new_r30_cnt,
 new_r60_cnt,
 new_r90_cnt,
 new_r120_cnt,
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
             SUM(new_anchor_cnt)             AS new_anchor_cnt,
             SUM(new_r30_cnt)                AS new_r30_cnt,
             SUM(new_r60_cnt)                AS new_r60_cnt,
             SUM(new_r90_cnt)                AS new_r90_cnt,
             SUM(new_r120_cnt)               AS new_r120_cnt,
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
      FROM bireport.rpt_day_yy_guild
      WHERE dt BETWEEN '{month}' AND LAST_DAY('{month}')
        AND channel_num != 'all'
        AND revenue_level != 'all'
        AND newold_state != 'all'
        AND active_state != 'all'
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
             SUM(new_anchor_cnt)             AS new_anchor_cnt,
             SUM(new_r30_cnt)                AS new_r30_cnt,
             SUM(new_r60_cnt)                AS new_r60_cnt,
             SUM(new_r90_cnt)                AS new_r90_cnt,
             SUM(new_r120_cnt)               AS new_r120_cnt,
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
      FROM bireport.rpt_day_yy_guild
      WHERE dt BETWEEN '{month}' AND LAST_DAY('{month}')
        AND channel_num != 'all'
        AND revenue_level != 'all'
        AND newold_state != 'all'
        AND active_state != 'all'
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
             SUM(new_anchor_cnt)             AS new_anchor_cnt,
             SUM(new_r30_cnt)                AS new_r30_cnt,
             SUM(new_r60_cnt)                AS new_r60_cnt,
             SUM(new_r90_cnt)                AS new_r90_cnt,
             SUM(new_r120_cnt)               AS new_r120_cnt,             
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
      FROM bireport.rpt_day_yy_guild
      WHERE dt BETWEEN '{month}' AND LAST_DAY('{month}')
        AND channel_num != 'all'
        AND revenue_level != 'all'
        AND newold_state != 'all'
        AND active_state != 'all'
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
             SUM(new_anchor_cnt)             AS new_anchor_cnt,
             SUM(new_r30_cnt)                AS new_r30_cnt,
             SUM(new_r60_cnt)                AS new_r60_cnt,
             SUM(new_r90_cnt)                AS new_r90_cnt,
             SUM(new_r120_cnt)               AS new_r120_cnt,             
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
      FROM bireport.rpt_day_yy_guild
      WHERE dt BETWEEN '{month}' AND LAST_DAY('{month}')
        AND channel_num != 'all'
        AND revenue_level != 'all'
        AND newold_state != 'all'
        AND active_state != 'all'
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
             SUM(new_anchor_cnt)             AS new_anchor_cnt,
             SUM(new_r30_cnt)                AS new_r30_cnt,
             SUM(new_r60_cnt)                AS new_r60_cnt,
             SUM(new_r90_cnt)                AS new_r90_cnt,
             SUM(new_r120_cnt)               AS new_r120_cnt,             
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
      FROM bireport.rpt_day_yy_guild
      WHERE dt BETWEEN '{month}' AND LAST_DAY('{month}')
        AND channel_num != 'all'
        AND revenue_level != 'all'
        AND newold_state != 'all'
        AND active_state != 'all'
      GROUP BY dt, active_state, revenue_level, channel_num, newold_state
      WITH ROLLUP
     ) t
WHERE dt IS NOT NULL
;


-- 报表用，计算上周、上月同期数据---
DELETE
FROM bireport.rpt_day_yy_guild_view
WHERE dt BETWEEN '{month}' AND LAST_DAY('{month}');
INSERT INTO bireport.rpt_day_yy_guild_view
(
  `dt`,
  `channel_num`,
  `revenue_level`,
  `newold_state`,
  `active_state`,
  `anchor_cnt`,
  `new_anchor_cnt`,
  `new_r30_ratio`,
  `new_r60_ratio`,
  `new_r90_ratio`,
  `new_r120_ratio`,
  `loss_anchor_cnt`,
  `increase_anchor_cnt`,
  `anchor_cnt_lastweek`,
  `anchor_cnt_lastmonth`,
  `live_cnt`,
  `live_cnt_lastweek`,
  `live_cnt_lastmonth`,
  `live_ratio`,
  `live_ratio_lastweek`,
  `live_ratio_lastmonth`,
  `duration`,
  `duration_lastweek`,
  `duration_lastmonth`,
  `revenue`,
  `revenue_lastweek`,
  `revenue_lastmonth`,
  `revenue_per_live`,
  `revenue_per_live_lastweek`,
  `revenue_per_live_lastmonth`,
  `guild_income`,
  `anchor_income`)
SELECT t1.dt,
       t1.channel_num,
       t1.revenue_level,
       t1.newold_state,
       t1.active_state,
       t1.anchor_cnt,
       t1.new_anchor_cnt                                               as `new_anchor_cnt`,
       if(t1.new_anchor_cnt=0,null,concat(ROUND(t1.new_r30_cnt/t1.new_anchor_cnt*100,1),'%'))  as new_r30_cnt,
       if(t1.new_anchor_cnt=0,null,concat(ROUND(t1.new_r60_cnt/t1.new_anchor_cnt*100,1),'%'))  as new_r60_cnt,
       if(t1.new_anchor_cnt=0,null,concat(ROUND(t1.new_r90_cnt/t1.new_anchor_cnt*100,1),'%'))  as new_r90_cnt,
       if(t1.new_anchor_cnt=0,null,concat(ROUND(t1.new_r120_cnt/t1.new_anchor_cnt*100,1),'%')) as new_r120_cnt,
       t1.new_anchor_cnt- t1.anchor_cnt+t4.anchor_cnt                  as `loss_anchor_cnt`,
       t1.anchor_cnt-t4.anchor_cnt                                     as `increase_anchor_cnt`,
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
       IF(t3.live_cnt > 0, ROUND(t3.revenue / t3.live_cnt, 0), 0)      AS revenue_per_live_lastmonth,
       t1.guild_income,
       t1.anchor_income
FROM bireport.rpt_day_yy_guild t1
         LEFT JOIN bireport.rpt_day_yy_guild t2
                   ON t1.dt - INTERVAL 7 DAY = t2.dt
                       AND t1.channel_num = t2.channel_num
                       AND t1.revenue_level = t2.revenue_level
                       AND t1.newold_state = t2.newold_state
                       AND t1.active_state = t2.active_state
         LEFT JOIN bireport.rpt_day_yy_guild t3
                   ON t1.dt - INTERVAL 1 MONTH = t3.dt
                       AND t1.channel_num = t3.channel_num
                       AND t1.revenue_level = t3.revenue_level
                       AND t1.newold_state = t3.newold_state
                       AND t1.active_state = t3.active_state
         LEFT JOIN bireport.rpt_day_yy_guild t4
                   ON t1.dt - INTERVAL 1 day = t4.dt
                       AND t1.channel_num = t4.channel_num
                       AND t1.revenue_level = t4.revenue_level
                       AND t1.newold_state = t4.newold_state
                       AND t1.active_state = t4.active_state
WHERE t1.dt BETWEEN '{month}' AND LAST_DAY('{month}');


-- -- 报表用，计算指标占比---
-- DELETE
-- FROM bireport.rpt_day_yy_guild_view_compare
-- WHERE dt BETWEEN '{start_date}' AND '{end_date}';
-- INSERT INTO bireport.rpt_day_yy_guild_view_compare
-- SELECT *
-- FROM (SELECT dt,
--              channel_num,
--              revenue_level,
--              newold_state,
--              active_state,
--              '主播数'      AS idx,
--              anchor_cnt AS val
--       FROM bireport.rpt_day_yy_guild
--       WHERE dt BETWEEN '{start_date}' AND '{end_date}'
--         AND revenue_level != 'all'
--       UNION
--       SELECT dt,
--              channel_num,
--              revenue_level,
--              newold_state,
--              active_state,
--              '开播数'    AS idx,
--              live_cnt AS val
--       FROM bireport.rpt_day_yy_guild
--       WHERE dt BETWEEN '{start_date}' AND '{end_date}'
--         AND revenue_level != 'all'
--       UNION
--       SELECT dt,
--              channel_num,
--              revenue_level,
--              newold_state,
--              active_state,
--              '流水'    AS idx,
--              revenue AS val
--       FROM bireport.rpt_day_yy_guild
--       WHERE dt BETWEEN '{start_date}' AND '{end_date}'
--         AND revenue_level != 'all'
--       UNION
--       SELECT dt,
--              channel_num,
--              revenue_level,
--              newold_state,
--              active_state,
--              '开播人均流水'                     AS idx,
--              round(revenue / live_cnt, 0) AS val
--       FROM bireport.rpt_day_yy_guild
--       WHERE dt BETWEEN '{start_date}' AND '{end_date}'
--         AND revenue_level != 'all'
--         AND live_cnt > 0) t;


