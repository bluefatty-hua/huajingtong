

-- rpt_month_bb_guild_new
DELETE
FROM bireport.rpt_month_bb_guild
WHERE dt = '{month}';
INSERT INTO bireport.rpt_month_bb_guild
(
  `dt`,
  `platform_id`,
  `platform`,
  `backend_account_id`,
  `remark`,
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
  `revenue`,
  `revenue_orig`
 
)
SELECT gl.dt,
       gl.platform_id,
       pf.platform_name   AS platform,
       gl.backend_account_id,
       ai.remark,
       gl.revenue_level,
       gl.newold_state,
       gl.active_state,
       gl.anchor_cnt,
       gl.new_anchor_cnt as new_anchor_cnt,
       gl.new_r30_cnt as new_r30_cnt,
       gl.new_r60_cnt as new_r60_cnt,
       gl.new_r90_cnt as new_r90_cnt,
       gl.new_r120_cnt as new_r120_cnt, 
       gl.anchor_live_cnt AS live_cnt,
       gl.duration,
       gl.revenue         AS revenue,
       gl.revenue_orig    AS revenune_orig
 
FROM warehouse.dw_bb_month_guild_live gl
-- FROM stage.stage_rpt_bb_month_guild_live gl
         LEFT JOIN spider_bb_backend.account_info ai ON gl.backend_account_id = ai.backend_account_id
         lEFT JOIN warehouse.platform pf ON pf.id = gl.platform_id
WHERE dt = '{month}'
;


REPLACE INTO bireport.rpt_month_bb_guild
(
  `dt`,
  `platform_id`,
  `platform`,
  `backend_account_id`,
  `remark`,
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
  `revenue`,
  `revenue_orig`,
  `guild_income`,
  `guild_income_orig`,
  `anchor_income`,
  `anchor_income_orig`
)
SELECT t.dt,
       t.platform_id,
       t.platform,
       t.backend_account_id,
       CASE WHEN t.backend_account_id = 0 THEN 'all' ELSE ai.remark END AS remark,
       t.revenue_level,
       t.newold_state,
       t.active_state,
       t.anchor_cnt,
       t.new_anchor_cnt,
       t.new_r30_cnt as new_r30_cnt,
       t.new_r60_cnt as new_r60_cnt,
       t.new_r90_cnt as new_r90_cnt,
       t.new_r120_cnt as new_r120_cnt, 
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
                MAX(platform_id)              AS platform_id,
                MAX(platform)                 AS platform,
                IFNULL(backend_account_id, 0) AS backend_account_id,
                IFNULL(revenue_level, 'all')  AS revenue_level,
                IFNULL(newold_state, 'all')   AS newold_state,
                IFNULL(active_state, 'all')   AS active_state,
                SUM(anchor_cnt)               AS anchor_cnt,
                SUM(new_anchor_cnt)             AS new_anchor_cnt,
                SUM(new_r30_cnt)              AS new_r30_cnt,
                SUM(new_r60_cnt)              AS new_r60_cnt,
                SUM(new_r90_cnt)              AS new_r90_cnt,
                SUM(new_r120_cnt)             AS new_r120_cnt,
                SUM(live_cnt)                 AS live_cnt,
                SUM(duration)                 AS duration,
                SUM(revenue)                  AS revenue,
                SUM(revenue_orig)             AS revenue_orig,
                SUM(guild_income)             AS guild_income,
                SUM(guild_income_orig)        AS guild_income_orig,
                SUM(anchor_income)            AS anchor_income,
                SUM(anchor_income_orig)       AS anchor_income_orig
         FROM bireport.rpt_month_bb_guild
         WHERE backend_account_id != 0
           AND revenue_level != 'all'
           AND newold_state != 'all'
           AND active_state != 'all'
           AND dt = '{month}'
         GROUP BY dt, backend_account_id, revenue_level, newold_state, active_state
         WITH ROLLUP

         UNION

         SELECT dt,
                MAX(platform_id)              AS platform_id,
                MAX(platform)                 AS platform,
                IFNULL(backend_account_id, 0) AS backend_account_id,
                IFNULL(revenue_level, 'all')  AS revenue_level,
                IFNULL(newold_state, 'all')   AS newold_state,
                IFNULL(active_state, 'all')   AS active_state,
                SUM(anchor_cnt)               AS anchor_cnt,
                SUM(new_anchor_cnt)             AS new_anchor_cnt,
                SUM(new_r30_cnt)              AS new_r30_cnt,
                SUM(new_r60_cnt)              AS new_r60_cnt,
                SUM(new_r90_cnt)              AS new_r90_cnt,
                SUM(new_r120_cnt)             AS new_r120_cnt,
                SUM(live_cnt)                 AS live_cnt,
                SUM(duration)                 AS duration,
                SUM(revenue)                  AS revenue,
                SUM(revenue_orig)             AS revenue_orig,
                SUM(guild_income)             AS guild_income,
                SUM(guild_income_orig)        AS guild_income_orig,
                SUM(anchor_income)            AS anchor_income,
                SUM(anchor_income_orig)       AS anchor_income_orig
         FROM bireport.rpt_month_bb_guild
         WHERE backend_account_id != 0
           AND revenue_level != 'all'
           AND newold_state != 'all'
           AND active_state != 'all'
           AND dt = '{month}'
         GROUP BY dt, revenue_level, newold_state, active_state, backend_account_id
         WITH ROLLUP

         UNION

         SELECT dt,
                MAX(platform_id)              AS platform_id,
                MAX(platform)                 AS platform,
                IFNULL(backend_account_id, 0) AS backend_account_id,
                IFNULL(revenue_level, 'all')  AS revenue_level,
                IFNULL(newold_state, 'all')   AS newold_state,
                IFNULL(active_state, 'all')   AS active_state,
                SUM(anchor_cnt)               AS anchor_cnt,
                SUM(new_anchor_cnt)             AS new_anchor_cnt,
                SUM(new_r30_cnt)              AS new_r30_cnt,
                SUM(new_r60_cnt)              AS new_r60_cnt,
                SUM(new_r90_cnt)              AS new_r90_cnt,
                SUM(new_r120_cnt)             AS new_r120_cnt,
                SUM(live_cnt)                 AS live_cnt,
                SUM(duration)                 AS duration,
                SUM(revenue)                  AS revenue,
                SUM(revenue_orig)             AS revenue_orig,
                SUM(guild_income)             AS guild_income,
                SUM(guild_income_orig)        AS guild_income_orig,
                SUM(anchor_income)            AS anchor_income,
                SUM(anchor_income_orig)       AS anchor_income_orig
         FROM bireport.rpt_month_bb_guild
         WHERE backend_account_id != 0
           AND revenue_level != 'all'
           AND newold_state != 'all'
           AND active_state != 'all'
           AND dt = '{month}'
         GROUP BY dt, newold_state, active_state, backend_account_id, revenue_level
         WITH ROLLUP

         UNION

         SELECT dt,
                MAX(platform_id)              AS platform_id,
                MAX(platform)                 AS platform,
                IFNULL(backend_account_id, 0) AS backend_account_id,
                IFNULL(revenue_level, 'all')  AS revenue_level,
                IFNULL(newold_state, 'all')   AS newold_state,
                IFNULL(active_state, 'all')   AS active_state,
                SUM(anchor_cnt)               AS anchor_cnt,
                SUM(new_anchor_cnt)             AS new_anchor_cnt,
                SUM(new_r30_cnt)              AS new_r30_cnt,
                SUM(new_r60_cnt)              AS new_r60_cnt,
                SUM(new_r90_cnt)              AS new_r90_cnt,
                SUM(new_r120_cnt)             AS new_r120_cnt,
                SUM(live_cnt)                 AS live_cnt,
                SUM(duration)                 AS duration,
                SUM(revenue)                  AS revenue,
                SUM(revenue_orig)             AS revenue_orig,
                SUM(guild_income)             AS guild_income,
                SUM(guild_income_orig)        AS guild_income_orig,
                SUM(anchor_income)            AS anchor_income,
                SUM(anchor_income_orig)       AS anchor_income_orig
         FROM bireport.rpt_month_bb_guild
         WHERE backend_account_id != 0
           AND revenue_level != 'all'
           AND newold_state != 'all'
           AND active_state != 'all'
           AND dt = '{month}'
         GROUP BY dt, active_state, backend_account_id, revenue_level, newold_state
         WITH ROLLUP
         UNION

         SELECT dt,
                MAX(platform_id)              AS platform_id,
                MAX(platform)                 AS platform,
                IFNULL(backend_account_id, 0) AS backend_account_id,
                IFNULL(revenue_level, 'all')  AS revenue_level,
                IFNULL(newold_state, 'all')   AS newold_state,
                IFNULL(active_state, 'all')   AS active_state,
                SUM(anchor_cnt)               AS anchor_cnt,
                SUM(new_anchor_cnt)             AS new_anchor_cnt,
                SUM(new_r30_cnt)              AS new_r30_cnt,
                SUM(new_r60_cnt)              AS new_r60_cnt,
                SUM(new_r90_cnt)              AS new_r90_cnt,
                SUM(new_r120_cnt)             AS new_r120_cnt,
                SUM(live_cnt)                 AS live_cnt,
                SUM(duration)                 AS duration,
                SUM(revenue)                  AS revenue,
                SUM(revenue_orig)             AS revenue_orig,
                SUM(guild_income)             AS guild_income,
                SUM(guild_income_orig)        AS guild_income_orig,
                SUM(anchor_income)            AS anchor_income,
                SUM(anchor_income_orig)       AS anchor_income_orig
         FROM bireport.rpt_month_bb_guild
         WHERE backend_account_id != 0
           AND revenue_level != 'all'
           AND newold_state != 'all'
           AND active_state != 'all'
           AND dt = '{month}'
         GROUP BY dt, backend_account_id, newold_state, revenue_level, active_state
         WITH ROLLUP) t
         LEFT JOIN spider_bb_backend.account_info ai
                   ON t.backend_account_id = ai.backend_account_id
WHERE dt IS NOT NULL
;


-- 报表用，计算上周、上月同期数据---
DELETE
FROM bireport.rpt_month_bb_guild_view
WHERE dt = '{month}';
INSERT INTO bireport.rpt_month_bb_guild_view
(
  `dt`,
  `remark`,
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
  `anchor_cnt_lastmonth`,
  `live_cnt`,
  `live_cnt_lastmonth`,
  `live_ratio`,
  `live_ratio_lastmonth`,
  `duration`,
  `duration_lastmonth`,
  `revenue`,
  `revenue_lastmonth`,
  `revenue_per_live`,
  `revenue_per_live_lastmonth`,
  `guild_income`,
  `anchor_income`
)
SELECT t1.dt,
       t1.remark,
       t1.revenue_level,
       t1.newold_state,
       t1.active_state,
       t1.anchor_cnt,
       t1.new_anchor_cnt                                               as `new_anchor_cnt`,
       if(t1.new_anchor_cnt=0,null,concat(ROUND(t1.new_r30_cnt/t1.new_anchor_cnt*100,1),'%'))  as new_r30_cnt,
       if(t1.new_anchor_cnt=0,null,concat(ROUND(t1.new_r60_cnt/t1.new_anchor_cnt*100,1),'%'))  as new_r60_cnt,
       if(t1.new_anchor_cnt=0,null,concat(ROUND(t1.new_r90_cnt/t1.new_anchor_cnt*100,1),'%'))  as new_r90_cnt,
       if(t1.new_anchor_cnt=0,null,concat(ROUND(t1.new_r120_cnt/t1.new_anchor_cnt*100,1),'%')) as new_r120_cnt,
       t3.anchor_cnt+t1.new_anchor_cnt-t1.anchor_cnt                   as `loss_anchor_cnt`,
       t1.anchor_cnt-t3.anchor_cnt                                     as `increase_anchor_cnt`,
       t3.anchor_cnt                                                   AS anchor_cnt_lastmonth,
       t1.live_cnt,
       t3.live_cnt                                                     AS live_cnt_lastmonth,
       IF(t1.anchor_cnt > 0, ROUND(t1.live_cnt / t1.anchor_cnt, 3), 0) AS live_ratio,
       IF(t3.anchor_cnt > 0, ROUND(t3.live_cnt / t3.anchor_cnt, 3), 0) AS live_ratio_lastmonth,
       ROUND(t1.duration / 3600, 1)                                    AS duration,
       ROUND(t3.duration / 3600, 1)                                    AS duration_lastmonth,
       t1.revenue,
       t3.revenue                                                      AS revenue_lastmonth,
       IF(t1.live_cnt > 0, ROUND(t1.revenue / t1.live_cnt, 0), 0)      AS revenue_per_live,
       IF(t3.live_cnt > 0, ROUND(t3.revenue / t3.live_cnt, 0), 0)      AS revenue_per_live_lastmonth,
       t1.guild_income                                                 AS guild_income,
       t1.anchor_income                                                AS anchor_income
FROM bireport.rpt_month_bb_guild t1
         LEFT JOIN bireport.rpt_month_bb_guild t3
                   ON t1.dt - INTERVAL 1 MONTH = t3.dt
                       AND t1.remark = t3.remark
                       AND t1.revenue_level = t3.revenue_level
                       AND t1.newold_state = t3.newold_state
                       AND t1.active_state = t3.active_state
WHERE t1.dt = '{month}'
;


-- -- 报表用，计算指标占比---
-- DELETE
-- FROM bireport.rpt_month_bb_guild_view_compare
-- WHERE dt = '{month}';
-- INSERT INTO bireport.rpt_month_bb_guild_view_compare
-- SELECT *
-- FROM (SELECT dt,
--              backend_account_id,
--              revenue_level,
--              newold_state,
--              active_state,
--              '主播数'      AS idx,
--              anchor_cnt AS val
--       FROM bireport.rpt_month_bb_guild
--       WHERE revenue_level != 'all'
--         AND dt = '{month}'
--       UNION
--       SELECT dt,
--              backend_account_id,
--              revenue_level,
--              newold_state,
--              active_state,
--              '开播数'    AS idx,
--              live_cnt AS val
--       FROM bireport.rpt_month_bb_guild
--       WHERE revenue_level != 'all'
--         AND dt = '{month}'
--       UNION
--       SELECT dt,
--              backend_account_id,
--              revenue_level,
--              newold_state,
--              active_state,
--              '流水'    AS idx,
--              revenue AS val
--       FROM bireport.rpt_month_bb_guild
--       WHERE revenue_level != 'all'
--         AND dt = '{month}'
--       UNION
--       SELECT dt,
--              backend_account_id,
--              revenue_level,
--              newold_state,
--              active_state,
--              '开播人均流水'                     AS idx,
--              round(revenue / live_cnt, 0) AS val
--       FROM bireport.rpt_month_bb_guild
--       WHERE revenue_level != 'all'
--         AND dt = '{month}'
--         AND live_cnt > 0
--      ) t
-- ;

