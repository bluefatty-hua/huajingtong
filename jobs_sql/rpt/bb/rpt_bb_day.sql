
-- ---------------------rpt_day_bb_guild_new-----------------------------
DELETE
FROM bireport.rpt_day_bb_guild
WHERE dt  >='{month}' and dt <= LAST_DAY('{month}') ;
INSERT INTO bireport.rpt_day_bb_guild
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
       t.platform_name                                                         AS platform,
       t.backend_account_id,
       ai.remark,
       t.revenue_level,
       t.newold_state,
       t.active_state,
       t.anchor_cnt                                                       AS anchor_cnt,
       t.new_anchor_cnt as new_anchor_cnt,
       t.live_cnt                                                         AS live_cnt,
       t.duration                                                        AS duration,
       t.revenue                                                         AS revenue,
       t.revenue_orig                                                          AS revenue_orig,
       0                                                                       AS guild_income,
       0                                                                       AS guild_income_orig,
       0 AS anchor_income,
       0 as                                                                       anchor_income_orig
FROM warehouse.dw_bb_day_guild_live t
         LEFT JOIN spider_bb_backend.account_info ai ON t.backend_account_id = ai.backend_account_id
WHERE t.dt  >='{month}' and t.dt <= LAST_DAY('{month}');



REPLACE INTO bireport.rpt_day_bb_guild 
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
       IFNULL(ai.remark, 'all') AS remark,
       t.revenue_level,
       t.newold_state,
       t.active_state,
       t.anchor_cnt,
       t.new_anchor_cnt,
       t.live_cnt,
       t.duration,
       t.revenue,
       t.revenue_orig,
       t.guild_income,
       t.guild_income_orig,
       t.anchor_income,
       t.anchor_income_orig
FROM (SELECT dt,
             MAX(platform_id)              AS platform_id,
             MAX(platform)                 AS platform,
             IFNULL(backend_account_id, 0) AS backend_account_id,
             IFNULL(revenue_level, 'all')  AS revenue_level,
             IFNULL(newold_state, 'all')   AS newold_state,
             IFNULL(active_state, 'all')   AS active_state,
             SUM(anchor_cnt)               AS anchor_cnt,
             SUM(new_anchor_cnt)             AS new_anchor_cnt,
             SUM(live_cnt)                 AS live_cnt,
             SUM(duration)                 AS duration,
             SUM(revenue)                  AS revenue,
             SUM(revenue_orig)             AS revenue_orig,
             SUM(guild_income)             AS guild_income,
             SUM(guild_income_orig)        AS guild_income_orig,
             SUM(anchor_income)            AS anchor_income,
             SUM(anchor_income_orig)       AS anchor_income_orig
      FROM bireport.rpt_day_bb_guild
      WHERE backend_account_id != 0
        AND revenue_level != 'all'
        AND newold_state != 'all'
        AND active_state != 'all'
        AND dt >= '{month}'
        AND dt <= LAST_DAY('{month}')
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
             SUM(live_cnt)                 AS live_cnt,
             SUM(duration)                 AS duration,
             SUM(revenue)                  AS revenue,
             SUM(revenue_orig)             AS revenue_orig,
             SUM(guild_income)             AS guild_income,
             SUM(guild_income_orig)        AS guild_income_orig,
             SUM(anchor_income)            AS anchor_income,
             SUM(anchor_income_orig)       AS anchor_income_orig
      FROM bireport.rpt_day_bb_guild
      WHERE backend_account_id != 0
        AND revenue_level != 'all'
        AND newold_state != 'all'
        AND active_state != 'all'
        AND dt >= '{month}'
        AND dt <= LAST_DAY('{month}')
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
             SUM(live_cnt)                 AS live_cnt,
             SUM(duration)                 AS duration,
             SUM(revenue)                  AS revenue,
             SUM(revenue_orig)             AS revenue_orig,
             SUM(guild_income)             AS guild_income,
             SUM(guild_income_orig)        AS guild_income_orig,
             SUM(anchor_income)            AS anchor_income,
             SUM(anchor_income_orig)       AS anchor_income_orig
      FROM bireport.rpt_day_bb_guild
      WHERE backend_account_id != 0
        AND revenue_level != 'all'
        AND newold_state != 'all'
        AND active_state != 'all'
        AND dt >= '{month}'
        AND dt <= LAST_DAY('{month}')
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
             SUM(live_cnt)                 AS live_cnt,
             SUM(duration)                 AS duration,
             SUM(revenue)                  AS revenue,
             SUM(revenue_orig)             AS revenue_orig,
             SUM(guild_income)             AS guild_income,
             SUM(guild_income_orig)        AS guild_income_orig,
             SUM(anchor_income)            AS anchor_income,
             SUM(anchor_income_orig)       AS anchor_income_orig
      FROM bireport.rpt_day_bb_guild
      WHERE backend_account_id != 0
        AND revenue_level != 'all'
        AND newold_state != 'all'
        AND active_state != 'all'
        AND dt >= '{month}'
        AND dt <= LAST_DAY('{month}')
      GROUP BY dt, active_state, backend_account_id, revenue_level, newold_state
      WITH ROLLUP

      UNION ALL
      SELECT dt,
             MAX(platform_id)              AS platform_id,
             MAX(platform)                 AS platform,
             IFNULL(backend_account_id, 0) AS backend_account_id,
             IFNULL(revenue_level, 'all')  AS revenue_level,
             IFNULL(newold_state, 'all')   AS newold_state,
             IFNULL(active_state, 'all')   AS active_state,
             SUM(anchor_cnt)               AS anchor_cnt,
             SUM(new_anchor_cnt)             AS new_anchor_cnt,
             SUM(live_cnt)                 AS live_cnt,
             SUM(duration)                 AS duration,
             SUM(revenue)                  AS revenue,
             SUM(revenue_orig)             AS revenue_orig,
             SUM(guild_income)             AS guild_income,
             SUM(guild_income_orig)        AS guild_income_orig,
             SUM(anchor_income)            AS anchor_income,
             SUM(anchor_income_orig)       AS anchor_income_orig
      FROM bireport.rpt_day_bb_guild
      WHERE backend_account_id != 0
        AND revenue_level != 'all'
        AND newold_state != 'all'
        AND active_state != 'all'
        AND dt >= '{month}'
        AND dt <= LAST_DAY('{month}')
      GROUP BY dt, backend_account_id, newold_state, revenue_level, active_state
      WITH ROLLUP
     ) t
         LEFT JOIN warehouse.ods_bb_account_info ai ON t.backend_account_id = ai.backend_account_id
WHERE dt IS NOT NULL
;


-- 报表用，计算上周、上月同期数据---
DELETE
FROM bireport.rpt_day_bb_guild_view
WHERE dt >= '{month}'
  AND dt <= LAST_DAY('{month}');
INSERT INTO bireport.rpt_day_bb_guild_view
(
  `dt`,
  `remark`,
  `revenue_level`,
  `newold_state`,
  `active_state`,
  `anchor_cnt`,
  `new_anchor_cnt`,
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
  `revenue_per_live_lastmonth`
  )
SELECT t1.dt,
       t1.remark,
       t1.revenue_level,
       t1.newold_state,
       t1.active_state,
       t1.anchor_cnt,
       t1.new_anchor_cnt                                               as `new_anchor_cnt`,
       t1.new_anchor_cnt-t1.anchor_cnt+t4.anchor_cnt                   as `loss_anchor_cnt`,
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
       IF(t3.live_cnt > 0, ROUND(t3.revenue / t3.live_cnt, 0), 0)      AS revenue_per_live_lastmonth
FROM bireport.rpt_day_bb_guild t1
         LEFT JOIN bireport.rpt_day_bb_guild t2
                   ON t1.dt - INTERVAL 7 DAY = t2.dt
                       AND t1.remark = t2.remark
                       AND t1.revenue_level = t2.revenue_level
                       AND t1.newold_state = t2.newold_state
                       AND t1.active_state = t2.active_state
         LEFT JOIN bireport.rpt_day_bb_guild t3
                   ON t1.dt - INTERVAL 1 MONTH = t3.dt
                       AND t1.remark = t3.remark
                       AND t1.revenue_level = t3.revenue_level
                       AND t1.newold_state = t3.newold_state
                       AND t1.active_state = t3.active_state
         LEFT JOIN bireport.rpt_day_bb_guild t4
                   ON t1.dt - INTERVAL 1 day = t4.dt
                       AND t1.remark = t4.remark
                       AND t1.revenue_level = t4.revenue_level
                       AND t1.newold_state = t4.newold_state
                       AND t1.active_state = t4.active_state
WHERE t1.dt >= '{month}'
  AND t1.dt <= LAST_DAY('{month}')
;


-- -- 报表用，计算指标占比---
-- DELETE
-- FROM bireport.rpt_day_bb_guild_view_compare
-- WHERE dt >= '{month}'
--   AND dt <= LAST_DAY('{month}');
-- INSERT INTO bireport.rpt_day_bb_guild_view_compare
-- SELECT *
-- FROM (SELECT dt,
--              remark,
--              revenue_level,
--              newold_state,
--              active_state,
--              '主播数'      AS idx,
--              anchor_cnt AS val
--       FROM bireport.rpt_day_bb_guild
--       WHERE revenue_level != 'all'
--         AND dt >= '{month}'
--         AND dt <= LAST_DAY('{month}')
--       UNION
--       SELECT dt,
--              remark,
--              revenue_level,
--              newold_state,
--              active_state,
--              '开播数'    AS idx,
--              live_cnt AS val
--       FROM bireport.rpt_day_bb_guild
--       WHERE revenue_level != 'all'
--         AND dt >= '{month}'
--         AND dt <= LAST_DAY('{month}')
--       UNION
--       SELECT dt,
--              remark,
--              revenue_level,
--              newold_state,
--              active_state,
--              '流水'    AS idx,
--              revenue AS val
--       FROM bireport.rpt_day_bb_guild
--       where revenue_level != 'all'
--         AND dt >= '{month}'
--         AND dt <= LAST_DAY('{month}')
--       UNION
--       SELECT dt,
--              remark,
--              revenue_level,
--              newold_state,
--              active_state,
--              '开播人均流水'                     AS idx,
--              round(revenue / live_cnt, 0) AS val
--       FROM bireport.rpt_day_bb_guild
--       WHERE revenue_level != 'all'
--         AND dt >= '{month}'
--         AND dt <= LAST_DAY('{month}')
--         AND live_cnt > 0) t
-- ;


