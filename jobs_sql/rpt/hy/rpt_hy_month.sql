-- rpt_month_now_guild_new
DELETE
FROM bireport.rpt_month_hy_guild
WHERE dt = '{month}';
INSERT INTO bireport.rpt_month_hy_guild
(
    dt,
    channel_type,
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
    revenue
)
SELECT dt,
       channel_type,
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
       revenue
FROM warehouse.dw_huya_month_guild_live
WHERE dt = '{month}';


REPLACE INTO bireport.rpt_month_hy_guild
(
    dt,
    channel_type,
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
    revenue
)
SELECT t.dt,
       t.channel_type,
       t.channel_num,
       t.revenue_level,
       t.newold_state,
       t.active_state,
       t.anchor_cnt,
       t.new_anchor_cnt,
       t.new_r30_cnt,
       t.new_r60_cnt,
       t.new_r90_cnt,
       t.new_r120_cnt,
       t.live_cnt,
       t.duration,
       t.revenue
FROM (
         SELECT dt,
                IFNULL(channel_type, 'all')  AS channel_type,
                IFNULL(channel_num, 'all')   AS channel_num,
                IFNULL(revenue_level, 'all') AS revenue_level,
                IFNULL(newold_state, 'all')  AS newold_state,
                IFNULL(active_state, 'all')  AS active_state,
                SUM(anchor_cnt)              AS anchor_cnt,
                SUM(new_anchor_cnt)               AS new_anchor_cnt,
                SUM(new_r30_cnt)                  AS new_r30_cnt,
                SUM(new_r60_cnt)                  AS new_r60_cnt,
                SUM(new_r90_cnt)                  AS new_r90_cnt,
                SUM(new_r120_cnt)                 AS new_r120_cnt,
                SUM(live_cnt)                AS live_cnt,
                SUM(duration)                AS duration,
                SUM(revenue)                 AS revenue
         FROM bireport.rpt_month_hy_guild
         WHERE channel_type != 'all'
           AND channel_num != 'all'
           AND revenue_level != 'all'
           AND newold_state != 'all'
           AND active_state != 'all'
           AND dt = '{month}'
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
                SUM(new_anchor_cnt)               AS new_anchor_cnt,
                SUM(new_r30_cnt)                  AS new_r30_cnt,
                SUM(new_r60_cnt)                  AS new_r60_cnt,
                SUM(new_r90_cnt)                  AS new_r90_cnt,
                SUM(new_r120_cnt)                 AS new_r120_cnt,
                SUM(live_cnt)                AS live_cnt,
                SUM(duration)                AS duration,
                SUM(revenue)                 AS revenue
         FROM bireport.rpt_month_hy_guild
         WHERE channel_type != 'all'
           AND channel_num != 'all'
           AND revenue_level != 'all'
           AND newold_state != 'all'
           AND active_state != 'all'
           AND dt = '{month}'
         GROUP BY dt, channel_type, revenue_level ,channel_num, newold_state, active_state
         WITH ROLLUP

         UNION

         SELECT dt,
                IFNULL(channel_type, 'all')  AS channel_type,
                IFNULL(channel_num, 'all')   AS channel_num,
                IFNULL(revenue_level, 'all') AS revenue_level,
                IFNULL(newold_state, 'all')  AS newold_state,
                IFNULL(active_state, 'all')  AS active_state,
                SUM(anchor_cnt)              AS anchor_cnt,
                SUM(new_anchor_cnt)               AS new_anchor_cnt,
                SUM(new_r30_cnt)                  AS new_r30_cnt,
                SUM(new_r60_cnt)                  AS new_r60_cnt,
                SUM(new_r90_cnt)                  AS new_r90_cnt,
                SUM(new_r120_cnt)                 AS new_r120_cnt,
                SUM(live_cnt)                AS live_cnt,
                SUM(duration)                AS duration,
                SUM(revenue)                 AS revenue
         FROM bireport.rpt_month_hy_guild
         WHERE channel_type != 'all'
           AND channel_num != 'all'
           AND revenue_level != 'all'
           AND newold_state != 'all'
           AND active_state != 'all'
           AND dt = '{month}'
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
                SUM(new_anchor_cnt)               AS new_anchor_cnt,
                SUM(new_r30_cnt)                  AS new_r30_cnt,
                SUM(new_r60_cnt)                  AS new_r60_cnt,
                SUM(new_r90_cnt)                  AS new_r90_cnt,
                SUM(new_r120_cnt)                 AS new_r120_cnt,
                SUM(live_cnt)                AS live_cnt,
                SUM(duration)                AS duration,
                SUM(revenue)                 AS revenue
         FROM bireport.rpt_month_hy_guild
         WHERE channel_type != 'all'
           AND channel_num != 'all'
           AND revenue_level != 'all'
           AND newold_state != 'all'
           AND active_state != 'all'
           AND dt = '{month}'
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
                SUM(new_anchor_cnt)               AS new_anchor_cnt,
                SUM(new_r30_cnt)                  AS new_r30_cnt,
                SUM(new_r60_cnt)                  AS new_r60_cnt,
                SUM(new_r90_cnt)                  AS new_r90_cnt,
                SUM(new_r120_cnt)                 AS new_r120_cnt,
                SUM(live_cnt)                AS live_cnt,
                SUM(duration)                AS duration,
                SUM(revenue)                 AS revenue
         FROM bireport.rpt_month_hy_guild
         WHERE channel_type != 'all'
           AND channel_num != 'all'
           AND revenue_level != 'all'
           AND newold_state != 'all'
           AND active_state != 'all'
           AND dt = '{month}'
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
                SUM(new_anchor_cnt)               AS new_anchor_cnt,
                SUM(new_r30_cnt)                  AS new_r30_cnt,
                SUM(new_r60_cnt)                  AS new_r60_cnt,
                SUM(new_r90_cnt)                  AS new_r90_cnt,
                SUM(new_r120_cnt)                 AS new_r120_cnt,
                SUM(live_cnt)                AS live_cnt,
                SUM(duration)                AS duration,
                SUM(revenue)                 AS revenue
         FROM bireport.rpt_month_hy_guild
         WHERE channel_type != 'all'
           AND channel_num != 'all'
           AND revenue_level != 'all'
           AND newold_state != 'all'
           AND active_state != 'all'
           AND dt = '{month}'
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
                SUM(new_anchor_cnt)               AS new_anchor_cnt,
                SUM(new_r30_cnt)                  AS new_r30_cnt,
                SUM(new_r60_cnt)                  AS new_r60_cnt,
                SUM(new_r90_cnt)                  AS new_r90_cnt,
                SUM(new_r120_cnt)                 AS new_r120_cnt,
                SUM(live_cnt)                AS live_cnt,
                SUM(duration)                AS duration,
                SUM(revenue)                 AS revenue
         FROM bireport.rpt_month_hy_guild
         WHERE channel_type != 'all'
           AND channel_num != 'all'
           AND revenue_level != 'all'
           AND newold_state != 'all'
           AND active_state != 'all'
           AND dt = '{month}'
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
                SUM(new_anchor_cnt)               AS new_anchor_cnt,
                SUM(new_r30_cnt)                  AS new_r30_cnt,
                SUM(new_r60_cnt)                  AS new_r60_cnt,
                SUM(new_r90_cnt)                  AS new_r90_cnt,
                SUM(new_r120_cnt)                 AS new_r120_cnt,
                SUM(live_cnt)                AS live_cnt,
                SUM(duration)                AS duration,
                SUM(revenue)                 AS revenue
         FROM bireport.rpt_month_hy_guild
         WHERE channel_type != 'all'
           AND channel_num != 'all'
           AND revenue_level != 'all'
           AND newold_state != 'all'
           AND active_state != 'all'
           AND dt = '{month}'
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
                SUM(new_anchor_cnt)               AS new_anchor_cnt,
                SUM(new_r30_cnt)                  AS new_r30_cnt,
                SUM(new_r60_cnt)                  AS new_r60_cnt,
                SUM(new_r90_cnt)                  AS new_r90_cnt,
                SUM(new_r120_cnt)                 AS new_r120_cnt,
                SUM(live_cnt)                AS live_cnt,
                SUM(duration)                AS duration,
                SUM(revenue)                 AS revenue
         FROM bireport.rpt_month_hy_guild
         WHERE channel_type != 'all'
           AND channel_num != 'all'
           AND revenue_level != 'all'
           AND newold_state != 'all'
           AND active_state != 'all'
           AND dt = '{month}'
         GROUP BY dt, active_state, channel_type, newold_state, revenue_level, channel_num
         WITH ROLLUP
     ) t
WHERE dt IS NOT NULL
;



UPDATE bireport.rpt_month_hy_guild a
    INNER JOIN warehouse.dw_huya_month_guild_live_true b
    ON a.dt = b.dt AND a.channel_num = b.channel_num
SET a.revenue = b.revenue
WHERE a.dt <= '2019-11-01'
  AND a.dt = '{month}'
  AND a.channel_type <> 'all'
  AND a.channel_num <> 'all'
  AND a.newold_state = 'all'
  AND a.active_state = 'all'
  AND a.revenue_level = 'all'
;


UPDATE bireport.rpt_month_hy_guild a
    INNER JOIN (SELECT gl.dt, ai.channel_type, SUM(gl.revenue) AS revenue
                FROM warehouse.dw_huya_month_guild_live_true gl
                         LEFT JOIN warehouse.ods_hy_account_info ai ON gl.channel_id = ai.channel_id
                GROUP BY gl.dt, ai.channel_type) b
    ON a.dt = b.dt AND a.channel_type = b.channel_type
SET a.revenue = b.revenue
WHERE a.dt <= '2019-11-01'
  AND a.dt = '{month}'
  AND a.channel_type <> 'all'
  AND a.channel_num = 'all'
  AND a.newold_state = 'all'
  AND a.active_state = 'all'
  AND a.revenue_level = 'all'
;


UPDATE bireport.rpt_month_hy_guild a
    INNER JOIN (SELECT dt, SUM(revenue) AS revenue FROM warehouse.dw_huya_month_guild_live_true GROUP BY dt) b
    ON a.dt = b.dt
SET a.revenue = b.revenue
WHERE a.dt <= '2019-11-01'
  AND a.dt = '{month}'
  AND a.channel_type = 'all'
  AND a.channel_num = 'all'
  AND a.newold_state = 'all'
  AND a.active_state = 'all'
  AND a.revenue_level = 'all'
;


-- 报表用，计算上周、上月同期数据---
DELETE
FROM bireport.rpt_month_hy_guild_view
WHERE dt = '{month}';
INSERT INTO bireport.rpt_month_hy_guild_view
(
  `dt`,
  `channel_type`,
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
  `revenue_per_live_lastmonth`
)
SELECT t1.dt,
       t1.channel_type,
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
       t1.new_anchor_cnt-t1.anchor_cnt+t3.anchor_cnt                   as `loss_anchor_cnt`,
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
       IF(t1.live_cnt > 0, ROUND(t1.`revenue` / t1.live_cnt, 0), 0)    AS revenue_per_live,
       IF(t3.live_cnt > 0, ROUND(t3.`revenue` / t3.live_cnt, 0), 0)    AS revenue_per_live_lastmonth
FROM bireport.rpt_month_hy_guild t1
         LEFT JOIN bireport.rpt_month_hy_guild t3
                   ON t1.dt - INTERVAL 1 MONTH = t3.dt
                       AND t1.channel_type = t3.channel_type
                       AND t1.channel_num = t3.channel_num
                       AND t1.revenue_level = t3.revenue_level
                       AND t1.newold_state = t3.newold_state
                       AND t1.active_state = t3.active_state
WHERE t1.dt = '{month}'
;


-- -- 报表用，计算指标占比---
-- DELETE
-- FROM bireport.rpt_month_hy_guild_view_compare
-- WHERE dt = '{month}';
-- INSERT INTO bireport.rpt_month_hy_guild_view_compare
-- SELECT *
-- FROM (SELECT dt,
--              channel_num,
--              revenue_level,
--              newold_state,
--              active_state,
--              '主播数'      AS idx,
--              anchor_cnt AS val
--       FROM bireport.rpt_month_hy_guild
--       WHERE revenue_level != 'all'
--         AND channel_type = 'all'
--         AND dt = '{month}'
--       UNION
--       SELECT dt,
--              channel_num,
--              revenue_level,
--              newold_state,
--              active_state,
--              '开播数'    AS idx,
--              live_cnt AS val
--       FROM bireport.rpt_month_hy_guild
--       WHERE revenue_level != 'all'
--         AND channel_type = 'all'
--         AND dt = '{month}'
--       UNION
--       SELECT dt,
--              channel_num,
--              revenue_level,
--              newold_state,
--              active_state,
--              '流水'    AS idx,
--              revenue AS val
--       FROM bireport.rpt_month_hy_guild
--       WHERE revenue_level != 'all'
--         AND channel_type = 'all'
--         AND dt = '{month}'
--       UNION
--       SELECT dt,
--              channel_num,
--              revenue_level,
--              newold_state,
--              active_state,
--              '开播人均流水'                     AS idx,
--              round(revenue / live_cnt, 0) AS val
--       FROM bireport.rpt_month_hy_guild
--       WHERE revenue_level != 'all'
--         AND channel_type = 'all'
--         AND dt = '{month}'
--         AND live_cnt > 0
--      ) t
-- ;

