# REPLACE INTO stage.bb_guild_income_rate
# SELECT backend_account_id,
#        AVG(guild_income / revenue)  AS guild_income_rate,
#        AVG(anchor_income / revenue) AS anchor_income_rate
# FROM bireport.rpt_month_bb_guild
# WHERE guild_income > 0
#   AND revenue > 0
#   AND dt <> DATE_FORMAT('{cur_date}', '%Y-%m-01')
# GROUP BY backend_account_id
# ;
#
#
# -- 公会每月流水、公会收入、主播收入
# -- DROP TABLE IF EXISTS bireport.rpt_month_bb_guild;
# -- CREATE TABLE bireport.rpt_month_bb_guild AS
# REPLACE INTO bireport.rpt_month_bb_guild
# SELECT t.dt,
#        t.platform_id,
#        pf.platform_name                              AS platform,
#        t.backend_account_id,
#        t1.remark,
#        SUM(t.anchor_cnt)                             AS anchor_cnt,
#        SUM(t.anchor_live_cnt)                        AS live_cnt,
#        SUM(t.revenue) / 1000                         AS revenue,
#        SUM(t.revenue)                                AS revenune_orig,
#        SUM(t.revenue * ig.guild_income_rate) / 1000  AS guild_income,
#        SUM(t.revenue * ig.guild_income_rate)         AS guild_income_orig,
#        SUM(t.revenue * ig.anchor_income_rate) / 1000 AS anchor_income,
#        SUM(t.revenue * ig.anchor_income_rate)        AS anchor_income_orig
# FROM warehouse.dw_bb_month_guild_live t
#          LEFT JOIN spider_bb_backend.account_info t1 ON t.backend_account_id = t1.backend_account_id
#          LEFT JOIN stage.bb_guild_income_rate ig ON t.backend_account_id = ig.backend_account_id
#          lEFT JOIN warehouse.platform pf ON pf.id = t.platform_id
# WHERE dt = '{month}'
# GROUP BY t.dt,
#          t.platform_id,
#          pf.platform_name,
#          t.backend_account_id,
#          t1.remark
# ;
#
#
# REPLACE INTO bireport.rpt_month_bb_guild
# SELECT gl.dt,
#        gl.platform_id,
#        gl.platform_name                                                              AS platform,
#        gl.backend_account_id,
#        ai.remark,
#        gl.anchor_cnt                                                                 AS anchor_cnt,
#        gl.anchor_live_cnt                                                            AS live_cnt,
#        gl.revenue / 1000                                                             AS revenue,
#        gl.revenue                                                                    AS revenue_orig,
#        (gr.guild_virtual_coin_true - gr.anchor_change_coin - gr.anchor_income_true -
#         gr.anchor_base_coin - gr.operate_award_punish_coin - gr.special_coin) / 1000 AS guild_income,
#        (gr.guild_virtual_coin_true - gr.anchor_change_coin - gr.anchor_income_true -
#         gr.anchor_base_coin - gr.operate_award_punish_coin - special_coin)           AS guild_income_orig,
#        (gr.anchor_income_true + gr.anchor_base_coin + gr.operate_award_punish_coin +
#         gr.special_coin) / 1000                                                      AS anchor_income,
#        (gr.anchor_income_true + gr.anchor_base_coin + gr.operate_award_punish_coin +
#         gr.special_coin)                                                             AS anchor_income_orig
# FROM (SELECT dt,
#              platform_id,
#              platform_name,
#              backend_account_id,
#              SUM(anchor_cnt)
#                                   AS anchor_cnt,
#              SUM(anchor_live_cnt) AS anchor_live_cnt,
#              SUM(revenue)         AS revenue
#       FROM warehouse.dw_bb_month_guild_live
#       WHERE dt >= '2019-02-01'
#         -- 结算数据只有2019年2月之后的
#         AND dt = '{month}'
#       GROUP BY dt,
#                platform_id,
#                platform_name,
#                backend_account_id) gl
#          INNER JOIN warehouse.dw_bb_month_guild_live_true gr
#                     ON gl.dt = gr.dt AND gl.backend_account_id = gr.backend_account_id
#          LEFT JOIN spider_bb_backend.account_info ai ON gl.backend_account_id = ai.backend_account_id
# ;


-- =====================================================================================================================
-- rpt_month_bb_guild_new
DELETE
FROM bireport.rpt_month_bb_guild_new
WHERE dt = '{month}';
REPLACE INTO bireport.rpt_month_bb_guild_new
SELECT gl.dt,
       gl.platform_id,
       pf.platform_name                            AS platform,
       gl.backend_account_id,
       ai.remark,
       gl.revenue_level,
       gl.newold_state,
       gl.active_state,
       gl.anchor_cnt,
       gl.anchor_live_cnt                          AS live_cnt,
       gl.duration,
       gl.revenue / 1000                           AS revenue,
       gl.revenue                                  AS revenune_orig,
       (gl.revenue * ig.guild_income_rate) / 1000  AS guild_income,
       gl.revenue * ig.guild_income_rate           AS guild_income_orig,
       (gl.revenue * ig.anchor_income_rate) / 1000 AS anchor_income,
       gl.revenue * ig.anchor_income_rate          AS anchor_income_orig
FROM warehouse.dw_bb_month_guild_live gl
         LEFT JOIN spider_bb_backend.account_info ai ON gl.backend_account_id = ai.backend_account_id
         LEFT JOIN stage.bb_guild_income_rate ig ON gl.backend_account_id = ig.backend_account_id
         lEFT JOIN warehouse.platform pf ON pf.id = gl.platform_id
WHERE dt = '{month}'
;


REPLACE INTO bireport.rpt_month_bb_guild_new
SELECT t.dt,
       t.platform_id,
       t.platform,
       t.backend_account_id,
       CASE WHEN t.backend_account_id = 0 THEN 'all' ELSE ai.remark END AS remark,
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
                MAX(platform_id)              AS platform_id,
                MAX(platform)                 AS platform,
                IFNULL(backend_account_id, 0) AS backend_account_id,
                IFNULL(revenue_level, 'all')  AS revenue_level,
                IFNULL(newold_state, 'all')   AS newold_state,
                IFNULL(active_state, 'all')   AS active_state,
                SUM(anchor_cnt)               AS anchor_cnt,
                SUM(live_cnt)                 AS live_cnt,
                SUM(duration)                 AS duration,
                SUM(revenue)                  AS revenue,
                SUM(revenue_orig)             AS revenue_orig,
                SUM(guild_income)             AS guild_income,
                SUM(guild_income_orig)        AS guild_income_orig,
                SUM(anchor_income)            AS anchor_income,
                SUM(anchor_income_orig)       AS anchor_income_orig
         FROM bireport.rpt_month_bb_guild_new
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
                SUM(live_cnt)                 AS live_cnt,
                SUM(duration)                 AS duration,
                SUM(revenue)                  AS revenue,
                SUM(revenue_orig)             AS revenue_orig,
                SUM(guild_income)             AS guild_income,
                SUM(guild_income_orig)        AS guild_income_orig,
                SUM(anchor_income)            AS anchor_income,
                SUM(anchor_income_orig)       AS anchor_income_orig
         FROM bireport.rpt_month_bb_guild_new
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
                SUM(live_cnt)                 AS live_cnt,
                SUM(duration)                 AS duration,
                SUM(revenue)                  AS revenue,
                SUM(revenue_orig)             AS revenue_orig,
                SUM(guild_income)             AS guild_income,
                SUM(guild_income_orig)        AS guild_income_orig,
                SUM(anchor_income)            AS anchor_income,
                SUM(anchor_income_orig)       AS anchor_income_orig
         FROM bireport.rpt_month_bb_guild_new
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
                SUM(live_cnt)                 AS live_cnt,
                SUM(duration)                 AS duration,
                SUM(revenue)                  AS revenue,
                SUM(revenue_orig)             AS revenue_orig,
                SUM(guild_income)             AS guild_income,
                SUM(guild_income_orig)        AS guild_income_orig,
                SUM(anchor_income)            AS anchor_income,
                SUM(anchor_income_orig)       AS anchor_income_orig
         FROM bireport.rpt_month_bb_guild_new
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
                SUM(live_cnt)                 AS live_cnt,
                SUM(duration)                 AS duration,
                SUM(revenue)                  AS revenue,
                SUM(revenue_orig)             AS revenue_orig,
                SUM(guild_income)             AS guild_income,
                SUM(guild_income_orig)        AS guild_income_orig,
                SUM(anchor_income)            AS anchor_income,
                SUM(anchor_income_orig)       AS anchor_income_orig
         FROM bireport.rpt_month_bb_guild_new
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
FROM bireport.rpt_month_bb_guild_new_view
WHERE dt = '{month}';
REPLACE INTO bireport.rpt_month_bb_guild_new_view
SELECT t1.dt,
       t1.remark,
       t1.revenue_level,
       t1.newold_state,
       t1.active_state,
       t1.anchor_cnt,
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
FROM bireport.rpt_month_bb_guild_new t1
         LEFT JOIN bireport.rpt_month_bb_guild_new t3
                   ON t1.dt - INTERVAL 1 MONTH = t3.dt
                       AND t1.remark = t3.remark
                       AND t1.revenue_level = t3.revenue_level
                       AND t1.newold_state = t3.newold_state
                       AND t1.active_state = t3.active_state
WHERE t1.dt = '{month}'
;


-- 报表用，计算指标占比---
DELETE
FROM bireport.rpt_month_bb_guild_new_view_compare
WHERE dt = '{month}';
REPLACE INTO bireport.rpt_month_bb_guild_new_view_compare
SELECT *
FROM (SELECT dt,
             backend_account_id,
             revenue_level,
             newold_state,
             active_state,
             '主播数'      AS idx,
             anchor_cnt AS val
      FROM bireport.rpt_month_bb_guild_new
      WHERE revenue_level != 'all'
        AND dt = '{month}'
      UNION
      SELECT dt,
             backend_account_id,
             revenue_level,
             newold_state,
             active_state,
             '开播数'    AS idx,
             live_cnt AS val
      FROM bireport.rpt_month_bb_guild_new
      WHERE revenue_level != 'all'
        AND dt = '{month}'
      UNION
      SELECT dt,
             backend_account_id,
             revenue_level,
             newold_state,
             active_state,
             '流水'    AS idx,
             revenue AS val
      FROM bireport.rpt_month_bb_guild_new
      WHERE revenue_level != 'all'
        AND dt = '{month}'
      UNION
      SELECT dt,
             backend_account_id,
             revenue_level,
             newold_state,
             active_state,
             '开播人均流水'                     AS idx,
             round(revenue / live_cnt, 0) AS val
      FROM bireport.rpt_month_bb_guild_new
      WHERE revenue_level != 'all'
        AND dt = '{month}'
        AND live_cnt > 0
     ) t
;

