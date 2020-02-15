DROP TABLE IF EXISTS stage.bb_guild_income_rate;
CREATE TABLE stage.bb_guild_income_rate AS
SELECT backend_account_id,
       AVG(guild_income / revenue)  AS guild_income_rate,
       AVG(anchor_income / revenue) AS anchor_income_rate
FROM bireport.rpt_month_bb_guild
WHERE guild_income > 0
  AND revenue > 0
  AND DATE_FORMAT(dt, '%Y-%m') <> DATE_FORMAT('{end_date}', '%Y-%m')
GROUP BY backend_account_id
;


-- 公会每月流水、公会收入、主播收入
-- DROP TABLE IF EXISTS bireport.rpt_month_bb_guild;
-- CREATE TABLE bireport.rpt_month_bb_guild AS
DELETE
FROM bireport.rpt_month_bb_guild
WHERE dt BETWEEN DATE_FORMAT('{start_date}', '%Y-%m-01') AND DATE_FORMAT('{end_date}', '%Y-%m-01');
INSERT INTO bireport.rpt_month_bb_guild
SELECT gl.dt,
       gl.platform_id,
       gl.platform_name                                                              AS platform,
       gl.backend_account_id,
       ai.remark,
       gl.anchor_cnt                                                                 AS anchor_cnt,
       gl.anchor_live_cnt                                                            AS live_cnt,
       gl.revenue / 1000                                                             AS revenue,
       gl.revenue                                                                    AS revenue_orig,
       (gr.guild_virtual_coin_true - gr.anchor_change_coin - gr.anchor_income_true -
        gr.anchor_base_coin - gr.operate_award_punish_coin - gr.special_coin) / 1000 AS guild_income,
       (gr.guild_virtual_coin_true - gr.anchor_change_coin - gr.anchor_income_true -
        gr.anchor_base_coin - gr.operate_award_punish_coin - special_coin)           AS guild_income_orig,
       (gr.anchor_income_true + gr.anchor_base_coin + gr.operate_award_punish_coin +
        gr.special_coin) / 1000                                                      AS anchor_income,
       (gr.anchor_income_true + gr.anchor_base_coin + gr.operate_award_punish_coin +
        gr.special_coin)                                                             AS anchor_income_orig
FROM (SELECT dt,
             platform_id,
             platform_name,
             backend_account_id,
             SUM(anchor_cnt)      AS anchor_cnt,
             SUM(anchor_live_cnt) AS anchor_live_cnt,
             SUM(revenue)         AS revenue
      FROM warehouse.dw_bb_month_guild_live
      WHERE dt BETWEEN DATE_FORMAT('{start_date}', '%Y-%m-01') AND DATE_FORMAT('{end_date}', '%Y-%m-01')
        AND dt <> DATE_FORMAT('{end_date}', '%Y-%m-01')
        -- AND dt > DATE_FORMAT('2019-01-01', '%Y-%m-01')
      GROUP BY dt,
               platform_id,
               platform_name,
               backend_account_id) gl
         LEFT JOIN warehouse.dw_bb_month_guild_live_true gr
                   ON gl.dt = gr.dt AND gl.backend_account_id = gr.backend_account_id
         LEFT JOIN spider_bb_backend.account_info ai ON gl.backend_account_id = ai.backend_account_id
UNION ALL
SELECT t.dt,
       t.platform_id,
       pf.platform_name                              AS platform,
       t.backend_account_id,
       t1.remark,
       SUM(t.anchor_cnt)                             AS anchor_cnt,
       SUM(t.anchor_live_cnt)                        AS live_cnt,
       SUM(t.revenue) / 1000                         AS revenue,
       SUM(t.revenue)                                AS revenune_orig,
       SUM(t.revenue * ig.guild_income_rate) / 1000  AS guild_income,
       SUM(t.revenue * ig.guild_income_rate)         AS guild_income_orig,
       SUM(t.revenue * ig.anchor_income_rate) / 1000 AS anchor_income,
       SUM(t.revenue * ig.anchor_income_rate)        AS anchor_income_orig
FROM warehouse.dw_bb_month_guild_live t
         LEFT JOIN spider_bb_backend.account_info t1 ON t.backend_account_id = t1.backend_account_id
         LEFT JOIN stage.bb_guild_income_rate ig ON t.backend_account_id = ig.backend_account_id
         lEFT JOIN warehouse.platform pf ON pf.id = t.platform_id
WHERE dt = DATE_FORMAT('{end_date}', '%Y-%m-01')
--   OR dt <= DATE_FORMAT('2019-01-01', '%Y-%m-01')
GROUP BY t.dt,
         t.platform_id,
         pf.platform_name,
         t.backend_account_id,
         t1.remark
;


DELETE
FROM bireport.rpt_month_all_guild
WHERE platform_id = 1001
  AND DATE_FORMAT(dt, '%Y-%m') BETWEEN DATE_FORMAT('{start_date}', '%Y-%m') AND DATE_FORMAT('{end_date}', '%Y-%m');
INSERT INTO bireport.rpt_month_all_guild
SELECT dt,
       platform_id,
       platform,
       channel_num,
       anchor_cnt         AS anchor_cnt,
       live_cnt           AS live_cnt,
       revenue            AS revenue,
       revenue_orig       AS revenue_orig,
       guild_income       AS guild_income,
       guild_income_orig  AS guild_income_orig,
       anchor_income      AS anchor_income,
       anchor_income_orig AS anchor_income_orig
FROM (SELECT dt,
             platform_id,
             platform,
             backend_account_id AS channel_num,
             anchor_cnt,
             live_cnt,
             revenue,
             revenue_orig,
             guild_income,
             guild_income_orig,
             anchor_income,
             anchor_income_orig
      FROM bireport.rpt_month_bb_guild) t
WHERE dt BETWEEN DATE_FORMAT('{start_date}', '%Y-%m-01') AND DATE_FORMAT('{end_date}', '%Y-%m-01')
;


-- rpt_month_bb_guild_new
DELETE
FROM bireport.rpt_month_bb_guild_new
WHERE dt BETWEEN DATE_FORMAT('{start_date}', '%Y-%m-01') AND DATE_FORMAT('{end_date}', '%Y-%m-01');
INSERT INTO bireport.rpt_month_bb_guild_new
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
WHERE dt BETWEEN DATE_FORMAT('{start_date}', '%Y-%m-01') AND DATE_FORMAT('{end_date}', '%Y-%m-01')
;


REPLACE into bireport.rpt_month_bb_guild_new (dt, platform_id, platform, backend_account_id, remark, revenue_level,
                                              newold_state,
                                              active_state,
                                              anchor_cnt, live_cnt, duration, revenue, revenue_orig,
                                              guild_income, guild_income_orig, anchor_income, anchor_income_orig)
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
                MAX(platform_id)                AS platform_id,
                MAX(platform)                   AS platform,
                IFNULL(backend_account_id, '0') AS backend_account_id,
                IFNULL(revenue_level, 'all')    AS revenue_level,
                IFNULL(newold_state, 'all')     AS newold_state,
                IFNULL(active_state, 'all')     AS active_state,
                SUM(anchor_cnt)                 AS anchor_cnt,
                SUM(live_cnt)                   AS live_cnt,
                SUM(duration)                   AS duration,
                SUM(revenue)                    AS revenue,
                SUM(revenue_orig)               AS revenue_orig,
                SUM(guild_income)               AS guild_income,
                SUM(guild_income_orig)          AS guild_income_orig,
                SUM(anchor_income)              AS anchor_income,
                SUM(anchor_income_orig)         AS anchor_income_orig
         FROM bireport.rpt_month_bb_guild_new
         WHERE (backend_account_id != 'all' OR revenue_level != 'all' OR newold_state != 'all' OR
                active_state != 'all')
           AND dt BETWEEN DATE_FORMAT('{start_date}', '%Y-%m-01') AND DATE_FORMAT('{end_date}', '%Y-%m-01')
         GROUP BY dt, backend_account_id, revenue_level, newold_state, active_state
         WITH ROLLUP

         UNION

         SELECT dt,
                MAX(platform_id)                AS platform_id,
                MAX(platform)                   AS platform,
                IFNULL(backend_account_id, '0') AS backend_account_id,
                IFNULL(revenue_level, 'all')    AS revenue_level,
                IFNULL(newold_state, 'all')     AS newold_state,
                IFNULL(active_state, 'all')     AS active_state,
                SUM(anchor_cnt)                 AS anchor_cnt,
                SUM(live_cnt)                   AS live_cnt,
                SUM(duration)                   AS duration,
                SUM(revenue)                    AS revenue,
                SUM(revenue_orig)               AS revenue_orig,
                SUM(guild_income)               AS guild_income,
                SUM(guild_income_orig)          AS guild_income_orig,
                SUM(anchor_income)              AS anchor_income,
                SUM(anchor_income_orig)         AS anchor_income_orig
         FROM bireport.rpt_month_bb_guild_new
         WHERE (backend_account_id != 'all' OR revenue_level != 'all' OR newold_state != 'all' OR
                active_state != 'all')
           AND dt BETWEEN DATE_FORMAT('{start_date}', '%Y-%m-01') AND DATE_FORMAT('{end_date}', '%Y-%m-01')
         GROUP BY dt, revenue_level, newold_state, active_state, backend_account_id
         WITH ROLLUP

         UNION

         SELECT dt,
                MAX(platform_id)                AS platform_id,
                MAX(platform)                   AS platform,
                IFNULL(backend_account_id, '0') AS backend_account_id,
                IFNULL(revenue_level, 'all')    AS revenue_level,
                IFNULL(newold_state, 'all')     AS newold_state,
                IFNULL(active_state, 'all')     AS active_state,
                SUM(anchor_cnt)                 AS anchor_cnt,
                SUM(live_cnt)                   AS live_cnt,
                SUM(duration)                   AS duration,
                SUM(revenue)                    AS revenue,
                SUM(revenue_orig)               AS revenue_orig,
                SUM(guild_income)               AS guild_income,
                SUM(guild_income_orig)          AS guild_income_orig,
                SUM(anchor_income)              AS anchor_income,
                SUM(anchor_income_orig)         AS anchor_income_orig
         FROM bireport.rpt_month_bb_guild_new
         WHERE (backend_account_id != 'all' OR revenue_level != 'all' OR newold_state != 'all' OR
                active_state != 'all')
           AND dt BETWEEN DATE_FORMAT('{start_date}', '%Y-%m-01') AND DATE_FORMAT('{end_date}', '%Y-%m-01')
         GROUP BY dt, newold_state, active_state, backend_account_id, revenue_level
         WITH ROLLUP

         UNION

         SELECT dt,
                MAX(platform_id)                AS platform_id,
                MAX(platform)                   AS platform,
                IFNULL(backend_account_id, '0') AS backend_account_id,
                IFNULL(revenue_level, 'all')    AS revenue_level,
                IFNULL(newold_state, 'all')     AS newold_state,
                IFNULL(active_state, 'all')     AS active_state,
                SUM(anchor_cnt)                 AS anchor_cnt,
                SUM(live_cnt)                   AS live_cnt,
                SUM(duration)                   AS duration,
                SUM(revenue)                    AS revenue,
                SUM(revenue_orig)               AS revenue_orig,
                SUM(guild_income)               AS guild_income,
                SUM(guild_income_orig)          AS guild_income_orig,
                SUM(anchor_income)              AS anchor_income,
                SUM(anchor_income_orig)         AS anchor_income_orig
         FROM bireport.rpt_month_bb_guild_new
         WHERE (backend_account_id != 'all' OR revenue_level != 'all' OR newold_state != 'all' OR
                active_state != 'all')
           AND dt BETWEEN DATE_FORMAT('{start_date}', '%Y-%m-01') AND DATE_FORMAT('{end_date}', '%Y-%m-01')
         GROUP BY dt, active_state, backend_account_id, revenue_level, newold_state
         WITH ROLLUP
         UNION

         SELECT dt,
                MAX(platform_id)                AS platform_id,
                MAX(platform)                   AS platform,
                IFNULL(backend_account_id, '0') AS backend_account_id,
                IFNULL(revenue_level, 'all')    AS revenue_level,
                IFNULL(newold_state, 'all')     AS newold_state,
                IFNULL(active_state, 'all')     AS active_state,
                SUM(anchor_cnt)                 AS anchor_cnt,
                SUM(live_cnt)                   AS live_cnt,
                SUM(duration)                   AS duration,
                SUM(revenue)                    AS revenue,
                SUM(revenue_orig)               AS revenue_orig,
                SUM(guild_income)               AS guild_income,
                SUM(guild_income_orig)          AS guild_income_orig,
                SUM(anchor_income)              AS anchor_income,
                SUM(anchor_income_orig)         AS anchor_income_orig
         FROM bireport.rpt_month_bb_guild_new
         WHERE (backend_account_id != 'all' OR revenue_level != 'all' OR newold_state != 'all' OR
                active_state != 'all')
           AND dt BETWEEN DATE_FORMAT('{start_date}', '%Y-%m-01') AND DATE_FORMAT('{end_date}', '%Y-%m-01')
         GROUP BY dt, newold_state, revenue_level, backend_account_id, active_state
         WITH ROLLUP
     ) t
         LEFT JOIN spider_bb_backend.account_info ai ON t.backend_account_id = ai.backend_account_id
;


replace into bireport.rpt_month_all_new
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
FROM bireport.rpt_month_bb_guild_new
WHERE backend_account_id = 0
  AND dt BETWEEN DATE_FORMAT('{start_date}', '%Y-%m-01') AND DATE_FORMAT('{end_date}', '%Y-%m-01')
;

