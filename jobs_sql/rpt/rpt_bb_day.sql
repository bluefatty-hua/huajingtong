-- DELETE
-- FROM bireport.rpt_day_all
-- WHERE platform = 'bilibili'
--   AND dt BETWEEN '{start_date}' AND '{end_date}';
-- INSERT INTO bireport.rpt_day_all
-- SELECT t.dt,
--        t.platform_name,
--        SUM(anchor_cnt)                                                         AS anchor_cnt,
--        SUM(live_cnt)                                                           AS live_cnt,
--        SUM(t.revenue) / 1000                                                   AS revenue,
--        SUM(t.revenue * gr.guild_income_rate) / 1000                            AS guild_income,
--        SUM(CASE
--                WHEN t.backend_account_id = 3 THEN
--                    t.revenue * gr.anchor_income_rate + t.send_coin + t.special_coin
--                ELSE t.anchor_income + t.send_coin + t.special_coin END) / 1000 AS anchor_income
-- FROM warehouse.dw_bb_day_guild_live t
--          LEFT JOIN stage.bb_guild_income_rate gr ON
--     t.backend_account_id = gr.backend_account_id
-- WHERE t.dt BETWEEN '{start_date}' AND '{end_date}'
-- GROUP BY t.dt,
--          t.platform_name
-- ;
--
-- DELETE
-- FROM bireport.rpt_day_bb_guild
-- WHERE dt BETWEEN '{start_date}' AND '{end_date}';
-- INSERT INTO bireport.rpt_day_bb_guild
-- SELECT t.dt,
--        t.platform_id,
--        t.platform_name                                                         AS platform,
--        t.backend_account_id,
--        ai.remark,
--        SUM(t.anchor_cnt)                                                       AS anchor_cnt,
--        SUM(t.live_cnt)                                                         AS live_cnt,
--        SUM(t.revenue) / 1000                                                   AS revenue,
--        SUM(t.revenue)                                                          AS revenue_orig,
--        SUM(t.revenue * gr.guild_income_rate) / 1000                            AS guild_income,
--        SUM(t.revenue * gr.guild_income_rate)                                   AS guild_income_orig,
--        SUM(CASE
--                WHEN t.backend_account_id = 3 THEN t.revenue * gr.anchor_income_rate + t.send_coin + t.special_coin
--                ELSE t.anchor_income + t.send_coin + t.special_coin END) / 1000 AS anchor_income,
--        SUM(CASE
--                WHEN t.backend_account_id = 3 THEN t.revenue * gr.anchor_income_rate + t.send_coin + t.special_coin
--                ELSE t.anchor_income END)                                       AS anchor_income_orig
-- FROM warehouse.dw_bb_day_guild_live t
--          LEFT JOIN spider_bb_backend.account_info ai ON t.backend_account_id = ai.backend_account_id
--          LEFT JOIN stage.bb_guild_income_rate gr ON t.backend_account_id = gr.backend_account_id
-- WHERE t.dt BETWEEN '{start_date}' AND '{end_date}'
-- GROUP BY t.dt,
--          t.platform_id,
--          t.platform_name,
--          t.backend_account_id,
--          ai.remark
-- ;
--
--
-- REPLACE INTO bireport.rpt_day_bb_guild
-- (dt, backend_account_id, remark, anchor_cnt, live_cnt, revenue, guild_income, anchor_income)
-- SELECT dt,
--        0     as backend_account_id,
--        'all' AS remark,
--        anchor_cnt,
--        live_cnt,
--        revenue,
--        guild_income,
--        anchor_income
-- FROM bireport.rpt_day_all
-- WHERE platform = 'bilibili'
--   AND dt BETWEEN '{start_date}' AND '{end_date}';


-- ---------------------rpt_day_bb_guild_new-----------------------------
DELETE
FROM bireport.rpt_day_bb_guild
WHERE dt BETWEEN '{start_date}' AND '{end_date}';
INSERT INTO bireport.rpt_day_bb_guild
SELECT t.dt,
       t.platform_id,
       t.platform_name                                                                   AS platform,
       t.backend_account_id,
       ai.remark,
       t.revenue_level,
       t.newold_state,
       t.active_state,
       SUM(IF(add_loss_state <> 'loss', t.anchor_cnt, 0))                                AS anchor_cnt,
       SUM(IF(add_loss_state = 'add', anchor_cnt, 0))                                    AS add_anchor_cnt,
       SUM(IF(add_loss_state = 'loss', anchor_cnt, 0))                                   AS loss_anchor_cnt,
       SUM(IF(add_loss_state = 'add', anchor_cnt, 0) -
           IF(add_loss_state = 'loss', anchor_cnt, 0))                                   AS increase_anchor_cnt,
       SUM(IF(t.add_loss_state <> 'loss', t.live_cnt, 0))                                AS live_cnt,
       SUM(IF(t.add_loss_state <> 'loss', t.duration, 0))                                AS duration,
       SUM(IF(t.add_loss_state <> 'loss', t.revenue_orig, 0)) / 1000                     AS revenue,
       SUM(IF(t.add_loss_state <> 'loss', t.revenue_orig, 0))                            AS revenue_orig,
       SUM(IF(t.add_loss_state <> 'loss', t.revenue_orig * gr.guild_income_rate, 0)) /
       1000                                                                              AS guild_income,
       SUM(
               IF(t.add_loss_state <> 'loss', t.revenue_orig * gr.guild_income_rate, 0)) AS guild_income_orig,
       SUM(IF(t.add_loss_state <> 'loss', CASE
                                              WHEN t.backend_account_id = 3
                                                  THEN t.revenue_orig * gr.anchor_income_rate + t.send_coin + t.special_coin
                                              ELSE t.anchor_income + t.send_coin + t.special_coin END, 0)) /
       1000                                                                              AS anchor_income,
       SUM(IF(t.add_loss_state <> 'loss', CASE
                                              WHEN t.backend_account_id = 3
                                                  THEN t.revenue_orig * gr.anchor_income_rate + t.send_coin + t.special_coin
                                              ELSE t.anchor_income END,
              0))                                                                        AS anchor_income_orig
FROM warehouse.dw_bb_day_guild_live t
         LEFT JOIN spider_bb_backend.account_info ai ON t.backend_account_id = ai.backend_account_id
         LEFT JOIN stage.bb_guild_income_rate gr ON t.backend_account_id = gr.backend_account_id
WHERE (add_loss_state = 'loss' OR contract_status <> 2)
  AND t.dt BETWEEN '{start_date}' AND '{end_date}'
GROUP BY t.dt,
         t.platform_id,
         t.platform_name,
         t.backend_account_id,
         ai.remark,
         t.revenue_level,
         t.newold_state,
         t.active_state
;


REPLACE INTO bireport.rpt_day_bb_guild (dt, platform_id, platform, backend_account_id, remark, revenue_level,
                                        newold_state, active_state, anchor_cnt, add_anchor_cnt, loss_anchor_cnt,
                                        increase_anchor_cnt, live_cnt, duration, revenue, revenue_orig, guild_income,
                                        guild_income_orig, anchor_income, anchor_income_orig)
SELECT t.dt,
       t.platform_id,
       t.platform,
       t.backend_account_id,
       IFNULL(ai.remark, 'all') AS remark,
       t.revenue_level,
       t.newold_state,
       t.active_state,
       t.anchor_cnt,
       t.add_anchor_cnt,
       t.loss_anchor_cnt,
       t.increase_anchor_cnt,
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
             SUM(add_anchor_cnt)           AS add_anchor_cnt,
             SUM(loss_anchor_cnt)          AS loss_anchor_cnt,
             SUM(increase_anchor_cnt)      AS increase_anchor_cnt,
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
        AND dt BETWEEN '{start_date}' AND '{end_date}'
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
             SUM(add_anchor_cnt)           AS add_anchor_cnt,
             SUM(loss_anchor_cnt)          AS loss_anchor_cnt,
             SUM(increase_anchor_cnt)      AS increase_anchor_cnt,
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
        AND dt BETWEEN '{start_date}' AND '{end_date}'
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
             SUM(add_anchor_cnt)           AS add_anchor_cnt,
             SUM(loss_anchor_cnt)          AS loss_anchor_cnt,
             SUM(increase_anchor_cnt)      AS increase_anchor_cnt,
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
        AND dt BETWEEN '{start_date}' AND '{end_date}'
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
             SUM(add_anchor_cnt)           AS add_anchor_cnt,
             SUM(loss_anchor_cnt)          AS loss_anchor_cnt,
             SUM(increase_anchor_cnt)      AS increase_anchor_cnt,
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
        AND dt BETWEEN '{start_date}' AND '{end_date}'
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
             SUM(add_anchor_cnt)           AS add_anchor_cnt,
             SUM(loss_anchor_cnt)          AS loss_anchor_cnt,
             SUM(increase_anchor_cnt)      AS increase_anchor_cnt,
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
        AND dt BETWEEN '{start_date}' AND '{end_date}'
      GROUP BY dt, backend_account_id, newold_state, revenue_level, active_state
      WITH ROLLUP
     ) t
         LEFT JOIN warehouse.ods_bb_account_info ai ON t.backend_account_id = ai.backend_account_id
WHERE dt IS NOT NULL
;


-- 报表用，计算上周、上月同期数据---
DELETE
FROM bireport.rpt_day_bb_guild_view
WHERE dt BETWEEN '{start_date}' AND '{end_date}';
INSERT INTO bireport.rpt_day_bb_guild_view
SELECT t1.dt,
       t1.remark,
       t1.revenue_level,
       t1.newold_state,
       t1.active_state,
       t1.anchor_cnt,
       t1.add_anchor_cnt,
       t1.loss_anchor_cnt,
       t1.increase_anchor_cnt,
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
WHERE t1.dt BETWEEN '{start_date}' AND '{end_date}'
;


-- 报表用，计算指标占比---
DELETE
FROM bireport.rpt_day_bb_guild_view_compare
WHERE dt BETWEEN '{start_date}' AND '{end_date}';
INSERT INTO bireport.rpt_day_bb_guild_view_compare
SELECT *
FROM (SELECT dt,
             remark,
             revenue_level,
             newold_state,
             active_state,
             '主播数'      AS idx,
             anchor_cnt AS val
      FROM bireport.rpt_day_bb_guild
      WHERE revenue_level != 'all'
        AND dt BETWEEN '{start_date}' AND '{end_date}'
      UNION
      SELECT dt,
             remark,
             revenue_level,
             newold_state,
             active_state,
             '开播数'    AS idx,
             live_cnt AS val
      FROM bireport.rpt_day_bb_guild
      WHERE revenue_level != 'all'
        AND dt BETWEEN '{start_date}' AND '{end_date}'
      UNION
      SELECT dt,
             remark,
             revenue_level,
             newold_state,
             active_state,
             '流水'    AS idx,
             revenue AS val
      FROM bireport.rpt_day_bb_guild
      where revenue_level != 'all'
        AND dt BETWEEN '{start_date}' AND '{end_date}'
      UNION
      SELECT dt,
             remark,
             revenue_level,
             newold_state,
             active_state,
             '开播人均流水'                     AS idx,
             round(revenue / live_cnt, 0) AS val
      FROM bireport.rpt_day_bb_guild
      WHERE revenue_level != 'all'
        AND dt BETWEEN '{start_date}' AND '{end_date}'
        AND live_cnt > 0) t
;


-- 主播数据 ---
DELETE
FROM bireport.rpt_day_bb_anchor
WHERE dt BETWEEN '{start_date}' AND '{end_date}';
INSERT INTO bireport.rpt_day_bb_anchor
SELECT al.dt,
       al.backend_account_id,
       ai.remark,
       al.min_live_dt                   AS first_live_date,
       al.min_sign_dt                   AS sign_date,
       al.newold_state,
       al1.duration / 3600              AS duration_lastmonth,
       al1.live_days                    AS live_days_lastmonth,
       al.active_state,
       al1.revenue_orig / 1000          AS revenue_lastmonth,
       al.revenue_level,
       al.anchor_no                     AS anchor_uid,
       al.anchor_no,
       al.dau,
       al.max_ppl,
       al.fc,
       al.anchor_nick_name,
       al.anchor_status_text,
       al.duration / 3600               AS duration,
       IF(al.live_status = 1, '是', '否') AS live_status,
       al.revenue_orig / 1000           AS revenue
FROM warehouse.dw_bb_day_anchor_live al
         LEFT JOIN warehouse.dw_bb_month_anchor_live al1
                   ON al1.dt = DATE_FORMAT(al.dt - INTERVAL 1 MONTH, '%Y-%m-01') AND
                      al.backend_account_id = al1.backend_account_id AND
                      al.anchor_no = al1.anchor_no
         LEFT JOIN spider_bb_backend.account_info ai ON al.backend_account_id = ai.backend_account_id
WHERE al.dt BETWEEN '{start_date}' AND '{end_date}'
;
