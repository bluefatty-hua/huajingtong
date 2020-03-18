-- 计算每日相对前一天新增主播;
-- 1、取出上月最后一天到当月倒数第二天数据
# DROP TABLE stage.stage_rpt_now_day_anchor_live_contrast;
# CREATE TABLE stage.stage_rpt_now_day_anchor_live_contrast AS
DELETE
FROM stage.stage_rpt_now_day_anchor_live_contrast
WHERE dt >= '{month}'
  AND dt <= LAST_DAY('{month}');
INSERT IGNORE INTO stage.stage_rpt_now_day_anchor_live_contrast
SELECT dt,
       platform_name,
       platform_id,
       anchor_no,
       dt + INTERVAL 1 DAY AS last_dt,
       dt - INTERVAL 1 DAY AS next_dt
FROM warehouse.dw_now_day_anchor_live al
WHERE dt >= '{month}'
  AND dt <= LAST_DAY('{month}')
;


-- 新增主播（在t-1天主播列表，不在t-2天的列表）
-- CREATE TABLE stage.stage_now_day_anchor_add_loss AS
DELETE
FROM stage.stage_rpt_now_day_anchor_add_loss
WHERE add_loss_state = 'add'
  AND dt >= '{month}';
INSERT INTO stage.stage_rpt_now_day_anchor_add_loss
SELECT al1.dt, al1.platform_name, al1.platform_id, al1.anchor_no, 'add' AS add_loss_state
FROM stage.stage_rpt_now_day_anchor_live_contrast al1
         LEFT JOIN stage.stage_rpt_now_day_anchor_live_contrast al2
                   ON al1.dt = al2.last_dt AND al1.anchor_no = al2.anchor_no
WHERE al2.anchor_no IS NULL
  AND al1.dt >= '{month}'
  AND al1.dt <= LAST_DAY('{month}')
;


-- 流失主播（在t-2天主播列表，不在t-1天的列表）
DELETE
FROM stage.stage_rpt_now_day_anchor_add_loss
WHERE add_loss_state = 'loss'
  AND dt >= '{month}'
  AND dt <= LAST_DAY('{month}');
INSERT INTO stage.stage_rpt_now_day_anchor_add_loss
SELECT al1.last_dt, al1.platform_name, al1.platform_id, al1.anchor_no, 'loss' AS add_loss_state
FROM stage.stage_rpt_now_day_anchor_live_contrast al1
         LEFT JOIN stage.stage_rpt_now_day_anchor_live_contrast al2
                   ON al1.dt = al2.next_dt AND al1.anchor_no = al2.anchor_no
WHERE al2.anchor_no IS NULL
  AND al1.last_dt <= '{cur_date}'
  AND al1.dt >= '{month}'
  AND al1.dt <= LAST_DAY('{month}')
;


DELETE
FROM stage.stage_rpt_now_day_guild_live
WHERE dt >= '{month}'
  AND dt <= LAST_DAY('{month}');
INSERT INTO stage.stage_rpt_now_day_guild_live
SELECT al.dt,
       al.platform_id,
       al.platform_name,
       al.backend_account_id,
       al.newold_state,
       al.active_state,
       al.revenue_level,
       al.city,
       COUNT(DISTINCT IF(add_loss_state <> 'loss', al.anchor_no, NULL))                            AS anchor_cnt,
       COUNT(DISTINCT IF(add_loss_state = 'add', al.anchor_no, NULL))                              AS add_anchor_cnt,
       COUNT(DISTINCT IF(add_loss_state = 'loss', al.anchor_no, NULL))                             AS loss_anchor_cnt,
       COUNT(DISTINCT IF(add_loss_state = 'add', al.anchor_no, NULL)) -
       COUNT(DISTINCT IF(add_loss_state = 'loss', al.anchor_no, NULL))                             AS increase_anchor_cnt,
       COUNT(DISTINCT IF(add_loss_state <> 'loss' AND al.live_status = 1, al.anchor_no,
                         NULL))                                                                    AS anchor_live_cnt,
       SUM(IF(add_loss_state <> 'loss' AND al.duration > 0, al.duration, 0))                       AS duration,
       SUM(IF(add_loss_state <> 'loss' AND al.revenue > 0, al.revenue, 0))                         AS revenue,
       SUM(IF(add_loss_state <> 'loss' AND al.revenue > 0, al.revenue, 0))                         AS revenue_orig,
       SUM(IF(add_loss_state <> 'loss' AND al.revenue * 0.6 * 0.5 > 0, al.revenue * 0.6 * 0.5,
              0))                                                                                  AS anchor_income,
       SUM(IF(add_loss_state <> 'loss' AND al.revenue * 0.6 * 0.5 > 0, al.revenue * 0.6 * 0.5,
              0))                                                                                  AS anchor_income_orig,
       SUM(IF(add_loss_state <> 'loss' AND al.revenue * 0.6 * 0.5 > 0, al.revenue * 0.6 * 0.5, 0)) AS guild_income,
       SUM(IF(add_loss_state <> 'loss' AND al.revenue * 0.6 * 0.5 > 0, al.revenue * 0.6 * 0.5,
              0))                                                                                  AS guild_income_orig
FROM (
         SELECT al.*, CASE WHEN aal.add_loss_state IS NULL THEN '' ELSE aal.add_loss_state END AS add_loss_state
         FROM warehouse.dw_now_day_anchor_live al
                  LEFT JOIN stage.stage_rpt_now_day_anchor_add_loss aal
                            ON al.dt = aal.dt AND al.anchor_no = aal.anchor_no
         WHERE al.dt >= '{month}'
           AND al.dt <= LAST_DAY('{month}')
         UNION ALL
         SELECT al.platform_id,
                al.platform_name,
                al.backend_account_id,
                al.anchor_uid,
                al.anchor_qq_no,
                al.anchor_no,
                al.anchor_nick_name,
                al.anchor_name,
                al.fans_cnt,
                al.fans_goup_cnt,
                al.live_status,
                al.duration_hour,
                al.duration,
                al.revenue,
                al.contract_sign_time,
                al.settle_method_code,
                al.settle_method_text,
                al.dt + INTERVAL 1 DAY                                                   AS dt,
                al.city,
                al.min_live_dt,
                al.min_sign_dt,
                al.newold_state,
                al.month_duration,
                al.month_live_days,
                al.active_state,
                al.month_revenue,
                al.revenue_level,
                CASE WHEN aal.add_loss_state IS NULL THEN '' ELSE aal.add_loss_state END AS add_loss_state
         FROM warehouse.dw_now_day_anchor_live al
                  INNER JOIN stage.stage_rpt_now_day_anchor_add_loss aal
                             ON al.dt + INTERVAL 1 DAY = aal.dt AND al.anchor_no = aal.anchor_no
         WHERE aal.add_loss_state = 'loss'
           AND al.dt >= '{month}'
           AND al.dt <= LAST_DAY('{month}')
     ) al
GROUP BY al.dt,
         al.platform_id,
         al.platform_name,
         al.backend_account_id,
         al.newold_state,
         al.active_state,
         al.revenue_level,
         al.city
;


-- rpt_day_now_guild_new
DELETE
FROM bireport.rpt_day_now_guild
WHERE dt >= '{month}'
  AND dt <= LAST_DAY('{month}');
INSERT INTO bireport.rpt_day_now_guild
SELECT t.dt,
       t.platform_id,
       t.platform_name            AS platform,
       t.backend_account_id,
       t.city,
       t.revenue_level,
       t.newold_state,
       t.active_state,
       SUM(t.anchor_cnt)          AS anchor_cnt,
       SUM(add_anchor_cnt)        AS add_anchor_cnt,
       SUM(loss_anchor_cnt)       AS loss_anchor_cnt,
       SUM(increase_anchor_cnt)   AS increase_anchor_cnt,
       SUM(t.anchor_live_cnt)     AS live_cnt,
       SUM(t.duration)            AS duration,
       SUM(t.revenue)             AS revenue,
       SUM(t.revenue)             AS revenue_orig,
       SUM(t.revenue) * 0.6 * 0.5 AS guild_income,
       SUM(t.revenue) * 0.6 * 0.5 AS guild_income_orig,
       SUM(t.revenue) * 0.6 * 0.5 AS anchor_income,
       SUM(t.revenue) * 0.6 * 0.5 AS anchor_income_orig
# FROM warehouse.dw_now_day_guild_live t
FROM stage.stage_rpt_now_day_guild_live t
WHERE dt >= '{month}'
  AND dt <= LAST_DAY('{month}')
GROUP BY t.dt,
         t.platform_id,
         t.platform_name,
         t.backend_account_id,
         t.city,
         t.revenue_level,
         t.newold_state,
         t.active_state
;


-- 补充汇总数据
REPLACE INTO bireport.rpt_day_now_guild (dt, platform_id, platform, backend_account_id, city, revenue_level,
                                         newold_state, active_state, anchor_cnt, add_anchor_cnt, loss_anchor_cnt,
                                         increase_anchor_cnt, live_cnt, duration, revenue, revenue_orig, guild_income,
                                         guild_income_orig, anchor_income, anchor_income_orig)
SELECT *
FROM (SELECT dt,
             MAX(platform_id)                  AS platform_id,
             MAX(platform)                     AS platform,
             IFNULL(backend_account_id, 'all') AS backend_account_id,
             IFNULL(city, 'all')               AS city,
             IFNULL(revenue_level, 'all')      AS revenue_level,
             IFNULL(newold_state, 'all')       AS newold_state,
             IFNULL(active_state, 'all')       AS active_state,
             SUM(anchor_cnt)                   AS anchor_cnt,
             SUM(add_anchor_cnt)               AS add_anchor_cnt,
             SUM(loss_anchor_cnt)              AS loss_anchor_cnt,
             SUM(increase_anchor_cnt)          AS increase_anchor_cnt,
             SUM(live_cnt)                     AS live_cnt,
             SUM(duration)                     AS duration,
             SUM(revenue)                      AS revenue,
             SUM(revenue_orig)                 AS revenue_orig,
             SUM(guild_income)                 AS guild_income,
             SUM(guild_income_orig)            AS guild_income_orig,
             SUM(anchor_income)                AS anchor_income,
             SUM(anchor_income_orig)           AS anchor_income_orig
      FROM bireport.rpt_day_now_guild
      WHERE backend_account_id != 'all'
        AND revenue_level != 'all'
        AND newold_state != 'all'
        AND active_state != 'all'
        AND city != 'all'
        AND dt >= '{month}'
        AND dt <= LAST_DAY('{month}')
      GROUP BY dt, backend_account_id, city, revenue_level, newold_state, active_state
      WITH ROLLUP

      UNION

      SELECT dt,
             MAX(platform_id)                  AS platform_id,
             MAX(platform)                     AS platform,
             IFNULL(backend_account_id, 'all') AS backend_account_id,
             IFNULL(city, 'all')               AS city,
             IFNULL(revenue_level, 'all')      AS revenue_level,
             IFNULL(newold_state, 'all')       AS newold_state,
             IFNULL(active_state, 'all')       AS active_state,
             SUM(anchor_cnt)                   AS anchor_cnt,
             SUM(add_anchor_cnt)               AS add_anchor_cnt,
             SUM(loss_anchor_cnt)              AS loss_anchor_cnt,
             SUM(increase_anchor_cnt)          AS increase_anchor_cnt,
             SUM(live_cnt)                     AS live_cnt,
             SUM(duration)                     AS duration,
             SUM(revenue)                      AS revenue,
             SUM(revenue_orig)                 AS revenue_orig,
             SUM(guild_income)                 AS guild_income,
             SUM(guild_income_orig)            AS guild_income_orig,
             SUM(anchor_income)                AS anchor_income,
             SUM(anchor_income_orig)           AS anchor_income_orig
      FROM bireport.rpt_day_now_guild
      WHERE backend_account_id != 'all'
        AND revenue_level != 'all'
        AND newold_state != 'all'
        AND active_state != 'all'
        AND city != 'all'
        AND dt >= '{month}'
        AND dt <= LAST_DAY('{month}')
      GROUP BY dt, city, revenue_level, newold_state, active_state, backend_account_id
      WITH ROLLUP

      UNION

      SELECT dt,
             MAX(platform_id)                  AS platform_id,
             MAX(platform)                     AS platform,
             IFNULL(backend_account_id, 'all') AS backend_account_id,
             IFNULL(city, 'all')               AS city,
             IFNULL(revenue_level, 'all')      AS revenue_level,
             IFNULL(newold_state, 'all')       AS newold_state,
             IFNULL(active_state, 'all')       AS active_state,
             SUM(anchor_cnt)                   AS anchor_cnt,
             SUM(add_anchor_cnt)               AS add_anchor_cnt,
             SUM(loss_anchor_cnt)              AS loss_anchor_cnt,
             SUM(increase_anchor_cnt)          AS increase_anchor_cnt,
             SUM(live_cnt)                     AS live_cnt,
             SUM(duration)                     AS duration,
             SUM(revenue)                      AS revenue,
             SUM(revenue_orig)                 AS revenue_orig,
             SUM(guild_income)                 AS guild_income,
             SUM(guild_income_orig)            AS guild_income_orig,
             SUM(anchor_income)                AS anchor_income,
             SUM(anchor_income_orig)           AS anchor_income_orig
      FROM bireport.rpt_day_now_guild
      WHERE backend_account_id != 'all'
        AND revenue_level != 'all'
        AND newold_state != 'all'
        AND active_state != 'all'
        AND city != 'all'
        AND dt >= '{month}'
        AND dt <= LAST_DAY('{month}')
      GROUP BY dt, revenue_level, newold_state, active_state, backend_account_id, city
      WITH ROLLUP

      UNION

      SELECT dt,
             MAX(platform_id)                  AS platform_id,
             MAX(platform)                     AS platform,
             IFNULL(backend_account_id, 'all') AS backend_account_id,
             IFNULL(city, 'all')               AS city,
             IFNULL(revenue_level, 'all')      AS revenue_level,
             IFNULL(newold_state, 'all')       AS newold_state,
             IFNULL(active_state, 'all')       AS active_state,
             SUM(anchor_cnt)                   AS anchor_cnt,
             SUM(add_anchor_cnt)               AS add_anchor_cnt,
             SUM(loss_anchor_cnt)              AS loss_anchor_cnt,
             SUM(increase_anchor_cnt)          AS increase_anchor_cnt,
             SUM(live_cnt)                     AS live_cnt,
             SUM(duration)                     AS duration,
             SUM(revenue)                      AS revenue,
             SUM(revenue_orig)                 AS revenue_orig,
             SUM(guild_income)                 AS guild_income,
             SUM(guild_income_orig)            AS guild_income_orig,
             SUM(anchor_income)                AS anchor_income,
             SUM(anchor_income_orig)           AS anchor_income_orig
      FROM bireport.rpt_day_now_guild
      WHERE backend_account_id != 'all'
        AND revenue_level != 'all'
        AND newold_state != 'all'
        AND active_state != 'all'
        AND city != 'all'
        AND dt >= '{month}'
        AND dt <= LAST_DAY('{month}')
      GROUP BY dt, newold_state, active_state, backend_account_id, city, revenue_level
      WITH ROLLUP

      UNION

      SELECT dt,
             MAX(platform_id)                  AS platform_id,
             MAX(platform)                     AS platform,
             IFNULL(backend_account_id, 'all') AS backend_account_id,
             IFNULL(city, 'all')               AS city,
             IFNULL(revenue_level, 'all')      AS revenue_level,
             IFNULL(newold_state, 'all')       AS newold_state,
             IFNULL(active_state, 'all')       AS active_state,
             SUM(anchor_cnt)                   AS anchor_cnt,
             SUM(add_anchor_cnt)               AS add_anchor_cnt,
             SUM(loss_anchor_cnt)              AS loss_anchor_cnt,
             SUM(increase_anchor_cnt)          AS increase_anchor_cnt,
             SUM(live_cnt)                     AS live_cnt,
             SUM(duration)                     AS duration,
             SUM(revenue)                      AS revenue,
             SUM(revenue_orig)                 AS revenue_orig,
             SUM(guild_income)                 AS guild_income,
             SUM(guild_income_orig)            AS guild_income_orig,
             SUM(anchor_income)                AS anchor_income,
             SUM(anchor_income_orig)           AS anchor_income_orig
      FROM bireport.rpt_day_now_guild
      WHERE backend_account_id != 'all'
        AND revenue_level != 'all'
        AND newold_state != 'all'
        AND active_state != 'all'
        AND city != 'all'
        AND dt >= '{month}'
        AND dt <= LAST_DAY('{month}')
      GROUP BY dt, active_state, backend_account_id, city, revenue_level, newold_state
      WITH ROLLUP

      UNION

      SELECT dt,
             MAX(platform_id)                  AS platform_id,
             MAX(platform)                     AS platform,
             IFNULL(backend_account_id, 'all') AS backend_account_id,
             IFNULL(city, 'all')               AS city,
             IFNULL(revenue_level, 'all')      AS revenue_level,
             IFNULL(newold_state, 'all')       AS newold_state,
             IFNULL(active_state, 'all')       AS active_state,
             SUM(anchor_cnt)                   AS anchor_cnt,
             SUM(add_anchor_cnt)               AS add_anchor_cnt,
             SUM(loss_anchor_cnt)              AS loss_anchor_cnt,
             SUM(increase_anchor_cnt)          AS increase_anchor_cnt,
             SUM(live_cnt)                     AS live_cnt,
             SUM(duration)                     AS duration,
             SUM(revenue)                      AS revenue,
             SUM(revenue_orig)                 AS revenue_orig,
             SUM(guild_income)                 AS guild_income,
             SUM(guild_income_orig)            AS guild_income_orig,
             SUM(anchor_income)                AS anchor_income,
             SUM(anchor_income_orig)           AS anchor_income_orig
      FROM bireport.rpt_day_now_guild
      WHERE backend_account_id != 'all'
        AND revenue_level != 'all'
        AND newold_state != 'all'
        AND active_state != 'all'
        AND city != 'all'
        AND dt >= '{month}'
        AND dt <= LAST_DAY('{month}')
      GROUP BY dt, backend_account_id, newold_state, revenue_level, city, active_state
      WITH ROLLUP

      UNION

      SELECT dt,
             MAX(platform_id)                  AS platform_id,
             MAX(platform)                     AS platform,
             IFNULL(backend_account_id, 'all') AS backend_account_id,
             IFNULL(city, 'all')               AS city,
             IFNULL(revenue_level, 'all')      AS revenue_level,
             IFNULL(newold_state, 'all')       AS newold_state,
             IFNULL(active_state, 'all')       AS active_state,
             SUM(anchor_cnt)                   AS anchor_cnt,
             SUM(add_anchor_cnt)               AS add_anchor_cnt,
             SUM(loss_anchor_cnt)              AS loss_anchor_cnt,
             SUM(increase_anchor_cnt)          AS increase_anchor_cnt,
             SUM(live_cnt)                     AS live_cnt,
             SUM(duration)                     AS duration,
             SUM(revenue)                      AS revenue,
             SUM(revenue_orig)                 AS revenue_orig,
             SUM(guild_income)                 AS guild_income,
             SUM(guild_income_orig)            AS guild_income_orig,
             SUM(anchor_income)                AS anchor_income,
             SUM(anchor_income_orig)           AS anchor_income_orig
      FROM bireport.rpt_day_now_guild
      WHERE backend_account_id != 'all'
        AND revenue_level != 'all'
        AND newold_state != 'all'
        AND active_state != 'all'
        AND city != 'all'
        AND dt >= '{month}'
        AND dt <= LAST_DAY('{month}')
      GROUP BY dt, revenue_level, city, backend_account_id, newold_state, active_state
      WITH ROLLUP

      UNION

      SELECT dt,
             MAX(platform_id)                  AS platform_id,
             MAX(platform)                     AS platform,
             IFNULL(backend_account_id, 'all') AS backend_account_id,
             IFNULL(city, 'all')               AS city,
             IFNULL(revenue_level, 'all')      AS revenue_level,
             IFNULL(newold_state, 'all')       AS newold_state,
             IFNULL(active_state, 'all')       AS active_state,
             SUM(anchor_cnt)                   AS anchor_cnt,
             SUM(add_anchor_cnt)               AS add_anchor_cnt,
             SUM(loss_anchor_cnt)              AS loss_anchor_cnt,
             SUM(increase_anchor_cnt)          AS increase_anchor_cnt,
             SUM(live_cnt)                     AS live_cnt,
             SUM(duration)                     AS duration,
             SUM(revenue)                      AS revenue,
             SUM(revenue_orig)                 AS revenue_orig,
             SUM(guild_income)                 AS guild_income,
             SUM(guild_income_orig)            AS guild_income_orig,
             SUM(anchor_income)                AS anchor_income,
             SUM(anchor_income_orig)           AS anchor_income_orig
      FROM bireport.rpt_day_now_guild
      WHERE backend_account_id != 'all'
        AND revenue_level != 'all'
        AND newold_state != 'all'
        AND active_state != 'all'
        AND city != 'all'
        AND dt >= '{month}'
        AND dt <= LAST_DAY('{month}')
      GROUP BY dt, backend_account_id, city, active_state, newold_state, revenue_level
      WITH ROLLUP

      UNION

      SELECT dt,
             MAX(platform_id)                  AS platform_id,
             MAX(platform)                     AS platform,
             IFNULL(backend_account_id, 'all') AS backend_account_id,
             IFNULL(city, 'all')               AS city,
             IFNULL(revenue_level, 'all')      AS revenue_level,
             IFNULL(newold_state, 'all')       AS newold_state,
             IFNULL(active_state, 'all')       AS active_state,
             SUM(anchor_cnt)                   AS anchor_cnt,
             SUM(add_anchor_cnt)               AS add_anchor_cnt,
             SUM(loss_anchor_cnt)              AS loss_anchor_cnt,
             SUM(increase_anchor_cnt)          AS increase_anchor_cnt,
             SUM(live_cnt)                     AS live_cnt,
             SUM(duration)                     AS duration,
             SUM(revenue)                      AS revenue,
             SUM(revenue_orig)                 AS revenue_orig,
             SUM(guild_income)                 AS guild_income,
             SUM(guild_income_orig)            AS guild_income_orig,
             SUM(anchor_income)                AS anchor_income,
             SUM(anchor_income_orig)           AS anchor_income_orig
      FROM bireport.rpt_day_now_guild
      WHERE backend_account_id != 'all'
        AND revenue_level != 'all'
        AND newold_state != 'all'
        AND active_state != 'all'
        AND city != 'all'
        AND dt >= '{month}'
        AND dt <= LAST_DAY('{month}')
      GROUP BY dt, city, backend_account_id, revenue_level, newold_state, active_state
      WITH ROLLUP

      UNION

      SELECT dt,
             MAX(platform_id)                  AS platform_id,
             MAX(platform)                     AS platform,
             IFNULL(backend_account_id, 'all') AS backend_account_id,
             IFNULL(city, 'all')               AS city,
             IFNULL(revenue_level, 'all')      AS revenue_level,
             IFNULL(newold_state, 'all')       AS newold_state,
             IFNULL(active_state, 'all')       AS active_state,
             SUM(anchor_cnt)                   AS anchor_cnt,
             SUM(add_anchor_cnt)               AS add_anchor_cnt,
             SUM(loss_anchor_cnt)              AS loss_anchor_cnt,
             SUM(increase_anchor_cnt)          AS increase_anchor_cnt,
             SUM(live_cnt)                     AS live_cnt,
             SUM(duration)                     AS duration,
             SUM(revenue)                      AS revenue,
             SUM(revenue_orig)                 AS revenue_orig,
             SUM(guild_income)                 AS guild_income,
             SUM(guild_income_orig)            AS guild_income_orig,
             SUM(anchor_income)                AS anchor_income,
             SUM(anchor_income_orig)           AS anchor_income_orig
      FROM bireport.rpt_day_now_guild
      WHERE backend_account_id != 'all'
        AND revenue_level != 'all'
        AND newold_state != 'all'
        AND active_state != 'all'
        AND city != 'all'
        AND dt >= '{month}'
        AND dt <= LAST_DAY('{month}')
      GROUP BY dt, city, newold_state, backend_account_id, active_state, revenue_level
      WITH ROLLUP

      UNION

      SELECT dt,
             MAX(platform_id)                  AS platform_id,
             MAX(platform)                     AS platform,
             IFNULL(backend_account_id, 'all') AS backend_account_id,
             IFNULL(city, 'all')               AS city,
             IFNULL(revenue_level, 'all')      AS revenue_level,
             IFNULL(newold_state, 'all')       AS newold_state,
             IFNULL(active_state, 'all')       AS active_state,
             SUM(anchor_cnt)                   AS anchor_cnt,
             SUM(add_anchor_cnt)               AS add_anchor_cnt,
             SUM(loss_anchor_cnt)              AS loss_anchor_cnt,
             SUM(increase_anchor_cnt)          AS increase_anchor_cnt,
             SUM(live_cnt)                     AS live_cnt,
             SUM(duration)                     AS duration,
             SUM(revenue)                      AS revenue,
             SUM(revenue_orig)                 AS revenue_orig,
             SUM(guild_income)                 AS guild_income,
             SUM(guild_income_orig)            AS guild_income_orig,
             SUM(anchor_income)                AS anchor_income,
             SUM(anchor_income_orig)           AS anchor_income_orig
      FROM bireport.rpt_day_now_guild
      WHERE backend_account_id != 'all'
        AND revenue_level != 'all'
        AND newold_state != 'all'
        AND active_state != 'all'
        AND city != 'all'
        AND dt >= '{month}'
        AND dt <= LAST_DAY('{month}')
      GROUP BY dt, city, active_state, newold_state, backend_account_id, revenue_level
      WITH ROLLUP

      UNION

      SELECT dt,
             MAX(platform_id)                  AS platform_id,
             MAX(platform)                     AS platform,
             IFNULL(backend_account_id, 'all') AS backend_account_id,
             IFNULL(city, 'all')               AS city,
             IFNULL(revenue_level, 'all')      AS revenue_level,
             IFNULL(newold_state, 'all')       AS newold_state,
             IFNULL(active_state, 'all')       AS active_state,
             SUM(anchor_cnt)                   AS anchor_cnt,
             SUM(add_anchor_cnt)               AS add_anchor_cnt,
             SUM(loss_anchor_cnt)              AS loss_anchor_cnt,
             SUM(increase_anchor_cnt)          AS increase_anchor_cnt,
             SUM(live_cnt)                     AS live_cnt,
             SUM(duration)                     AS duration,
             SUM(revenue)                      AS revenue,
             SUM(revenue_orig)                 AS revenue_orig,
             SUM(guild_income)                 AS guild_income,
             SUM(guild_income_orig)            AS guild_income_orig,
             SUM(anchor_income)                AS anchor_income,
             SUM(anchor_income_orig)           AS anchor_income_orig
      FROM bireport.rpt_day_now_guild
      WHERE backend_account_id != 'all'
        AND revenue_level != 'all'
        AND newold_state != 'all'
        AND active_state != 'all'
        AND city != 'all'
        AND dt >= '{month}'
        AND dt <= LAST_DAY('{month}')
      GROUP BY dt, revenue_level, active_state, city, newold_state, backend_account_id
      WITH ROLLUP
     ) t
WHERE t.dt IS NOT NULL
;


-- 报表用，计算上周、上月同期数据---
DELETE
FROM bireport.rpt_day_now_guild_view
WHERE dt >= '{month}'
  AND dt <= LAST_DAY('{month}');
INSERT INTO bireport.rpt_day_now_guild_view
SELECT t1.dt,
       t1.backend_account_id,
       t1.city,
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
FROM bireport.rpt_day_now_guild t1
         LEFT JOIN bireport.rpt_day_now_guild t2
                   ON t1.dt - INTERVAL 7 DAY = t2.dt
                       AND t1.city = t2.city
                       AND t1.backend_account_id = t2.backend_account_id
                       AND t1.revenue_level = t2.revenue_level
                       AND t1.newold_state = t2.newold_state
                       AND t1.active_state = t2.active_state
         LEFT JOIN bireport.rpt_day_now_guild t3
                   ON t1.dt - INTERVAL 1 MONTH = t3.dt
                       AND t1.city = t3.city
                       AND t1.backend_account_id = t3.backend_account_id
                       AND t1.revenue_level = t3.revenue_level
                       AND t1.newold_state = t3.newold_state
                       AND t1.active_state = t3.active_state
WHERE t1.dt >= '{month}'
  AND t1.dt <= LAST_DAY('{month}')
;


-- 报表用，计算指标占比---
DELETE
FROM bireport.rpt_day_now_guild_view_compare
WHERE dt >= '{month}'
  AND dt <= LAST_DAY('{month}');
INSERT INTO bireport.rpt_day_now_guild_view_compare
SELECT *
FROM (SELECT dt,
             backend_account_id,
             city,
             revenue_level,
             newold_state,
             active_state,
             '主播数'      AS idx,
             anchor_cnt AS val
      FROM bireport.rpt_day_now_guild
      WHERE revenue_level != 'all'
        AND city = 'all'
        AND dt >= '{month}'
        AND dt <= LAST_DAY('{month}')
      UNION
      SELECT dt,
             backend_account_id,
             city,
             revenue_level,
             newold_state,
             active_state,
             '开播数'    AS idx,
             live_cnt AS val
      FROM bireport.rpt_day_now_guild
      WHERE revenue_level != 'all'
        AND city = 'all'
        AND dt >= '{month}'
        AND dt <= LAST_DAY('{month}')
      UNION
      SELECT dt,
             backend_account_id,
             city,
             revenue_level,
             newold_state,
             active_state,
             '流水'    AS idx,
             revenue AS val
      FROM bireport.rpt_day_now_guild
      WHERE revenue_level != 'all'
        AND city = 'all'
        AND dt >= '{month}'
        AND dt <= LAST_DAY('{month}')
      UNION
      SELECT dt,
             backend_account_id,
             city,
             revenue_level,
             newold_state,
             active_state,
             '开播人均流水'                     AS idx,
             round(revenue / live_cnt, 0) AS val
      FROM bireport.rpt_day_now_guild
      WHERE revenue_level != 'all'
        AND city = 'all'
        AND dt >= '{month}'
        AND dt <= LAST_DAY('{month}')
        AND live_cnt > 0) t
;


-- 主播数据 ---
DELETE
FROM bireport.rpt_day_now_anchor
WHERE dt >= '{month}'
  AND dt <= LAST_DAY('{month}');
INSERT INTO bireport.rpt_day_now_anchor
SELECT al.dt,
       al.backend_account_id,
       al.city,
       al.min_live_dt                   AS first_live_date,
       al.min_sign_dt                   AS sign_date,
       al.newold_state,
       al1.month_duration / 3600        AS duration_lastmonth,
       al1.month_live_days              AS live_days_lastmonth,
       al.active_state,
       al1.month_revenue                AS revenue_lastmonth,
       al.revenue_level,
       al.anchor_no                     AS anchor_uid,
       al.anchor_no,
       al.fans_cnt,
       al.fans_goup_cnt,
       al.anchor_nick_name,
       al.duration / 3600               AS duration,
       IF(al.live_status = 1, '是', '否') AS live_status,
       al.revenue                       AS revenue
FROM warehouse.dw_now_day_anchor_live al
         LEFT JOIN warehouse.dw_now_day_anchor_live al1
                   ON al1.dt = DATE_FORMAT(al.dt - INTERVAL 1 MONTH, '%Y-%m-01') AND
                      al.backend_account_id = al1.backend_account_id AND
                      al.anchor_no = al1.anchor_no
WHERE al.dt >= '{month}'
  AND al.dt <= LAST_DAY('{month}')
;
