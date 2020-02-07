-- 公会每月流水、公会收入、主播收入
-- DROP TABLE IF EXISTS bireport.rpt_month_now_guild;
-- CREATE TABLE bireport.rpt_month_now_guild AS
DELETE
FROM bireport.rpt_month_now_guild
WHERE DATE_FORMAT(dt, '%Y-%m') BETWEEN DATE_FORMAT('{start_date}', '%Y-%m') AND DATE_FORMAT('{end_date}', '%Y-%m');
INSERT INTO bireport.rpt_month_now_guild
SELECT t0.dt,
       t0.platform_id,
       pf.platform_name                          AS platform,
       t0.backend_account_id,
       t0.anchor_cnt_true                        AS anchor_cnt,
       t0.anchor_live_cnt                        AS live_cnt,
       t0.revenue_rmb_true                       AS revenue,
       t0.revenue_rmb_true                       AS revenue_orig,
       round(t0.revenue_rmb_true * 0.6 * 0.5, 2) AS guild_income,
       t0.revenue_rmb_true * 0.6 * 0.5           AS guild_income_orig,
       round(t0.revenue_rmb_true * 0.6 * 0.5, 2) AS anchor_income,
       t0.revenue_rmb_true * 0.6 * 0.5           AS anchor_income_orig
FROM warehouse.dw_now_month_guild_live t0
         lEFT JOIN warehouse.platform pf ON pf.id = t0.platform_id
WHERE DATE_FORMAT(dt, '%Y-%m') BETWEEN DATE_FORMAT('{start_date}', '%Y-%m') AND DATE_FORMAT('{end_date}', '%Y-%m')
  AND DATE_FORMAT(dt, '%Y-%m') <> DATE_FORMAT('{end_date}', '%Y-%m')
UNION ALL
SELECT t0.dt,
       t0.platform_id,
       pf.platform_name                     AS platform,
       t0.backend_account_id,
       t0.anchor_cnt_true                   AS anchor_cnt,
       t0.anchor_live_cnt                   AS live_cnt,
       t0.revenue_rmb                       AS revenue,
       t0.revenue_rmb                       AS revenue_orig,
       round(t0.revenue_rmb * 0.6 * 0.5, 2) AS guild_income,
       t0.revenue_rmb * 0.6 * 0.5           AS guild_income_orig,
       round(t0.revenue_rmb * 0.6 * 0.5, 2) AS anchor_income,
       t0.revenue_rmb * 0.6 * 0.5           AS anchor_income_orig
FROM warehouse.dw_now_month_guild_live t0
         lEFT JOIN warehouse.platform pf ON pf.id = t0.platform_id
WHERE DATE_FORMAT(dt, '%Y-%m') = DATE_FORMAT('{end_date}', '%Y-%m')
;


DELETE
FROM bireport.rpt_month_all_guild
WHERE platform_id = 1003
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
      FROM bireport.rpt_month_now_guild) t
WHERE DATE_FORMAT(dt, '%Y-%m') BETWEEN DATE_FORMAT('{start_date}', '%Y-%m') AND DATE_FORMAT('{end_date}', '%Y-%m')
;

