-- 公会每月流水、公会收入、主播收入
-- DROP TABLE IF EXISTS bireport.rpt_month_yy_guild;
-- CREATE TABLE bireport.rpt_month_yy_guild AS
# DELETE
# FROM bireport.rpt_month_yy_guild
# WHERE DATE_FORMAT(dt, '%Y-%m') BETWEEN DATE_FORMAT('2019-12-01', '%Y-%m') AND DATE_FORMAT('2020-02-03', '%Y-%m');
# INSERT INTO bireport.rpt_month_yy_guild
SELECT t0.dt,
       t0.platform_id,
       pf.platform_name                                                          AS platform,
       t0.channel_num,
       t0.anchor_cnt,
       t0.anchor_live_cnt                                                        AS live_cnt,
       -- 平台流水
       t0.anchor_bluediamond                                                     AS anchor_bluediamond_revenue,
       ROUND(t0.guild_commission_true / 1000, 2)                                 AS guild_commission_revenue,
       ROUND((t0.anchor_bluediamond_true + t0.guild_commission_true) / 500, 2)   AS revenue,
       t0.anchor_bluediamond_true + t0.guild_commission_true                     AS revenue_orig,
       -- 公会收入
       t0.guild_bluediamond_true                                                 AS guild_income_bluediamond,
       ROUND((t0.guild_bluediamond_true + t0.guild_commission_true) / 1000, 2)   AS guild_income,
       t0.guild_bluediamond_true + t0.guild_commission_true                      AS guild_income_orig,
       -- 主播收入
       ROUND((t0.anchor_bluediamond_true - t0.guild_bluediamond_true) / 1000, 2) AS anchor_income,
       t0.anchor_bluediamond_true - t0.guild_bluediamond_true                    AS anchor_income_orig
FROM warehouse.dw_yy_month_guild_live t0
         lEFT JOIN warehouse.platform pf ON pf.id = t0.platform_id
WHERE comment = 'orig'
  AND DATE_FORMAT(dt, '%Y-%m') BETWEEN DATE_FORMAT('2019-12-01', '%Y-%m') AND DATE_FORMAT('2020-02-03', '%Y-%m')
  AND DATE_FORMAT(dt, '%Y-%m') <> DATE_FORMAT('2020-02-03', '%Y-%m')
UNION ALL
SELECT t0.dt,
       t0.platform_id,
       pf.platform_name                                                          AS platform,
       t0.channel_num,
       t0.anchor_cnt,
       t0.anchor_live_cnt                                                        AS live_cnt,
       -- 平台流水
       t0.anchor_bluediamond                                                     AS anchor_bluediamond_revenue,
       ROUND(t0.guild_commission_true / 1000, 2)                                 AS guild_commission_revenue,
       ROUND((t0.anchor_bluediamond + t0.guild_commission_true) / 500, 2)        AS revenue,
       t0.anchor_bluediamond + t0.guild_commission_true                          AS revenue_orig,
       -- 公会收入
       t0.guild_income_bluediamond                                               AS guild_income_bluediamond,
       ROUND((t0.guild_income_bluediamond + t0.guild_commission_true) / 1000, 2) AS guild_income,
       t0.guild_income_bluediamond + t0.guild_commission_true                    AS guild_income_orig,
       -- 主播收入
       ROUND((t0.anchor_bluediamond - t0.guild_income_bluediamond) / 1000, 2)    AS anchor_income,
       t0.anchor_bluediamond - t0.guild_income_bluediamond                       AS anchor_income_orig
FROM warehouse.dw_yy_month_guild_live t0
         lEFT JOIN warehouse.platform pf ON pf.id = t0.platform_id
WHERE comment = 'orig'
  AND DATE_FORMAT(dt, '%Y-%m') = DATE_FORMAT('2020-02-03', '%Y-%m')
;


DELETE
FROM bireport.rpt_month_all_guild
WHERE platform_id = 1000
  AND DATE_FORMAT(dt, '%Y-%m') BETWEEN DATE_FORMAT('2019-12-01', '%Y-%m') AND DATE_FORMAT('2020-02-03', '%Y-%m');
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
             channel_num,
             anchor_cnt,
             live_cnt,
             revenue,
             revenue_orig,
             guild_income,
             guild_income_orig,
             anchor_income,
             anchor_income_orig
      FROM bireport.rpt_month_yy_guild) t
WHERE DATE_FORMAT(dt, '%Y-%m') BETWEEN DATE_FORMAT('2019-12-01', '%Y-%m') AND DATE_FORMAT('2020-02-03', '%Y-%m')
;