DROP TABLE IF EXISTS stage.bb_guild_income_rate;
CREATE TABLE stage.bb_guild_income_rate AS
SELECT backend_account_id, AVG(guild_virtual_coin_true / virtual_coin_revenue) AS avg_rate
FROM warehouse.dw_bb_month_guild_live
WHERE virtual_coin_revenue > 0
  AND DATE_FORMAT(dt, '%Y-%m') <> DATE_FORMAT('{end_date}', '%Y-%m')
GROUP BY backend_account_id
;


-- 公会每月流水、公会收入、主播收入
-- DROP TABLE IF EXISTS bireport.rpt_month_bb_guild;
-- CREATE TABLE bireport.rpt_month_bb_guild AS
DELETE
FROM bireport.rpt_month_bb_guild
WHERE DATE_FORMAT(dt, '%Y-%m') BETWEEN DATE_FORMAT('2019-01-01', '%Y-%m') AND DATE_FORMAT('{end_date}', '%Y-%m');
INSERT INTO bireport.rpt_month_bb_guild
SELECT t.dt,
       t.platform_id,
       pf.platform_name                                                                         AS platform,
       t.backend_account_id,
       t1.remark,
       SUM(t.anchor_cnt)                                                                        AS anchor_cnt,
       SUM(t.anchor_live_cnt)                                                                   AS live_cnt,
       SUM(t.virtual_coin_revenue) / 1000                                                       AS revenue,
       SUM(t.virtual_coin_revenue)                                                              AS revenue_orig,
       SUM(t.guild_virtual_coin_true - t.anchor_change_coin - t.anchor_income_true -
           t.anchor_base_coin_true - operate_award_punish_coin_true - special_coin_true) / 1000 AS guild_income,
       SUM(t.guild_virtual_coin_true - t.anchor_change_coin - t.anchor_income_true -
           t.anchor_base_coin_true - operate_award_punish_coin_true - special_coin_true)        AS guild_income_orig,
       SUM(t.anchor_income_true + t.anchor_base_coin_true + operate_award_punish_coin_true +
           special_coin_true) / 1000                                                            AS anchor_income,
       SUM(t.anchor_income_true + t.anchor_base_coin_true + operate_award_punish_coin_true +
           special_coin_true)                                                                   AS anchor_income_orig
FROM warehouse.dw_bb_month_guild_live t
         LEFT JOIN spider_bb_backend.account_info t1 ON t.backend_account_id = t1.backend_account_id
         lEFT JOIN warehouse.platform pf ON pf.id = t.platform_id
WHERE DATE_FORMAT(dt, '%Y-%m') BETWEEN DATE_FORMAT('{start_date}', '%Y-%m') AND DATE_FORMAT('{end_date}', '%Y-%m')
  AND DATE_FORMAT(dt, '%Y-%m') <> DATE_FORMAT('{end_date}', '%Y-%m')
GROUP BY t.dt,
         t.platform_id,
         pf.platform_name,
         t.backend_account_id,
         t1.remark
UNION ALL
SELECT t.dt,
       t.platform_id,
       pf.platform_name                                     AS platform,
       t.backend_account_id,
       t1.remark,
       SUM(t.anchor_cnt)                                    AS anchor_cnt,
       SUM(t.anchor_live_cnt)                               AS live_cnt,
       SUM(t.virtual_coin_revenue) / 1000                   AS revenue,
       SUM(t.virtual_coin_revenue)                          AS revenune_orig,
       SUM(t.virtual_coin_revenue * ig.avg_rate - t.anchor_change_coin - t.anchor_income - t.anchor_base_coin -
           operate_award_punish_coin - special_coin) / 1000 AS guild_income,
       SUM(t.virtual_coin_revenue * ig.avg_rate - t.anchor_change_coin - t.anchor_income - t.anchor_base_coin -
           operate_award_punish_coin - special_coin)        AS guild_income_orig,
       SUM(t.anchor_income + t.anchor_base_coin + operate_award_punish_coin +
           special_coin) / 1000                             AS anchor_income,
       SUM(t.anchor_income + t.anchor_base_coin + operate_award_punish_coin +
           special_coin)                                    AS anchor_income_orig
FROM warehouse.dw_bb_month_guild_live t
         LEFT JOIN spider_bb_backend.account_info t1 ON t.backend_account_id = t1.backend_account_id
         LEFT JOIN stage.bb_guild_income_rate ig ON t.backend_account_id = ig.backend_account_id
         lEFT JOIN warehouse.platform pf ON pf.id = t.platform_id
WHERE DATE_FORMAT(dt, '%Y-%m') = DATE_FORMAT('{end_date}', '%Y-%m')
GROUP BY t.dt,
         t.platform_id,
         pf.platform_name,
         t.backend_account_id,
         t1.remark
;


DELETE
FROM bireport.rpt_month_all_guild
WHERE platform_id = 1001
  AND DATE_FORMAT(dt, '%Y-%m') BETWEEN DATE_FORMAT('2019-01-01', '%Y-%m') AND DATE_FORMAT('{end_date}', '%Y-%m');
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
      FROM bireport.rpt_month_bb_guild) t
WHERE DATE_FORMAT(dt, '%Y-%m') BETWEEN DATE_FORMAT('2019-01-01', '%Y-%m') AND DATE_FORMAT('{end_date}', '%Y-%m')
;
