-- 公会每月流水、公会收入、主播收入
-- DROP TABLE IF EXISTS bireport.rpt_month_bb_guild;
-- CREATE TABLE bireport.rpt_month_bb_guild AS
DELETE
FROM bireport.rpt_month_bb_guild
WHERE DATE_FORMAT(dt, '%Y-%m') BETWEEN DATE_FORMAT('{start_date}', '%Y-%m') AND DATE_FORMAT('{end_date}', '%Y-%m');
INSERT INTO bireport.rpt_month_bb_guild
SELECT t.dt,
       t.platform_id,
       pf.platform_name                                                                  AS platform,
       t.backend_account_id,
       t1.remark,
       SUM(t.anchor_cnt)                                                                 AS anchor_cnt,
       SUM(t.anchor_live_cnt)                                                            AS live_cnt,
       SUM(t.anchor_virtual_coin) / 1000                                                 AS revenue,
       SUM(t.anchor_virtual_coin)                                                        AS revenune_orig,
       SUM(t.guild_virtual_coin_ture - t.anchor_change_vir_coin - t.anchor_vitual_coin_ture -
           t.anchor_base_coin - operate_award_punish_coin - special_virtual_coin) / 1000 AS guild_income,
       SUM(t.guild_virtual_coin_ture - t.anchor_change_vir_coin - t.anchor_vitual_coin_ture -
           t.anchor_base_coin - operate_award_punish_coin - special_virtual_coin)        AS guild_income_orig,
       SUM(t.anchor_vitual_coin_ture + t.anchor_base_coin + operate_award_punish_coin +
           special_virtual_coin) / 1000                                                  AS anchor_income,
       SUM(t.anchor_vitual_coin_ture + t.anchor_base_coin + operate_award_punish_coin +
           special_virtual_coin)                                                         AS anchor_income_orig
FROM warehouse.dw_bb_month_guild_live t
         LEFT JOIN spider_bb_backend.account_info t1 ON t.backend_account_id = t1.backend_account_id
         lEFT JOIN warehouse.platform pf ON pf.id = t.platform_id
WHERE DATE_FORMAT(t.dt, '%Y-%m') BETWEEN DATE_FORMAT('{start_date}', '%Y-%m') AND DATE_FORMAT('{end_date}', '%Y-%m')
GROUP BY t.dt,
         t.platform_id,
         pf.platform_name,
         t.backend_account_id,
         t1.remark
;
