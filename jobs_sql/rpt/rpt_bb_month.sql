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
WHERE DATE_FORMAT(dt, '%Y-%m') BETWEEN DATE_FORMAT('{start_date}', '%Y-%m') AND DATE_FORMAT('{end_date}', '%Y-%m');
INSERT INTO bireport.rpt_month_bb_guild
SELECT gl.dt,
       gl.platform_id,
       gl.platform_name                                                                   AS platform,
       gl.backend_account_id,
       t1.remark,
       SUM(gl.anchor_cnt)                                                                 AS anchor_cnt,
       SUM(gl.anchor_live_cnt)                                                            AS live_cnt,
       SUM(gl.revenue) / 1000                                                             AS revenue,
       SUM(gl.revenue)                                                                    AS revenue_orig,
       SUM(gr.guild_virtual_coin_true - gr.anchor_change_coin - gr.anchor_income_true -
           gr.anchor_base_coin - gr.operate_award_punish_coin - special_coin_true) / 1000 AS guild_income,
       SUM(gr.guild_virtual_coin_true - gr.anchor_change_coin - gr.anchor_income_true -
           gr.anchor_base_coin - gr.operate_award_punish_coin - special_coin_true)        AS guild_income_orig,
       SUM(gr.anchor_income_true + gr.anchor_base_coin + gr.operate_award_punish_coin +
           gr.special_coin) / 1000                                                      AS anchor_income,
       SUM(gr.anchor_income_true + gr.anchor_base_coin + gr.operate_award_punish_coin +
           gr.special_coin)                                                             AS anchor_income_orig
FROM warehouse.dw_bb_month_guild_live gl
         LEFT JOIN warehouse.dw_bb_month_guild_live_true gr
                   ON gl.dt = gr.dt AND gl.backend_account_id = gr.backend_account_id
         LEFT JOIN spider_bb_backend.account_info t1 ON gl.backend_account_id = t1.backend_account_id
         lEFT JOIN warehouse.platform pf ON pf.id = gl.platform_id
WHERE DATE_FORMAT(gl.dt, '%Y-%m') BETWEEN DATE_FORMAT('{start_date}', '%Y-%m') AND DATE_FORMAT('{end_date}', '%Y-%m')
  AND DATE_FORMAT(gl.dt, '%Y-%m') <> DATE_FORMAT('{end_date}', '%Y-%m')
#   AND DATE_FORMAT(gl.dt, '%Y-%m') > DATE_FORMAT('2019-01-01', '%Y-%m')
GROUP BY gl.dt,
         gl.platform_id,
         pf.platform_name,
         gl.backend_account_id,
         t1.remark
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
WHERE DATE_FORMAT(dt, '%Y-%m') = DATE_FORMAT('{end_date}', '%Y-%m')
#    OR DATE_FORMAT(dt, '%Y-%m') <= DATE_FORMAT('2019-01-01', '%Y-%m')
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
WHERE DATE_FORMAT(dt, '%Y-%m') BETWEEN DATE_FORMAT('{start_date}', '%Y-%m') AND DATE_FORMAT('{end_date}', '%Y-%m')
;
