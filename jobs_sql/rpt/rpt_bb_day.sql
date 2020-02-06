DELETE
FROM bireport.rpt_day_bb_guild
WHERE dt BETWEEN '{start_date}' AND '{end_date}';
INSERT INTO bireport.rpt_day_bb_guild
SELECT t.dt,
       t.platform_id,
       t.platform_name                                                    AS platform,
       t.backend_account_id,
       ai.remark,
       t.anchor_cnt,
       t.live_cnt,
       t.revenue / 1000                                                   AS revenue,
       t.revenue                                                          AS revenue_orig,
       CASE
           WHEN t.backend_account_id = 3 THEN t.revenue * gr.anchor_income_rate + t.send_coin + t.special_coin
           ELSE t.anchor_income + t.send_coin + t.special_coin END / 1000 AS anchor_income,
       CASE
           WHEN t.backend_account_id = 3 THEN t.revenue * gr.anchor_income_rate + t.send_coin + t.special_coin
           ELSE t.anchor_income END                                       AS anchor_income_orig,
       t.revenue * gr.guild_income_rate / 1000                            AS guild_income,
       t.revenue * gr.guild_income_rate                                   AS guild_income_orig
FROM warehouse.dw_bb_day_guild_live t
         LEFT JOIN spider_bb_backend.account_info ai ON t.backend_account_id = ai.backend_account_id
         LEFT JOIN stage.bb_guild_income_rate gr ON t.backend_account_id = gr.backend_account_id
WHERE t.dt BETWEEN '{start_date}' AND '{end_date}'
;


DELETE
FROM bireport.rpt_day_all
WHERE platform = 'bilibili'
  AND dt BETWEEN '{start_date}' AND '{end_date}';
INSERT INTO bireport.rpt_day_all
SELECT t.dt,
       t.platform_name,
       COUNT(DISTINCT t.anchor_no)                                             AS anchor_cnt,
       COUNT(CASE WHEN t.live_status = 1 THEN t.anchor_no ELSE NULL END)       AS live_cnt,
       SUM(t.anchor_total_coin) / 1000                                         AS revenue,
       SUM(CASE
               WHEN t.backend_account_id = 3 THEN
                       t.anchor_total_coin * gr.anchor_income_rate + t.send_coin + t.special_coin
               ELSE t.anchor_income + t.send_coin + t.special_coin END) / 1000 AS anchor_income,
       SUM(t.anchor_total_coin * gr.guild_income_rate) / 1000                  AS guild_income
FROM warehouse.ods_bb_day_anchor_live t
         LEFT JOIN stage.bb_guild_income_rate gr ON
    t.backend_account_id = gr.backend_account_id
WHERE t.dt BETWEEN '{start_date}' AND '{end_date}'
GROUP BY t.dt,
         t.platform_name
;

