DELETE
FROM bireport.rpt_day_all
WHERE platform = 'bilibili'
  AND dt BETWEEN '{start_date}' AND '{end_date}';
INSERT INTO bireport.rpt_day_all
SELECT t.dt,
       t.platform_name,
       SUM(anchor_cnt)                                                         AS anchor_cnt,
       SUM(live_cnt)                                                           AS live_cnt,
       SUM(t.revenue) / 1000                                         AS revenue,
       SUM(t.revenue * gr.guild_income_rate) / 1000                  AS guild_income,
       SUM(CASE
               WHEN t.backend_account_id = 3 THEN
                       t.revenue * gr.anchor_income_rate + t.send_coin + t.special_coin
               ELSE t.anchor_income + t.send_coin + t.special_coin END) / 1000 AS anchor_income
FROM warehouse.dw_bb_day_guild_live t
         LEFT JOIN stage.bb_guild_income_rate gr ON
    t.backend_account_id = gr.backend_account_id
WHERE t.dt BETWEEN '{start_date}' AND '{end_date}'
GROUP BY t.dt,
         t.platform_name
;

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
       t.revenue * gr.guild_income_rate / 1000                            AS guild_income,
       t.revenue * gr.guild_income_rate                                   AS guild_income_orig,
       CASE
           WHEN t.backend_account_id = 3 THEN t.revenue * gr.anchor_income_rate + t.send_coin + t.special_coin
           ELSE t.anchor_income + t.send_coin + t.special_coin END / 1000 AS anchor_income,
       CASE
           WHEN t.backend_account_id = 3 THEN t.revenue * gr.anchor_income_rate + t.send_coin + t.special_coin
           ELSE t.anchor_income END                                       AS anchor_income_orig
FROM warehouse.dw_bb_day_guild_live t
         LEFT JOIN spider_bb_backend.account_info ai ON t.backend_account_id = ai.backend_account_id
         LEFT JOIN stage.bb_guild_income_rate gr ON t.backend_account_id = gr.backend_account_id
WHERE t.dt BETWEEN '{start_date}' AND '{end_date}'
;


REPLACE INTO bireport.`rpt_day_bb_guild`
(dt,backend_account_id,remark,anchor_cnt,live_cnt,revenue,guild_income,anchor_income)
SELECT  dt,0 as backend_account_id ,'all' AS remark,anchor_cnt,
live_cnt,
revenue,
guild_income,
anchor_income
FROM  bireport.rpt_day_all
WHERE platform='bilibili' AND  dt BETWEEN '{start_date}' AND '{end_date}';

