DELETE
FROM bireport.rpt_day_all
WHERE platform = 'bilibili'
  AND dt BETWEEN '{start_date}' AND '{end_date}';
INSERT INTO bireport.rpt_day_all
SELECT t.dt,
       t.platform_name,
       SUM(anchor_cnt)                                                         AS anchor_cnt,
       SUM(live_cnt)                                                           AS live_cnt,
       SUM(t.revenue) / 1000                                                   AS revenue,
       SUM(t.revenue * gr.guild_income_rate) / 1000                            AS guild_income,
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
       t.platform_name                                                         AS platform,
       t.backend_account_id,
       ai.remark,
       SUM(t.anchor_cnt)                                                       AS anchor_cnt,
       SUM(t.live_cnt)                                                         AS live_cnt,
       SUM(t.revenue) / 1000                                                   AS revenue,
       SUM(t.revenue)                                                          AS revenue_orig,
       SUM(t.revenue * gr.guild_income_rate) / 1000                            AS guild_income,
       SUM(t.revenue * gr.guild_income_rate)                                   AS guild_income_orig,
       SUM(CASE
               WHEN t.backend_account_id = 3 THEN t.revenue * gr.anchor_income_rate + t.send_coin + t.special_coin
               ELSE t.anchor_income + t.send_coin + t.special_coin END) / 1000 AS anchor_income,
       SUM(CASE
               WHEN t.backend_account_id = 3 THEN t.revenue * gr.anchor_income_rate + t.send_coin + t.special_coin
               ELSE t.anchor_income END)                                       AS anchor_income_orig
FROM warehouse.dw_bb_day_guild_live t
         LEFT JOIN spider_bb_backend.account_info ai ON t.backend_account_id = ai.backend_account_id
         LEFT JOIN stage.bb_guild_income_rate gr ON t.backend_account_id = gr.backend_account_id
WHERE t.dt BETWEEN '{start_date}' AND '{end_date}'
GROUP BY t.dt,
         t.platform_id,
         t.platform_name,
         t.backend_account_id,
         ai.remark
;


REPLACE INTO bireport.rpt_day_bb_guild
(dt, backend_account_id, remark, anchor_cnt, live_cnt, revenue, guild_income, anchor_income)
SELECT dt,
       0     as backend_account_id,
       'all' AS remark,
       anchor_cnt,
       live_cnt,
       revenue,
       guild_income,
       anchor_income
FROM bireport.rpt_day_all
WHERE platform = 'bilibili'
  AND dt BETWEEN '{start_date}' AND '{end_date}';


-- rpt_day_bb_guild_new
DELETE
FROM bireport.rpt_day_bb_guild_new
WHERE dt BETWEEN '{start_date}' AND '{end_date}';
INSERT INTO bireport.rpt_day_bb_guild_new
SELECT t.dt,
       t.platform_id,
       t.platform_name                                                         AS platform,
       t.backend_account_id,
       ai.remark,
       t.revenue_level,
       t.newold_state,
       t.active_state,
       SUM(t.anchor_cnt)                                                       AS anchor_cnt,
       SUM(t.live_cnt)                                                         AS live_cnt,
       SUM(t.duration)                                                         AS duration,
       SUM(t.revenue) / 1000                                                   AS revenue,
       SUM(t.revenue)                                                          AS revenue_orig,
       SUM(t.revenue * gr.guild_income_rate) / 1000                            AS guild_income,
       SUM(t.revenue * gr.guild_income_rate)                                   AS guild_income_orig,
       SUM(CASE
               WHEN t.backend_account_id = 3 THEN t.revenue * gr.anchor_income_rate + t.send_coin + t.special_coin
               ELSE t.anchor_income + t.send_coin + t.special_coin END) / 1000 AS anchor_income,
       SUM(CASE
               WHEN t.backend_account_id = 3 THEN t.revenue * gr.anchor_income_rate + t.send_coin + t.special_coin
               ELSE t.anchor_income END)                                       AS anchor_income_orig
FROM warehouse.dw_bb_day_guild_live t
         LEFT JOIN spider_bb_backend.account_info ai ON t.backend_account_id = ai.backend_account_id
         LEFT JOIN stage.bb_guild_income_rate gr ON t.backend_account_id = gr.backend_account_id
WHERE t.dt BETWEEN '{start_date}' AND '{end_date}'
GROUP BY t.dt,
         t.platform_id,
         t.platform_name,
         t.backend_account_id,
         ai.remark,
         t.revenue_level,
         t.newold_state,
         t.active_state
;


-- 补充汇总数据
DELETE
FROM bireport.rpt_day_bb_guild_new
WHERE platform = 'bilibili'
  AND backend_account_id = 0
  AND dt BETWEEN '{start_date}' AND '{end_date}';
INSERT INTO bireport.rpt_day_bb_guild_new
SELECT t.dt,
       t.platform_id,
       t.platform_name                                                         AS platform,
       0                                                                       AS backend_account_id,
       'ALL'                                                                   AS remark,
       t.revenue_level,
       t.newold_state,
       t.active_state,
       SUM(t.anchor_cnt)                                                       AS anchor_cnt,
       SUM(t.live_cnt)                                                         AS live_cnt,
       SUM(t.duration)                                                         AS duration,
       SUM(t.revenue) / 1000                                                   AS revenue,
       SUM(t.revenue)                                                          AS revenue_orig,
       SUM(t.revenue * gr.guild_income_rate) / 1000                            AS guild_income,
       SUM(t.revenue * gr.guild_income_rate)                                   AS guild_income_orig,
       SUM(CASE
               WHEN t.backend_account_id = 3 THEN t.revenue * gr.anchor_income_rate + t.send_coin + t.special_coin
               ELSE t.anchor_income + t.send_coin + t.special_coin END) / 1000 AS anchor_income,
       SUM(CASE
               WHEN t.backend_account_id = 3 THEN t.revenue * gr.anchor_income_rate + t.send_coin + t.special_coin
               ELSE t.anchor_income END)                                       AS anchor_income_orig
FROM warehouse.dw_bb_day_guild_live t
         LEFT JOIN spider_bb_backend.account_info ai ON t.backend_account_id = ai.backend_account_id
         LEFT JOIN stage.bb_guild_income_rate gr ON t.backend_account_id = gr.backend_account_id
WHERE t.dt BETWEEN '{start_date}' AND '{end_date}'
GROUP BY t.dt,
         t.platform_id,
         t.platform_name,
         -- t.backend_account_id,
         -- ai.remark,
         t.revenue_level,
         t.newold_state,
         t.active_state
;


