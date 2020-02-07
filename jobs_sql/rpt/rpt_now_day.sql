DELETE
FROM bireport.rpt_day_all
WHERE platform = 'NOW'
  AND dt BETWEEN '{start_date}' AND '{end_date}';
INSERT INTO bireport.rpt_day_all
SELECT t.dt,
       t.platform_name                AS platform,
       SUM(t.anchor_cnt)              AS anchor_cnt,
       SUM(t.anchor_live_cnt)         AS live_cnt,
       SUM(t.revenue_rmb)             AS revenue,
       SUM(t.revenue_rmb) * 0.6 * 0.5 AS guild_income,
       SUM(t.revenue_rmb) * 0.6 * 0.5 AS anchor_income
FROM warehouse.dw_now_day_guild_live t
WHERE dt BETWEEN '{start_date}' AND '{end_date}'
GROUP BY t.dt,
         t.platform_name
;


DELETE
FROM bireport.rpt_day_now_guild
WHERE dt BETWEEN '{start_date}' AND '{end_date}';
INSERT INTO bireport.rpt_day_now_guild
SELECT t.dt,
       t.platform_id,
       t.platform_name           AS platform,
       t.backend_account_id,
       t.anchor_cnt,
       t.anchor_live_cnt         AS live_cnt,
       t.revenue_rmb             AS revenue,
       t.revenue_rmb             AS revenue_orig,
       t.revenue_rmb * 0.6 * 0.5 AS guild_income,
       t.revenue_rmb * 0.6 * 0.5 AS guild_income_orig,
       t.revenue_rmb * 0.6 * 0.5 AS anchor_income,
       t.revenue_rmb * 0.6 * 0.5 AS anchor_income_orig
FROM warehouse.dw_now_day_guild_live t
WHERE dt BETWEEN '{start_date}' AND '{end_date}'
;


-- 补充汇总数据
REPLACE INTO bireport.rpt_day_now_guild
(dt, backend_account_id, anchor_cnt, live_cnt, revenue, guild_income, anchor_income)
SELECT dt,
       'all' AS backend_account_id,
       anchor_cnt,
       live_cnt,
       revenue,
       guild_income,
       anchor_income
FROM bireport.rpt_day_all
WHERE platform = 'NOW'
  AND dt BETWEEN '{start_date}' AND '{end_date}'
;
