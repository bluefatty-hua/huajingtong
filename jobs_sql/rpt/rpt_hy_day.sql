DELETE
FROM bireport.rpt_day_all
WHERE platform = '虎牙'
  AND dt BETWEEN '{start_date}' AND '{end_date}';
INSERT INTO bireport.rpt_day_all
SELECT t.dt,
       t.platform_name                                                           AS platform,
       SUM(t.sign_count)                                                         AS anchor_cnt,
       SUM(t.live_cnt)                                                           AS live_cnt,
       SUM(t.revenue)                                                            AS revenue,
       SUM(t.gift_income + t.guard_income + t.noble_income) / 1000               AS guild_income,
       SUM(t.gift_income + t.guard_income + t.noble_income) * 0.7 / (0.3 * 1000) AS anchor_income
FROM warehouse.dw_huya_day_guild_live t
# WHERE dt BETWEEN '{start_date}' AND '{end_date}'
GROUP BY t.dt,
         t.platform_name
;


DELETE
FROM bireport.rpt_day_hy_guild
WHERE dt BETWEEN '{start_date}' AND '{end_date}';
INSERT INTO bireport.rpt_day_hy_guild
SELECT t.dt,
       t.platform_id,
       t.platform_name                                                        AS platform,
       t.channel_num,
       t.sign_count                                                           AS anchor_cnt,
       t.live_cnt,
       t.revenue,
       t.revenue                                                              AS revenue_orig,
       (t.gift_income + t.guard_income + t.noble_income) / 1000               AS guild_income,
       t.gift_income + t.guard_income + t.noble_income                        AS guild_income_orig,
       (t.gift_income + t.guard_income + t.noble_income) * 0.7 / (0.3 * 1000) AS anchor_income,
       t.gift_income + t.guard_income + t.noble_income                        AS anchor_incom_orig
FROM warehouse.dw_huya_day_guild_live t
WHERE dt BETWEEN '{start_date}' AND '{end_date}'
;


-- 补充汇总数据
REPLACE INTO bireport.`rpt_day_yy_guild`
(dt, channel_num, anchor_cnt, live_cnt, revenue, guild_income, anchor_income)
SELECT dt,
       'all' AS channel_num,
       anchor_cnt,
       live_cnt,
       revenue,
       guild_income,
       anchor_income
FROM bireport.rpt_day_all
WHERE platform = '虎牙'
  AND dt BETWEEN '{start_date}' AND '{end_date}';

