-- 公会每月流水、公会收入、主播收入
# DROP TABLE IF EXISTS bireport.rpt_month_hy_guild;
# CREATE TABLE bireport.rpt_month_hy_guild AS
delete
from bireport.rpt_month_hy_guild
WHERE DATE_FORMAT(dt, '%Y-%m') BETWEEN DATE_FORMAT('start_date', '%Y-%m') AND DATE_FORMAT('end_date', '%Y-%m');
INSERT INTO bireport.rpt_month_hy_guild
SELECT t0.dt,
       t0.platform_id,
       pf.platform_name                                                                      AS platform,
       t0.channel_num,
       t0.sign_count                                                             AS anchor_cnt,
       t1.anchor_live_cnt                                                        AS live_cnt,
       t0.revenue,
       t0.revenue                                                                AS revenue_orig,
       (t0.gift_income + t0.guard_income + t0.noble_income) / 1000               AS guild_income,
       t0.gift_income + t0.guard_income + t0.noble_income                        AS guild_income_orig,
       (t0.gift_income + t0.guard_income + t0.noble_income) * 0.7 / (0.3 * 1000) AS anchor_income,
       t0.gift_income + t0.guard_income + t0.noble_income                        AS anchor_incom_orig
FROM warehouse.dw_huya_month_guild_live t0
         LEFT JOIN (SELECT CONCAT(DATE_FORMAT(t.dt, '%Y-%m'), '-01')                          AS dt,
                           t.channel_id,
                           COUNT(DISTINCT t.anchor_uid)                                       AS anchor_cnt,
                           COUNT(DISTINCT
                                 CASE WHEN t.live_status = 1 THEN t.anchor_uid ELSE NULL END) AS anchor_live_cnt,
                           SUM(t.income)                                                      AS anchor_income
                    FROM warehouse.ods_huya_day_anchor_live t
                    GROUP BY CONCAT(DATE_FORMAT(t.dt, '%Y-%m'), '-01'),
                             t.channel_id) t1 ON t0.dt = t1.dt AND t0.channel_id = t1.channel_id
         lEFT JOIN warehouse.platform pf ON pf.id = t0.platform_id
WHERE DATE_FORMAT(t0.dt, '%Y-%m') BETWEEN DATE_FORMAT('start_date', '%Y-%m') AND DATE_FORMAT('end_date', '%Y-%m')
;

