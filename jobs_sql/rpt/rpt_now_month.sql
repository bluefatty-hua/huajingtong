-- 公会每月流水、公会收入、主播收入
-- DROP TABLE IF EXISTS bireport.rpt_month_now_guild;
-- CREATE TABLE bireport.rpt_month_now_guild AS
delete
from bireport.rpt_month_now_guild
WHERE DATE_FORMAT(dt, '%Y-%m') BETWEEN DATE_FORMAT('start_date', '%Y-%m') AND DATE_FORMAT('end_date', '%Y-%m');
INSERT INTO bireport.rpt_month_now_guild
select t0.dt,
       t0.platform_id,
       pf.platform_name                                AS platform,
       t0.backend_account_id,
       t0.anchor_cnt,
       t0.anchor_live_cnt                              AS live_cnt,
       t0.guild_revenue_rmb_ture                       AS revenue,
       t0.guild_revenue_rmb_ture                       AS revenue_orig,
       round(t0.guild_revenue_rmb_ture * 0.6 * 0.5, 2) AS guild_income,
       t0.guild_revenue_rmb_ture * 0.6 * 0.5           AS guild_income_orig,
       round(t0.guild_revenue_rmb_ture * 0.6 * 0.5, 2) AS anchor_income,
       t0.guild_revenue_rmb_ture * 0.6 * 0.5           AS anchor_income_orig
from warehouse.dw_now_month_guild_live_commission t0
         lEFT JOIN warehouse.platform pf ON pf.id = t0.platform_id
WHERE DATE_FORMAT(t0.dt, '%Y-%m') BETWEEN DATE_FORMAT('start_date', '%Y-%m') AND DATE_FORMAT('end_date', '%Y-%m')
;

