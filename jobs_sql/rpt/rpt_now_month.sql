-- 公会每月流水、公会收入、主播收入
-- DROP TABLE IF EXISTS bireport.rpt_month_now_guild;
-- CREATE TABLE bireport.rpt_month_now_guild AS
delete from bireport.rpt_month_now_guild WHERE platform = 'now';
INSERT INTO bireport.rpt_month_now_guild
select t0.dt,
       t0.platform_name AS platform,
       t0.backend_account_id,
       t0.anchor_cnt,
       t0.anchor_live_cnt AS live_cnt,
       t0.guild_revenue_rmb_ture AS revenue,
       t0.guild_revenue_rmb_ture AS revenue_orig,
       round(t0.guild_revenue_rmb_ture * 0.6 * 0.5, 2) AS guild_income,
       t0.guild_revenue_rmb_ture * 0.6 * 0.5 AS guild_income_orig,
       round(t0.guild_revenue_rmb_ture * 0.6 * 0.5, 2) AS anchor_income,
       t0.guild_revenue_rmb_ture * 0.6 * 0.5 AS anchor_income_orig
from warehouse.dw_now_month_guild_live_commission t0;

