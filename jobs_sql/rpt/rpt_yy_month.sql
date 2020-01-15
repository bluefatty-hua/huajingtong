-- 公会每月流水、公会收入、主播收入
-- DROP TABLE IF EXISTS bireport.rpt_month_yy_guild;
-- CREATE TABLE bireport.rpt_month_yy_guild AS
delete from bireport.rpt_month_yy_guild WHERE platform_id = 1000;
INSERT INTO bireport.rpt_month_yy_guild
select t0.dt,
       t0.platform_id,
       pf.platform_name AS platform,
       t0.channel_num,
       t0.anchor_cnt,
       t0.anchor_live_cnt AS live_cnt,
       -- 平台流水
       t0.anchor_bluediamond_true AS anchor_bluediamond_revenue,
       ROUND(t0.guild_commission_ture / 1000, 2) AS guild_commission_revenue,
       ROUND((t0.anchor_bluediamond_true + t0.guild_commission_ture) / 500, 2) AS revenue,
       t0.anchor_bluediamond_true + t0.guild_commission_ture AS revenue_orig,
       -- 公会收入
       t0.guild_bluediamond_true AS guild_income_bluediamond,
       ROUND((t0.guild_bluediamond_true + t0.guild_commission_ture) / 1000, 2) AS guild_income,
       t0.guild_bluediamond_true + t0.guild_commission_ture AS guild_income_orig,
       -- 主播收入
       ROUND((t0.anchor_bluediamond_true - t0.guild_bluediamond_true) / 1000, 2) AS anchor_income,
       t0.anchor_bluediamond_true - t0.guild_bluediamond_true AS anchor_income_orig
from warehouse.dw_yy_month_guild_live t0
lEFT JOIN warehouse.platform pf ON pf.id = t0.platform_id
;

