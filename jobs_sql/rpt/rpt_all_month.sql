-- create table bireport.rpt_month_all
-- (
--     dt            varchar(10) charset utf8           null,
--     platform      varchar(2) charset utf8 default '' not null,
--     anchor_cnt    bigint(21)              default 0  not null,
--     live_cnt      bigint(21)              default 0  not null,
--     revenue       decimal(64, 2)                     null,
--     guild_income  decimal(64, 2)                     null,
--     anchor_income decimal(64, 2)                     null,
--     constraint rpt_month_all_pk
--         unique (dt, platform)
-- );


-- yy
delete
from bireport.rpt_month_all t
WHERE t.dt = '2020-01-01'
  AND t.platform = 'YY';
INSERT INTO bireport.rpt_month_all
-- DROP TABLE IF EXISTS bireport.rpt_month_all;
-- CREATE TABLE bireport.rpt_month_all AS
SELECT t0.dt,
       t0.platform_name                                                                            AS platform,
       COUNT(DISTINCT t0.anchor_uid)                                                               AS anchor_cnt,
       COUNT(DISTINCT CASE WHEN live_days > 0 THEN t0.anchor_uid ELSE NULL END)                    AS live_cnt,
       ROUND(SUM(t0.anchor_bluediamond_true + anchor_commission + guild_commission) / 500, 2)      AS revenue,
       ROUND(SUM(t0.guild_bluediamond_true + guild_commission) / 1000, 2)                          AS guild_income,
       ROUND(SUM(t0.anchor_bluediamond - t0.guild_bluediamond_true + anchor_commission) / 1000, 2) AS anchor_income
FROM warehouse.dw_yy_month_anchor_live t0
GROUP BY t0.dt,
         t0.platform_id,
         t0.platform_name
;



