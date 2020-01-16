-- create table bireport.rpt_month_all
-- (
--     dt            varchar(10) charset utf8           null,
--     platform      varchar(8) charset utf8 default '' not null,
--     anchor_cnt    bigint(21)              default 0  not null,
--     live_cnt      bigint(21)              default 0  not null,
--     revenue       decimal(64, 2)                     null,
--     guild_income  decimal(64, 2)                     null,
--     anchor_income decimal(64, 2)                     null,
--     constraint rpt_month_all_pk
--         unique (dt, platform)
-- );

DELETE
FROM bireport.rpt_month_all_guild
WHERE DATE_FORMAT(dt, '%Y-%m') BETWEEN DATE_FORMAT('{start_date}', '%Y-%m') AND DATE_FORMAT('{end_date}', '%Y-%m');
INSERT INTO bireport.rpt_month_all_guild
SELECT dt,
       platform_id,
       platform,
       channel_num,
       CASE WHEN anchor_cnt >= 0 THEN anchor_cnt ELSE 0 END                 AS anchor_cnt,
       CASE WHEN live_cnt >= 0 THEN live_cnt ELSE 0 END                     AS live_cnt,
       CASE WHEN revenue >= 0 THEN revenue ELSE 0 END                       AS revenue,
       CASE WHEN revenue_orig >= 0 THEN revenue_orig ELSE 0 END             AS revenue_orig,
       CASE WHEN guild_income >= 0 THEN guild_income ELSE 0 END             AS guild_income,
       CASE WHEN guild_income_orig >= 0 THEN guild_income_orig ELSE 0 END   AS guild_income_orig,
       CASE WHEN anchor_income >= 0 THEN anchor_income ELSE 0 END           AS anchor_income,
       CASE WHEN anchor_income_orig >= 0 THEN anchor_income_orig ELSE 0 END AS anchor_income_orig
FROM (
-- YY
         SELECT dt,
                platform_id,
                platform,
                channel_num,
                anchor_cnt,
                live_cnt,
                revenue,
                revenue_orig,
                guild_income,
                guild_income_orig,
                anchor_income,
                anchor_income_orig
         FROM bireport.rpt_month_yy_guild
         UNION ALL
--  BILIBILI
         SELECT dt,
                platform_id,
                platform,
                backend_account_id AS channel_num,
                anchor_cnt,
                live_cnt,
                revenue,
                revenue_orig,
                guild_income,
                guild_income_orig,
                anchor_income,
                anchor_income_orig
         FROM bireport.rpt_month_bb_guild
         UNION ALL
-- NOW
         SELECT dt,
                platform_id,
                platform,
                backend_account_id AS channel_num,
                anchor_cnt,
                live_cnt,
                revenue,
                revenue_orig,
                guild_income,
                guild_income_orig,
                anchor_income,
                anchor_income_orig
         FROM bireport.rpt_month_now_guild
         UNION ALL
-- HuYa
         SELECT dt,
                platform_id,
                platform,
                channel_num,
                anchor_cnt,
                live_cnt,
                revenue,
                revenue_orig,
                guild_income,
                guild_income_orig,
                anchor_income,
                anchor_income_orig
         FROM bireport.rpt_month_hy_guild) t
WHERE DATE_FORMAT(dt, '%Y-%m') BETWEEN DATE_FORMAT('{start_date}', '%Y-%m') AND DATE_FORMAT('{end_date}', '%Y-%m')
;












