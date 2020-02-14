-- 公会每月流水、公会收入、主播收入
-- DROP TABLE IF EXISTS bireport.rpt_month_yy_guild;
-- CREATE TABLE bireport.rpt_month_yy_guild AS
DELETE
FROM bireport.rpt_month_yy_guild
WHERE dt = '{month}';
-- WHERE dt BETWEEN DATE_FORMAT('{start_date}', '%Y-%m-01') AND DATE_FORMAT('{end_date}', '%Y-%m-01');
INSERT INTO bireport.rpt_month_yy_guild
SELECT t0.dt,
       t0.platform_id,
       pf.platform_name                                                                    AS platform,
       t0.channel_num,
       t0.anchor_cnt,
       t0.anchor_live_cnt                                                                  AS live_cnt,
       -- 平台流水
       IFNULL(alt.anchor_bluediamond_true, 0)                                              AS anchor_bluediamond_revenue,
       ROUND(IFNULL(alt.guild_commission_true, 0) / 1000, 2)                               AS guild_commission_revenue,
       ROUND((IFNULL(alt.anchor_bluediamond_true, 0) + alt.guild_commission_true) / 1000 * 2, 2) AS revenue,
       IFNULL(alt.anchor_bluediamond_true, 0) +
       IFNULL(alt.guild_commission_true, 0)                                                AS revenue_orig,
       -- 公会收入
       IFNULL(alt.guild_bluediamond_true, 0)                                               AS guild_income_bluediamond,
       ROUND((IFNULL(alt.guild_bluediamond_true, 0) + IFNULL(alt.guild_commission_true, 0)) / 1000,
             2)                                                                            AS guild_income,
       IFNULL(alt.guild_bluediamond_true, 0) +
       IFNULL(alt.guild_commission_true, 0)                                                AS guild_income_orig,
       -- 主播收入
       ROUND((IFNULL(alt.anchor_bluediamond_true, 0) - IFNULL(alt.guild_bluediamond_true, 0)) / 1000,
             2)                                                                            AS anchor_income,
       IFNULL(alt.anchor_bluediamond_true, 0) -
       IFNULL(alt.guild_bluediamond_true, 0)                                               AS anchor_income_orig
FROM (SELECT dt,
             platform_id,
             backend_account_id,
             channel_num,
             SUM(anchor_cnt)       AS anchor_cnt,
             SUM(anchor_live_cnt)  AS anchor_live_cnt,
             SUM(guild_commission) AS guild_commission
      FROM warehouse.dw_yy_month_guild_live
      WHERE comment = 'orig'
        AND dt = '{month}'
      GROUP BY dt,
               platform_id,
               backend_account_id,
               channel_num
     ) t0
         LEFT JOIN warehouse.dw_yy_month_guild_live_true alt
                   ON t0.dt = alt.dt AND alt.backend_account_id = t0.backend_account_id
         lEFT JOIN warehouse.platform pf ON pf.id = t0.platform_id
;
