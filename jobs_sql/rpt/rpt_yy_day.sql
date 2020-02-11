DELETE
FROM bireport.rpt_day_all
WHERE platform = 'YY'
  AND dt BETWEEN '{start_date}' AND '{end_date}';
INSERT INTO bireport.rpt_day_all
SELECT gl.dt,
       pf.platform_name                                                       AS platform,
       gl.anchor_cnt,
       gl.anchor_live_cnt                                                     AS live_cnt,
       -- 平台流水
       ROUND((gl.bluediamond + gl.guild_commission) / 500, 2)                 AS revenue,
       -- 公会收入
       ROUND((gl.guild_income_bluediamond + gl.guild_commission) / 1000, 2)   AS guild_income,
       -- 主播收入
       ROUND((gl.anchor_income_bluediamond + gl.anchor_commission) / 1000, 2) AS anchor_income
FROM (SELECT al.dt,
             al.platform_id,
             COUNT(DISTINCT al.anchor_uid)                                                 AS anchor_cnt,
             COUNT(DISTINCT CASE WHEN al.live_status = 1 THEN al.anchor_uid ELSE NULL END) AS anchor_live_cnt,
             SUM(IF(al.duration > 0, al.duration, 0))                                      AS duration,
             SUM(IF(al.bluediamond > 0, al.bluediamond, 0))                                AS bluediamond,
             SUM(IF(al.bluediamond > 0, al.bluediamond * al.anchor_settle_rate, 0))        AS anchor_income_bluediamond,
             SUM(IF(al.bluediamond > 0, al.bluediamond * (1 - al.anchor_settle_rate), 0))  AS guild_income_bluediamond,
             SUM(IF(al.anchor_commission > 0, al.anchor_commission, 0))                    AS anchor_commission,
             SUM(IF(al.guild_commission > 0, al.guild_commission, 0))                      AS guild_commission
      FROM warehouse.ods_yy_day_anchor_live al
      WHERE comment = 'orig'
        AND al.dt BETWEEN '{start_date}' AND '{end_date}'
      GROUP BY al.dt,
               al.platform_id) gl
         LEFT JOIN warehouse.platform pf ON gl.platform_id = pf.id
;


DELETE
FROM bireport.rpt_day_yy_guild
WHERE dt BETWEEN '{start_date}' AND '{end_date}';
INSERT INTO bireport.rpt_day_yy_guild
SELECT gl.dt,
       gl.platform_id,
       pf.platform_name                                                       AS platform_name,
       gl.channel_num,
       gl.anchor_cnt,
       gl.anchor_live_cnt                                                     AS live_cnt,
       -- 平台流水
       gl.bluediamond                                                         AS anchor_bluediamond_revenue,
       ROUND(gl.guild_commission / 1000, 2)                                   AS guild_commssion_revenue,
       ROUND((gl.bluediamond + gl.guild_commission) / 500, 2)                 AS revenue,
       gl.bluediamond + gl.guild_commission                                   AS revenue_orig,
       -- 公会收入
       gl.guild_income_bluediamond,
       ROUND((gl.guild_income_bluediamond + gl.guild_commission) / 1000, 2)   AS guild_income,
       gl.guild_income_bluediamond + gl.guild_commission                      AS guild_income_orig,
       -- 主播收入
       ROUND((gl.anchor_income_bluediamond + gl.anchor_commission) / 1000, 2) AS anchor_income,
       gl.anchor_income_bluediamond + gl.anchor_commission                    AS anchor_income_orig
FROM warehouse.dw_yy_day_guild_live gl
         LEFT JOIN warehouse.platform pf ON gl.platform_id = pf.id
WHERE comment = 'orig'
  AND gl.dt BETWEEN '{start_date}' AND '{end_date}'
;


-- 补充汇总数据
REPLACE INTO bireport.rpt_day_yy_guild
(dt, channel_num, anchor_cnt, live_cnt, revenue, guild_income, anchor_income)
SELECT dt,
       'all' AS channel_num,
       anchor_cnt,
       live_cnt,
       revenue,
       guild_income,
       anchor_income
FROM bireport.rpt_day_all
WHERE platform = 'YY'
  AND dt BETWEEN '{start_date}' AND '{end_date}';


-- rpt_day_yy_guild_new
DELETE
FROM bireport.rpt_day_yy_guild_new
WHERE dt BETWEEN '{start_date}' AND '{end_date}';
INSERT INTO bireport.rpt_day_yy_guild_new
SELECT gl.dt,
       gl.platform_id,
       pf.platform_name                                                              AS platform_name,
       gl.channel_num,
       gl.revenue_level,
       gl.newold_state,
       gl.active_state,
       gl.anchor_cnt,
       gl.anchor_live_cnt                                                            AS live_cnt,
       gl.duration,
       -- 平台流水
       gl.bluediamond                                                                AS anchor_bluediamond_revenue,
       ROUND(gl.guild_commission / 1000, 2)                                          AS guild_commssion_revenue,
       ROUND((gl.bluediamond + gl.guild_commission + gl.anchor_commission) / 500, 2) AS revenue,
       gl.bluediamond + gl.guild_commission                                          AS revenue_orig,
       -- 公会收入
       gl.guild_income_bluediamond,
       ROUND((gl.guild_income_bluediamond + gl.guild_commission) / 1000, 2)          AS guild_income,
       gl.guild_income_bluediamond + gl.guild_commission                             AS guild_income_orig,
       -- 主播收入
       ROUND((gl.anchor_income_bluediamond + gl.anchor_commission) / 1000, 2)        AS anchor_income,
       gl.anchor_income_bluediamond + gl.anchor_commission                           AS anchor_income_orig
FROM warehouse.dw_yy_day_guild_live gl
         LEFT JOIN warehouse.platform pf ON gl.platform_id = pf.id
WHERE comment = 'orig'
  AND gl.dt BETWEEN '{start_date}' AND '{end_date}'
;


-- 补充汇总数据
DELETE
FROM bireport.rpt_day_yy_guild_new
WHERE platform = 'YY'
  AND channel_num = 'ALL'
  AND dt BETWEEN '{start_date}' AND '{end_date}';
INSERT INTO bireport.rpt_day_yy_guild_new
SELECT gl.dt,
       gl.platform_id,
       pf.platform_name                                                       AS platform,
       'ALL'                                                                  AS channel_num,
       gl.revenue_level,
       gl.newold_state,
       active_state,
       gl.anchor_cnt,
       gl.anchor_live_cnt                                                     AS live_cnt,
       gl.duration,
       -- 平台流水
       gl.bluediamond                                                         AS anchor_bluediamond_revenue,
       ROUND(gl.guild_commission / 1000, 2)                                   AS guild_commssion_revenue,
       ROUND((gl.bluediamond + gl.guild_commission) / 500, 2)                 AS revenue,
       gl.bluediamond + gl.guild_commission                                   AS revenue_orig,
       -- 公会收入
       gl.guild_income_bluediamond,
       ROUND((gl.guild_income_bluediamond + gl.guild_commission) / 1000, 2)   AS guild_income,
       gl.guild_income_bluediamond + gl.guild_commission                      AS guild_income_orig,
       -- 主播收入
       ROUND((gl.anchor_income_bluediamond + gl.anchor_commission) / 1000, 2) AS anchor_income,
       gl.anchor_income_bluediamond + gl.anchor_commission                    AS anchor_income_orig
FROM (SELECT al.dt,
             al.platform_id,
             al.revenue_level,
             al.newold_state,
             al.active_state,
             COUNT(DISTINCT al.anchor_uid)                                                 AS anchor_cnt,
             COUNT(DISTINCT CASE WHEN al.live_status = 1 THEN al.anchor_uid ELSE NULL END) AS anchor_live_cnt,
             SUM(IF(al.duration > 0, al.duration, 0))                                      AS duration,
             SUM(IF(al.bluediamond > 0, al.bluediamond, 0))                                AS bluediamond,
             SUM(IF(al.bluediamond > 0, al.bluediamond * al.anchor_settle_rate, 0))        AS anchor_income_bluediamond,
             SUM(IF(al.bluediamond > 0, al.bluediamond * (1 - al.anchor_settle_rate), 0))  AS guild_income_bluediamond,
             SUM(IF(al.anchor_commission > 0, al.anchor_commission, 0))                    AS anchor_commission,
             SUM(IF(al.guild_commission > 0, al.guild_commission, 0))                      AS guild_commission
      FROM warehouse.dw_yy_day_anchor_live al
      WHERE comment = 'orig'
        AND al.dt BETWEEN '{start_date}' AND '{end_date}'
      GROUP BY al.dt,
               al.platform_id,
               al.revenue_level,
               al.newold_state,
               al.active_state) gl
         LEFT JOIN warehouse.platform pf ON gl.platform_id = pf.id
;

