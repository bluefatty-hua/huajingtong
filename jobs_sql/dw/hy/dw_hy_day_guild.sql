-- ===================================================================
-- 公会收入
-- DROP TABLE IF EXISTS warehouse.dw_huya_day_guild_live_true;
-- CREATE TABLE warehouse.dw_huya_day_guild_live_true AS
-- DELETE
-- FROM warehouse.dw_huya_day_guild_live_true
-- WHERE dt >= '{month}'
--   AND dt < '{month}' + INTERVAL 1 MONTH;
-- INSERT INTO warehouse.dw_huya_day_guild_live_true
-- SELECT gi.dt,
--        gi.platform_id,
--        gi.platform_name,
--        ai.channel_type,
--        gi.channel_id,
--        gi.channel_num              AS channel_num,
--        gi.ow                       AS ow,
--        gi.channel_name             AS channel_name,
--        gi.is_platinum,
--        gi.sign_count,
--        gi.sign_limit,
--        cr.live_cnt,
--        IFNULL(cr.revenue, 0)       AS revenue,
--        IFNULL(cgi.gift_income, 0)  AS gift_income,
--        IFNULL(cgu.guard_income, 0) AS guard_income,
--        IFNULL(cn.noble_income, 0)  AS noble_income,
--        gi.logo,
--        gi.desc,
--        gi.create_time,
--        cgi.calc_month              AS gift_calc_month,
--        cgu.calc_month              AS guard_calc_month,
--        cn.calc_month               AS noble_calc_month
-- FROM warehouse.dw_huya_day_guild_info gi
--          LEFT JOIN warehouse.ods_huya_day_guild_live_revenue cr ON gi.dt = cr.dt AND gi.channel_id = cr.channel_id
--          LEFT JOIN warehouse.ods_huya_day_guild_live_income_gift cgi
--                    ON gi.dt = cgi.dt AND gi.channel_id = cgi.channel_id
--          LEFT JOIN warehouse.ods_huya_day_guild_live_income_guard cgu
--                    ON gi.dt = cgu.dt AND gi.channel_id = cgu.channel_id
--          LEFT JOIN warehouse.ods_huya_day_guild_live_income_noble cn ON gi.dt = cn.dt AND gi.channel_id = cn.channel_id
--          LEFT JOIN warehouse.ods_hy_account_info ai ON gi.channel_id = ai.channel_id
-- WHERE gi.dt >= '{month}'
--   AND gi.dt < '{month}' + INTERVAL 1 MONTH
-- ;



DELETE
FROM warehouse.dw_huya_day_guild_live
WHERE dt >= '{month}'
  AND dt < '{month}' + INTERVAL 1 MONTH;
INSERT INTO warehouse.dw_huya_day_guild_live
(
  `dt`,
  `channel_type`,
  `channel_id`,
  `channel_num`,
  `newold_state`,
  `active_state`,
  `revenue_level`,
  `anchor_cnt`,
  `live_cnt`,
  `revenue`,
  `duration`,
  `new_anchor_cnt`
)
SELECT al.dt,
       al.channel_type,
       al.channel_id,
       al.channel_num,
       al.newold_state,
       al.active_state,
       al.revenue_level,
       COUNT(DISTINCT al.anchor_no)                                                 AS anchor_cnt,
       COUNT(DISTINCT CASE WHEN al.live_status = 1 THEN al.anchor_no ELSE NULL END) AS live_cnt,
       SUM(IFNULL(al.revenue, 0))                                                   AS revenue,
       sum(duration)                                                                as duration,
       sum(if(add_loss_state='new',1,0))                                            as new_anchor_cnt
FROM warehouse.dw_huya_day_anchor_live al
WHERE al.dt >= '{month}'
  AND al.dt < '{month}' + INTERVAL 1 MONTH
GROUP BY al.dt,
         al.channel_type,
         al.channel_id,
         al.channel_num,
         al.newold_state,
         al.active_state,
         al.revenue_level
;