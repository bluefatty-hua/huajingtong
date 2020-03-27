
-- DELETE
-- FROM warehouse.dw_yy_month_anchor_live_bluediamond
-- WHERE dt = '{month}';
-- INSERT INTO warehouse.dw_yy_month_anchor_live_bluediamond
-- SELECT dt,
--        platform_id,
--        platform_name,
--        backend_account_id,
--        anchor_no,
--        SUM(anchor_bluediamond) AS anchor_bluediamond,
--        SUM(guild_bluediamond)  AS guild_bluediamond
-- FROM warehouse.ods_yy_guild_live_bluediamond
-- WHERE dt >= '{month}'
--   AND dt < '{month}' + INTERVAL 1 MONTH
-- GROUP BY dt,
--          platform_id,
--          platform_name,
--          backend_account_id,
--          anchor_no
-- ;


-- -- 公会佣金收入
-- -- 汇总维度 月-公会-主播
-- -- 汇总指标 公会分成佣金
-- -- DROP TABLE IF EXISTS warehouse.dw_yy_month_anchor_live_commission;
-- -- CREATE TABLE warehouse.dw_yy_month_anchor_live_commission AS
-- DELETE
-- FROM warehouse.dw_yy_month_anchor_live_commission
-- WHERE dt = '{month}';
-- INSERT INTO warehouse.dw_yy_month_anchor_live_commission
-- SELECT dt,
--        platform_id,
--        platform_name,
--        backend_account_id,
--        channel_num,
--        anchor_no,
--        SUM(guild_commission) AS guild_commission
-- FROM warehouse.ods_yy_guild_live_commission
-- WHERE dt >= '{month}'
--   AND dt < '{month}' + INTERVAL 1 MONTH
-- GROUP BY dt,
--          platform_id,
--          platform_name,
--          backend_account_id,
--          channel_num,
--          anchor_no
-- ;


-- -- 公会主播月收入
-- -- 汇总维度 月-公会—主播
-- -- 汇总指标 主播总蓝钻 公会分成蓝钻 公会分成佣金

-- --  DROP TABLE IF EXISTS warehouse.dw_yy_month_anchor_live_true;
-- --  CREATE TABLE warehouse.dw_yy_month_anchor_live_true AS
-- DELETE
-- FROM warehouse.dw_yy_month_anchor_live_true
-- WHERE dt = '{month}';
-- INSERT INTO warehouse.dw_yy_month_anchor_live_true
-- SELECT ab.dt,
--        ab.platform_id,
--        ab.backend_account_id,
--        ac.channel_num,
--        ab.anchor_no,
--        ab.anchor_bluediamond AS anchor_bluediamond_true,
--        ab.guild_bluediamond  AS guild_bluediamond_true,
--        ac.guild_commission   AS guild_commission_true
-- FROM warehouse.dw_yy_month_anchor_live_bluediamond ab
--          LEFT JOIN warehouse.dw_yy_month_anchor_live_commission ac
--                    ON ab.dt = ac.dt AND ab.backend_account_id = ac.backend_account_id AND ab.anchor_no = ac.anchor_no
-- WHERE ab.dt = '{month}'
-- ;


-- DROP TABLE IF EXISTS warehouse.dw_yy_month_anchor_live;
-- CREATE TABLE warehouse.dw_yy_month_anchor_live AS
DELETE
FROM warehouse.dw_yy_month_anchor_live
WHERE dt = '{month}';
INSERT INTO warehouse.dw_yy_month_anchor_live
(
  `dt`,
  `platform_id`,
  `platform_name`,
  `backend_account_id`,
  `channel_num`,
  `anchor_no`,
  `anchor_uid`,
  `revenue_level`,
  `newold_state`,
  `active_state`,
  `comment`,
  `live_days`,
  `duration`,
  `bluediamond`,
  `anchor_income_bluediamond`,
  `guild_income_bluediamond`,
  `anchor_commission`,
  `guild_commission`,
  `dt_cnt`

)
SELECT '{month}'                                         AS dt,
       al.platform_id,
       al.platform_name,
       al.backend_account_id,
       al.channel_num,
       max(al.anchor_no),
       al.anchor_uid,
       al.revenue_level,
       al.month_newold_state                                                   AS newold_state,
       al.active_state,
       al.comment,
       COUNT(CASE WHEN live_status = 1 THEN dt ELSE NULL END)                  AS live_days,
       SUM(CASE WHEN duration >= 0 THEN duration ELSE 0 END)                   AS duration,
       SUM(CASE WHEN bluediamond >= 0 THEN bluediamond ELSE 0 END)             AS bluediamond,
       SUM(CASE
               WHEN bluediamond > 0 THEN bluediamond * anchor_settle_rate
               ELSE 0 END)                                                     AS anchor_income_bluediamond,
       SUM(CASE
               WHEN bluediamond > 0 THEN bluediamond * (1 - anchor_settle_rate)
               ELSE 0 END)                                                     AS guild_income_bluediamond,
       SUM(CASE WHEN anchor_commission >= 0 THEN anchor_commission ELSE 0 END) AS anchor_commission,
       SUM(CASE WHEN guild_commission >= 0 THEN guild_commission ELSE 0 END)   AS guild_commission,
       COUNT(DISTINCT dt)                                                      AS dt_cnt
FROM (SELECT *,
             warehouse.ANCHOR_NEW_OLD(min_live_dt, min_sign_dt, LAST_DAY(dt), 180) AS month_newold_state
      FROM warehouse.dw_yy_day_anchor_live
      WHERE dt >= '{month}'
        AND dt < '{month}' + INTERVAL 1 MONTH
     ) al
GROUP BY DATE_FORMAT(al.dt, '%Y-%m-01'),
         al.platform_id,
         al.platform_name,
         al.backend_account_id,
         al.channel_num,
         al.anchor_uid,
         al.revenue_level,
         al.month_newold_state,
         al.active_state,
         al.comment
;

