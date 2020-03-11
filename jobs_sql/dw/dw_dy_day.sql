-- DROP TABLE IF EXISTS warehouse.dw_dy_day_anchor_live;
-- CREATE TABLE warehouse.dw_dy_day_anchor_live AS
DELETE
FROM warehouse.dw_dy_day_anchor_live
WHERE dt BETWEEN '{start_date}' AND '{end_date}';
INSERT INTO warehouse.dw_dy_day_anchor_live
SELECT al.*,
       IFNULL(al.revenue, 0) * IFNULL(al.anchor_settle_rate, 0) / 100         AS anchor_income,
       IFNULL(al.revenue, 0) * 0.1                                            AS guild_income,
       aml.min_live_dt,
       ams.min_sign_dt,
       -- 通过判断主播最小注册时间和最小开播时间，取两者之间最小的时间作为判断新老主播条件，两者为NULL则为‘未知’
       warehouse.ANCHOR_NEW_OLD(aml.min_live_dt, ams.min_sign_dt, al.dt, 180) AS newold_state,
       IFNULL(mal.duration, 0)                                                AS month_duration,
       IFNULL(mal.live_days, 0)                                               AS month_live_days,
       -- 开播天数大于等于20天且开播时长大于等于60小时（t-1月累计）
       mal.active_state,
       IFNULL(mal.revenue, 0)                                                 AS month_revenue,
       -- 主播流水分级（t-1月，单位：万元）
       mal.revenue_level
FROM warehouse.ods_dy_day_anchor_live al
         LEFT JOIN stage.stage_dy_anchor_min_live_dt aml ON al.anchor_uid = aml.anchor_uid
         LEFT JOIN stage.stage_dy_anchor_min_sign_dt ams ON al.anchor_uid = ams.anchor_uid
         LEFT JOIN stage.stage_dy_month_anchor_live mal
                   ON mal.dt = DATE_FORMAT(al.dt, '%Y-%m-01') AND
                      al.anchor_uid = mal.anchor_uid
WHERE al.dt BETWEEN '{start_date}' AND '{end_date}'
;


-- 刷新主播活跃及流水分档(按月)
UPDATE
    warehouse.dw_dy_day_anchor_live al, stage.stage_dy_month_anchor_live mal
SET al.active_state  = mal.active_state,
    al.month_duration = IFNULL(mal.duration, 0),
    al.month_live_days = IFNULL(mal.live_days, 0),
    al.revenue_level = mal.revenue_level,
    al.month_revenue = IFNULL(mal.revenue, 0)
WHERE al.anchor_uid = mal.anchor_uid
  AND al.dt >= mal.dt
  AND al.dt < mal.dt + INTERVAL 1 MONTH
  AND mal.dt BETWEEN DATE_FORMAT('{start_date}', '%Y-%m-01') AND '{end_date}'
--   AND '{end_date}' = LAST_DAY('{end_date}')
;


-- DROP TABLE IF EXISTS warehouse.dw_dy_day_guild_live;
-- CREATE TABLE warehouse.dw_dy_day_guild_live AS
DELETE
FROM warehouse.dw_dy_day_guild_live
WHERE dt BETWEEN '{start_date}' AND '{end_date}';
INSERT INTO warehouse.dw_dy_day_guild_live
SELECT al.dt,
       al.platform_id,
       al.platform_name,
       al.backend_account_id,
       al.newold_state,
       al.active_state,
       al.revenue_level,
       COUNT(DISTINCT al.anchor_uid)                                                 AS anchor_cnt,
       COUNT(DISTINCT CASE WHEN al.live_status = 1 THEN al.anchor_uid ELSE NULL END) AS anchor_live_cnt,
       SUM(IF(al.duration > 0, al.duration, 0))                                      AS duration,
       SUM(IF(al.revenue > 0, al.revenue, 0))                                        AS revenue,
       SUM(IF(al.anchor_income > 0, al.anchor_income, 0))                            AS anchor_income,
       SUM(IF(al.guild_income > 0, al.guild_income, 0))                              AS guild_income
FROM warehouse.dw_dy_day_anchor_live al
WHERE dt BETWEEN '{start_date}' AND '{end_date}'
GROUP BY al.dt,
         al.platform_id,
         al.platform_name,
         al.backend_account_id,
         al.newold_state,
         al.active_state,
         al.revenue_level
;


