-- dw_fx_day_anchor_live
-- DROP TABLE IF EXISTS warehouse.dw_fx_day_anchor_live;
-- CREATE TABLE warehouse.dw_fx_day_anchor_live AS
DELETE
FROM warehouse.dw_fx_day_anchor_live
WHERE dt BETWEEN '{start_date}' AND '{end_date}';
INSERT INTO warehouse.dw_fx_day_anchor_live
SELECT al.*,
       aml.min_live_dt,
       ams.min_sign_dt,
       -- 通过判断主播最小注册时间和最小开播时间，取两者之间最小的时间作为判断新老主播条件，两者为NULL则为‘未知’
       warehouse.ANCHOR_NEW_OLD(aml.min_live_dt, ams.min_sign_dt, al.dt, 180) AS newold_state,
       mal.duration                                                           AS month_duration,
       mal.live_days,
       -- 开播天数大于等于20天且开播时长大于等于60小时（t-1月累计）
       mal.active_state,
       mal.revenue                                                            AS month_revenue,
       -- 主播流水分级（t-1月）
       mal.revenue_level
FROM warehouse.ods_fx_day_anchor_live al
         LEFT JOIN stage.stage_fx_anchor_min_live_dt aml ON al.anchor_no = aml.anchor_no
         LEFT JOIN stage.stage_fx_anchor_min_sign_dt ams ON al.anchor_no = ams.anchor_no
         LEFT JOIN stage.stage_fx_month_anchor_live mal
                   ON mal.dt = DATE_FORMAT(al.dt, '%Y-%m-01') AND
                      al.anchor_no = mal.anchor_no
WHERE al.dt BETWEEN '{start_date}' AND '{end_date}'
;


UPDATE
    warehouse.dw_fx_day_anchor_live al, stage.stage_fx_month_anchor_live mal
SET al.active_state    = mal.active_state,
    al.month_duration  = mal.duration,
    al.month_live_days = mal.live_days,
    al.revenue_level   = mal.revenue_level,
    al.month_revenue   = mal.revenue
WHERE al.anchor_no = mal.anchor_no
  AND al.dt >= mal.dt
  AND al.dt < mal.dt + INTERVAL 1 MONTH
  AND mal.dt BETWEEN DATE_FORMAT('{start_date}', '%Y-%m-01') AND '{end_date}'
  AND al.dt BETWEEN DATE_FORMAT('{start_date}', '%Y-%m-01') AND '{end_date}'
--   AND '{end_date}' = LAST_DAY('{end_date}')
;


-- DROP TABLE IF EXISTS warehouse.dw_fx_day_guild_live;
-- CREATE TABLE warehouse.dw_fx_day_guild_live AS
DELETE
FROM warehouse.dw_fx_day_guild_live
WHERE DATE_FORMAT(dt, '%Y%m') BETWEEN DATE_FORMAT('{start_date}', '%Y%m') AND DATE_FORMAT('{end_date}', '%Y%m');
INSERT INTO warehouse.dw_fx_day_guild_live
SELECT al.dt,
       al.platform_id,
       al.platform_name,
       al.backend_account_id,
       al.newold_state,
       al.active_state,
       al.revenue_level,
       COUNT(DISTINCT al.anchor_no)                                                 AS anchor_cnt,
       COUNT(DISTINCT CASE WHEN al.live_status = 1 THEN al.anchor_no ELSE NULL END) AS live_cnt,
       SUM(al.duration)                                                             AS duration,
       SUM(al.anchor_income)                                                        AS anchor_income
FROM warehouse.dw_fx_day_anchor_live al
WHERE al.dt BETWEEN '{start_date}' AND '{end_date}'
GROUP BY al.dt,
         al.platform_id,
         al.platform_name,
         al.backend_account_id,
         al.newold_state,
         al.active_state,
         al.revenue_level
;