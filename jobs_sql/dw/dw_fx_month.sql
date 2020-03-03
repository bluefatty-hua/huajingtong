-- dw_fx_month_anchor_live
-- DROP TABLE IF EXISTS warehouse.dw_fx_month_guild_live;
-- CREATE TABLE warehouse.dw_fx_month_guild_live AS
REPLACE INTO warehouse.dw_fx_month_guild_live
SELECT DATE_FORMAT(al.dt, '%Y-%m-01')                                               AS dt,
       al.platform_id,
       al.platform_name,
       al.backend_account_id,
       al.month_newold_state                                                        AS newold_state,
       al.active_state,
       al.revenue_level,
       COUNT(DISTINCT al.anchor_no)                                                 AS anchor_cnt,
       COUNT(DISTINCT CASE WHEN al.live_status = 1 THEN al.anchor_no ELSE NULL END) AS anchor_live_cnt,
       SUM(duration)                                                                AS duration,
       SUM(al.anchor_income / 0.4)                                                  AS revenue,
       SUM(al.anchor_income)                                                        AS anchor_income,
       SUM(al.anchor_income / 0.4 * 0.09)                                           AS guild_income
FROM (SELECT *,
             warehouse.ANCHOR_NEW_OLD(min_live_dt, min_sign_dt, CASE
                                                                    WHEN dt < DATE_FORMAT('{cur_date}', '%Y-%m-01')
                                                                        THEN LAST_DAY(dt)
                                                                    ELSE '{cur_date}' END, 180) AS month_newold_state
      FROM warehouse.dw_fx_day_anchor_live
      WHERE dt >= '{month}'
        AND dt < '{month}' + INTERVAL 1 MONTH
     ) al
GROUP BY DATE_FORMAT(dt, '%Y-%m-01'),
         al.platform_id,
         al.platform_name,
         al.backend_account_id,
         al.month_newold_state,
         al.active_state,
         al.revenue_level
;


REPLACE INTO warehouse.dw_fx_month_anchor_live
SELECT DATE_FORMAT(al.dt, '%Y-%m-01')                               AS dt,
       al.platform_id,
       al.platform_name,
       al.backend_account_id,
       al.anchor_no,
       al.month_newold_state                                        AS newold_state,
       al.active_state,
       al.revenue_level,
       COUNT(CASE WHEN al.live_status = 1 THEN al.dt ELSE NULL END) AS live_days,
       SUM(duration)                                                AS duration,
       SUM(al.anchor_income / 0.4)                                  AS revenue,
       SUM(al.anchor_income)                                        AS anchor_income,
       SUM(al.anchor_income / 0.4 * 0.09)                           AS guild_income
FROM (SELECT *,
             warehouse.ANCHOR_NEW_OLD(min_live_dt, min_sign_dt, CASE
                                                                    WHEN dt < DATE_FORMAT('{cur_date}', '%Y-%m-01')
                                                                        THEN LAST_DAY(dt)
                                                                    ELSE '{cur_date}' END, 180) AS month_newold_state
      FROM warehouse.dw_fx_day_anchor_live
      WHERE dt >= '{month}'
        AND dt < '{month}' + INTERVAL 1 MONTH
     ) al
GROUP BY DATE_FORMAT(dt, '%Y-%m-01'),
         al.platform_id,
         al.platform_name,
         al.backend_account_id,
         al.anchor_no,
         al.month_newold_state,
         al.active_state,
         al.revenue_level
;