-- dw_fx_month_anchor_live
# DROP TABLE IF EXISTS warehouse.dw_fx_month_guild_live;
# CREATE TABLE warehouse.dw_fx_month_guild_live AS
DELETE
FROM warehouse.dw_fx_month_guild_live
WHERE dt BETWEEN DATE_FORMAT('{start_date}', '%Y-%m-01') AND DATE_FORMAT('{end_date}', '%Y-%m-01');
INSERT INTO warehouse.dw_fx_month_guild_live
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
       SUM(al.anchor_income)                                                        AS anchor_income
FROM (SELECT *,
             warehouse.ANCHOR_NEW_OLD(min_live_dt, min_sign_dt, CASE
                                                                    WHEN dt < DATE_FORMAT('{end_date}', '%Y-%m-01')
                                                                        THEN LAST_DAY(dt)
                                                                    ELSE '{end_date}' END, 180) AS month_newold_state
      FROM warehouse.dw_fx_day_anchor_live
      WHERE dt BETWEEN DATE_FORMAT('{start_date}', '%Y-%m-01') AND '{end_date}'
     ) al
GROUP BY DATE_FORMAT(dt, '%Y-%m-01'),
         al.platform_id,
         al.platform_name,
         al.backend_account_id,
         al.month_newold_state,
         al.active_state,
         al.revenue_level
;