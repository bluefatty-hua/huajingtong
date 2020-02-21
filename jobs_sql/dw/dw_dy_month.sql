-- DROP TABLE IF EXISTS warehouse.dw_dy_month_guild_live;
-- CREATE TABLE warehouse.dw_dy_month_guild_live AS
DELETE
FROM warehouse.dw_dy_month_guild_live
WHERE dt BETWEEN DATE_FORMAT('{start_date}', '%Y-%m-01') AND '{end_date}';
INSERT INTO warehouse.dw_dy_month_guild_live
SELECT DATE_FORMAT(al.dt, '%Y-%m-01')                                                    AS dt,
       al.platform_id,
       al.platform_name,
       al.backend_account_id,
       al.revenue_level,
       al.month_newold_state                                                             AS newold_state,
       al.active_state,
       COUNT(DISTINCT al.anchor_no)                                                      AS anchor_cnt,
       COUNT(DISTINCT
             CASE WHEN al.live_status = 1 THEN al.anchor_no ELSE NULL END)               AS anchor_live_cnt,
       SUM(al.duration)                                                                  AS duration,
       SUM(CASE WHEN al.revenue >= 0 THEN al.revenue ELSE 0 END)                 AS revenue
FROM (SELECT *,
             warehouse.ANCHOR_NEW_OLD(min_live_dt, min_sign_dt, CASE
                                                                    WHEN dt < DATE_FORMAT('{end_date}', '%Y-%m-01')
                                                                        THEN LAST_DAY(dt)
                                                                    ELSE '{end_date}' END, 180
                 ) AS month_newold_state
      FROM warehouse.dw_dy_day_anchor_live
      WHERE DATE_FORMAT(dt, '%Y-%m-01') BETWEEN DATE_FORMAT('{start_date}', '%Y-%m-01') AND DATE_FORMAT('{end_date}', '%Y-%m-01')
     ) al
GROUP BY DATE_FORMAT(al.dt, '%Y-%m-01'),
         al.platform_id,
         al.platform_name,
         al.backend_account_id,
         al.revenue_level,
         al.month_newold_state,
         al.active_state
;