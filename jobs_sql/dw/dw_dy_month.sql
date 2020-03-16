-- DROP TABLE IF EXISTS warehouse.dw_dy_month_guild_live;
-- CREATE TABLE warehouse.dw_dy_month_guild_live AS
DELETE
FROM warehouse.dw_dy_month_guild_live
WHERE dt = '{month}';
INSERT INTO warehouse.dw_dy_month_guild_live
SELECT DATE_FORMAT(al.dt, '%Y-%m-01')                                       AS dt,
       al.platform_id,
       al.platform_name,
       al.backend_account_id,
       al.revenue_level,
       al.month_newold_state                                                AS newold_state,
       al.active_state,
       COUNT(DISTINCT al.anchor_uid)                                        AS anchor_cnt,
       COUNT(DISTINCT
             CASE WHEN al.live_status = 1 THEN al.anchor_uid ELSE NULL END) AS anchor_live_cnt,
       SUM(IF(al.duration >= 0, al.duration, 0))                            AS duration,
       SUM(IF(al.revenue_orig >= 0, al.revenue_orig, 0))                    AS revenue,
       SUM(IF(al.anchor_income >= 0, al.anchor_income, 0))                  AS anchor_income,
       SUM(IF(al.guild_income >= 0, al.guild_income, 0))                    AS guild_income
FROM (SELECT *,
             -- cur_date: t-1
             warehouse.ANCHOR_NEW_OLD(min_live_dt, min_sign_dt, CASE
                                                                    WHEN dt < DATE_FORMAT('{cur_date}', '%Y-%m-01')
                                                                        THEN LAST_DAY(dt)
                                                                    ELSE '{cur_date}' END, 180
                 ) AS month_newold_state
      FROM warehouse.dw_dy_day_anchor_live
      WHERE dt >= '{month}'
        AND dt < '{month}' + INTERVAL 1 MONTH
     ) al
GROUP BY DATE_FORMAT(al.dt, '%Y-%m-01'),
         al.platform_id,
         al.platform_name,
         al.backend_account_id,
         al.revenue_level,
         al.month_newold_state,
         al.active_state
;


-- DROP TABLE IF EXISTS warehouse.dw_dy_month_anchor_live;
-- CREATE TABLE warehouse.dw_dy_month_anchor_live AS
DELETE
FROM warehouse.dw_dy_month_anchor_live
WHERE dt = '{month}';
# INSERT INTO warehouse.dw_dy_month_anchor_live
SELECT DATE_FORMAT(al.dt, '%Y-%m-01')                               AS dt,
       al.platform_id,
       al.platform_name,
       al.backend_account_id,
       al.anchor_uid,
       al.revenue_level,
       al.month_newold_state                                        AS newold_state,
       al.active_state,
       COUNT(CASE WHEN al.live_status = 1 THEN al.dt ELSE NULL END) AS live_days,
       SUM(IF(al.duration >= 0, al.duration, 0))                    AS duration,
       SUM(IF(al.revenue_orig >= 0, al.revenue_orig, 0))            AS revenue,
       SUM(IF(al.anchor_income >= 0, al.anchor_income, 0))          AS anchor_income,
       SUM(IF(al.guild_income >= 0, al.guild_income, 0))            AS guild_income
FROM (SELECT *,
             warehouse.ANCHOR_NEW_OLD(min_live_dt, min_sign_dt, CASE
                                                                    WHEN dt < DATE_FORMAT('2020-03-10', '%Y-%m-01')
                                                                        THEN LAST_DAY(dt)
                                                                    ELSE '2020-03-10' END, 180
                 ) AS month_newold_state
      FROM warehouse.dw_dy_day_anchor_live
      WHERE dt >= '{month}'
        AND dt < '{month}' + INTERVAL 1 MONTH
     ) al
GROUP BY DATE_FORMAT(al.dt, '%Y-%m-01'),
         al.platform_id,
         al.platform_name,
         al.backend_account_id,
         al.anchor_uid,
         al.revenue_level,
         al.month_newold_state,
         al.active_state
;

