-- DROP TABLE IF EXISTS warehouse.dw_dy_month_guild_live;
-- CREATE TABLE warehouse.dw_dy_month_guild_live AS
DELETE
FROM warehouse.dw_dy_month_guild_live
WHERE dt = '2020-03-01';
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
       SUM(IF(al.revenue >= 0, al.revenue, 0))                              AS revenue,
       SUM(IF(al.anchor_income >= 0, al.anchor_income, 0))                  AS anchor_income,
       SUM(IF(al.guild_income >= 0, al.guild_income, 0))                    AS guild_income
FROM (SELECT *,
             -- cur_date: t-1
             warehouse.ANCHOR_NEW_OLD(min_live_dt, min_sign_dt, CASE
                                                                    WHEN dt < DATE_FORMAT('2020-03-09', '%Y-%m-01')
                                                                        THEN LAST_DAY(dt)
                                                                    ELSE '2020-03-09' END, 180
                 ) AS month_newold_state
      FROM warehouse.dw_dy_day_anchor_live
      WHERE dt >= '2020-03-01'
        AND dt < '2020-03-01' + INTERVAL 1 MONTH
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
INSERT INTO warehouse.dw_dy_month_anchor_live
SELECT DATE_FORMAT(al.dt, '%Y-%m-01')                      AS dt,
       al.platform_id,
       al.platform_name,
       al.backend_account_id,
       al.anchor_uid,
       CASE
           WHEN mal.revenue / 10000 >= 50 THEN '50+'
           WHEN mal.revenue / 10000 >= 10 THEN '10-50'
           WHEN mal.revenue / 10000 >= 3 THEN '3-10'
           WHEN mal.revenue / 10000 > 0 THEN '0-3'
           ELSE '0' END                                    AS revenue_level,
       al.month_newold_state                               AS newold_state,
       al.active_state,
       SUM(IF(al.duration >= 0, al.duration, 0))           AS duration,
       SUM(IF(al.revenue >= 0, al.revenue, 0))             AS revenue,
       SUM(IF(al.anchor_income >= 0, al.anchor_income, 0)) AS anchor_income,
       SUM(IF(al.guild_income >= 0, al.guild_income, 0))   AS guild_income
FROM (SELECT *,
             warehouse.ANCHOR_NEW_OLD(min_live_dt, min_sign_dt, CASE
                                                                    WHEN dt < DATE_FORMAT('2020-03-09', '%Y-%m-01')
                                                                        THEN LAST_DAY(dt)
                                                                    ELSE '2020-03-09' END, 180
                 ) AS month_newold_state
      FROM warehouse.dw_dy_day_anchor_live
      WHERE dt >= '2020-03-01'
        AND dt < '2020-03-01' + INTERVAL 1 MONTH
     ) al
         LEFT JOIN stage.stage_dy_month_anchor_live mal
                   ON mal.dt = al.dt AND al.anchor_uid = mal.anchor_uid
GROUP BY DATE_FORMAT(al.dt, '%Y-%m-01'),
         al.platform_id,
         al.platform_name,
         al.backend_account_id,
         al.anchor_uid,
         CASE
             WHEN mal.revenue / 10000 >= 50 THEN '50+'
             WHEN mal.revenue / 10000 >= 10 THEN '10-50'
             WHEN mal.revenue / 10000 >= 3 THEN '3-10'
             WHEN mal.revenue / 10000 > 0 THEN '0-3'
             ELSE '0' END,
         al.month_newold_state,
         al.active_state
;

