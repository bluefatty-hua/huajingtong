
DELETE
FROM warehouse.dw_fx_month_anchor_live
WHERE dt = '{month}';
INSERT INTO warehouse.dw_fx_month_anchor_live
(
  `dt`,
  `backend_account_id`,
  `anchor_no`,
  `newold_state`,
  `active_state`,
  `revenue_level`,
  `live_days`,
  `duration`,
  `revenue`,
  `revenue_orig`,
  `anchor_income`,
  `guild_income`
)
SELECT '{month}'                               AS dt,
       al.backend_account_id,
       al.anchor_no,
       al.month_newold_state                                        AS newold_state,
       al.active_state,
       al.revenue_level,
       COUNT(CASE WHEN al.live_status = 1 THEN al.dt ELSE NULL END) AS live_days,
       SUM(duration)                                                AS duration,
       SUM(al.revenue)                                              AS revenue,
       SUM(al.revenue_orig)                                         AS revenue_orig,
       SUM(al.anchor_income)                                        AS anchor_income,
       SUM(al.guild_income)                                         AS guild_income
FROM (SELECT *,
             warehouse.ANCHOR_NEW_OLD(min_live_dt, min_sign_dt,  LAST_DAY(dt), 180) AS month_newold_state
      FROM warehouse.dw_fx_day_anchor_live
      WHERE dt >= '{month}'
        AND dt < '{month}' + INTERVAL 1 MONTH
     ) al
GROUP BY 
         al.backend_account_id,
         al.anchor_no,
         al.month_newold_state,
         al.active_state,
         al.revenue_level
;