

-- 汇总维度 月-公会-主播
-- 汇总指标 主播数，开播主播数，开播时长，流水
-- DROP TABLE IF EXISTS warehouse.dw_now_month_anchor_live;
-- CREATE TABLE warehouse.dw_now_month_anchor_live AS
DELETE
FROM warehouse.dw_now_month_anchor_live
WHERE dt = '{month}';
INSERT INTO warehouse.dw_now_month_anchor_live
(
  `dt`,
  `backend_account_id`,
  `city`,
  `active_state`,
  `newold_state`,
  `revenue_level`,
  `anchor_no`,
  `live_days`,
  `duration`,
  `revenue`
)
SELECT '{month}'                                                           AS dt,
       t.backend_account_id,
       t.city,
       t.active_state,
       t.month_newold_state                                                AS newold_state,
       t.revenue_level,
       t.anchor_no,
       COUNT(DISTINCT CASE WHEN t.live_status = 1 THEN t.dt ELSE NULL END) AS live_days,
       SUM(t.duration)                                                     AS duration,
       SUM(t.revenue)                                                      AS revenue
-- cur_date: t-1
FROM (SELECT *,
             warehouse.ANCHOR_NEW_OLD(min_live_dt, min_sign_dt,  LAST_DAY(dt) , 180) AS month_newold_state
      FROM warehouse.dw_now_day_anchor_live
      WHERE dt >= '{month}'
        AND dt < '{month}' + INTERVAL 1 MONTH
     ) t
GROUP BY 
         t.backend_account_id,
         t.city,
         t.active_state,
         t.month_newold_state,
         t.revenue_level,
         t.anchor_no
;