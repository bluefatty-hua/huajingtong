-- 汇总维度 日-公会
-- 汇总指标 开播天数，开播时长，主播流水，公会流水，公会收入
-- DROP TABLE IF EXISTS warehouse.dw_now_day_guild_live;
-- CREATE TABLE warehouse.dw_now_day_guild_live AS
DELETE
FROM warehouse.dw_now_day_guild_live
WHERE dt >= '{month}'
  AND dt <= LAST_DAY('{month}');
INSERT INTO warehouse.dw_now_day_guild_live
(
      `dt`,
      `backend_account_id`,
      `active_state`,
      `newold_state`,
      `revenue_level`,
      `city`,
      `anchor_cnt`,
      `anchor_live_cnt`,
      `duration`,
      `revenue`,
      `new_anchor_cnt`
)
SELECT t.dt,
       t.backend_account_id,
       t.active_state,
       t.newold_state,
       t.revenue_level,
       t.city,
       COUNT(t.anchor_no)                                                         AS anchor_cnt,
       COUNT(DISTINCT CASE WHEN t.live_status = 1 THEN t.anchor_no ELSE NULL END) AS anchor_live_cnt,
       SUM(t.duration)                                                            AS duration,
       SUM(t.revenue)                                                             AS revenue,
       sum(if(add_loss_state='new',1,0))                                       as new_anchor_cnt
FROM warehouse.dw_now_day_anchor_live t
WHERE t.dt >= '{month}'
  AND t.dt <= LAST_DAY('{month}')
GROUP BY t.dt,
         t.backend_account_id,
         t.city,
         t.active_state,
         t.newold_state,
         t.revenue_level
;
