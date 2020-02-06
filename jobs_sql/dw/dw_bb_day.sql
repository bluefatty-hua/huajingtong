-- 主播
-- 汇总维度 日-公会
-- 汇总指标 开播天数，开播时长，虚拟币收入
# DROP TABLE IF EXISTS warehouse.dw_bb_day_guild_live;
# CREATE TABLE warehouse.dw_bb_day_guild_live AS
DELETE
FROM warehouse.dw_bb_day_guild_live
WHERE DATE_FORMAT(dt, '%Y%m') BETWEEN DATE_FORMAT('{start_date}', '%Y%m') AND DATE_FORMAT('{end_date}', '%Y%m');
INSERT INTO warehouse.dw_bb_day_guild_live
SELECT t.dt,
       t.platform_id,
       t.platform_name,
       t.backend_account_id,
       COUNT(DISTINCT t.anchor_no)                                       AS anchor_cnt,
       COUNT(CASE WHEN t.live_status = 1 THEN t.anchor_no ELSE NULL END) AS live_cnt,
       SUM(t.duration)                                                   AS duration,
       SUM(t.anchor_total_coin)                                          AS revenue,
       SUM(t.special_coin)                                               AS special_coin,
       SUM(t.send_coin)                                                  AS send_coin,
       SUM(t.anchor_base_coin)                                           AS anchor_base_coin,
       SUM(t.anchor_income)                                              AS anchor_income
FROM warehouse.ods_bb_day_anchor_live t
WHERE t.dt BETWEEN '{start_date}' AND '{end_date}'
GROUP BY t.dt,
         t.platform_id,
         t.platform_name,
         t.backend_account_id
;

