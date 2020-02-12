# DROP TABLE IF EXISTS warehouse.dw_bb_day_anchor_live;
# CREATE TABLE warehouse.dw_bb_day_anchor_live AS
DELETE
FROM warehouse.dw_bb_day_anchor_live
WHERE dt BETWEEN '{start_date}' AND '{end_date}';
INSERT INTO warehouse.dw_bb_day_anchor_live
SELECT al.*,
       aml.min_live_dt,
       ams.min_sign_dt,
       -- 通过判断主播最小注册时间和最小开播时间，取两者之间最小的时间作为判断新老主播条件，两者为NULL则为‘未知’
       warehouse.ANCHOR_NEW_OLD(aml.min_live_dt, ams.min_sign_dt, '{end_date}', 180)    AS newold_state,
       mal.duration         AS last_month_duration,
       mal.live_days,
       -- 开播天数大于等于20天且开播时长大于等于60小时（t-1月累计）
       CASE
           WHEN mal.live_days >= 20 AND mal.duration >= 60 * 60 * 60 THEN '活跃主播'
           ELSE '非活跃主播' END AS active_state,
       mal.revenue          AS last_month_revenue,
       -- 主播流水分级（t-1月）
       CASE
           WHEN mal.revenue / 1000 / 10000 >= 50 THEN '50+'
           WHEN mal.revenue / 1000 / 10000 >= 10 THEN '10-50'
           WHEN mal.revenue / 1000 / 10000 >= 3 THEN '3-10'
           WHEN mal.revenue / 1000 / 10000 > 0 THEN '0-3'
           ELSE '0' END     AS revenue_level
FROM warehouse.ods_bb_day_anchor_live al
         LEFT JOIN stage.stage_bb_anchor_min_live_dt aml ON al.anchor_uid = aml.anchor_uid
         LEFT JOIN stage.stage_bb_anchor_min_sign_dt ams ON al.anchor_uid = ams.anchor_uid
         LEFT JOIN stage.stage_bb_month_anchor_live mal
                   ON mal.dt = DATE_FORMAT(DATE_SUB(al.dt, INTERVAL 1 MONTH), '%Y-%m-01') AND
                      al.anchor_uid = mal.anchor_uid
WHERE al.dt BETWEEN '{start_date}' AND '{end_date}'
;


-- 主播
-- 汇总维度 日-公会
-- 汇总指标 开播天数，开播时长，虚拟币收入
-- DROP TABLE IF EXISTS warehouse.dw_bb_day_guild_live;
-- CREATE TABLE warehouse.dw_bb_day_guild_live AS
DELETE
FROM warehouse.dw_bb_day_guild_live
WHERE DATE_FORMAT(dt, '%Y%m') BETWEEN DATE_FORMAT('{start_date}', '%Y%m') AND DATE_FORMAT('{end_date}', '%Y%m');
INSERT INTO warehouse.dw_bb_day_guild_live
SELECT t.dt,
       t.platform_id,
       t.platform_name,
       t.backend_account_id,
       t.newold_state,
       t.active_state,
       t.revenue_level,
       COUNT(DISTINCT t.anchor_no)                                                AS anchor_cnt,
       COUNT(DISTINCT CASE WHEN t.live_status = 1 THEN t.anchor_no ELSE NULL END) AS live_cnt,
       SUM(t.duration)                                                            AS duration,
       SUM(t.anchor_total_coin)                                                   AS revenue,
       SUM(t.special_coin)                                                        AS special_coin,
       SUM(t.send_coin)                                                           AS send_coin,
       SUM(t.anchor_base_coin)                                                    AS anchor_base_coin,
       SUM(t.anchor_income)                                                       AS anchor_income
FROM warehouse.dw_bb_day_anchor_live t
WHERE (t.contract_status <> 2 OR t.contract_status IS NULL)
  AND t.dt BETWEEN '{start_date}' AND '{end_date}'
GROUP BY t.dt,
         t.platform_id,
         t.platform_name,
         t.backend_account_id,
         t.newold_state,
         t.active_state,
         t.revenue_level
;

