-- 获取公会每个月最后一天的信息
-- 汇总月guild、anchor info数据
-- DROP TABLE IF EXISTS stage.stage_huya_month_guild_info;
-- CREATE TABLE stage.stage_huya_month_guild_info AS
DELETE
FROM stage.stage_huya_month_guild_info
WHERE dt = '{month}';
INSERT INTO stage.stage_huya_month_guild_info
SELECT DATE_FORMAT(dt, '%Y-%m-01') AS dt, channel_id, MAX(dt) AS max_dt
FROM warehouse.dw_huya_day_guild_info
WHERE dt >= '{month}'
  AND dt < '{month}' + INTERVAL 1 MONTH
GROUP BY DATE_FORMAT(dt, '%Y-%m-01'), channel_id
;


-- DROP TABLE IF EXISTS warehouse.dw_huya_month_guild_info;
-- CREATE TABLE warehouse.dw_huya_month_guild_info AS
DELETE
FROM warehouse.dw_huya_month_guild_info
WHERE dt = '{month}';
INSERT INTO warehouse.dw_huya_month_guild_info
SELECT DATE_FORMAT(t1.dt, '%Y-%m-01') AS dt,
       t1.channel_id,
       t1.channel_num,
       t1.platform_id,
       t1.platform_name,
       t1.ow,
       t1.channel_name,
       t1.logo,
       t1.`desc`,
       t1.create_time,
       t1.is_platinum,
       t1.sign_count,
       t1.sign_limit
FROM warehouse.dw_huya_day_guild_info t1
         INNER JOIN stage.stage_huya_month_guild_info t2
                    ON t1.dt = t2.max_dt AND t1.channel_id = t2.channel_id
WHERE t1.dt >= '{month}'
  AND t1.dt < '{month}' + INTERVAL 1 MONTH
;


-- 获取主播每个月最后一天的信息
-- DROP TABLE IF EXISTS stage.stage_huya_month_anchor_info;
-- CREATE TABLE stage.stage_huya_month_anchor_info AS
DELETE
FROM stage.stage_huya_month_anchor_info
WHERE dt = '{month}';
INSERT INTO stage.stage_huya_month_anchor_info
SELECT DATE_FORMAT(dt, '%Y-%m-01') AS dt, anchor_uid, channel_id, MAX(dt) AS max_dt
FROM warehouse.dw_huya_day_anchor_info
WHERE dt >= '{month}'
  AND dt < '{month}' + INTERVAL 1 MONTH
GROUP BY DATE_FORMAT(dt, '%Y-%m-01'), anchor_uid, channel_id
;


-- DROP TABLE IF EXISTS warehouse.dw_huya_month_anchor_info;
-- CREATE TABLE warehouse.dw_huya_month_anchor_info AS
DELETE
FROM warehouse.dw_huya_month_anchor_info
WHERE dt = '{month}';
INSERT INTO warehouse.dw_huya_month_anchor_info
SELECT DATE_FORMAT(t1.dt, '%Y-%m-01') AS dt,
       t1.channel_id,
       t1.channel_num,
       t1.anchor_uid,
       t1.anchor_no,
       t1.platform_id,
       t1.platform_name,
       t1.comment,
       t1.nick,
       t1.activity_days,
       t1.months,
       t1.ow_percent,
       t1.sign_time,
       t1.surplus_days,
       t1.avatar,
       t2.dt                          AS last_active_date
FROM warehouse.dw_huya_day_anchor_info t1
         INNER JOIN stage.stage_huya_month_anchor_info t2
                    ON t1.dt = t2.max_dt AND t1.anchor_uid = t2.anchor_uid AND t1.channel_id = t2.channel_id
WHERE t1.dt >= '{month}'
  AND t1.dt < '{month}' + INTERVAL 1 MONTH
;


-- 合并每月主播的信息和开播行为
-- DROP TABLE IF EXISTS warehouse.dw_huya_month_anchor_live;
-- CREATE TABLE warehouse.dw_huya_month_anchor_live AS
DELETE
FROM warehouse.dw_huya_month_anchor_live
WHERE dt = '{month}';
INSERT INTO warehouse.dw_huya_month_anchor_live
SELECT al.dt,
       aci.channel_type,
       al.channel_id,
       ai.channel_num,
       al.anchor_uid,
       ai.anchor_no,
       ai.nick,
       ai.comment,
       al.duration,
       al.live_days,
       al.revenue,
       al.peak_pcu_avg,
       al.peak_pcu_max,
       al.peak_pcu_min,
       ai.platform_id,
       ai.platform_name,
       ai.activity_days,
       ai.months,
       ai.ow_percent,
       ai.sign_time,
       ai.surplus_days,
       ai.avatar AS avatar,
       al.vir_coin_name,
       al.vir_coin_rate,
       al.include_pf_amt,
       al.pf_amt_rate
FROM (SELECT DATE_FORMAT(dt, '%Y-%m-01')           AS dt,
             anchor_uid,
             channel_id,
             SUM(IFNULL(duration, 0))              AS duration,
             SUM(IFNULL(live_status, 0))           AS live_days,
             SUM(IFNULL(revenue, 0))               AS revenue,
             AVG(IF(peak_pcu > 0, peak_pcu, NULL)) AS peak_pcu_avg,
             MAX(peak_pcu)                         AS peak_pcu_max,
             MIN(IF(peak_pcu > 0, peak_pcu, NULL)) AS peak_pcu_min,
             MAX(vir_coin_name)                    AS vir_coin_name,
             MAX(vir_coin_rate)                    AS vir_coin_rate,
             MAX(include_pf_amt)                   AS include_pf_amt,
             MAX(pf_amt_rate)                      AS pf_amt_rate
      FROM warehouse.dw_huya_day_anchor_live
      WHERE dt >= '{month}'
        AND dt < '{month}' + INTERVAL 1 MONTH
      GROUP BY DATE_FORMAT(dt, '%Y-%m-01'),
               anchor_uid,
               channel_id) al
         LEFT JOIN warehouse.dw_huya_month_anchor_info ai
                   ON al.channel_id = ai.channel_id AND al.anchor_uid = ai.anchor_uid AND ai.dt = al.dt
         LEFT JOIN warehouse.ods_hy_account_info aci ON al.channel_id = aci.channel_id
;


-- DROP TABLE IF EXISTS stage.stage_huya_month_guild_live_revenue;
-- CREATE TABLE stage.stage_huya_month_guild_live_revenue AS
DELETE
FROM stage.stage_huya_month_guild_live_revenue
WHERE dt = '{month}';
INSERT INTO stage.stage_huya_month_guild_live_revenue
SELECT DATE_FORMAT(dt, '%Y-%m-01'),
       t1.channel_id,
       SUM(IFNULL(revenue, 0)) AS revenue
FROM warehouse.dw_huya_day_guild_live t1
WHERE t1.dt >= '{month}'
  AND t1.dt < '{month}' + INTERVAL 1 MONTH
GROUP BY DATE_FORMAT(dt, '%Y-%m-01'),
         t1.channel_id
;


-- DROP TABLE IF EXISTS stage.stage_huya_month_guild_live_gift_income;
-- CREATE TABLE stage.stage_huya_month_guild_live_gift_income AS
DELETE
FROM stage.stage_huya_month_guild_live_gift_income
WHERE dt = '{monht}';
INSERT INTO stage.stage_huya_month_guild_live_gift_income
SELECT DATE_FORMAT(dt, '%Y-%m-01') AS dt,
       t1.channel_id,
       SUM(IFNULL(gift_income, 0))             AS gift_income
FROM warehouse.dw_huya_day_guild_live t1
WHERE dt >= '{month}'
  AND dt < '{month}' + INTERVAL 1 MONTH
GROUP BY DATE_FORMAT(dt, '%Y-%m-01'),
         t1.channel_id
;


-- DROP TABLE IF EXISTS stage.stage_huya_month_guild_live_guard_income;
-- CREATE TABLE stage.stage_huya_month_guild_live_guard_income AS
DELETE
FROM stage.stage_huya_month_guild_live_guard_income
WHERE dt = '{month}';
INSERT INTO stage.stage_huya_month_guild_live_guard_income
SELECT DATE_FORMAT(dt, '%Y-%m-01')  AS dt,
       t1.channel_id,
       SUM(ifnull(guard_income, 0)) AS guard_income
FROM warehouse.dw_huya_day_guild_live t1
WHERE dt >= '{month}'
  AND dt < '{month}' + INTERVAL 1 MONTH
GROUP BY DATE_FORMAT(dt, '%Y-%m-01'),
         t1.channel_id
;


-- 有几个频道缺失数据
-- DROP TABLE IF EXISTS stage.stage_huya_month_guild_live_noble_income;
-- CREATE TABLE stage.stage_huya_month_guild_live_noble_income AS
DELETE
FROM stage.stage_huya_month_guild_live_noble_income
WHERE dt = '{month}';
INSERT INTO stage.stage_huya_month_guild_live_noble_income
SELECT DATE_FORMAT(dt, '%Y-%m-01')  AS dt,
       t1.channel_id,
       SUM(IFNULL(noble_income, 0)) AS noble_income
FROM warehouse.dw_huya_day_guild_live t1
WHERE dt >= '{month}'
  AND dt < '{month}' + INTERVAL 1 MONTH
GROUP BY DATE_FORMAT(dt, '%Y-%m-01'),
         t1.channel_id
;


-- DROP TABLE IF EXISTS warehouse.dw_huya_month_guild_live;
-- CREATE TABLE warehouse.dw_huya_month_guild_live AS
DELETE
FROM warehouse.dw_huya_month_guild_live
WHERE dt = '{month}';
INSERT INTO warehouse.dw_huya_month_guild_live
SELECT t1.dt AS dt,
       t1.platform_id,
       t1.platform_name,
       aci.channel_type,
       t1.channel_id,
       t1.channel_num,
       t1.ow,
       t1.channel_name,
       t1.logo,
       t1.`desc`,
       t1.create_time,
       t1.is_platinum,
       t1.sign_count,
       t1.sign_limit,
       t2.revenue,
       t3.gift_income,
       t4.guard_income,
       t5.noble_income
FROM warehouse.dw_huya_month_guild_info t1
         LEFT JOIN
     stage.stage_huya_month_guild_live_revenue t2
     ON t2.channel_id = t1.channel_id AND t1.dt = t2.dt
         LEFT JOIN
     stage.stage_huya_month_guild_live_gift_income t3
     ON t3.channel_id = t1.channel_id AND t1.dt = t3.dt
         LEFT JOIN
     stage.stage_huya_month_guild_live_guard_income t4
     ON t1.channel_id AND t4.dt = t1.dt
         LEFT JOIN stage.stage_huya_month_guild_live_noble_income t5
                   ON t5.channel_id = t1.channel_id AND t5.dt = t1.dt
         LEFT JOIN warehouse.ods_hy_account_info aci ON t1.channel_id = aci.channel_id
WHERE t1.dt = '{month}'
;

