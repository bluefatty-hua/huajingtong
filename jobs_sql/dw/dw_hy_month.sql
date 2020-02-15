-- 获取公会每个月最后一天的信息
-- 汇总月guild、anchor info数据
-- DROP TABLE IF EXISTS stage.stage_huya_month_guild_info;
-- CREATE TABLE stage.stage_huya_month_guild_info AS
DELETE
FROM stage.stage_huya_month_guild_info
WHERE DATE_FORMAT(dt, '%Y-%m') BETWEEN DATE_FORMAT('{start_date}', '%Y-%m') AND DATE_FORMAT('{end_date}', '%Y-%m');
INSERT INTO stage.stage_huya_month_guild_info
SELECT CONCAT(DATE_FORMAT(dt, '%Y-%m'), '-01') AS dt, channel_id, MAX(dt) AS max_dt
FROM warehouse.dw_huya_day_guild_info
WHERE DATE_FORMAT(dt, '%Y-%m') BETWEEN DATE_FORMAT('{start_date}', '%Y-%m') AND DATE_FORMAT('{end_date}', '%Y-%m')
GROUP BY CONCAT(DATE_FORMAT(dt, '%Y-%m'), '-01'), channel_id
;


-- DROP TABLE IF EXISTS warehouse.dw_huya_month_guild_info;
-- CREATE TABLE warehouse.dw_huya_month_guild_info AS
DELETE
FROM warehouse.dw_huya_month_guild_info
WHERE DATE_FORMAT(dt, '%Y-%m') BETWEEN DATE_FORMAT('{start_date}', '%Y-%m') AND DATE_FORMAT('{end_date}', '%Y-%m');
INSERT INTO warehouse.dw_huya_month_guild_info
SELECT CONCAT(DATE_FORMAT(t1.dt, '%Y-%m'), '-01') AS dt,
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
WHERE DATE_FORMAT(t1.dt, '%Y-%m') BETWEEN DATE_FORMAT('{start_date}', '%Y-%m') AND DATE_FORMAT('{end_date}', '%Y-%m');


-- 获取主播每个月最后一天的信息
-- DROP TABLE IF EXISTS stage.stage_huya_month_anchor_info;
-- CREATE TABLE stage.stage_huya_month_anchor_info AS
DELETE
FROM stage.stage_huya_month_anchor_info
WHERE DATE_FORMAT(dt, '%Y-%m') BETWEEN DATE_FORMAT('{start_date}', '%Y-%m') AND DATE_FORMAT('{end_date}', '%Y-%m');
INSERT INTO stage.stage_huya_month_anchor_info
SELECT CONCAT(DATE_FORMAT(dt, '%Y-%m'), '-01') AS dt, anchor_uid, channel_id, MAX(dt) AS max_dt
FROM warehouse.dw_huya_day_anchor_info
WHERE DATE_FORMAT(dt, '%Y-%m') BETWEEN DATE_FORMAT('{start_date}', '%Y-%m') AND DATE_FORMAT('{end_date}', '%Y-%m')
GROUP BY CONCAT(DATE_FORMAT(dt, '%Y-%m'), '-01'), anchor_uid, channel_id
;


-- DROP TABLE IF EXISTS warehouse.dw_huya_month_anchor_info;
-- CREATE TABLE warehouse.dw_huya_month_anchor_info AS
DELETE
FROM warehouse.dw_huya_month_anchor_info
WHERE DATE_FORMAT(dt, '%Y-%m') BETWEEN DATE_FORMAT('{start_date}', '%Y-%m') AND DATE_FORMAT('{end_date}', '%Y-%m');
INSERT INTO warehouse.dw_huya_month_anchor_info
SELECT CONCAT(DATE_FORMAT(t1.dt, '%Y-%m'), '-01') AS dt,
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
       t2.dt                                      AS last_active_date
FROM warehouse.dw_huya_day_anchor_info t1
         INNER JOIN stage.stage_huya_month_anchor_info t2
                    ON t1.dt = t2.max_dt AND t1.anchor_uid = t2.anchor_uid AND t1.channel_id = t2.channel_id
WHERE DATE_FORMAT(t1.dt, '%Y-%m') BETWEEN DATE_FORMAT('{start_date}', '%Y-%m') AND DATE_FORMAT('{end_date}', '%Y-%m');


-- 合并每月主播的信息和开播行为
-- DROP TABLE IF EXISTS warehouse.dw_huya_month_anchor_live;
-- CREATE TABLE warehouse.dw_huya_month_anchor_live AS
DELETE
FROM warehouse.dw_huya_month_anchor_live
WHERE DATE_FORMAT(dt, '%Y-%m') BETWEEN DATE_FORMAT('{start_date}', '%Y-%m') AND DATE_FORMAT('{end_date}', '%Y-%m');
INSERT INTO warehouse.dw_huya_month_anchor_live
SELECT t1.dt,
       t1.channel_id,
       t2.channel_num,
       t1.anchor_uid,
       t2.anchor_no,
       t2.nick,
       t2.comment,
       t1.duration,
       t1.live_days,
       t1.revenue,
       t1.peak_pcu_avg,
       t1.peak_pcu_max,
       t1.peak_pcu_min,
       t2.platform_id,
       t2.platform_name,
       t2.activity_days,
       t2.months,
       t2.ow_percent,
       t2.sign_time,
       t2.surplus_days,
       t2.avatar AS avatar,
       t1.vir_coin_name,
       t1.vir_coin_rate,
       t1.include_pf_amt,
       t1.pf_amt_rate
FROM (SELECT CONCAT(DATE_FORMAT(dt, '%Y-%m'), '-01') AS dt,
             anchor_uid,
             channel_id,
             SUM(IFNULL(duration, 0))                AS duration,
             SUM(IFNULL(live_status, 0))             AS live_days,
             SUM(IFNULL(revenue, 0))                  AS revenue,
             AVG(IF(peak_pcu > 0, peak_pcu, NULL))   AS peak_pcu_avg,
             MAX(peak_pcu)                           AS peak_pcu_max,
             MIN(IF(peak_pcu > 0, peak_pcu, NULL))   AS peak_pcu_min,
             MAX(vir_coin_name)                      AS vir_coin_name,
             MAX(vir_coin_rate)                      AS vir_coin_rate,
             MAX(include_pf_amt)                     AS include_pf_amt,
             MAX(pf_amt_rate)                        AS pf_amt_rate
      FROM warehouse.dw_huya_day_anchor_live
      WHERE DATE_FORMAT(dt, '%Y-%m') BETWEEN DATE_FORMAT('{start_date}', '%Y-%m') AND DATE_FORMAT('{end_date}', '%Y-%m')
      GROUP BY CONCAT(DATE_FORMAT(dt, '%Y-%m'), '-01'),
               anchor_uid,
               channel_id) t1
         LEFT JOIN warehouse.dw_huya_month_anchor_info t2
                   ON t1.channel_id = t2.channel_id AND t1.anchor_uid = t2.anchor_uid AND t2.dt = t1.dt
;


-- DROP TABLE IF EXISTS stage.stage_huya_month_guild_live_revenue;
-- CREATE TABLE stage.stage_huya_month_guild_live_revenue AS
DELETE
FROM stage.stage_huya_month_guild_live_revenue
WHERE DATE_FORMAT(dt, '%Y-%m') BETWEEN DATE_FORMAT('{start_date}', '%Y-%m') AND DATE_FORMAT('{end_date}', '%Y-%m');
INSERT INTO stage.stage_huya_month_guild_live_revenue
SELECT CONCAT(DATE_FORMAT(dt, '%Y-%m'), '-01'),
       t1.channel_id,
       SUM(IFNULL(revenue, 0)) AS revenue
FROM warehouse.dw_huya_day_guild_live t1
WHERE DATE_FORMAT(dt, '%Y-%m') BETWEEN DATE_FORMAT('{start_date}', '%Y-%m') AND DATE_FORMAT('{end_date}', '%Y-%m')
GROUP BY CONCAT(DATE_FORMAT(dt, '%Y-%m'), '-01'),
         t1.channel_id
;


-- DROP TABLE IF EXISTS stage.stage_huya_month_guild_live_gift_income;
-- CREATE TABLE stage.stage_huya_month_guild_live_gift_income AS
DELETE
FROM stage.stage_huya_month_guild_live_gift_income
WHERE DATE_FORMAT(dt, '%Y-%m') BETWEEN DATE_FORMAT('{start_date}', '%Y-%m') AND DATE_FORMAT('{end_date}', '%Y-%m');
INSERT INTO stage.stage_huya_month_guild_live_gift_income
SELECT CONCAT(DATE_FORMAT(dt, '%Y-%m'), '-01') AS dt,
       t1.channel_id,
       SUM(IFNULL(gift_income, 0))             AS gift_income
FROM warehouse.dw_huya_day_guild_live t1
WHERE DATE_FORMAT(dt, '%Y-%m') BETWEEN DATE_FORMAT('{start_date}', '%Y-%m') AND DATE_FORMAT('{end_date}', '%Y-%m')
GROUP BY CONCAT(DATE_FORMAT(dt, '%Y-%m'), '-01'),
         t1.channel_id
;


# DROP TABLE IF EXISTS stage.stage_huya_month_guild_live_guard_income;
# CREATE TABLE stage.stage_huya_month_guild_live_guard_income AS
DELETE
FROM stage.stage_huya_month_guild_live_guard_income
WHERE DATE_FORMAT(dt, '%Y-%m') BETWEEN DATE_FORMAT('{start_date}', '%Y-%m') AND DATE_FORMAT('{end_date}', '%Y-%m');
INSERT INTO stage.stage_huya_month_guild_live_guard_income
SELECT CONCAT(DATE_FORMAT(dt, '%Y-%m'), '-01') AS dt,
       t1.channel_id,
       SUM(ifnull(guard_income, 0))            AS guard_income
FROM warehouse.dw_huya_day_guild_live t1
WHERE DATE_FORMAT(dt, '%Y-%m') BETWEEN DATE_FORMAT('{start_date}', '%Y-%m') AND DATE_FORMAT('{end_date}', '%Y-%m')
GROUP BY CONCAT(DATE_FORMAT(dt, '%Y-%m'), '-01'),
         t1.channel_id
;


-- 有几个频道缺失数据
-- DROP TABLE IF EXISTS stage.stage_huya_month_guild_live_noble_income;
-- CREATE TABLE stage.stage_huya_month_guild_live_noble_income AS
DELETE
FROM stage.stage_huya_month_guild_live_noble_income
WHERE DATE_FORMAT(dt, '%Y-%m') BETWEEN DATE_FORMAT('{start_date}', '%Y-%m') AND DATE_FORMAT('{end_date}', '%Y-%m');
INSERT INTO stage.stage_huya_month_guild_live_noble_income
SELECT CONCAT(DATE_FORMAT(dt, '%Y-%m'), '-01') AS dt,
       t1.channel_id,
       SUM(IFNULL(noble_income, 0))            AS noble_income
FROM warehouse.dw_huya_day_guild_live t1
WHERE DATE_FORMAT(dt, '%Y-%m') BETWEEN DATE_FORMAT('{start_date}', '%Y-%m') AND DATE_FORMAT('{end_date}', '%Y-%m')
GROUP BY CONCAT(DATE_FORMAT(dt, '%Y-%m'), '-01'),
         t1.channel_id;


-- DROP TABLE IF EXISTS warehouse.dw_huya_month_guild_live;
-- CREATE TABLE warehouse.dw_huya_month_guild_live AS
DELETE
FROM warehouse.dw_huya_month_guild_live
WHERE DATE_FORMAT(dt, '%Y-%m') BETWEEN DATE_FORMAT('{start_date}', '%Y-%m') AND DATE_FORMAT('{end_date}', '%Y-%m');
INSERT INTO warehouse.dw_huya_month_guild_live
SELECT CONCAT(DATE_FORMAT(t3.dt, '%Y-%m'), '-01') AS dt,
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
       t1.sign_limit,
       t2.revenue,
       t3.gift_income,
       t4.guard_income,
       t5.noble_income
FROM  warehouse.dw_huya_month_guild_info t1
         LEFT JOIN
         stage.stage_huya_month_guild_live_revenue t2
     ON t2.channel_id = t1.channel_id AND t1.dt = t2.dt
         LEFT JOIN
         stage.stage_huya_month_guild_live_gift_income t3
     ON t3.channel_id = t1.channel_id AND t1.dt = t3.dt
         LEFT JOIN
         stage.stage_huya_month_guild_live_guard_income t4
     ON t4.channel_id = t1.channel_id AND t4.dt = t1.dt
         LEFT JOIN stage.stage_huya_month_guild_live_noble_income t5
                   ON t5.channel_id = t1.channel_id AND t5.dt = t1.dt
WHERE DATE_FORMAT(t1.dt, '%Y-%m') BETWEEN DATE_FORMAT('{start_date}', '%Y-%m') AND DATE_FORMAT('{end_date}', '%Y-%m')
;

