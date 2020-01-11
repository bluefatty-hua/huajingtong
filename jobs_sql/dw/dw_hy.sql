-- 数据源 warehouse.dw_month_huya_guild_info、dw_month_huya_anchor_info
-- ===============================================================
-- 汇总月guild、anchor info数据
-- DROP TABLE IF EXISTS stage.stage_month_huya_guild_info;
-- CREATE TABLE stage.stage_month_huya_guild_info AS
delete from stage.stage_month_huya_guild_info where dt >='{month.start}' AND dt<='{month.end}'
insert into stage.stage_month_huya_guild_info
SELECT channel_id,MAX(dt) AS dt
FROM warehouse.ods_day_huya_guild_info
WHERE dt >='{month.start}' AND dt<='{month.end}'
GROUP BY channel_id

-- DROP TABLE IF EXISTS warehouse.dw_month_huya_guild_info;
-- CREATE TABLE warehouse.dw_month_huya_guild_info AS
delete from warehouse.dw_month_huya_guild_info where dt >='{month.start}' AND dt<='{month.end}'
insert into warehouse.dw_month_huya_guild_info
select `platform_id`,
  `platform_name`,
  t1.`channel_id`,
  `channel_num`,
  `ow`,
  `channel_name`,
  `logo`,
  `desc`,
  `create_time`,
  `is_platinum`,
  `sign_count`,
  `sign_limit`,
  '{month.start}' as `dt`
from `warehouse`.`ods_day_huya_guild_info`  t1
join stage.stage_month_huya_guild_info t2
on t1.dt= t2.dt and t1.channel_id = t2.channel_id
where t1.dt >='{month.start}' AND t1.dt<='{month.end}'


-- DROP TABLE IF EXISTS stage.stage_month_huya_anchor_info;
-- CREATE TABLE stage.stage_month_huya_anchor_info AS
delete from stage.stage_month_huya_anchor_info where dt >='{month.start}' AND dt<='{month.end}'
insert into stage.stage_month_huya_anchor_info
SELECT anchor_uid,MAX(dt) AS dt,channel_id
FROM warehouse.ods_day_huya_anchor_info
WHERE dt >='{month.start}' AND dt<='{month.end}'
GROUP BY anchor_uid,channel_id


-- DROP TABLE IF EXISTS warehouse.dw_month_huya_anchor_info;
-- CREATE TABLE warehouse.dw_month_huya_anchor_info AS
delete from warehouse.dw_month_huya_anchor_info where dt >='{month.start}' AND dt<='{month.end}'
insert into warehouse.dw_month_huya_anchor_info
select  `platform_id`,
  `platform_name`,
  t1.`channel_id`,
  `channel_num`,
  t1.`anchor_uid`,
  `anchor_no`,
  `comment`,
  `nick`,
  `activity_days`,
  `months`,
  `ow_percent`,
  `sign_time`,
  `surplus_days`,
  '{month.start}' as `dt`,
  `avatar`,
  t2.dt as last_active_date
from `warehouse`.`ods_day_huya_anchor_info`  t1
join stage.stage_month_huya_anchor_info t2
on t1.dt= t2.dt and t1.anchor_uid = t2.anchor_uid and t1.channel_id= t2.channel_id
where t1.dt >='{month.start}' AND t1.dt<='{month.end}'



-- DROP TABLE IF EXISTS warehouse.dw_month_huya_anchor_live;
-- CREATE TABLE warehouse.dw_month_huya_anchor_live AS
delete from warehouse.dw_month_huya_anchor_live where dt ='{month.start}' 
insert into warehouse.dw_month_huya_anchor_live
SELECT

  t2.platform_id,
  t2.platform_name,
  t1.channel_id,
  t2.channel_num,
  t1.anchor_uid,
  t2.anchor_no,
  t2.nick,
  t2.comment,
  SUM(IFNULL(t1.`duration`,0)) AS duration,
  SUM(IFNULL(t1.live_status,0)) AS live_cnt,
  SUM(IFNULL(t1.income,0)) AS income,
  AVG(IF(t1.peak_pcu>0,t1.peak_pcu,NULL)) AS peak_pcu_avg,
  MAX(t1.peak_pcu) AS peak_pcu_max,
  MIN(t1.peak_pcu) AS peak_pcu_min,
  '{month.start}' AS dt,
  t2.activity_days,
  t2.months,
  t2.ow_percent,
  t2.sign_time,
  t2.surplus_days,
  t2.`avatar` AS avatar,
  MAX(t1.vir_coin_name) AS vir_coin_name,
  MAX(t1.vir_coin_rate) AS vir_coin_rate,
  MAX(t1.include_pf_amt) AS include_pf_amt,
  MAX(t1.pf_amt_rate) AS pf_amt_rate
 
  
FROM `warehouse`.`ods_day_huya_anchor_live` t1
LEFT JOIN warehouse.dw_month_huya_anchor_info t2
        ON t1.channel_id = t2.channel_id AND t1.anchor_uid = t2.anchor_uid AND t2.dt = '{month.end}'
WHERE t1.dt BETWEEN '{month.start}' AND '{month.end}'
GROUP BY anchor_uid,channel_id






-- 数据源 warehouse.ods_bb_anchor_live_detail_daily
-- ===============================================================
-- 汇总数据
-- 汇总维度 月-主播
-- 汇总指标 开播天数，开播时长，虚拟币收入
DROP TABLE IF EXISTS warehouse.dw_sum_hy_an_mon;
CREATE TABLE warehouse.dw_sum_hy_an_mon AS
SELECT DATE_FORMAT(CONCAT(YEAR(t.dt), '-', MONTH(t.dt), '-01'), '%Y-%m-%d')        AS rpt_month,
       t.platform_id,
       t.platform_name,
       t.anchor_no,
       COUNT(CASE WHEN t.live_status = 1 THEN t.dt ELSE NULL END) AS live_days,
       SUM(t.duration)                                            AS sum_duration,
       ROUND(SUM(t.amt), 2)                                                 AS sum_amt
FROM warehouse.ods_anchor_hy_live_detail_daily t
WHERE t.dt < CURRENT_DATE
GROUP BY DATE_FORMAT(CONCAT(YEAR(t.dt), '-', MONTH(t.dt), '-01'), '%Y-%m-%d'),
         t.platform_id,
         t.platform_name,
         t.anchor_no
;

-- 汇总数据源 ods_anchor_hy_live_detail_daily
-- 汇总维度 月-公会或频道
-- 汇总指标 主播数，开播主播数，主播收入
DROP TABLE IF EXISTS warehouse.dw_sum_hy_g_mon;
CREATE TABLE warehouse.dw_sum_hy_g_mon AS
SELECT DATE_FORMAT(CONCAT(YEAR(t.dt), '-', MONTH(t.dt), '-01'), '%Y-%m-%d')                                                                AS rpt_month,
       t.platform_id,
       t.platform_name,
       t.channel_id,
       COUNT(DISTINCT t.anchor_no)                                                AS an_cnt,
       COUNT(DISTINCT CASE WHEN t.live_status = 1 THEN t.anchor_no ELSE NULL END) AS an_live_cnt,
       ROUND(SUM(t.amt), 2)                                                       AS sum_amt
FROM warehouse.ods_anchor_hy_live_detail_daily t
WHERE t.dt < CURRENT_DATE
GROUP BY DATE_FORMAT(CONCAT(YEAR(t.dt), '-', MONTH(t.dt), '-01'), '%Y-%m-%d'),
         t.platform_id,
         t.platform_name,
         t.channel_id
;


-- ==========================================================================
-- 公会或频道
-- 数据来源 ods_guild_hy_amt_daily
-- 汇总维度 月-公会或频道
-- 汇总指标 公会流水，公会收入，礼物|贵族|守护公会分成，统计天数
DROP TABLE IF EXISTS warehouse.dw_sum_hy_g_amt_mon;
CREATE TABLE warehouse.dw_sum_hy_g_amt_mon AS
SELECT t.platform_id,
       t.platform_name,
       DATE_FORMAT(CONCAT(YEAR(t.dt), '-', MONTH(t.dt), '-01'), '%Y-%m-%d') AS rpt_month,
       t.channel_id,
       t.channel_num,
       SUM(total_amt) AS g_total_amt,
       SUM(t.g_gift_vir_coin + t.g_guard_vir_coin + t.g_nobel_vir_coin) / 1000 AS g_final_amt,
       SUM(t.g_gift_vir_coin) AS g_gift_vir_coin,
       SUM(t.g_guard_vir_coin) AS g_guard_vir_coin,
       SUM(t.g_nobel_vir_coin) AS g_nobel_vir_coin,
       COUNT(DISTINCT t.dt) AS dt_cnt
FROM warehouse.ods_guild_hy_amt_daily t
GROUP BY t.platform_id,
         t.platform_name,
         DATE_FORMAT(CONCAT(YEAR(t.dt), '-', MONTH(t.dt), '-01'), '%Y-%m-%d'),
         t.channel_id,
         t.channel_num
;
