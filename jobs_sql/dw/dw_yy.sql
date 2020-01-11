-- 公会蓝钻收入
-- 汇总维度 月-公会-主播
-- 汇总指标 主播总蓝钻 公会分成蓝钻
-- DROP TABLE IF EXISTS warehouse.dw_month_yy_guild_anchor_virtual_coin;
-- CREATE TABLE warehouse.dw_month_yy_guild_anchor_virtual_coin AS
DELETE
FROM warehouse.dw_month_yy_guild_anchor_virtual_coin
WHERE dt BETWEEN CONCAT(YEAR('{start_date}'), '-', MONTH('{start_date}'), '-01') AND '{end_date}';
INSERT INTO warehouse.dw_month_yy_guild_anchor_virtual_coin
SELECT dt,
       platform_id,
       platform_name,
       backend_account_id,
       anchor_no,
       SUM(anchor_virtual_coin) AS anchor_virtual_coin,
       SUM(guild_virtual_coin)  AS guild_virtual_coin
FROM warehouse.ods_yy_guild_virtual_coin_detail
WHERE dt BETWEEN CONCAT(YEAR('{start_date}'), '-', MONTH('{start_date}'), '-01') AND '{end_date}'
GROUP BY dt,
         platform_id,
         platform_name,
         backend_account_id,
         anchor_no
;


-- 公会佣金收入
-- 汇总维度 月-公会-主播
-- 汇总指标 公会分成佣金
-- DROP TABLE IF EXISTS warehouse.dw_month_yy_guild_anchor_commission;
-- CREATE TABLE warehouse.dw_month_yy_guild_anchor_commission AS
DELETE
FROM warehouse.dw_month_yy_guild_anchor_commission
WHERE dt BETWEEN CONCAT(YEAR('{start_date}'), '-', MONTH('{start_date}'), '-01') AND '{end_date}';
INSERT INTO warehouse.dw_month_yy_guild_anchor_commission
SELECT dt,
       platform_id,
       platform_name,
       backend_account_id,
       channel_num,
       anchor_no,
       SUM(guild_commission) AS guild_commission
FROM warehouse.ods_yy_guild_commission_detail
WHERE dt BETWEEN CONCAT(YEAR('{start_date}'), '-', MONTH('{start_date}'), '-01') AND '{end_date}'
GROUP BY dt,
         platform_id,
         platform_name,
         backend_account_id,
         channel_num,
         anchor_no
;

-- =================================================================
-- 公会月蓝钻收入
-- 汇总维度 月-主播
-- 汇总指标 主播总蓝钻 公会分成蓝钻
-- DROP TABLE IF EXISTS warehouse.dw_month_yy_guild_virtual_coin;
-- CREATE TABLE warehouse.dw_month_yy_guild_virtual_coin AS
DELETE
FROM warehouse.dw_month_yy_guild_virtual_coin
WHERE dt BETWEEN CONCAT(YEAR('{start_date}'), '-', MONTH('{start_date}'), '-01') AND '{end_date}';
INSERT INTO warehouse.dw_month_yy_guild_virtual_coin
SELECT dt,
       platform_id,
       platform_name,
       anchor_no,
       SUM(anchor_virtual_coin) AS anchor_virtual_coin,
       SUM(guild_virtual_coin)  AS guild_virtual_coin
FROM warehouse.ods_yy_guild_virtual_coin_detail
-- WHERE dt BETWEEN CONCAT(YEAR('{start_date}'), '-', MONTH('{start_date}'), '-01') AND '{end_date}'
GROUP BY dt,
         platform_id,
         platform_name,
         anchor_no
;


-- 公会月佣金收入
-- 汇总维度 月-主播
-- 汇总指标 公会分成佣金
-- DROP TABLE IF EXISTS warehouse.dw_month_yy_guild_commission;
-- CREATE TABLE warehouse.dw_month_yy_guild_commission AS
DELETE
FROM warehouse.dw_month_yy_guild_commission
WHERE dt BETWEEN CONCAT(YEAR('{start_date}'), '-', MONTH('{start_date}'), '-01') AND '{end_date}';
INSERT INTO warehouse.dw_month_yy_guild_commission
SELECT dt,
       platform_id,
       platform_name,
       anchor_no,
       SUM(guild_commission) AS guild_commission
FROM warehouse.ods_yy_guild_commission_detail
WHERE dt BETWEEN CONCAT(YEAR('{start_date}'), '-', MONTH('{start_date}'), '-01') AND '{end_date}'
GROUP BY dt,
         platform_id,
         platform_name,
         anchor_no
;


-- 主播月收入
-- 汇总维度 月-公会—主播
-- 汇总指标 开播天数，开播时长，虚拟币收入，主播佣金，公会佣金
-- DROP TABLE IF EXISTS warehouse.dw_month_yy_guild_anchor_salary;
-- CREATE TABLE warehouse.dw_month_yy_guild_anchor_salary AS
DELETE
FROM warehouse.dw_month_yy_guild_anchor_salary
WHERE dt = CONCAT(YEAR('{start_date}'), '-', MONTH('{start_date}'), '-01');
INSERT INTO warehouse.dw_month_yy_guild_anchor_salary
SELECT ad.dt,
       ad.platform_id,
       ad.platform_name,
       ad.backend_account_id,
       ad.channel_num,
       ad.anchor_no,
       ad.anchor_uid,
       ad.live_days,
       ad.duration,
       ad.anchor_virtual_coin,
       gv.anchor_virtual_coin AS anchor_virtual_coin_true,
       gv.guild_virtual_coin  AS guild_virtual_coin_true,
       ad.anchor_commission,
       ad.guild_commission,
       gc.guild_commission    AS guild_commission_ture,
       ad.dt_cnt
FROM (SELECT DATE_FORMAT(CONCAT(YEAR(dt), '-', MONTH(dt), '-01'), '%Y-%m-%d')        AS dt,
             platform_id,
             platform_name,
             backend_account_id,
             channel_num,
             anchor_no,
             anchor_uid,
             COUNT(CASE WHEN live_status = 1 THEN dt ELSE NULL END)                  AS live_days,
             SUM(CASE WHEN duration >= 0 THEN duration ELSE 0 END)                   AS duration,
             SUM(CASE WHEN virtual_coin >= 0 THEN virtual_coin ELSE 0 END)           AS anchor_virtual_coin,
             SUM(CASE WHEN anchor_commission >= 0 THEN anchor_commission ELSE 0 END) AS anchor_commission,
             SUM(CASE WHEN guild_commission >= 0 THEN guild_commission ELSE 0 END)   AS guild_commission,
             COUNT(DISTINCT dt)                                                      AS dt_cnt
      FROM warehouse.ods_yy_anchor_live_detail
--       WHERE dt BETWEEN CONCAT(YEAR('{start_date}'), '-', MONTH('{start_date}'), '-01') AND '{end_date}'
      GROUP BY DATE_FORMAT(CONCAT(YEAR(dt), '-', MONTH(dt), '-01'), '%Y-%m-%d'),
               platform_id,
               platform_name,
               backend_account_id,
               channel_num,
               anchor_no,
               anchor_uid) ad
         LEFT JOIN warehouse.dw_month_yy_guild_anchor_virtual_coin gv
                   ON ad.dt = gv.dt AND ad.anchor_no = gv.anchor_no AND ad.backend_account_id = gv.backend_account_id
         LEFT JOIN warehouse.dw_month_yy_guild_anchor_commission gc
                   ON ad.dt = gc.dt AND ad.anchor_no = gc.anchor_no AND ad.backend_account_id = gc.backend_account_id
;


-- 主播月收入
-- 汇总维度 月-主播
-- 汇总指标 主播数，开播主播数，主播虚拟币收入,主播佣金收入，公会佣金收入
-- DROP TABLE IF EXISTS warehouse.dw_month_yy_anchor_salary;
-- CREATE TABLE warehouse.dw_month_yy_anchor_salary AS
DELETE
FROM warehouse.dw_month_yy_anchor_salary
WHERE dt = CONCAT(YEAR('{start_date}'), '-', MONTH('{start_date}'), '-01');
INSERT INTO warehouse.dw_month_yy_anchor_salary
SELECT ad.dt,
       ad.platform_id,
       ad.platform_name,
       ad.anchor_no,
       ad.anchor_uid,
       ad.live_days,
       ad.duration,
       ad.anchor_virtual_coin,
       gv.anchor_virtual_coin AS anchor_virtual_coin_true,
       gv.guild_virtual_coin  AS guild_virtual_coin_true,
       ad.anchor_commission,
       ad.guild_commission,
       gc.guild_commission    AS guild_commission_ture,
       ad.dt_cnt
FROM (
         SELECT DATE_FORMAT(CONCAT(YEAR(t.dt), '-', MONTH(t.dt), '-01'), '%Y-%m-%d')        AS dt,
                t.platform_id,
                t.platform_name,
                t.anchor_no,
                t.anchor_uid,
                COUNT(DISTINCT CASE WHEN t.live_status = 1 THEN t.dt ELSE NULL END)         AS live_days,
                SUM(CASE WHEN duration >= 0 THEN duration ELSE 0 END)                       AS duration,
                SUM(CASE WHEN t.virtual_coin >= 0 THEN t.virtual_coin ELSE 0 END)           AS anchor_virtual_coin,
                SUM(CASE WHEN t.anchor_commission >= 0 THEN t.anchor_commission ELSE 0 END) AS anchor_commission,
                SUM(CASE WHEN t.guild_commission >= 0 THEN t.guild_commission ELSE 0 END)   AS guild_commission,
                COUNT(DISTINCT t.dt)                                                        AS dt_cnt
         FROM warehouse.ods_yy_anchor_live_detail t
         WHERE dt BETWEEN CONCAT(YEAR('{start_date}'), '-', MONTH('{start_date}'), '-01') AND '{end_date}'
         GROUP BY DATE_FORMAT(CONCAT(YEAR(t.dt), '-', MONTH(t.dt), '-01'), '%Y-%m-%d'),
                  t.platform_id,
                  t.platform_name,
                  t.anchor_no,
                  t.anchor_uid) ad
         LEFT JOIN warehouse.dw_month_yy_guild_virtual_coin gv ON ad.dt = gv.dt AND ad.anchor_no = gv.anchor_no
         LEFT JOIN warehouse.dw_month_yy_guild_commission gc ON ad.dt = gc.dt AND ad.anchor_no = gc.anchor_no
;

