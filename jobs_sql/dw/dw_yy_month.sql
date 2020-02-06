-- table list
-- 月-公会-主播
-- warehouse.dw_yy_month_guild_anchor_live_bluediamond
-- warehouse.dw_yy_month_guild_anchor_live_commission
-- warehouse.dw_yy_month_guild_anchor_live
-- 月-公会
-- warehouse.dw_yy_month_guild_live_bluediamond
-- warehouse.dw_yy_month_guild_live_commission
-- warehouse.dw_yy_month_guild_live
-- 月-主播
-- warehouse.dw_yy_month_anchor_live_bluediamond
-- warehouse.dw_yy_month_anchor_live_commission
-- warehouse.dw_yy_month_anchor_live


-- 取出每月主播数（以每个月最后一天为准）
DROP TABLE IF EXISTS stage.stage_yy_month_guild_info_max_day;
CREATE TABLE stage.stage_yy_month_guild_info_max_day AS
SELECT CONCAT(DATE_FORMAT(dt, '%Y-%m'), '-01') AS dt,
       backend_account_id,
       MAX(dt)                                 AS max_dt
FROM warehouse.dw_yy_day_anchor_live
WHERE comment = 'orig'
  AND dt BETWEEN DATE_FORMAT(CONCAT(YEAR('{start_date}'), '-', MONTH('{start_date}'), '-01'),
                             '%Y-%m-%d') AND '{end_date}'
GROUP BY CONCAT(DATE_FORMAT(dt, '%Y-%m'), '-01'),
         backend_account_id
;


-- DROP TABLE IF EXISTS stage.stage_yy_month_anchor_info;
-- CREATE TABLE stage.stage_yy_month_anchor_info AS
DELETE
FROM stage.stage_yy_month_anchor_info
WHERE dt BETWEEN DATE_FORMAT(CONCAT(YEAR('{start_date}'), '-', MONTH('{start_date}'), '-01'),
                             '%Y-%m-%d') AND '{end_date}';
INSERT INTO stage.stage_yy_month_anchor_info
SELECT t0.dt, t0.backend_account_id, anchor_uid
FROM stage.stage_yy_month_guild_info_max_day t0
         LEFT JOIN (SELECT dt, backend_account_id, anchor_uid
                    FROM warehouse.dw_yy_day_anchor_live
                    WHERE comment = 'orig'
                      AND dt BETWEEN DATE_FORMAT(CONCAT(YEAR('{start_date}'), '-', MONTH('{start_date}'), '-01'),
                                                 '%Y-%m-%d') AND '{end_date}') t1
                   ON t0.backend_account_id = t1.backend_account_id AND t0.max_dt = t1.dt
;


-- 取出每日主播数（以每个月最后一天为准）
# DROP TABLE IF EXISTS stage.stage_yy_day_anchor_live;
# CREATE TABLE stage.stage_yy_day_anchor_live
DELETE
FROM stage.stage_yy_day_anchor_live
WHERE dt BETWEEN DATE_FORMAT(CONCAT(YEAR('{start_date}'), '-', MONTH('{start_date}'), '-01'),
                             '%Y-%m-%d') AND '{end_date}';
INSERT INTO stage.stage_yy_day_anchor_live
SELECT al.dt,
       al.platform_id,
       al.platform_name,
       ai.backend_account_id,
       al.channel_num,
       al.anchor_uid,
       al.anchor_no,
       al.anchor_nick_name,
       al.anchor_type,
       al.anchor_type_text,
       al.duration,
       al.live_status,
       al.bluediamond,
       al.anchor_commission,
       al.guild_commission,
       al.anchor_settle_rate,
       al.comment
FROM warehouse.dw_yy_day_anchor_live al
         INNER JOIN stage.stage_yy_month_anchor_info ai ON DATE_FORMAT(ai.dt, '%Y-%m') = DATE_FORMAT(al.dt, '%Y-%m') AND
                                                         ai.backend_account_id = al.backend_account_id AND
                                                         ai.anchor_uid = al.anchor_uid
WHERE ai.dt BETWEEN CONCAT(DATE_FORMAT('{start_date}', '%Y-%m'), '-01') AND '{end_date}'
  AND al.dt BETWEEN CONCAT(DATE_FORMAT('{start_date}', '%Y-%m'), '-01') AND '{end_date}'
;


-- 公会
-- 公会蓝钻收入
-- 汇总维度 月-公会-主播
-- 汇总指标 主播总蓝钻 公会分成蓝钻
-- DROP TABLE IF EXISTS warehouse.dw_yy_month_guild_anchor_live_bluediamond;
-- CREATE TABLE warehouse.dw_yy_month_guild_anchor_live_bluediamond AS
DELETE
FROM warehouse.dw_yy_month_guild_anchor_live_bluediamond
WHERE dt BETWEEN DATE_FORMAT(CONCAT(YEAR('{start_date}'), '-', MONTH('{start_date}'), '-01'),
                             '%Y-%m-%d') AND '{end_date}';
INSERT INTO warehouse.dw_yy_month_guild_anchor_live_bluediamond
SELECT dt,
       platform_id,
       platform_name,
       backend_account_id,
       anchor_no,
       SUM(anchor_bluediamond) AS anchor_bluediamond,
       SUM(guild_bluediamond)  AS guild_bluediamond
FROM warehouse.ods_yy_guild_live_bluediamond
WHERE dt BETWEEN DATE_FORMAT(CONCAT(YEAR('{start_date}'), '-', MONTH('{start_date}'), '-01'),
                             '%Y-%m-%d') AND '{end_date}'
GROUP BY dt,
         platform_id,
         platform_name,
         backend_account_id,
         anchor_no
;


-- 公会佣金收入
-- 汇总维度 月-公会-主播
-- 汇总指标 公会分成佣金
-- DROP TABLE IF EXISTS warehouse.dw_yy_month_guild_anchor_live_commission;
-- CREATE TABLE warehouse.dw_yy_month_guild_anchor_live_commission AS
DELETE
FROM warehouse.dw_yy_month_guild_anchor_live_commission
WHERE dt BETWEEN DATE_FORMAT(CONCAT(YEAR('{start_date}'), '-', MONTH('{start_date}'), '-01'),
                             '%Y-%m-%d') AND '{end_date}';
INSERT INTO warehouse.dw_yy_month_guild_anchor_live_commission
SELECT dt,
       platform_id,
       platform_name,
       backend_account_id,
       channel_num,
       anchor_no,
       SUM(guild_commission) AS guild_commission
FROM warehouse.ods_yy_guild_live_commission
WHERE dt BETWEEN DATE_FORMAT(CONCAT(YEAR('{start_date}'), '-', MONTH('{start_date}'), '-01'),
                             '%Y-%m-%d') AND '{end_date}'
GROUP BY dt,
         platform_id,
         platform_name,
         backend_account_id,
         channel_num,
         anchor_no
;


-- 公会主播月收入
-- 汇总维度 月-公会—主播
-- 汇总指标 开播天数，开播时长，蓝钻收入，主播佣金，公会佣金
-- DROP TABLE IF EXISTS warehouse.dw_yy_month_guild_anchor_live;
-- CREATE TABLE warehouse.dw_yy_month_guild_anchor_live AS
DELETE
FROM warehouse.dw_yy_month_guild_anchor_live
WHERE dt BETWEEN DATE_FORMAT(CONCAT(YEAR('{start_date}'), '-', MONTH('{start_date}'), '-01'),
                             '%Y-%m-%d') AND '{end_date}';
INSERT INTO warehouse.dw_yy_month_guild_anchor_live
SELECT ad.dt,
       ad.platform_id,
       ad.platform_name,
       ad.backend_account_id,
       ad.channel_num,
       ad.anchor_no,
       ad.anchor_uid,
       ad.live_days,
       ad.duration,
       ad.anchor_bluediamond,
       IFNULL(ad.anchor_income_bluediamond, 0) AS anchor_income_bluediamond,
       IFNULL(ad.guild_income_bluediamond, 0)  AS guild_income_bluediamond,
       IFNULL(gv.anchor_bluediamond, 0)        AS anchor_bluediamond_true,
       IFNULL(gv.guild_bluediamond, 0)         AS guild_bluediamond_true,
       IFNULL(ad.anchor_commission, 0)         AS anchor_commission,
       IFNULL(ad.guild_commission, 0)          AS guild_commission,
       IFNULL(gc.guild_commission, 0)          AS guild_commission_true,
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
             SUM(CASE WHEN bluediamond >= 0 THEN bluediamond ELSE 0 END)             AS anchor_bluediamond,
             SUM(CASE
                     WHEN bluediamond > 0 THEN bluediamond * anchor_settle_rate
                     ELSE 0 END)                                                     AS anchor_income_bluediamond,
             SUM(CASE
                     WHEN bluediamond > 0 THEN bluediamond * (1 - anchor_settle_rate)
                     ELSE 0 END)                                                     AS guild_income_bluediamond,
             SUM(CASE WHEN anchor_commission >= 0 THEN anchor_commission ELSE 0 END) AS anchor_commission,
             SUM(CASE WHEN guild_commission >= 0 THEN guild_commission ELSE 0 END)   AS guild_commission,
             COUNT(DISTINCT dt)                                                      AS dt_cnt
      FROM stage.stage_yy_day_anchor_live
      WHERE dt BETWEEN DATE_FORMAT(CONCAT(YEAR('{start_date}'), '-', MONTH('{start_date}'), '-01'),
                                   '%Y-%m-%d') AND '{end_date}'
      GROUP BY DATE_FORMAT(CONCAT(YEAR(dt), '-', MONTH(dt), '-01'), '%Y-%m-%d'),
               platform_id,
               platform_name,
               backend_account_id,
               channel_num,
               anchor_no,
               anchor_uid) ad
         LEFT JOIN warehouse.dw_yy_month_guild_anchor_live_bluediamond gv
                   ON ad.dt = gv.dt AND ad.anchor_no = gv.anchor_no AND ad.backend_account_id = gv.backend_account_id
         LEFT JOIN warehouse.dw_yy_month_guild_anchor_live_commission gc
                   ON ad.dt = gc.dt AND ad.anchor_no = gc.anchor_no AND ad.backend_account_id = gc.backend_account_id
;


-- 公会蓝钻月收入
-- 汇总维度 月-公会
-- 汇总指标 主播总蓝钻 公会分成蓝钻
-- DROP TABLE IF EXISTS warehouse.dw_yy_month_guild_live_bluediamond;
-- CREATE TABLE warehouse.dw_yy_month_guild_live_bluediamond AS
DELETE
FROM warehouse.dw_yy_month_guild_live_bluediamond
WHERE dt BETWEEN DATE_FORMAT(CONCAT(YEAR('{start_date}'), '-', MONTH('{start_date}'), '-01'),
                             '%Y-%m-%d') AND '{end_date}';
INSERT INTO warehouse.dw_yy_month_guild_live_bluediamond
SELECT dt,
       platform_id,
       platform_name,
       backend_account_id,
       SUM(anchor_bluediamond) AS anchor_bluediamond,
       SUM(guild_bluediamond)  AS guild_bluediamond
FROM warehouse.ods_yy_guild_live_bluediamond
WHERE dt BETWEEN DATE_FORMAT(CONCAT(YEAR('{start_date}'), '-', MONTH('{start_date}'), '-01'),
                             '%Y-%m-%d') AND '{end_date}'
GROUP BY dt,
         platform_id,
         platform_name,
         backend_account_id
;


-- 公会佣金月收入
-- 汇总维度 月-公会
-- 汇总指标 公会分成佣金
-- DROP TABLE IF EXISTS warehouse.dw_yy_month_guild_live_commission;
-- CREATE TABLE warehouse.dw_yy_month_guild_live_commission AS
DELETE
FROM warehouse.dw_yy_month_guild_live_commission
WHERE dt BETWEEN DATE_FORMAT(CONCAT(YEAR('{start_date}'), '-', MONTH('{start_date}'), '-01'),
                             '%Y-%m-%d') AND '{end_date}';
INSERT INTO warehouse.dw_yy_month_guild_live_commission
SELECT dt,
       platform_id,
       platform_name,
       backend_account_id,
       channel_num,
       SUM(guild_commission) AS guild_commission
FROM warehouse.ods_yy_guild_live_commission
WHERE dt BETWEEN DATE_FORMAT(CONCAT(YEAR('{start_date}'), '-', MONTH('{start_date}'), '-01'),
                             '%Y-%m-%d') AND '{end_date}'
GROUP BY dt,
         platform_id,
         platform_name,
         backend_account_id,
         channel_num
;


-- DROP TABLE IF EXISTS warehouse.dw_yy_month_guild_live;
-- CREATE TABLE warehouse.dw_yy_month_guild_live AS
DELETE
FROM warehouse.dw_yy_month_guild_live
WHERE dt BETWEEN DATE_FORMAT(CONCAT(YEAR('{start_date}'), '-', MONTH('{start_date}'), '-01'),
                             '%Y-%m-%d') AND '{end_date}';
INSERT INTO warehouse.dw_yy_month_guild_live
SELECT ad.dt,
       ad.platform_id,
       ad.platform_name,
       ad.backend_account_id,
       ad.channel_num,
       ad.anchor_cnt,
       ad.anchor_live_cnt,
       IFNULL(ad.anchor_bluediamond, 0)        AS anchor_bluediamond,
       IFNULL(ad.anchor_income_bluediamond, 0) AS anchor_income_bluediamond,
       IFNULL(ad.guild_income_bluediamond, 0)  AS guild_income_bluediamond,
       IFNULL(gv.anchor_bluediamond, 0)        AS anchor_bluediamond_true,
       IFNULL(gv.guild_bluediamond, 0)         AS guild_income_bluediamond_true,
       IFNULL(ad.anchor_commission, 0)         AS anchor_commission,
       IFNULL(ad.guild_commission, 0)          AS guild_commission,
       IFNULL(gc.guild_commission, 0)          AS guild_commission_true,
       IFNULL(ad.dt_cnt, 0)                    AS dt_cnt
FROM (
         SELECT DATE_FORMAT(CONCAT(YEAR(t.dt), '-', MONTH(t.dt), '-01'), '%Y-%m-%d')        AS dt,
                t.platform_id,
                t.platform_name,
                t.backend_account_id,
                max(channel_num)                                                            as channel_num,
                COUNT(DISTINCT t.anchor_no)                                                 AS anchor_cnt,
                COUNT(DISTINCT CASE WHEN t.live_status = 1 THEN t.anchor_no ELSE NULL END)  AS anchor_live_cnt,
                SUM(CASE WHEN t.bluediamond >= 0 THEN t.bluediamond ELSE 0 END)             AS anchor_bluediamond,
                SUM(CASE
                        WHEN t.bluediamond >= 0 THEN t.bluediamond * t.anchor_settle_rate
                        ELSE 0 END)                                                         AS anchor_income_bluediamond,
                SUM(CASE
                        WHEN t.bluediamond >= 0 THEN t.bluediamond * (1 - t.anchor_settle_rate)
                        ELSE 0 END)                                                         AS guild_income_bluediamond,
                SUM(CASE WHEN t.anchor_commission >= 0 THEN t.anchor_commission ELSE 0 END) AS anchor_commission,
                SUM(CASE WHEN t.guild_commission >= 0 THEN t.guild_commission ELSE 0 END)   AS guild_commission,
                COUNT(DISTINCT t.dt)                                                        AS dt_cnt
         FROM stage.stage_yy_day_anchor_live t
         WHERE t.dt BETWEEN DATE_FORMAT(CONCAT(YEAR('{start_date}'), '-', MONTH('{start_date}'), '-01'),
                                        '%Y-%m-%d') AND '{end_date}'
         GROUP BY DATE_FORMAT(CONCAT(YEAR(t.dt), '-', MONTH(t.dt), '-01'), '%Y-%m-%d'),
                  t.platform_id,
                  t.platform_name,
                  t.backend_account_id
     ) ad
         LEFT JOIN warehouse.dw_yy_month_guild_live_bluediamond gv
                   ON ad.dt = gv.dt AND ad.backend_account_id = gv.backend_account_id
         LEFT JOIN warehouse.dw_yy_month_guild_live_commission gc
                   ON ad.dt = gc.dt AND ad.backend_account_id = gc.backend_account_id
;


-- 公会蓝钻收入
-- 汇总维度 月-主播
-- 汇总指标 主播总蓝钻 公会分成蓝钻
-- DROP TABLE IF EXISTS warehouse.dw_yy_month_anchor_live_bluediamond;
-- CREATE TABLE warehouse.dw_yy_month_anchor_live_bluediamond AS
DELETE
FROM warehouse.dw_yy_month_anchor_live_bluediamond
WHERE dt BETWEEN DATE_FORMAT(CONCAT(YEAR('{start_date}'), '-', MONTH('{start_date}'), '-01'),
                             '%Y-%m-%d') AND '{end_date}';
INSERT INTO warehouse.dw_yy_month_anchor_live_bluediamond
SELECT dt,
       platform_id,
       platform_name,
       anchor_no,
       SUM(anchor_bluediamond) AS anchor_bluediamond,
       SUM(guild_bluediamond)  AS guild_bluediamond
FROM warehouse.ods_yy_guild_live_bluediamond
WHERE dt BETWEEN DATE_FORMAT(CONCAT(YEAR('{start_date}'), '-', MONTH('{start_date}'), '-01'),
                             '%Y-%m-%d') AND '{end_date}'
GROUP BY dt,
         platform_id,
         platform_name,
         anchor_no
;



-- 公会佣金收入
-- 汇总维度 月-主播
-- 汇总指标 公会分成佣金
-- DROP TABLE IF EXISTS warehouse.dw_yy_month_anchor_live_commission;
-- CREATE TABLE warehouse.dw_yy_month_anchor_live_commission AS
DELETE
FROM warehouse.dw_yy_month_anchor_live_commission
WHERE dt BETWEEN DATE_FORMAT(CONCAT(YEAR('{start_date}'), '-', MONTH('{start_date}'), '-01'),
                             '%Y-%m-%d') AND '{end_date}';
INSERT INTO warehouse.dw_yy_month_anchor_live_commission
SELECT dt,
       platform_id,
       platform_name,
       anchor_no,
       SUM(guild_commission) AS guild_commission
FROM warehouse.ods_yy_guild_live_commission
WHERE dt BETWEEN DATE_FORMAT(CONCAT(YEAR('{start_date}'), '-', MONTH('{start_date}'), '-01'),
                             '%Y-%m-%d') AND '{end_date}'
GROUP BY dt,
         platform_id,
         platform_name,
         anchor_no
;



-- 主播月收入
-- 汇总维度 月-主播
-- 汇总指标 主播数，开播主播数，主播虚拟币收入,主播佣金收入，公会佣金收入
-- DROP TABLE IF EXISTS warehouse.dw_yy_month_anchor_live;
-- CREATE TABLE warehouse.dw_yy_month_anchor_live AS
DELETE
FROM warehouse.dw_yy_month_anchor_live
WHERE dt BETWEEN DATE_FORMAT(CONCAT(YEAR('{start_date}'), '-', MONTH('{start_date}'), '-01'),
                             '%Y-%m-%d') AND '{end_date}';
INSERT INTO warehouse.dw_yy_month_anchor_live
SELECT ad.dt,
       ad.platform_id,
       ad.platform_name,
       ad.anchor_no,
       ad.anchor_uid,
       ad.backend_account_id,
       channel_num,
       ad.live_days,
       ad.duration,
       ad.anchor_bluediamond,
       gv.anchor_bluediamond AS anchor_bluediamond_true,
       gv.guild_bluediamond  AS guild_bluediamond_true,
       ad.anchor_commission,
       ad.guild_commission,
       gc.guild_commission   AS guild_commission_true,
       ad.dt_cnt
FROM (
         SELECT DATE_FORMAT(CONCAT(YEAR(t.dt), '-', MONTH(t.dt), '-01'), '%Y-%m-%d')        AS dt,
                t.platform_id,
                t.platform_name,
                t.anchor_no,
                t.anchor_uid,
                t.backend_account_id,
                max(channel_num)                                                            as channel_num,
                COUNT(DISTINCT CASE WHEN t.live_status = 1 THEN t.dt ELSE NULL END)         AS live_days,
                SUM(CASE WHEN duration >= 0 THEN duration ELSE 0 END)                       AS duration,
                SUM(CASE WHEN t.bluediamond >= 0 THEN t.bluediamond ELSE 0 END)             AS anchor_bluediamond,
                SUM(CASE WHEN t.anchor_commission >= 0 THEN t.anchor_commission ELSE 0 END) AS anchor_commission,
                SUM(CASE WHEN t.guild_commission >= 0 THEN t.guild_commission ELSE 0 END)   AS guild_commission,
                COUNT(DISTINCT t.dt)                                                        AS dt_cnt
         FROM stage.stage_yy_day_anchor_live t
         WHERE dt BETWEEN DATE_FORMAT(CONCAT(YEAR('{start_date}'), '-', MONTH('{start_date}'), '-01'),
                                      '%Y-%m-%d') AND '{end_date}'
         GROUP BY DATE_FORMAT(CONCAT(YEAR(t.dt), '-', MONTH(t.dt), '-01'), '%Y-%m-%d'),
                  t.platform_id,
                  t.platform_name,
                  t.anchor_no,
                  t.anchor_uid,
                  t.backend_account_id) ad
         LEFT JOIN warehouse.dw_yy_month_anchor_live_bluediamond gv ON ad.dt = gv.dt AND ad.anchor_no = gv.anchor_no
         LEFT JOIN warehouse.dw_yy_month_anchor_live_commission gc ON ad.dt = gc.dt AND ad.anchor_no = gc.anchor_no
;

