-- warehouse.dw_yy_day_anchor_live_duration
-- warehouse.dw_yy_day_anchor_live_commission
-- warehouse.dw_yy_day_anchor_live_bluediamond
-- warehouse.dw_yy_day_anchor_live


-- DROP TABLE IF EXISTS warehouse.dw_yy_day_anchor_live_duration;
-- CREATE TABLE warehouse.dw_yy_day_anchor_live_duration AS
DELETE
FROM warehouse.dw_yy_day_anchor_live_duration
WHERE dt BETWEEN '{start_date}' AND '{end_date}';
INSERT INTO warehouse.dw_yy_day_anchor_live_duration
SELECT *
FROM warehouse.ods_yy_day_anchor_live_duration
WHERE dt BETWEEN '{start_date}' AND '{end_date}'
;


-- 按日汇总主播佣金收入及工会分成（佣金）
-- DROP TABLE IF EXISTS warehouse.dw_yy_day_anchor_live_commission;
-- CREATE TABLE warehouse.dw_yy_day_anchor_live_commission AS
DELETE
FROM warehouse.dw_yy_day_anchor_live_commission
WHERE dt BETWEEN '{start_date}' AND '{end_date}';
INSERT INTO warehouse.dw_yy_day_anchor_live_commission
SELECT *
FROM warehouse.ods_yy_day_anchor_live_commission
WHERE dt BETWEEN '{start_date}' AND '{end_date}'
;


-- 主播收入（蓝钻）
-- DROP TABLE IF EXISTS warehouse.dw_yy_day_anchor_live_bluediamond;
-- CREATE TABLE warehouse.dw_yy_day_anchor_live_bluediamond AS
DELETE
FROM warehouse.dw_yy_day_anchor_live_bluediamond
WHERE dt BETWEEN '{start_date}' AND '{end_date}';
INSERT INTO warehouse.dw_yy_day_anchor_live_bluediamond
SELECT *
FROM warehouse.ods_yy_day_anchor_live_bluediamond
WHERE dt BETWEEN '{start_date}' AND '{end_date}'
;


-- DROP TABLE IF EXISTS warehouse.dw_yy_day_anchor_live;
-- CREATE TABLE warehouse.dw_yy_day_anchor_live AS
DELETE
FROM warehouse.dw_yy_day_anchor_live
WHERE dt BETWEEN '{start_date}' AND '{end_date}';
INSERT INTO warehouse.dw_yy_day_anchor_live
SELECT al.*,
       aml.min_live_dt,
       ams.min_sign_dt,
       -- 通过判断主播最小注册时间和最小开播时间，取两者之间最小的时间作为判断新老主播条件，两者为NULL则为‘未知’
       CASE
           WHEN aml.min_live_dt IS NOT NULL AND ams.min_sign_dt IS NOT NULL THEN
               CASE
                   WHEN aml.min_live_dt <= ams.min_sign_dt
                       THEN CASE
                                WHEN DATEDIFF('{end_date}', aml.min_live_dt) >= 180 THEN '老主播'
                                ELSE '新主播' END
                   ELSE CASE
                            WHEN DATEDIFF('{end_date}', ams.min_sign_dt) >= 180 THEN '老主播'
                            ELSE '新主播' END
                   END
           WHEN aml.min_live_dt IS NOT NULL THEN
               CASE
                   WHEN DATEDIFF('{end_date}', aml.min_live_dt) >= 180 THEN '老主播'
                   ELSE '新主播' END
           WHEN ams.min_sign_dt IS NOT NULL THEN
               CASE
                   WHEN DATEDIFF('{end_date}', ams.min_sign_dt) >= 180 THEN '老主播'
                   ELSE '新主播' END
           ELSE '未知' END    AS newold_state,
       mal.duration AS last_month_duration,
       mal.live_days,
       -- 开播天数大于等于20天且开播时长大于等于20小时（t-1月累计）
       CASE
           WHEN mal.live_days >= 20 AND mal.duration >= 20 * 60 * 60 THEN '活跃主播'
           ELSE '非活跃主播' END AS active_state,
       mal.revenue AS last_month_revenue,
       -- 主播流水分级（t-1月）
       CASE
           WHEN mal.revenue * 2 / 1000 / 10000 >= 50 THEN '50+'
           WHEN mal.revenue * 2 / 1000 / 10000 >= 10 THEN '10-50'
           WHEN mal.revenue * 2 / 1000 / 10000 >= 3 THEN '3-10'
           WHEN mal.revenue * 2 / 1000 / 10000 > 0 THEN '0-3'
           ELSE '0' END     AS revenue_level
FROM warehouse.ods_yy_day_anchor_live al
         LEFT JOIN stage.stage_yy_anchor_min_live_dt aml ON al.anchor_uid = aml.anchor_uid
         LEFT JOIN stage.stage_yy_anchor_min_sign_dt ams ON al.anchor_uid = ams.anchor_uid
         LEFT JOIN stage.stage_yy_month_anchor_live mal
                   ON CONCAT(DATE_FORMAT(al.dt, '%Y-%m'), '-01') = DATE_ADD(mal.dt, INTERVAL 1 MONTH) AND
                      al.anchor_uid = mal.anchor_uid
WHERE al.dt BETWEEN '{start_date}' AND '{end_date}'
  AND mal.dt BETWEEN DATE_SUB(CONCAT(DATE_FORMAT('{start_date}', '%Y-%m'), '-01'), INTERVAL 1 MONTH)
    AND DATE_SUB(CONCAT(DATE_FORMAT('{end_date}', '%Y-%m'), '-01'), INTERVAL 1 MONTH)
;


-- 维度 日-公会
-- 指标 主播数、开播主播数、主播流水、
-- DROP TABLE IF EXISTS warehouse.dw_yy_day_guild_live;
-- CREATE TABLE warehouse.dw_yy_day_guild_live AS
DELETE
FROM warehouse.dw_yy_day_guild_live
WHERE dt BETWEEN '{start_date}' AND '{end_date}';
INSERT INTO warehouse.dw_yy_day_guild_live
SELECT al.dt,
       al.platform_id,
       al.backend_account_id,
       al.channel_num,
       al.comment,
       COUNT(DISTINCT al.anchor_uid)                                                 AS anchor_cnt,
       COUNT(DISTINCT CASE WHEN al.live_status = 1 THEN al.anchor_uid ELSE NULL END) AS anchor_live_cnt,
       SUM(IF(al.duration > 0, al.duration, 0))                                      AS duration,
       SUM(IF(al.bluediamond > 0, al.bluediamond, 0))                                AS bluediamond,
       SUM(IF(al.bluediamond > 0, al.bluediamond * al.anchor_settle_rate, 0))        AS anchor_income_bluediamond,
       SUM(IF(al.bluediamond > 0, al.bluediamond * (1 - al.anchor_settle_rate), 0))  AS guild_income_bluediamond,
       SUM(IF(al.anchor_commission > 0, al.anchor_commission, 0))                    AS anchor_commission,
       SUM(IF(al.guild_commission > 0, al.guild_commission, 0))                      AS guild_commission
FROM warehouse.ods_yy_day_anchor_live al
WHERE dt BETWEEN '{start_date}' AND '{end_date}'
-- where comment <> 'from guild_anchor_sign_tran'
GROUP BY al.dt,
         al.platform_id,
         al.backend_account_id,
         al.channel_num,
         comment
;
