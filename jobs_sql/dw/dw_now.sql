-- 汇总维度 日-公会
-- 汇总指标 开播天数，开播时长，主播流水，公会流水，公会收入
-- DROP TABLE IF EXISTS warehouse.dw_day_now_guild_commission;
-- CREATE TABLE warehouse.dw_day_now_guild_commission AS
DELETE
FROM warehouse.dw_day_now_guild_commission
WHERE dt BETWEEN '{start_date}' AND '{end_date}';
INSERT INTO warehouse.dw_day_now_guild_commission
SELECT al.dt,
       al.platform_id,
       al.platform_name,
       al.backend_account_id,
       anchor_cnt,
       al.anchor_live_cnt,
       ac.anchor_live_cnt      AS anchor_live_cnt_ture,
       al.duration,
       al.anchor_commission_rmb,
       ac.guild_commission_rmb AS guild_commission_rmb_true,
       ac.guild_salary_rmb     AS guild_salary_rmb_ture
FROM (SELECT t.dt,
             t.platform_id,
             t.platform_name,
             t.backend_account_id,
             COUNT(t.anchor_no)                                                         AS anchor_cnt,
             COUNT(DISTINCT CASE WHEN t.live_status = 1 THEN t.anchor_no ELSE NULL END) AS anchor_live_cnt,
             ROUND(SUM(t.duration), 2)                                                  AS duration,
             ROUND(SUM(t.anchor_commission_rmb), 2)                                     AS anchor_commission_rmb
      FROM warehouse.ods_day_now_anchor_live_detail t
      WHERE t.dt BETWEEN '{start_date}' AND '{end_date}'
      GROUP BY t.dt,
               t.platform_id,
               t.platform_name,
               t.backend_account_id) al
         LEFT JOIN warehouse.ods_day_now_guild_commission ac
                   ON al.dt = ac.dt AND al.backend_account_id = ac.backend_account_id
;


-- 汇总维度 月-公会
-- 汇总指标 公会主播数 公会开播主播数 公会主播收入
-- DROP TABLE IF EXISTS warehouse.dw_month_now_guild_commission;
-- CREATE TABLE warehouse.dw_month_now_guild_commission AS
DELETE
FROM warehouse.dw_month_now_guild_commission
WHERE dt BETWEEN CONCAT(YEAR('{start_date}'), '-', MONTH('{start_date}'), '-01') AND '{end_date}';
INSERT INTO warehouse.dw_month_now_guild_commission
SELECT al.dt,
       al.platform_id,
       al.platform_name,
       al.backend_account_id,
       al.anchor_cnt,
       al.anchor_live_cnt,
       al.anchor_commission_rmb,
       gc.anchor_cnt           AS anchor_cnt_ture,
       gc.guild_commission_rmb AS guild_commission_rmb_ture,
       gc.anchor_live_rate,
       gc.average_anchor_commission_rmb
FROM (
         SELECT DATE_FORMAT(CONCAT(YEAR(t.dt), '-', MONTH(t.dt), '-01'), '%Y-%m-%d')       AS dt,
                t.platform_id,
                t.platform_name,
                t.backend_account_id,
                COUNT(DISTINCT t.anchor_no)                                                AS anchor_cnt,
                COUNT(DISTINCT CASE WHEN t.live_status = 1 THEN t.anchor_no ELSE NULL END) AS anchor_live_cnt,
                ROUND(SUM(t.anchor_commission_rmb), 2)                                     AS anchor_commission_rmb
         FROM warehouse.ods_day_now_anchor_live_detail t
         WHERE t.dt BETWEEN '{start_date}' AND '{end_date}'
         GROUP BY DATE_FORMAT(CONCAT(YEAR(t.dt), '-', MONTH(t.dt), '-01'), '%Y-%m-%d'),
                  t.platform_id,
                  t.platform_name,
                  t.backend_account_id) al
         LEFT JOIN warehouse.ods_month_now_guild_commission gc
                   ON al.dt = gc.dt AND al.backend_account_id = gc.backend_account_id
;


-- 汇总维度 月-公会-主播
-- 汇总指标 主播数，开播主播数，虚拟币收入,主播佣金，公会佣金
-- DROP TABLE IF EXISTS warehouse.dw_month_now_guild_anchor_commission;
-- CREATE TABLE warehouse.dw_month_now_guild_anchor_commission AS
DELETE
FROM warehouse.dw_month_now_guild_anchor_commission
WHERE dt BETWEEN CONCAT(YEAR('{start_date}'), '-', MONTH('{start_date}'), '-01') AND '{end_date}';
INSERT INTO warehouse.dw_month_now_guild_anchor_commission
SELECT DATE_FORMAT(CONCAT(YEAR(t.dt), '-', MONTH(t.dt), '-01'), '%Y-%m-%d') AS dt,
       t.platform_id,
       t.platform_name,
       t.backend_account_id,
       anchor_uid,
       anchor_no,
       COUNT(DISTINCT CASE WHEN t.live_status = 1 THEN t.dt ELSE NULL END)  AS live_days,
       ROUND(SUM(t.duration), 2)                                            AS duration,
       ROUND(SUM(t.anchor_commission_rmb), 2)                               AS anchor_commission_rmb
FROM warehouse.ods_now_anchor_live_detail t
WHERE t.dt BETWEEN '{start_date}' AND '{end_date}'
GROUP BY DATE_FORMAT(CONCAT(YEAR(t.dt), '-', MONTH(t.dt), '-01'), '%Y-%m-%d'),
         t.platform_id,
         t.platform_name,
         t.backend_account_id,
         anchor_uid,
         anchor_no
;


