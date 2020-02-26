# DROP TABLE IF EXISTS warehouse.dw_now_day_anchor_live;
# CREATE TABLE warehouse.dw_now_day_anchor_live AS
DELETE
FROM warehouse.dw_now_day_anchor_live
WHERE dt BETWEEN '{start_date}' AND '{end_date}';
INSERT INTO warehouse.dw_now_day_anchor_live
SELECT al.*,
       IFNULL(at.city, '未知')                                                  AS city,
       aml.min_live_dt,
       ams.min_sign_dt,
       -- 通过判断主播最小注册时间和最小开播时间，取两者之间最小的时间作为判断新老主播条件，两者为NULL则为‘未知’
       warehouse.ANCHOR_NEW_OLD(aml.min_live_dt, ams.min_sign_dt, al.dt, 180) AS newold_state,
       mal.duration                                                           AS last_month_duration,
       mal.live_days,
       -- 开播天数大于等于20天且开播时长大于等于60小时（t-1月累计）
       CASE
           WHEN mal.live_days >= 20 AND mal.duration >= 60 * 60 * 60 THEN '活跃主播'
           ELSE '非活跃主播' END                                                   AS active_state,
       mal.revenue                                                            AS last_month_revenue,
       -- 主播流水分级（t-1月）
       CASE
           WHEN mal.revenue / 10000 >= 50 THEN '50+'
           WHEN mal.revenue / 10000 >= 10 THEN '10-50'
           WHEN mal.revenue / 10000 >= 3 THEN '3-10'
           WHEN mal.revenue / 10000 > 0 THEN '0-3'
           ELSE '0' END                                                       AS revenue_level
FROM warehouse.ods_now_day_anchor_live al
         LEFT JOIN stage.stage_now_anchor_min_live_dt aml ON al.anchor_no = aml.anchor_no
         LEFT JOIN stage.stage_now_anchor_min_sign_dt ams ON al.anchor_no = ams.anchor_no
         LEFT JOIN stage.stage_now_month_anchor_live mal
                   ON mal.dt = DATE_FORMAT(DATE_SUB(al.dt, INTERVAL 1 MONTH), '%Y-%m-01') AND
                      al.anchor_no = mal.anchor_no
         LEFT JOIN warehouse.ods_yj_anchor_team at ON al.anchor_no = at.anchor_no
WHERE (aml.min_live_dt <= al.dt OR al.contract_sign_time <= al.dt)
  AND al.dt BETWEEN '{start_date}' AND '{end_date}'
;


-- 汇总维度 日-公会
-- 汇总指标 开播天数，开播时长，主播流水，公会流水，公会收入
-- DROP TABLE IF EXISTS warehouse.dw_now_day_guild_live;
-- CREATE TABLE warehouse.dw_now_day_guild_live AS
DELETE
FROM warehouse.dw_now_day_guild_live
WHERE dt BETWEEN '{start_date}' AND '{end_date}';
INSERT INTO warehouse.dw_now_day_guild_live
SELECT al.dt,
       al.platform_id,
       al.platform_name,
       al.backend_account_id,
       al.active_state,
       al.newold_state,
       al.revenue_level,
       al.city,
       al.anchor_cnt,
       al.anchor_live_cnt,
       ac.anchor_live_cnt  AS anchor_live_cnt_true,
       al.duration,
       al.revenue_rmb,
       ac.revenue_rmb      AS revenue_rmb_true,
       ac.guild_income_rmb AS guild_income_rmb_true
FROM (SELECT t.dt,
             t.platform_id,
             t.platform_name,
             t.backend_account_id,
             t.city,
             t.active_state,
             t.newold_state,
             t.revenue_level,
             COUNT(t.anchor_no)                                                         AS anchor_cnt,
             COUNT(DISTINCT CASE WHEN t.live_status = 1 THEN t.anchor_no ELSE NULL END) AS anchor_live_cnt,
             SUM(t.duration)                                                            AS duration,
             SUM(t.revenue_rmb)                                                         AS revenue_rmb
      FROM warehouse.dw_now_day_anchor_live t
      WHERE t.dt BETWEEN '{start_date}' AND '{end_date}'
      GROUP BY t.dt,
               t.platform_id,
               t.platform_name,
               t.backend_account_id,
               t.city,
               t.active_state,
               t.newold_state,
               t.revenue_level) al
         LEFT JOIN warehouse.ods_now_day_guild_live ac
                   ON al.dt = ac.dt AND al.backend_account_id = ac.backend_account_id
;