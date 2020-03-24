-- 主播最早开播时间（基于现有数据）
-- DROP TABLE IF EXISTS stage.stage_fx_anchor_min_live_dt;
-- CREATE TABLE stage.stage_fx_anchor_min_live_dt
INSERT IGNORE INTO stage.stage_fx_anchor_min_live_dt
SELECT 1004 AS platform_id,
       t.anchor_no,
       MIN(t.min_live_dt)
FROM (SELECT al.anchor_no,
             MIN(dt) AS min_live_dt
      FROM warehouse.ods_fx_day_anchor_live al
      WHERE al.live_status = 1
      GROUP BY al.anchor_no
      UNION
      SELECT yj.uid             AS anchor_no,
             yj.first_live_time AS min_live_time
      FROM warehouse.ods_yujia_anchor_list yj
      WHERE platform = '繁星'
        AND first_live_time != '1970-01-01') t
GROUP BY t.anchor_no
;


-- 主播最早签约时间（基于现有数据）,后期结合公司现有主播的签约时间
-- DROP TABLE IF EXISTS stage.stage_fx_anchor_min_sign_dt;
-- CREATE TABLE stage.stage_fx_anchor_min_sign_dt
INSERT IGNORE INTO stage.stage_fx_anchor_min_sign_dt
SELECT 1001             AS platform_id,
       t.anchor_no,
       MIN(min_sign_dt) AS min_sign_dt
FROM (SELECT al.anchor_no,
             MIN(DATE(sign_time)) AS min_sign_dt
      FROM warehouse.ods_fx_day_anchor_live al
      WHERE al.sign_time IS NOT NULL
      GROUP BY al.anchor_no
      UNION
      SELECT yj.uid       AS anchor_no,
             yj.sign_time AS min_sign_dt
      FROM warehouse.ods_yujia_anchor_list yj
      WHERE platform = '繁星'
        AND yj.sign_time <> '1970-01-01'
     ) t
GROUP BY t.anchor_no
;


-- 计算每月主播开播天数，开播时长，流水
-- DROP TABLE IF EXISTS stage.stage_fx_month_anchor_live;
-- CREATE TABLE stage.stage_fx_month_anchor_live
DELETE
FROM stage.stage_fx_month_anchor_live
WHERE dt = '{month}';
INSERT INTO stage.stage_fx_month_anchor_live
SELECT t.dt,
       t.platform_id,
       t.anchor_no,
       t.revenue,
       CASE
           WHEN t.revenue / 125 / 10000 >= 50 THEN '50+'
           WHEN t.revenue / 125 / 10000 >= 10 THEN '10-50'
           WHEN t.revenue / 125 / 10000 >= 3 THEN '3-10'
           WHEN t.revenue / 125 / 10000 > 0 THEN '0-3'
           ELSE '0' END     AS revenue_level,
       t.live_days,
       t.duration,
       CASE
           WHEN t.live_days >= 20 AND t.duration >= 60 * 60 * 60 THEN '活跃主播'
           ELSE '非活跃主播' END AS active_state
FROM (
         SELECT DATE_FORMAT(al.dt, '%Y-%m-01')                                     AS dt,
                al.platform_id,
                al.anchor_no,
                SUM(anchor_income / 0.4)                                           AS revenue,
                COUNT(DISTINCT CASE WHEN al.live_status = 1 THEN dt ELSE NULL END) AS live_days,
                SUM(al.duration)                                                   AS duration
         FROM warehouse.ods_fx_day_anchor_live al
         WHERE dt >= '{month}'
           AND dt <= LAST_DAY('{month}')
         GROUP BY DATE_FORMAT(al.dt, '%Y-%m-01'),
                  al.platform_id,
                  al.anchor_no) t
;


-- dw_fx_day_anchor_live
-- DROP TABLE IF EXISTS warehouse.dw_fx_day_anchor_live;
-- CREATE TABLE warehouse.dw_fx_day_anchor_live AS
DELETE
FROM warehouse.dw_fx_day_anchor_live
WHERE dt >= '{month}'
  AND dt <= LAST_DAY('{month}');
INSERT INTO warehouse.dw_fx_day_anchor_live
SELECT al.dt,
       al.platform_id,
       al.platform_name,
       al.backend_account_id,
       al.anchor_no,
       al.anchor_nick_name,
       al.gender,
       al.level,
       al.star_level,
       al.group_name,
       al.contract_sign_type,
       al.sign_time,
       al.latest_time,
       al.live_status,
       al.total_live_time,
       al.duration,
       al.pc_live_time,
       al.pc_duration,
       al.mobile_live_time,
       al.mob_duration,
       al.voice_live_time,
       al.voi_duration,
       al.game_live_time,
       al.game_duration,
       al.dual_cam_live_time,
       al.dual_duration,
       al.anchor_income / 0.4 / 125                                           AS revenue,
       al.anchor_income / 0.4                                                 AS revenue_orig,
       al.anchor_income,
       al.anchor_income / 0.4 * 0.09                                          AS guild_income,
       al.bean_to_coin_num,
       al.bean_to_rmb,
       al.clan_share_bean_num,
       al.avatar_url,
       aml.min_live_dt,
       ams.min_sign_dt,
       -- 通过判断主播最小注册时间和最小开播时间，取两者之间最小的时间作为判断新老主播条件，两者为NULL则为‘未知’
       warehouse.ANCHOR_NEW_OLD(aml.min_live_dt, ams.min_sign_dt, al.dt, 180) AS newold_state,
       mal.duration                                                           AS month_duration,
       mal.live_days,
       -- 开播天数大于等于20天且开播时长大于等于60小时（t-1月累计）
       mal.active_state,
       mal.revenue                                                            AS month_revenue,
       -- 主播流水分级（t-1月）
       mal.revenue_level
FROM warehouse.ods_fx_day_anchor_live al
         LEFT JOIN stage.stage_fx_anchor_min_live_dt aml ON al.anchor_no = aml.anchor_no
         LEFT JOIN stage.stage_fx_anchor_min_sign_dt ams ON al.anchor_no = ams.anchor_no
         LEFT JOIN stage.stage_fx_month_anchor_live mal
                   ON mal.dt = DATE_FORMAT(al.dt, '%Y-%m-01') AND
                      al.anchor_no = mal.anchor_no
WHERE al.dt >= '{month}'
  AND al.dt <= LAST_DAY('{month}')
  AND mal.dt = '{month}'
;


-- UPDATE
--     warehouse.dw_fx_day_anchor_live al, stage.stage_fx_month_anchor_live mal
-- SET al.active_state    = mal.active_state,
--     al.month_duration  = mal.duration,
--     al.month_live_days = mal.live_days,
--     al.revenue_level   = mal.revenue_level,
--     al.month_revenue   = mal.revenue
-- WHERE al.anchor_no = mal.anchor_no
--   AND al.dt >= mal.dt
--   AND al.dt < mal.dt + INTERVAL 1 MONTH
--   AND mal.dt BETWEEN DATE_FORMAT('{start_date}', '%Y-%m-01') AND '{end_date}'
--   AND al.dt BETWEEN DATE_FORMAT('{start_date}', '%Y-%m-01') AND '{end_date}'
-- --   AND '{end_date}' = LAST_DAY('{end_date}')
-- ;


-- DROP TABLE IF EXISTS warehouse.dw_fx_day_guild_live;
-- CREATE TABLE warehouse.dw_fx_day_guild_live AS
DELETE
FROM warehouse.dw_fx_day_guild_live
WHERE dt >= '{month}'
  AND dt <= LAST_DAY('{month}');
INSERT INTO warehouse.dw_fx_day_guild_live
SELECT al.dt,
       al.platform_id,
       al.platform_name,
       al.backend_account_id,
       al.newold_state,
       al.active_state,
       al.revenue_level,
       COUNT(DISTINCT al.anchor_no)                                                 AS anchor_cnt,
       COUNT(DISTINCT CASE WHEN al.live_status = 1 THEN al.anchor_no ELSE NULL END) AS live_cnt,
       SUM(al.duration)                                                             AS duration,
       SUM(al.revenue)                                                              AS revenue,
       SUM(al.revenue_orig)                                                         AS revenue_orig,
       SUM(al.anchor_income)                                                        AS anchor_income,
       SUM(al.guild_income)                                                         AS guild_income
FROM warehouse.dw_fx_day_anchor_live al
WHERE al.dt >= '{month}'
  AND al.dt <= LAST_DAY('{month}')
GROUP BY al.dt,
         al.platform_id,
         al.platform_name,
         al.backend_account_id,
         al.newold_state,
         al.active_state,
         al.revenue_level
;