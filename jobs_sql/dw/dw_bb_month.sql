-- 汇总维度 月-公会—主播
-- 汇总指标 开播天数，开播时长，虚拟币收入
# DROP TABLE IF EXISTS stage.stage_bb_month_anchor_info;
# CREATE TABLE stage.stage_bb_month_anchor_info AS
# DELETE
# FROM stage.stage_bb_month_anchor_info
# WHERE DATE_FORMAT(dt, '%Y-%m') BETWEEN DATE_FORMAT('2018-01-01', '%Y-%m') AND DATE_FORMAT('2020-01-20', '%Y-%m');
# INSERT INTO stage.stage_bb_month_anchor_info
# SELECT CONCAT(DATE_FORMAT(al.dt, '%Y-%m'), '-01') AS dt,
#        al.platform_id,
#        al.platform_name,
#        al.backend_account_id,
#        al.anchor_no,
#        al.contract_status,
#        al.contract_status_text,
#        MAX(dt)                                    AS max_dt
# FROM warehouse.ods_bb_day_anchor_live al
# WHERE DATE_FORMAT(al.dt, '%Y-%m') BETWEEN DATE_FORMAT('2018-01-01', '%Y-%m') AND DATE_FORMAT('2020-01-20', '%Y-%m')
# GROUP BY CONCAT(DATE_FORMAT(al.dt, '%Y-%m'), '-01'),
#          al.platform_id,
#          al.platform_name,
#          al.backend_account_id,
#          al.anchor_no,
#          contract_status,
#          contract_status_text
# ;
#
#
# # DROP TABLE IF EXISTS warehouse.dw_bb_month_anchor_info;
# # CREATE TABLE warehouse.dw_bb_month_anchor_info AS
# DELETE
# FROM warehouse.dw_bb_month_anchor_info
# WHERE DATE_FORMAT(dt, '%Y-%m') BETWEEN DATE_FORMAT('2018-01-01', '%Y-%m') AND DATE_FORMAT('2020-01-20', '%Y-%m');
# INSERT INTO warehouse.dw_bb_month_anchor_info
# SELECT al1.dt,
#        al1.platform_id,
#        al1.platform_name,
#        al1.backend_account_id,
#        al1.anchor_no,
#        al2.anchor_uid,
#        al2.anchor_nick_name,
#        al2.anchor_status,
#        al2.anchor_status_text,
#        al2.contract_status,
#        al2.contract_status_text,
#        al2.contract_signtime,
#        al2.contract_endtime
# FROM stage.stage_bb_month_anchor_info al1
#          LEFT JOIN warehouse.ods_bb_day_anchor_live al2
#                    ON al1.max_dt = al2.dt AND al1.backend_account_id = al2.backend_account_id AND
#                       al1.anchor_no = al2.anchor_no
# WHERE DATE_FORMAT(al1.dt, '%Y-%m') BETWEEN DATE_FORMAT('2018-01-01', '%Y-%m') AND DATE_FORMAT('2020-01-20', '%Y-%m')
# ;


-- DROP TABLE IF EXISTS warehouse.dw_month_bb_anchor_live;
-- CREATE TABLE warehouse.dw_month_bb_anchor_live AS
DELETE
FROM warehouse.dw_bb_month_anchor_live
WHERE DATE_FORMAT(dt, '%Y%m') BETWEEN DATE_FORMAT('{start_date}', '%Y%m') AND DATE_FORMAT('{end_date}', '%Y%m');
INSERT INTO warehouse.dw_bb_month_anchor_live
SELECT DATE_FORMAT(CONCAT(YEAR(t.dt), '-', MONTH(t.dt), '-01'), '%Y-%m-%d') AS dt,
       t.platform_id,
       t.platform_name,
       t.backend_account_id,
       t.anchor_no,
       COUNT(CASE WHEN t.live_status = 1 THEN t.dt ELSE NULL END)           AS live_days,
       SUM(t.duration)                                                      AS duration,
       SUM(t.anchor_total_coin)                                             AS anchor_virtual_coin
FROM warehouse.ods_bb_day_anchor_live t
WHERE (contract_status <> 2 OR contract_status IS NULL)
  AND DATE_FORMAT(dt, '%Y%m') BETWEEN DATE_FORMAT('{start_date}', '%Y%m') AND DATE_FORMAT('{end_date}', '%Y%m')
GROUP BY DATE_FORMAT(CONCAT(YEAR(t.dt), '-', MONTH(t.dt), '-01'), '%Y-%m-%d'),
         t.platform_id,
         t.platform_name,
         t.backend_account_id,
         t.anchor_no
;


-- 汇总维度 月-公会
-- 汇总指标 主播数，开播主播数，虚拟币收入
# DROP TABLE IF EXISTS warehouse.dw_bb_month_guild_live_true;
# CREATE TABLE warehouse.dw_bb_month_guild_live_true AS
DELETE
FROM warehouse.dw_bb_month_guild_live_true
WHERE dt BETWEEN DATE_FORMAT('{start_date}', '%Y-%m-01') AND '{end_date}';
INSERT INTO warehouse.dw_bb_month_guild_live_true
SELECT dt,
       backend_account_id,
       guild_salart_rmb   AS guild_salart_rmb_true,
       guild_virtual_coin AS guild_virtual_coin_true,
       type,
       anchor_income,
       anchor_base_coin,
       guild_award_coin,
       operate_award_punish_coin,
       special_coin,
       guild_change_coin,
       anchor_change_coin,
       comment
FROM warehouse.ods_bb_month_guild_live
WHERE type = '公会总收益'
  AND dt BETWEEN DATE_FORMAT('{start_date}', '%Y-%m-01') AND '{end_date}'
;


-- 注意backend_account_id = 3  数据中没有主播收入
-- DROP TABLE IF EXISTS warehouse.dw_bb_month_guild_live;
-- CREATE TABLE warehouse.dw_bb_month_guild_live AS
DELETE
FROM warehouse.dw_bb_month_guild_live
WHERE DATE_FORMAT(dt, '%Y-%m-01') BETWEEN DATE_FORMAT('{start_date}', '%Y-%m-01') AND DATE_FORMAT('{end_date}', '%Y-%m-01');
INSERT INTO warehouse.dw_bb_month_guild_live
SELECT DATE_FORMAT(al.dt, '%Y-%m-01')                                               AS dt,
       al.platform_id,
       al.platform_name,
       al.backend_account_id,
       al.month_newold_state,
       al.active_state,
       al.revenue_level,
       COUNT(DISTINCT al.anchor_no)                                                 AS anchor_cnt,
       COUNT(DISTINCT CASE WHEN al.live_status = 1 THEN al.anchor_no ELSE NULL END) AS anchor_live_cnt,
       SUM(al.duration)                                                             AS duration,
       SUM(al.anchor_total_coin)                                                    AS revenue,
       SUM(al.anchor_income)                                                        AS anchor_income,
       SUM(al.send_coin)                                                            AS operate_award_punish_coin,
       SUM(al.anchor_base_coin)                                                     AS anchor_base_coin,
       SUM(al.special_coin)                                                         AS special_coin
FROM (SELECT *,
             warehouse.ANCHOR_NEW_OLD(min_live_dt, min_sign_dt, CASE
                                                                    WHEN dt < DATE_FORMAT('{end_date}', '%Y-%m-01')
                                                                        THEN LAST_DAY(dt)
                                                                    ELSE '{end_date}' END, 180) AS month_newold_state
      FROM warehouse.dw_bb_day_anchor_live
      WHERE (contract_status <> 2 OR contract_status IS NULL)
        AND DATE_FORMAT(dt, '%Y-%m-01') BETWEEN DATE_FORMAT('{start_date}', '%Y-%m-01') AND '{end_date}'
     ) al
GROUP BY DATE_FORMAT(dt, '%Y-%m-01'),
         al.platform_id,
         al.platform_name,
         al.backend_account_id,
         al.month_newold_state,
         al.active_state,
         al.revenue_level
;
