-- CREATE DATABASE tmp DEFAULT CHARACTER SET utf8mb4;

-- 按平台汇总两天(t-1, t-2)主播直播汇总
-- explain
DROP TABLE IF EXISTS tmp.rs_an_live_daily_cnt_tmp0;
CREATE TABLE tmp.rs_an_live_daily_cnt_tmp0 AS 
SELECT CURRENT_DATE() AS rpt_date,
       anday.platform_id,
       COUNT(DISTINCT CASE WHEN anday.dt = DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY) THEN anday.id ELSE NULL END) AS an_cnt_t2,
       COUNT(DISTINCT CASE WHEN anday.dt = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY) THEN anday.id ELSE NULL END) AS an_cnt_t1,
       COUNT(DISTINCT CASE WHEN anday.live_status = 1 AND anday.dt = DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY) THEN anday.id ELSE NULL END) AS live_an_cnt_t2,
       COUNT(DISTINCT CASE WHEN anday.live_status = 1 AND anday.dt = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY) THEN anday.id ELSE NULL END) AS live_an_cnt_t1,
       COUNT(DISTINCT CASE WHEN anday.amt_level = 50 THEN anday.id ELSE NULL END) AS an50_cnt_t1,
       COUNT(DISTINCT CASE WHEN anday.amt_level = 50 AND anday.dt = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY) AND anday.live_status = 1 THEN anday.id ELSE NULL END) AS an50l_cnt_t1,
       SUM(CASE WHEN anday.dt = DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY) THEN lday.virtual_coin / 1000 + lday.commission ELSE 0 END) AS amt_t2,
       SUM(CASE WHEN anday.dt = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY) THEN lday.virtual_coin / 1000 + lday.commission ELSE 0 END) AS amt_t1
FROM warehouse.an_anchor_day anday
LEFT JOIN warehouse.an_live_day lday 
       ON anday.dt = lday.dt
	  AND anday.platform_id = lday.platform_id
	  AND anday.id = anchor_id
WHERE anday.dt BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY) AND DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
  AND lday.dt BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY) AND DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
GROUP BY anday.platform_id
;

DROP TABLE IF EXISTS tmp.rs_an_live_daily_cnt_20191223_tmp1;
CREATE TABLE tmp.rs_an_live_daily_cnt_tmp1 AS 
SELECT t0.rpt_date,
       pf.platform_name,
       t0.an_cnt_t1,
       ROUND((t0.an_cnt_t1 - t0.an_cnt_t2) / t0.an_cnt_t2, 5) AS an_dond,
       ROUND(t0.live_an_cnt_t1 / t0.an_cnt_t1, 5) AS d_anl_rate_t1,
       ROUND((t0.live_an_cnt_t1 - live_an_cnt_t2) / live_an_cnt_t2, 5) AS d_anl_dond,
       ROUND(an50l_cnt_t1 / an50_cnt_t1, 5) AS d_an50l_rate_t1,
	   t0.amt_t1,
       ROUND((t0.amt_t1 - t0.amt_t2) / t0.amt_t1) AS amt_dond
FROM tmp.rs_an_live_daily_cnt_20191223_tmp0 t0
LEFT JOIN warehouse.platform pf ON t0.platform_id = pf.id
;

-- ----------------------------------------------------------------------------------------------------------
-- 汇总前日所在月各级别流水
SELECT anl.platform_id,
       SUM(CASE WHEN pf.include_pf_amt = 0 THEN (anl.virtual_coin / pf.vir_coin_rate + anl.commission) / pf.pf_amt_rate ELSE anl.virtual_coin / pf.vir_coin_rate + anl.commission END) AS an50_amt_m,
	   SUM(CASE WHEN info.amt_level = 50 THEN 
										      CASE WHEN pf.include_pf_amt = 0 THEN (anl.virtual_coin / pf.vir_coin_rate + anl.commission) / pf.pf_amt_rate
											       WHEN pf.include_pf_amt = 1 THEN anl.virtual_coin / pf.vir_coin_rate + anl.commission ELSE 0 END
                                         ELSE 0 END) AS an50_amt_m,
	   SUM(CASE WHEN info.amt_level = 10 THEN 
										      CASE WHEN pf.include_pf_amt = 0 THEN (anl.virtual_coin / pf.vir_coin_rate + anl.commission) / pf.pf_amt_rate
											       WHEN pf.include_pf_amt = 1 THEN anl.virtual_coin / pf.vir_coin_rate + anl.commission ELSE 0 END
                                         ELSE 0 END) AS an10_amt_m,
	   SUM(CASE WHEN info.amt_level = 3 THEN 
										      CASE WHEN pf.include_pf_amt = 0 THEN (anl.virtual_coin / pf.vir_coin_rate + anl.commission) / pf.pf_amt_rate
											       WHEN pf.include_pf_amt = 1 THEN anl.virtual_coin / pf.vir_coin_rate + anl.commission ELSE 0 END
                                         ELSE 0 END) AS an3_amt_m
FROM warehouse.an_live_day anl
LEFT JOIN warehouse.an_anchor_info info
ON anl.anchor_id = info.id AND anl.platform_id = info.platform_id
LEFT JOIN warehouse.platform pf
ON info.platform_id = pf.id
WHERE MONTH(anl.dt) = MONTH(DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)) AND anl.dt <= DATE_SUB(CURRENT_DATE, INTERVAL 1 DAY)
GROUP BY anl.platform_id
;


SELECT * FROM warehouse.platform;

-- 月汇总   注意 环比更换成同比
DROP TABLE IF EXISTS tmp.rs_an_live_mon_cnt_tmp0;
CREATE TABLE tmp.rs_an_live_mon_cnt_tmp0 AS
SELECT CURRENT_DATE() AS rpt_date,
	   anday.platform_id,
       -- 上月50主播数
       COUNT(DISTINCT CASE WHEN anday.amt_level = 50 AND anday.dt = LAST_DAY(DATE_SUB(DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY), INTERVAL 1 MONTH)) THEN anday.id ELSE NULL END) AS an50_cnt_m1,
       -- 昨日50主播数
       COUNT(DISTINCT CASE WHEN anday.amt_level = 50 AND anday.dt = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY) THEN anday.id ELSE NULL END) AS an50_cnt_m,
       -- 昨日10主播数
       COUNT(DISTINCT CASE WHEN anday.amt_level = 10 AND anday.dt = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY) THEN anday.id ELSE NULL END) AS an50_cnt_m,
       -- 昨日3主播数
       COUNT(DISTINCT CASE WHEN anday.amt_level = 3 AND anday.dt = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY) THEN anday.id ELSE NULL END) AS an50_cnt_m,
       
       -- 截止前日主播数
       COUNT(DISTINCT CASE WHEN anday.dt = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY) THEN anday.id ELSE NULL END) AS an_cnt_m,
       
       -- 前日所在月份首日截止前日开播主播数
       COUNT(DISTINCT CASE WHEN anday.live_status = 1 AND MONTH(anday.dt) = MONTH(DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)) AND anday.dt <= DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY) THEN anday.id ELSE NULL END) AS anl_cnt_m,
       -- 前日对应上月首日截止前日对应上月同日开播主播数
       COUNT(DISTINCT CASE WHEN anday.live_status = 1 AND anday.dt <= DATE_SUB(DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY), INTERVAL 1 MONTH) THEN anday.id ELSE NULL END) AS anl_cnt_m1,
       
       -- 前日所在月份首日截止前日新主播数
       COUNT(DISTINCT CASE WHEN anday.n_o_status = 1 AND MONTH(anday.dt) = MONTH(DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)) AND anday.dt <= DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY) THEN anday.id ELSE NULL END) AS ann_cnt_m,
       -- 前日所在月份首日截止前日新主播开播数
       COUNT(DISTINCT CASE WHEN anday.live_status = 1 AND anday.n_o_status = 1 AND MONTH(anday.dt) = MONTH(DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)) AND anday.dt <= DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY) THEN anday.id ELSE NULL END) AS annl_cnt_m,
       -- 前日对应上月首日截止前日对应上月同日新主播开播数
       COUNT(DISTINCT CASE WHEN anday.live_status = 1 AND anday.n_o_status = 1 AND anday.dt <= DATE_SUB(DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY), INTERVAL 1 MONTH) THEN anday.id ELSE NULL END) AS annl_cnt_m1,
       
       -- 前日所在月份首日截止前日老主播数
       COUNT(DISTINCT CASE WHEN anday.n_o_status = 2 AND MONTH(anday.dt) = MONTH(DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)) AND anday.dt <= DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY) THEN anday.id ELSE NULL END) AS ano_cnt_m,
       -- 前日所在月份首日截止前日老主播开播数
       COUNT(DISTINCT CASE WHEN anday.live_status = 1 AND anday.n_o_status = 2 AND MONTH(anday.dt) = MONTH(DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)) AND anday.dt <= DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY) THEN anday.id ELSE NULL END) AS anol_cnt_m,
       -- 前日对应上月首日截止前日对应上月同日老主播开播数
       COUNT(DISTINCT CASE WHEN anday.live_status = 1 AND anday.n_o_status = 2 AND anday.dt <= DATE_SUB(DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY), INTERVAL 1 MONTH) THEN anday.id ELSE NULL END) AS anol_cnt_m1,
       
       -- 前日所在月份首日截止前日活跃主播数
       COUNT(DISTINCT CASE WHEN anday.active_status = 1 AND MONTH(anday.dt) = MONTH(DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)) AND anday.dt <= DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY) THEN anday.id ELSE NULL END) AS ana_cnt_m,
       -- 前日对应上月首日截止前日对应上月同日活跃主播数
       COUNT(DISTINCT CASE WHEN anday.active_status = 1 AND anday.dt <= DATE_SUB(DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY), INTERVAL 1 MONTH) THEN anday.id ELSE NULL END) AS ana_cnt_m1,
       -- 前日所在月份首日截止前日活跃新主播数
       COUNT(DISTINCT CASE WHEN anday.active_status = 1 AND anday.n_o_status = 1 AND MONTH(anday.dt) = MONTH(DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)) AND anday.dt <= DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY) THEN anday.id ELSE NULL END) AS anan_cnt_m,
       -- 前日所在月份首日截止前日活跃老主播数
       COUNT(DISTINCT CASE WHEN anday.active_status = 1 AND anday.n_o_status = 2 AND MONTH(anday.dt) = MONTH(DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)) AND anday.dt <= DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY) THEN anday.id ELSE NULL END) AS anao_cnt_m,
       -- 前日对应上月首日截止前日对应上月同日活跃新主播数
       COUNT(DISTINCT CASE WHEN anday.active_status = 1 AND anday.n_o_status = 1 AND anday.dt <= DATE_SUB(DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY), INTERVAL 1 MONTH) THEN anday.id ELSE NULL END) AS anan_cnt_m1,
       -- 前日对应上月首日截止前日对应上月同日活跃老主播数
       COUNT(DISTINCT CASE WHEN anday.active_status = 1 AND anday.n_o_status = 2 AND anday.dt <= DATE_SUB(DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY), INTERVAL 1 MONTH) THEN anday.id ELSE NULL END) AS anao_cnt_m1
FROM warehouse.an_anchor_day anday
WHERE anday.dt BETWEEN DATE_ADD(LAST_DAY(DATE_SUB(DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY), INTERVAL 1 MONTH)), INTERVAL 1 DAY) AND DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
GROUP BY CURRENT_DATE(),
         anday.platform_id
;

-- 拆分rs_an_live_mon_cnt_tmp0
-- 同比计算待续




























SELECT DATE_ADD(LAST_DAY(DATE_SUB(DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY), INTERVAL 2 MONTH)), INTERVAL 1 DAY);









select LAST_DAY(DATE_SUB(DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY), INTERVAL 1 MONTH))
;

SELECT DATE_SUB('2020-03-31', INTERVAL 1 MONTH);

SELECT CURDATE()
















































































