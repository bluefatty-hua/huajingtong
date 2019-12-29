-- 一、计算主播上月直播数据
-- 时间控制  dt&m-1
-- 统计字段：live_days: 直播天数 duration: 直播时长 last_mon_amt: 主播上月总流水
-- 数据来源：(spider_yy_backend) anchor_duration

-- 直播时长转秒
DELIMITER $$
DROP FUNCTION IF EXISTS DURATION_CH$$
CREATE FUNCTION DURATION_CH(duration varchar(20))
    RETURNS INT(10)
BEGIN
    DECLARE sec INT(10) DEFAULT 0;
    SET sec = (CASE WHEN duration RLIKE '小时' THEN SUBSTRING_INDEX(duration, '小时', 1) + 0 ELSE 0 END) * 60 * 60 + (CASE
                                                                                                                      WHEN duration RLIKE '分'
                                                                                                                          THEN SUBSTRING_INDEX(SUBSTRING_INDEX(duration, '小时', -1), '分', 1) + 0
                                                                                                                      ELSE 0 END) *
                                                                                                                 60 +
              (SUBSTRING_INDEX(SUBSTRING_INDEX(duration, '分', -1), '秒', 1) + 0);
    RETURN sec;
END $$
DELIMITER ;


-- anchor_duration直播时长格式化
DROP TABLE IF EXISTS tmp.anchor_duration_time_format;
CREATE TABLE tmp.anchor_duration_time_format AS
SELECT backend_account_id,
       uid,
       dt,
       yynum,
       duration,
       DURATION_CH(duration)    AS duration_second,
       livedays,
       nick,
       chaid,
       mobduration,
       DURATION_CH(mobduration) AS mobduration_second,
       pcduration,
       DURATION_CH(pcduration)  AS pcduration_second,
       timestamp
FROM spider_yy_backend.anchor_duration
;


DROP TABLE IF EXISTS tmp.anchor_duration_time_format_sum;
CREATE TABLE tmp.anchor_duration_time_format_sum AS
SELECT ga.yynum,
       ga.uid,
       SUM(ad.livedays)                                                                                         AS live_days,
       SUM(ad.duration_second)                                                                                  AS duration_second,
       ROUND(SUM(((CASE WHEN ac.usrMoney IS NULL OR ac.usrMoney = '' THEN 0 ELSE ac.owMoney END) +
                  (CASE WHEN ac.owMoney IS NULL OR ac.owMoney = '' THEN 0 ELSE ac.owMoney END) +
                  (CASE WHEN ab.diamond IS NULL OR ab.diamond = '' THEN 0 ELSE ab.diamond END)) * 2 / 1000),
             2)                                                                                                 AS anchor_totla_amt_m1,
       MAX(CASE WHEN ad.livedays = 1 THEN ad.dt ELSE '' END)                                                    AS final_live_date
FROM spider_yy_backend.guild_anchor ga
         LEFT JOIN tmp.anchor_duration_time_format ad ON ga.yynum = ad.yynum AND ga.uid = ad.uid
         LEFT JOIN spider_yy_backend.anchor_commission ac ON ad.yynum = ac.yynum AND ad.dt = DATE(ac.dtime)
         LEFT JOIN spider_yy_backend.anchor_bluediamond ab ON ad.yynum = ab.yynum AND ab.dt = ad.dt
WHERE YEAR(ad.dt) = YEAR(DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY))
  AND MONTH(ad.dt) = MONTH(DATE_SUB(DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY), INTERVAL 1 MONTH))
GROUP BY ga.yynum,
         ga.uid
;


DROP TABLE IF EXISTS tmp.anchor_duration_time_format_t1;
CREATE TABLE tmp.anchor_duration_time_format_t1 AS
SELECT *
FROM tmp.anchor_duration_time_format ad
WHERE ad.dt = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
;


-- 构建warehouse.an_anchor_info数据
DROP TABLE IF EXISTS tmp.an_anchor_info_20191226_tmp;
CREATE TABLE tmp.an_anchor_info_20191226_tmp AS
SELECT ga.uid                                                                                             AS anchor_uid,
       ga.yynum                                                                                           AS anchor_no,
       ''                                                                                                 AS name,
       ga.nick                                                                                            AS nick_name,
       1000                                                                                               AS platform_id,
       'YY'                                                                                               AS platform_name,
       ga.backend_account_id                                                                              AS guild_id,
       ''                                                                                                 AS guild_name,
       t1.chaid                                                                                           AS channel_id,
       0                                                                                                  AS anchor_status,    -- 暂通过运营获取
       CASE WHEN t1.livedays = 1 THEN 1 ELSE 0 END                                                        AS live_status,
       1                                                                                                  AS trail_live_grade, -- 通过首播开始12天开播情况判断
       ga.anchortype                                                                                      AS type,
       CASE
           WHEN ads.duration_second >= 20 AND ads.duration_second >= 60 * 24 * 60 * 60 THEN 1
           ELSE 0 END                                                                                     AS active_status,
       1                                                                                                  AS n_o_status,       -- 通过主播首播日期计算播龄 > 6m
       ads.anchor_totla_amt_m1                                                                            AS last_mon_amt,
       CASE
           WHEN ads.anchor_totla_amt_m1 >= 500000 THEN 50
           WHEN ads.anchor_totla_amt_m1 >= 100000 THEN 10
           WHEN ads.anchor_totla_amt_m1 >= 30000 THEN 3
           ELSE 0 END                                                                                     AS amt_level,
       ''                                                                                                 AS first_live_date,  -- 主播首播日期(注册日期)
       ads.final_live_date                                                                                AS final_live_date,
       ga.contype                                                                                         AS settle_method,
       ga.anchorRate                                                                                      AS settle_rate,
       ''                                                                                                 AS join_operation,
       ''                                                                                                 AS source,
       ''                                                                                                 AS recruiter,
       ''                                                                                                 AS operator
FROM spider_yy_backend.guild_anchor ga
         LEFT JOIN tmp.anchor_duration_time_format_sum ads ON ga.yynum = ads.yynum AND ga.uid = ads.uid
         LEFT JOIN tmp.anchor_duration_time_format_t1 t1 ON ga.yynum = t1.yynum AND ga.uid = t1.uid
;


-- 删除临时表
DROP TABLE IF EXISTS tmp.anchor_duration_time_format;
DROP TABLE IF EXISTS tmp.anchor_duration_time_format_sum;
DROP TABLE IF EXISTS tmp.anchor_duration_time_format_t1;

