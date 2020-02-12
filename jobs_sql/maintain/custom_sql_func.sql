-- 注意函数使用
-- 直播时长转秒 DURATION_CH 用于ods_yy_sql
DELIMITER $$
DROP FUNCTION IF EXISTS DURATION_CH$$
CREATE FUNCTION DURATION_CH(duration varchar(20))
RETURNS INT(10)
BEGIN
    DECLARE sec INT(10) DEFAULT 0;
    SET sec = (CASE WHEN duration RLIKE '小时' THEN SUBSTRING_INDEX(duration, '小时', 1) + 0 ELSE 0 END) * 60 * 60 + (CASE WHEN duration RLIKE '分' THEN SUBSTRING_INDEX(SUBSTRING_INDEX(duration, '小时', -1), '分', 1) + 0 ELSE 0 END) * 60 + (SUBSTRING_INDEX(SUBSTRING_INDEX(duration, '分', -1), '秒', 1) + 0);
    RETURN sec;
END $$
DELIMITER ;



-- 用于判断新老主播
-- live_dt：开播时长，sign_dt：签约时间，dt：用于判断新老主播的时间
DELIMITER $$
DROP FUNCTION IF EXISTS ANCHOR_NEW_OLD$$
CREATE FUNCTION ANCHOR_NEW_OLD(live_dt date, sign_dt date, dt date, days int)
RETURNS varchar(4)
BEGIN
    DECLARE newOld_state varchar(4) DEFAULT '';
    DECLARE days varchar(4) DEFAULT 180;
    SET newold_state = (CASE
           WHEN live_dt IS NOT NULL AND sign_dt IS NOT NULL THEN
               CASE
                   WHEN live_dt <= sign_dt
                       THEN CASE
                                WHEN DATEDIFF(dt, live_dt) >= days THEN '老主播'
                                ELSE '新主播' END
                   ELSE CASE
                            WHEN DATEDIFF(dt, sign_dt) >= days THEN '老主播'
                            ELSE '新主播' END
                   END
           WHEN live_dt IS NOT NULL THEN
               CASE
                   WHEN DATEDIFF(dt, live_dt) >= days THEN '老主播'
                   ELSE '新主播' END
           WHEN sign_dt IS NOT NULL THEN
               CASE
                   WHEN DATEDIFF(dt, sign_dt) >= days THEN '老主播'
                   ELSE '新主播' END
           ELSE '未知' END);
    RETURN newOld_state;
END $$
DELIMITER ;

SELECT ANCHOR_NEW_OLD('2020-01-01', '2020-01-01', '2020-02-01', 180)