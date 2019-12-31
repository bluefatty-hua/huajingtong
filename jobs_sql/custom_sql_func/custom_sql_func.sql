-- 注意函数间使用
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