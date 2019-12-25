-- yy主播每日蓝钻流水
SELECT * 
FROM spider_yy_backend.anchor_bluediamond
;
-- ALTER TABLE spider_yy_backend.anchor_bluediamond COMMENT '主播每日蓝钻流水（https://www.yy.com/i/anchorIncome）'

SELECT backend_account_id AS guild_id,
       yynum AS anchor_no,
	   diamond as virtual_coin,
       dt
FROM spider_yy_backend.anchor_bluediamond
;

SELECT COUNT(yynum),
       COUNT(DISTINCT yynum),
       COUNT(CONCAT(yynum, dt)),
       COUNT(DISTINCT CONCAT(yynum, dt))
FROM spider_yy_backend.anchor_bluediamond
;
-- 1908 30 1908 1908

-- --------------------------------------------------------------------------------------------------------------------------
-- yy主播每日直播时长
SELECT * 
FROM spider_yy_backend.anchor_duration
;

SELECT COUNT(yynum),
       COUNT(DISTINCT yynum) 
FROM spider_yy_backend.anchor_duration
;
-- 13604 38
SELECT *
FROM (SELECT DISTINCT yynum FROM spider_yy_backend.anchor_bluediamond) anbd
RIGHT JOIN (SELECT DISTINCT yynum FROM spider_yy_backend.anchor_duration) andu ON anbd.yynum = andu.yynum;


SELECT COUNT(DISTINCT dt)
FROM spider_yy_backend.anchor_bluediamond
;
-- --------------------------------------------------------------------------------------------------------------------------

-- select date_add('2019-01-01',interval @i:=@i+1 day) as date, tmp.* 
-- from spider_yy_backend.anchor_bluediamond tmp,
--  (select @i:= -1) t
-- ;
 

 
-- ====================================================
-- 提取yy号, 得到所有主播
SELECT 
    uid AS anchor_uid,
    yynum AS anchor_no
FROM
    spider_yy_backend.anchor_bluediamond 
UNION SELECT 
    uid AS anchor_uid,
    yynum AS anchor_no
FROM
    spider_yy_backend.anchor_duration
;

SELECT count(distinct dt) from 
(SELECT 
    dt AS dt
FROM
    spider_yy_backend.anchor_bluediamond 
UNION SELECT 
    dt AS dt
FROM
    spider_yy_backend.anchor_duration) t
;



-- 构建an_anchor_day会员基础信息
-- INSERT INTO warehouse.an_anchor_day (anchor_uid, anchor_no, nick_name, platform_id, platform_name, guild_id, channel_id, live_status, dt)
SELECT 
    yn.anchor_uid,
    yn.anchor_no,
    ad.nick AS nick_name,
    1000 AS platform_id,
    'YY' AS plat_name,
    ad.chaid AS channel_id
    
FROM
    (SELECT 
        uid AS anchor_uid, yynum AS anchor_no
    FROM
        spider_yy_backend.anchor_bluediamond UNION SELECT 
        uid AS anchor_uid, yynum AS anchor_no
    FROM
        spider_yy_backend.anchor_duration) yn
LEFT JOIN
    spider_yy_backend.anchor_duration ad ON yn.anchor_uid = ad.uid AND yn.anchor_no = ad.yynum
; 

-- 构建an_live_day数据g














