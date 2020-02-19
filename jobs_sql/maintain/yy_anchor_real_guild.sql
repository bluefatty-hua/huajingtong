此脚本不能直接执行

use spider_yy_backend;
DELETE FROM guild_anchor_real;
CALL p_split_bluediamond();


--------------------------------------------
CREATE TABLE `guild_anchor_new` (
  `backend_account_id` INT(10) NOT NULL,
  `dt` DATE NOT NULL,
  `uid` BIGINT(20) NOT NULL,
  `yynum` BIGINT(20) DEFAULT NULL,
  `nick` TEXT CHARACTER SET utf8mb4,
  `logo` TEXT,
  `roomid` BIGINT(20) DEFAULT NULL,
  `roomaid` BIGINT(20) DEFAULT NULL,
  `anchortype` VARCHAR(3) DEFAULT NULL,
  `conId` TEXT,
  `signtime` VARCHAR(10) DEFAULT NULL,
  `endtime` VARCHAR(10) DEFAULT NULL,
  `contype` VARCHAR(3) DEFAULT NULL,
  `anchorRate` VARCHAR(3) DEFAULT NULL,
  `ghrate` VARCHAR(3) DEFAULT NULL,
  `isFreeze` TINYINT(1) DEFAULT NULL,
  `isRed` TINYINT(1) DEFAULT NULL,
  `islive` TINYINT(1) DEFAULT NULL,
  `livingSubSid` VARCHAR(30) DEFAULT NULL,
  `livingTopSid` VARCHAR(30) DEFAULT NULL,
  `timestamp` DATETIME DEFAULT NULL,
  INDEX (yynum,`dt`)
) ENGINE=INNODB DEFAULT CHARSET=utf8;

INSERT INTO `guild_anchor_new`
SELECT * FROM guild_anchor;


UPDATE `guild_anchor_new` t1,`guild_anchor_real` t2
SET t1.backend_account_id = t2.backend_account_id
WHERE t1.`yynum` = t2.yynum
AND t1.dt = t2.dt;

TRUNCATE TABLE guild_anchor;
INSERT IGNORE INTO guild_anchor
SELECT * FROM guild_anchor_new;


