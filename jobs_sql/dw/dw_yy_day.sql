-- warehouse.dw_yy_day_anchor_live_duration
-- warehouse.dw_yy_day_anchor_live_commission
-- warehouse.dw_yy_day_anchor_live_bluediamond
-- warehouse.dw_yy_day_anchor_live


# DROP TABLE IF EXISTS warehouse.dw_yy_day_anchor_live_duration;
# CREATE TABLE warehouse.dw_yy_day_anchor_live_duration AS
DELETE
FROM warehouse.dw_yy_day_anchor_live_duration
WHERE dt BETWEEN '2019-01-01' AND '2020-02-02';
INSERT INTO warehouse.dw_yy_day_anchor_live_duration
SELECT *
FROM warehouse.ods_yy_day_anchor_live_duration
WHERE dt BETWEEN '2019-01-01' AND '2020-02-02'
;


-- 按日汇总主播佣金收入及工会分成（佣金）
# DROP TABLE IF EXISTS warehouse.dw_yy_day_anchor_live_commission;
# CREATE TABLE warehouse.dw_yy_day_anchor_live_commission AS
DELETE
FROM warehouse.dw_yy_day_anchor_live_commission
WHERE dt BETWEEN '2019-01-01' AND '2020-02-02';
INSERT INTO warehouse.dw_yy_day_anchor_live_commission
SELECT *
FROM warehouse.ods_yy_day_anchor_live_commission
WHERE dt BETWEEN '2019-01-01' AND '2020-02-02'
;


-- 主播收入（蓝钻）
# DROP TABLE IF EXISTS warehouse.dw_yy_day_anchor_live_bluediamond;
# CREATE TABLE warehouse.dw_yy_day_anchor_live_bluediamond AS
DELETE
FROM warehouse.dw_yy_day_anchor_live_bluediamond
WHERE dt BETWEEN '2019-01-01' AND '2020-02-02';
INSERT INTO warehouse.dw_yy_day_anchor_live_bluediamond
SELECT *
FROM warehouse.ods_yy_day_anchor_live_bluediamond
WHERE dt BETWEEN '2019-01-01' AND '2020-02-02'
;


# DROP TABLE IF EXISTS warehouse.dw_yy_day_anchor_live;
# CREATE TABLE warehouse.dw_yy_day_anchor_live AS
DELETE
FROM warehouse.dw_yy_day_anchor_live
WHERE dt BETWEEN '2019-01-01' AND '2020-02-02';
INSERT INTO warehouse.dw_yy_day_anchor_live
SELECT *
FROM warehouse.ods_yy_day_anchor_live
WHERE dt BETWEEN '2019-01-01' AND '2020-02-02'
;
