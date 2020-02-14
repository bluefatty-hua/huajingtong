Drop TABLE IF EXISTS stage.rs_test_tmp0_20191229;
CREATE TABLE stage.rs_test_tmp0_20191229 AS
SELECT *
-- 测
FROM warehouse.ods_now_month_guild_live
WHERE dt BETWEEN '{month}' AND '{end_date}'
LIMIT 1
;

Drop TABLE IF EXISTS stage.rs_test_tmp1_20191229;
CREATE TABLE stage.rs_test_tmp1_20191229 AS
SELECT *
FROM warehouse.ods_now_month_guild_live
LIMIT 1
;