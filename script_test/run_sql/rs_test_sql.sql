Drop TABLE IF EXISTS stage.rs_test_tmp0_20191229;
CREATE TABLE stage.rs_test_tmp0_20191229 AS
SELECT *
-- 测
FROM warehouse.platform
WHERE pay_from BETWEEN {start_date} AND {end_date}
  AND ID IN {platform_id}
LIMIT 1
;

Drop TABLE IF EXISTS warehouse.rs_test_tmp1_20191229;
CREATE TABLE warehouse.rs_test_tmp1_20191229 AS
SELECT *
FROM warehouse.platform
WHERE ID IN {platform_id}
LIMIT 1
;
