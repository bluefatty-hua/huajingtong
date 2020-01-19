-- 同步历史的旧主播数据

UPDATE bireport.`rpt_month_now_guild` t1, `spider_now_backend`.`union_stat_info_by_month` t2
SET t1.anchor_cnt = t2.anchor_num
WHERE t1.backend_account_id = t2.backend_account_id
AND LEFT(REPLACE(t1.dt,'-',''),6)  = t2.date AND t1.dt < 20200101