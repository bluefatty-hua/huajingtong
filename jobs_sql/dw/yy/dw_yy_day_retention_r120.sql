
-- 120天留存
create table if not exists stage.stage_120days_live
(
 dt date,
 anchor_uid bigint,
 live_days int,
 PRIMARY KEY (`dt`,`anchor_uid`)
);

delete from stage.stage_120days_live 
where dt  >='{month}' and dt <= LAST_DAY('{month}'); 

insert into stage.stage_120days_live 
SELECT
t1.dt,t1.anchor_uid,
SUM(IFNULL(t2.`live_status`,0)) AS live_days
FROM warehouse.dw_yy_day_anchor_live t1 
JOIN warehouse.dw_yy_day_anchor_live t2
	ON t2.dt > t1.dt + INTERVAL 90 DAY
	AND t2.dt <= t1.dt + INTERVAL 120 DAY
	AND t1.anchor_uid = t2.anchor_uid
WHERE t1.dt  >='{month}' and t1.dt <= LAST_DAY('{month}') AND t1.add_loss_state='new' 
GROUP BY t1.dt,t1.anchor_uid;



create table if not exists stage.stage_120th_missing
(
 dt date,
 anchor_uid bigint,
 PRIMARY KEY (`dt`,`anchor_uid`)
);

delete from stage.stage_120th_missing 
where dt  >='{month}' and dt <= LAST_DAY('{month}'); 

insert into stage.stage_120th_missing 
SELECT
t1.dt,t1.anchor_uid
FROM warehouse.dw_yy_day_anchor_live t1 
LEFT JOIN warehouse.dw_yy_day_anchor_live t2
ON  t2.dt = t1.dt + INTERVAL 120 DAY
AND t1.anchor_uid = t2.anchor_uid
WHERE t1.dt  >='{month}' and t1.dt <= LAST_DAY('{month}') AND t1.add_loss_state='new' 
AND t2.dt IS NULL;



UPDATE warehouse.dw_yy_day_anchor_live
SET retention_r120_lives = 0,retention_r120_missing = 0,retention_r120=1
where dt  >='{month}' and dt <= LAST_DAY('{month}');


-- 更新开播数据到dw
UPDATE warehouse.dw_yy_day_anchor_live t1,stage.stage_120days_live t2
SET t1.retention_r120_lives = t2.live_days
WHERE t1.dt  >='{month}' and t1.dt <= LAST_DAY('{month}') AND t1.add_loss_state='new' 
and t1.dt = t2.dt and t1.anchor_uid = t2.anchor_uid;


-- 更新流失数据到dw
UPDATE warehouse.dw_yy_day_anchor_live t1,stage.stage_120th_missing t2
SET t1.retention_r120_missing = 1
WHERE t1.dt  >='{month}' and t1.dt <= LAST_DAY('{month}') AND t1.add_loss_state='new' 
and t1.dt = t2.dt and t1.anchor_uid = t2.anchor_uid;

-- 更新流失状态到dw
UPDATE warehouse.dw_yy_day_anchor_live
SET retention_r120 = 0
where dt  >='{month}' and dt <= LAST_DAY('{month}')
and retention_r120_lives<15 or retention_r120_missing=1 ; 