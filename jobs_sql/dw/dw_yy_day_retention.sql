create table if not exists stage.stage_30days_live
(
 dt date,
 anchor_uid int,
 live_days int,
 PRIMARY KEY (`dt`,`anchor_uid`)
);

delete from stage.stage_30days_live 
where dt  >='{month}' and dt <= LAST_DAY('{month}'); 

insert into stage.stage_30days_live 
SELECT
t1.dt,t1.anchor_uid,
SUM(IFNULL(t2.`live_status`,0)) AS live_days
FROM warehouse.dw_yy_day_anchor_live t1 
JOIN warehouse.dw_yy_day_anchor_live t2
ON t2.dt > t1.dt AND t2.dt <= t1.dt + INTERVAL 30 DAY
AND t1.anchor_uid = t2.anchor_uid
WHERE t1.dt  >='{month}' and t1.dt <= LAST_DAY('{month}') AND t1.add_loss_state='new' 
GROUP BY t1.dt,t1.anchor_uid;

create table if not exists stage.stage_30th_missing
(
 dt date,
 anchor_uid int,
 PRIMARY KEY (`dt`,`anchor_uid`)
);

delete from stage.stage_30th_missing 
where dt  >='{month}' and dt <= LAST_DAY('{month}'); 
SELECT
t1.dt,t2.dt
FROM dw_yy_day_anchor_live t1 
LEFT JOIN dw_yy_day_anchor_live t2
ON  t2.dt = t1.dt + INTERVAL 30 DAY
AND t1.anchor_uid = t2.anchor_uid
WHERE t1.dt  >='{month}' and t1.dt <= LAST_DAY('{month}') AND t1.add_loss_state='new' 
AND t2.dt IS NULL;

