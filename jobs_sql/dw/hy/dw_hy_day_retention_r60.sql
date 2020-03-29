
-- 60天留存
create table if not exists stage.stage_huya_60days_live
(
 dt date,
 anchor_uid bigint,
 live_days int,
 PRIMARY KEY (`dt`,`anchor_uid`)
);

delete from stage.stage_huya_60days_live 
where dt  >='{month}' and dt <= LAST_DAY('{month}'); 

insert into stage.stage_huya_60days_live 
SELECT
t1.dt,t1.anchor_uid,
SUM(IFNULL(t2.`live_status`,0)) AS live_days
FROM warehouse.dw_huya_day_anchor_live t1 
JOIN warehouse.dw_huya_day_anchor_live t2
	ON t2.dt > t1.dt+INTERVAL 30 DAY 
	AND t2.dt <= t1.dt + INTERVAL 60 DAY
	AND t1.anchor_uid = t2.anchor_uid
WHERE t1.dt  >='{month}' and t1.dt <= LAST_DAY('{month}') AND t1.add_loss_state='new' 
GROUP BY t1.dt,t1.anchor_uid;



create table if not exists stage.stage_huya_60th_missing
(
 dt date,
 anchor_uid bigint,
 PRIMARY KEY (`dt`,`anchor_uid`)
);

delete from stage.stage_huya_60th_missing 
where dt  >='{month}' and dt <= LAST_DAY('{month}');

insert into stage.stage_huya_60th_missing
SELECT
t1.dt,t1.anchor_uid
FROM warehouse.dw_huya_day_anchor_live t1 
LEFT JOIN warehouse.dw_huya_day_anchor_live t2
ON  t2.dt = t1.dt + INTERVAL 60 DAY
AND t1.anchor_uid = t2.anchor_uid
WHERE t1.dt  >='{month}' and t1.dt <= LAST_DAY('{month}') AND t1.add_loss_state='new' 
AND t2.dt IS NULL;



UPDATE warehouse.dw_huya_day_anchor_live
SET retention_r60_lives = 0,retention_r60_missing = 0,retention_r60=1
where dt  >='{month}' and dt <= LAST_DAY('{month}');


-- 更新开播数据到dw
UPDATE warehouse.dw_huya_day_anchor_live t1,stage.stage_huya_60days_live t2
SET t1.retention_r60_lives = t2.live_days
WHERE t1.dt  >='{month}' and t1.dt <= LAST_DAY('{month}') AND t1.add_loss_state='new' 
and t1.dt = t2.dt and t1.anchor_uid = t2.anchor_uid;


-- 更新流失数据到dw
UPDATE warehouse.dw_huya_day_anchor_live t1,stage.stage_huya_60th_missing t2
SET t1.retention_r60_missing = 1
WHERE t1.dt  >='{month}' and t1.dt <= LAST_DAY('{month}') AND t1.add_loss_state='new' 
and t1.dt = t2.dt and t1.anchor_uid = t2.anchor_uid;

-- 更新流失状态到dw
UPDATE warehouse.dw_huya_day_anchor_live
SET retention_r60 = 0
where dt  >='{month}' and dt <= LAST_DAY('{month}')
and retention_r60_lives<15 or retention_r60_missing=1 ; 


-- 回写anchor month表

update  warehouse.dw_huya_month_anchor_live
  set retention_r60 = 0
WHERE dt >= '{month}'
  AND dt <= LAST_DAY('{month}');

INSERT INTO warehouse.dw_huya_month_anchor_live
(
  `dt`,
  `channel_id`,
  `anchor_uid`,
  `retention_r60`
)
SELECT '{month}'                                                    AS dt,
       channel_id,
       anchor_uid,
       if(sum(ifnull(retention_r60,0))>0,1,0)                                  as retention_r60
FROM  warehouse.dw_huya_day_anchor_live
      WHERE  dt >= '{month}'
        AND dt < '{month}' + INTERVAL 1 MONTH
group by
      channel_id,
      anchor_uid

ON DUPLICATE KEY UPDATE `retention_r60`=values(retention_r60);


-- 回写留存数据到guild表

update  warehouse.dw_huya_day_guild_live
	set new_r60_cnt = 0
WHERE dt >= '{month}'
  AND dt <= LAST_DAY('{month}');


INSERT INTO warehouse.dw_huya_day_guild_live
(
  `dt`,
  `channel_id`,
  `newold_state`,
  `active_state`,
  `revenue_level`,
  `channel_type`,
  `new_r60_cnt`
)
SELECT t.dt,
       t.channel_id,
       t.newold_state,
       t.active_state,
       t.revenue_level,
       t.channel_type,
       sum(retention_r60)  as new_r60_cnt
FROM warehouse.dw_huya_day_anchor_live t
WHERE 
t.dt >= '{month}' AND t.dt <= LAST_DAY('{month}')
GROUP BY t.dt,
         t.channel_id,
         t.newold_state,
         t.active_state,
         t.revenue_level,
         t.channel_type
ON DUPLICATE KEY UPDATE `new_r60_cnt`=values(new_r60_cnt);



-- 回写留存数据到guild month表


update  warehouse.dw_huya_month_guild_live
	set new_r60_cnt = 0
WHERE dt >= '{month}'
  AND dt <= LAST_DAY('{month}');
  
INSERT INTO warehouse.dw_huya_month_guild_live
(
  `dt`,
  `channel_id`,
  `newold_state`,
  `active_state`,
  `revenue_level`,
  `channel_type`,
  `new_r60_cnt`
)
SELECT '{month}' AS dt,
       al.channel_id,
       al.newold_state AS newold_state,
       al.active_state,
       al.revenue_level,
       al.channel_type,
       sum(retention_r60) as new_r60_cnt
FROM warehouse.dw_huya_month_anchor_live al
WHERE  dt >= '{month}'
        AND dt <= LAST_DAY('{month}') and add_loss_state = 'new'

GROUP BY 
         al.channel_id,
         al.newold_state,
         al.active_state,
         al.revenue_level,
         al.channel_type
ON DUPLICATE KEY UPDATE `new_r60_cnt`=values(new_r60_cnt);