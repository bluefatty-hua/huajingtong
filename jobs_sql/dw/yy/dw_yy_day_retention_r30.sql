
-- 30天留存
create table if not exists stage.stage_30days_live
(
 dt date,
 anchor_uid bigint,
 live_days int,
 PRIMARY KEY (`dt`,`anchor_uid`)
);

delete from stage.stage_30days_live 
where dt  >='{month}' and dt <= LAST_DAY('{month}'); 


-- 计算30天开播天数
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
 anchor_uid bigint,
 PRIMARY KEY (`dt`,`anchor_uid`)
);

delete from stage.stage_30th_missing 
where dt  >='{month}' and dt <= LAST_DAY('{month}'); 

-- 计算第30天不在主播列表主播
insert into stage.stage_30th_missing 
SELECT
t1.dt,t1.anchor_uid
FROM warehouse.dw_yy_day_anchor_live t1 
LEFT JOIN warehouse.dw_yy_day_anchor_live t2
ON  t2.dt = t1.dt + INTERVAL 30 DAY
AND t1.anchor_uid = t2.anchor_uid
WHERE t1.dt  >='{month}' and t1.dt <= LAST_DAY('{month}') AND t1.add_loss_state='new' 
AND t2.dt IS NULL;




-- 回写anchor day表
UPDATE warehouse.dw_yy_day_anchor_live
SET retention_r30_lives = 0,retention_r30_missing = 0,retention_r30=1
where dt  >='{month}' and dt <= LAST_DAY('{month}');


-- 更新开播数据到dw
UPDATE warehouse.dw_yy_day_anchor_live t1,stage.stage_30days_live t2
SET t1.retention_r30_lives = t2.live_days
WHERE t1.dt  >='{month}' and t1.dt <= LAST_DAY('{month}') AND t1.add_loss_state='new' 
and t1.dt = t2.dt and t1.anchor_uid = t2.anchor_uid;


-- 更新流失数据到dw
UPDATE warehouse.dw_yy_day_anchor_live t1,stage.stage_30th_missing t2
SET t1.retention_r30_missing = 1
WHERE t1.dt  >='{month}' and t1.dt <= LAST_DAY('{month}') AND t1.add_loss_state='new' 
and t1.dt = t2.dt and t1.anchor_uid = t2.anchor_uid;

-- 更新流失状态到dw
UPDATE warehouse.dw_yy_day_anchor_live
SET retention_r30 = 0
where dt  >='{month}' and dt <= LAST_DAY('{month}')
and retention_r30_lives<15 or retention_r30_missing=1 ; 







-- 回写anchor month表

update  warehouse.dw_yy_month_anchor_live
  set retention_r30 = 0
WHERE dt >= '{month}'
  AND dt <= LAST_DAY('{month}');

INSERT INTO warehouse.dw_yy_month_anchor_live
(
  `dt`,
  `backend_account_id`,
  `anchor_uid`,
  `retention_r30`
)
SELECT '{month}'                                                    AS dt,
       backend_account_id,
       anchor_uid,
       if(sum(ifnull(retention_r30,0))>0,1,0)                       as retention_r30
FROM warehouse.dw_yy_day_anchor_live
WHERE  dt >= '{month}'
  AND dt <= LAST_DAY('{month}')
group by 
  backend_account_id,
  anchor_uid
ON DUPLICATE KEY UPDATE `retention_r30`=values(retention_r30);







-- 回写留存数据到guild day表

update  warehouse.dw_yy_day_guild_live
	set new_r30_cnt = 0
WHERE dt >= '{month}'
  AND dt <= LAST_DAY('{month}');


INSERT INTO warehouse.dw_yy_day_guild_live
(
  dt,
  backend_account_id,
  channel_num,
  comment,
  newold_state,
  active_state,
  revenue_level,
  `new_r30_cnt`
)
SELECT dt,
        backend_account_id,
        channel_num,
        comment,
        newold_state,
        active_state,
        revenue_level,
       sum(retention_r30)  as new_r30_cnt
FROM warehouse.dw_yy_day_anchor_live t
WHERE 
t.dt >= '{month}' AND t.dt <= LAST_DAY('{month}')
GROUP BY dt,
         backend_account_id,
         channel_num,
         comment,
         newold_state,
         active_state,
         revenue_level
ON DUPLICATE KEY UPDATE `new_r30_cnt`=values(new_r30_cnt);




-- 回写留存数据到guild month表


update  warehouse.dw_yy_month_guild_live
	set new_r30_cnt = 0
WHERE dt >= '{month}'
  AND dt <= LAST_DAY('{month}');

INSERT INTO warehouse.dw_yy_month_guild_live
(
        `dt`,
        backend_account_id,
        channel_num,
        comment,
        newold_state,
        active_state,
        revenue_level,
        `new_r30_cnt`
)
SELECT '{month}' AS dt,
        backend_account_id,
        channel_num,
        comment,
        newold_state,
        active_state,
        revenue_level,
        sum(retention_r30) as new_r30_cnt
FROM warehouse.dw_yy_month_anchor_live al
WHERE  dt >= '{month}'
        AND dt <= LAST_DAY('{month}') and add_loss_state = 'new'

GROUP BY 
        backend_account_id,
        channel_num,
        comment,
        newold_state,
        active_state,
        revenue_level
ON DUPLICATE KEY UPDATE `new_r30_cnt`=values(new_r30_cnt);


