
-- 120天留存
create table if not exists stage.stage_fx_120days_live
(
 dt date,
 anchor_no bigint,
 live_days int,
 PRIMARY KEY (`dt`,`anchor_no`)
);

delete from stage.stage_fx_120days_live 
where dt  >='{month}' and dt <= LAST_DAY('{month}'); 

insert into stage.stage_fx_120days_live 
SELECT
t1.dt,t1.anchor_no,
SUM(IFNULL(t2.`live_status`,0)) AS live_days
FROM warehouse.dw_fx_day_anchor_live t1 
JOIN warehouse.dw_fx_day_anchor_live t2
	ON t2.dt > t1.dt + INTERVAL 90 DAY
	AND t2.dt <= t1.dt + INTERVAL 120 DAY
	AND t1.anchor_no = t2.anchor_no
WHERE t1.dt  >='{month}' and t1.dt <= LAST_DAY('{month}') AND t1.add_loss_state='new' 
GROUP BY t1.dt,t1.anchor_no;



create table if not exists stage.stage_fx_120th_missing
(
 dt date,
 anchor_no bigint,
 PRIMARY KEY (`dt`,`anchor_no`)
);

delete from stage.stage_fx_120th_missing 
where dt  >='{month}' and dt <= LAST_DAY('{month}'); 

insert into stage.stage_fx_120th_missing 
SELECT
t1.dt,t1.anchor_no
FROM warehouse.dw_fx_day_anchor_live t1 
LEFT JOIN warehouse.dw_fx_day_anchor_live t2
ON  t2.dt = t1.dt + INTERVAL 120 DAY
AND t1.anchor_no = t2.anchor_no
WHERE t1.dt  >='{month}' and t1.dt <= LAST_DAY('{month}') AND t1.add_loss_state='new' 
AND t2.dt IS NULL;



UPDATE warehouse.dw_fx_day_anchor_live
SET retention_r120_lives = 0,retention_r120_missing = 0,retention_r120=1
where dt  >='{month}' and dt <= LAST_DAY('{month}');


-- 更新开播数据到dw
UPDATE warehouse.dw_fx_day_anchor_live t1,stage.stage_fx_120days_live t2
SET t1.retention_r120_lives = t2.live_days
WHERE t1.dt  >='{month}' and t1.dt <= LAST_DAY('{month}') AND t1.add_loss_state='new' 
and t1.dt = t2.dt and t1.anchor_no = t2.anchor_no;


-- 更新流失数据到dw
UPDATE warehouse.dw_fx_day_anchor_live t1,stage.stage_fx_120th_missing t2
SET t1.retention_r120_missing = 1
WHERE t1.dt  >='{month}' and t1.dt <= LAST_DAY('{month}') AND t1.add_loss_state='new' 
and t1.dt = t2.dt and t1.anchor_no = t2.anchor_no;

-- 更新流失状态到dw
UPDATE warehouse.dw_fx_day_anchor_live
SET retention_r120 = 0
where dt  >='{month}' and dt <= LAST_DAY('{month}')
and retention_r120_lives<15 or retention_r120_missing=1 ; 



-- 回写anchor month表

update  warehouse.dw_fx_month_anchor_live
  set retention_r120 = 0
WHERE dt >= '{month}'
  AND dt <= LAST_DAY('{month}');

INSERT INTO warehouse.dw_fx_month_anchor_live
(
  `dt`,
  `backend_account_id`,
  `anchor_no`,
  `retention_r120`
)
SELECT '{month}'                                                    AS dt,
       backend_account_id,
       anchor_no,
       if(sum(ifnull(retention_r120,0))>0,1,0)                                  as retention_r120
FROM  warehouse.dw_fx_day_anchor_live
      WHERE  dt >= '{month}'
        AND dt < '{month}' + INTERVAL 1 MONTH
group by
      backend_account_id,
      anchor_no

ON DUPLICATE KEY UPDATE `retention_r120`=values(retention_r120);

-- 回写留存数据到guild表

update  warehouse.dw_fx_day_guild_live
	set new_r120_cnt = 0
WHERE dt >= '{month}'
  AND dt <= LAST_DAY('{month}');


INSERT INTO warehouse.dw_fx_day_guild_live
(
  `dt`,
  `backend_account_id`,
  `newold_state`,
  `active_state`,
  `revenue_level`,
  `new_r120_cnt`
)
SELECT t.dt,
       t.backend_account_id,
       t.newold_state,
       t.active_state,
       t.revenue_level,
       sum(retention_r120)  as new_r120_cnt
FROM warehouse.dw_fx_day_anchor_live t
WHERE 
t.dt >= '{month}' AND t.dt <= LAST_DAY('{month}')
GROUP BY t.dt,
         t.backend_account_id,
         t.newold_state,
         t.active_state,
         t.revenue_level
ON DUPLICATE KEY UPDATE `new_r120_cnt`=values(new_r120_cnt);



-- 回写留存数据到guild month表


update  warehouse.dw_fx_month_guild_live
	set new_r120_cnt = 0
WHERE dt >= '{month}'
  AND dt <= LAST_DAY('{month}');
  
INSERT INTO warehouse.dw_fx_month_guild_live
(
  `dt`,
  `backend_account_id`,
  `newold_state`,
  `active_state`,
  `revenue_level`,
  `new_r120_cnt`
)
SELECT '{month}' AS dt,
       al.backend_account_id,
       al.newold_state AS newold_state,
       al.active_state,
       al.revenue_level,
       sum(retention_r120) as new_r120_cnt
FROM warehouse.dw_fx_month_anchor_live al
WHERE  dt >= '{month}'
        AND dt <= LAST_DAY('{month}') and add_loss_state = 'new'

GROUP BY 
         al.backend_account_id,
         al.newold_state,
         al.active_state,
         al.revenue_level
ON DUPLICATE KEY UPDATE `new_r120_cnt`=values(new_r120_cnt);