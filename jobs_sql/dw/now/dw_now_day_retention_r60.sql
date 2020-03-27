
-- 30天留存
create table if not exists stage.stage_now_60days_live
(
 dt date,
 anchor_no bigint,
 live_days int,
 PRIMARY KEY (`dt`,`anchor_no`)
);

delete from stage.stage_now_60days_live 
where dt  >='{month}' and dt <= LAST_DAY('{month}'); 


-- 计算30天开播天数
insert into stage.stage_now_60days_live 
SELECT
t1.dt,t1.anchor_no,
SUM(IFNULL(t2.`live_status`,0)) AS live_days
FROM warehouse.dw_now_day_anchor_live t1 
JOIN warehouse.dw_now_day_anchor_live t2
ON t2.dt > t1.dt+INTERVAL 30 DAY 
AND t2.dt <= t1.dt + INTERVAL 60 DAY
AND t1.anchor_no = t2.anchor_no
WHERE t1.dt  >='{month}' and t1.dt <= LAST_DAY('{month}') AND t1.add_loss_state='new' 
GROUP BY t1.dt,t1.anchor_no;



create table if not exists stage.stage_now_60th_missing
(
 dt date,
 anchor_no bigint,
 PRIMARY KEY (`dt`,`anchor_no`)
);

delete from stage.stage_now_60th_missing 
where dt  >='{month}' and dt <= LAST_DAY('{month}'); 

-- 计算第30天不在主播列表主播
insert into stage.stage_now_60th_missing 
SELECT
t1.dt,t1.anchor_no
FROM warehouse.dw_now_day_anchor_live t1 
LEFT JOIN warehouse.dw_now_day_anchor_live t2
ON  t2.dt = t1.dt + INTERVAL 60 DAY
AND t1.anchor_no = t2.anchor_no
WHERE t1.dt  >='{month}' and t1.dt <= LAST_DAY('{month}') AND t1.add_loss_state='new' 
AND t2.dt IS NULL;




-- 回写anchor day表
UPDATE warehouse.dw_now_day_anchor_live
SET retention_r60_lives = 0,retention_r60_missing = 0,retention_r60=1
where dt  >='{month}' and dt <= LAST_DAY('{month}');


-- 更新开播数据到dw
UPDATE warehouse.dw_now_day_anchor_live t1,stage.stage_now_60days_live t2
SET t1.retention_r60_lives = t2.live_days
WHERE t1.dt  >='{month}' and t1.dt <= LAST_DAY('{month}') AND t1.add_loss_state='new' 
and t1.dt = t2.dt and t1.anchor_no = t2.anchor_no;


-- 更新流失数据到dw
UPDATE warehouse.dw_now_day_anchor_live t1,stage.stage_now_60th_missing t2
SET t1.retention_r60_missing = 1
WHERE t1.dt  >='{month}' and t1.dt <= LAST_DAY('{month}') AND t1.add_loss_state='new' 
and t1.dt = t2.dt and t1.anchor_no = t2.anchor_no;

-- 更新流失状态到dw
UPDATE warehouse.dw_now_day_anchor_live
SET retention_r60 = 0
where dt  >='{month}' and dt <= LAST_DAY('{month}')
and retention_r60_lives<15 or retention_r60_missing=1 ; 







-- 回写anchor month表

update  warehouse.dw_now_month_anchor_live
  set retention_r60 = 0
WHERE dt >= '{month}'
  AND dt <= LAST_DAY('{month}');

INSERT INTO warehouse.dw_now_month_anchor_live
(
  `dt`,
  `backend_account_id`,
  `anchor_no`,
  `retention_r60`
)
SELECT '{month}'                                                    AS dt,
       backend_account_id,
       anchor_no,
       if(sum(ifnull(retention_r60,0))>0,1,0)                       as retention_r60
FROM warehouse.dw_now_day_anchor_live
WHERE  dt >= '{month}'
  AND dt <= LAST_DAY('{month}')
group by 
  backend_account_id,
  anchor_no
ON DUPLICATE KEY UPDATE `retention_r60`=values(retention_r60);







-- 回写留存数据到guild day表

update  warehouse.dw_now_day_guild_live
  set new_r60_cnt = 0
WHERE dt >= '{month}'
  AND dt <= LAST_DAY('{month}');


INSERT INTO warehouse.dw_now_day_guild_live
(
  dt,
  backend_account_id,
  city,
  newold_state,
  active_state,
  revenue_level,
  `new_r60_cnt`
)
SELECT dt,
        backend_account_id,
        city,
        newold_state,
        active_state,
        revenue_level,
       sum(retention_r60)  as new_r60_cnt
FROM warehouse.dw_now_day_anchor_live t
WHERE 
t.dt >= '{month}' AND t.dt <= LAST_DAY('{month}')
GROUP BY dt,
         backend_account_id,
         city,
         newold_state,
         active_state,
         revenue_level
ON DUPLICATE KEY UPDATE `new_r60_cnt`=values(new_r60_cnt);




-- 回写留存数据到guild month表


update  warehouse.dw_now_month_guild_live
  set new_r60_cnt = 0
WHERE dt >= '{month}'
  AND dt <= LAST_DAY('{month}');

INSERT INTO warehouse.dw_now_month_guild_live
(
        `dt`,
        backend_account_id,
        city,
        newold_state,
        active_state,
        revenue_level,
        `new_r60_cnt`
)
SELECT '{month}' AS dt,
        backend_account_id,
        city,
        newold_state,
        active_state,
        revenue_level,
        sum(retention_r60) as new_r60_cnt
FROM warehouse.dw_now_month_anchor_live al
WHERE  dt >= '{month}'
        AND dt <= LAST_DAY('{month}')

GROUP BY 
        backend_account_id,
        city,
        newold_state,
        active_state,
        revenue_level
ON DUPLICATE KEY UPDATE `new_r60_cnt`=values(new_r60_cnt);


