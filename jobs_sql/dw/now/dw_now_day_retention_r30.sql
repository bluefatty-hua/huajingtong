
-- 30天留存
create table if not exists stage.stage_now_30days_live
(
 dt date,
 anchor_no bigint,
 live_days int,
 PRIMARY KEY (`dt`,`anchor_no`)
);

delete from stage.stage_now_30days_live 
where dt  >='{month}' and dt <= LAST_DAY('{month}'); 


-- 计算30天开播天数
insert into stage.stage_now_30days_live 
SELECT
t1.dt,t1.anchor_no,
SUM(IFNULL(t2.`live_status`,0)) AS live_days
FROM warehouse.dw_now_day_anchor_live t1 
JOIN warehouse.dw_now_day_anchor_live t2
ON t2.dt > t1.dt AND t2.dt <= t1.dt + INTERVAL 30 DAY
AND t1.anchor_no = t2.anchor_no
WHERE t1.dt  >='{month}' and t1.dt <= LAST_DAY('{month}') AND t1.add_loss_state='new' 
GROUP BY t1.dt,t1.anchor_no;



create table if not exists stage.stage_now_30th_missing
(
 dt date,
 anchor_no bigint,
 PRIMARY KEY (`dt`,`anchor_no`)
);

delete from stage.stage_now_30th_missing 
where dt  >='{month}' and dt <= LAST_DAY('{month}'); 

-- 计算第30天不在主播列表主播
insert into stage.stage_now_30th_missing 
SELECT
t1.dt,t1.anchor_no
FROM warehouse.dw_now_day_anchor_live t1 
LEFT JOIN warehouse.dw_now_day_anchor_live t2
ON  t2.dt = t1.dt + INTERVAL 30 DAY
AND t1.anchor_no = t2.anchor_no
WHERE t1.dt  >='{month}' and t1.dt <= LAST_DAY('{month}') AND t1.add_loss_state='new' 
AND t2.dt IS NULL;





UPDATE warehouse.dw_now_day_anchor_live
SET retention_r30_lives = 0,retention_r30_missing = 0,retention_r30=1
where dt  >='{month}' and dt <= LAST_DAY('{month}');


-- 更新开播数据到dw
UPDATE warehouse.dw_now_day_anchor_live t1,stage.stage_now_30days_live t2
SET t1.retention_r30_lives = t2.live_days
WHERE t1.dt  >='{month}' and t1.dt <= LAST_DAY('{month}') AND t1.add_loss_state='new' 
and t1.dt = t2.dt and t1.anchor_no = t2.anchor_no;


-- 更新流失数据到dw
UPDATE warehouse.dw_now_day_anchor_live t1,stage.stage_now_30th_missing t2
SET t1.retention_r30_missing = 1
WHERE t1.dt  >='{month}' and t1.dt <= LAST_DAY('{month}') AND t1.add_loss_state='new' 
and t1.dt = t2.dt and t1.anchor_no = t2.anchor_no;

-- 更新流失状态到dw
UPDATE warehouse.dw_now_day_anchor_live
SET retention_r30 = 0
where dt  >='{month}' and dt <= LAST_DAY('{month}')
and retention_r30_lives<15 or retention_r30_missing=1 ; 





-- 回写anchor month表


update  warehouse.dw_now_month_anchor_live
  set retention_r30 = 0
WHERE dt >= '{month}'
  AND dt <= LAST_DAY('{month}');

INSERT INTO warehouse.dw_now_month_anchor_live
(
  `dt`,
  `backend_account_id`,
  `anchor_no`,
  `retention_r30`
)
SELECT '{month}'                                                    AS dt,
       backend_account_id,
       anchor_no,
       if(sum(ifnull(retention_r30,0))>0,1,0)                                  as retention_r30
FROM  warehouse.dw_now_day_anchor_live
      WHERE  dt >= '{month}'
        AND dt < '{month}' + INTERVAL 1 MONTH
group by
      backend_account_id,
      anchor_no

ON DUPLICATE KEY UPDATE `retention_r30`=values(retention_r30);



-- 回写留存数据到guild day表

update  warehouse.dw_now_day_guild_live
	set new_r30_cnt = 0
WHERE dt >= '{month}'
  AND dt <= LAST_DAY('{month}');


INSERT INTO warehouse.dw_now_day_guild_live
(
  `dt`,
  `backend_account_id`,
  `newold_state`,
  `active_state`,
  `revenue_level`,
  `city`,
  `new_r30_cnt`
)
SELECT t.dt,
       t.backend_account_id,
       t.newold_state,
       t.active_state,
       t.revenue_level,
       t.city,
       sum(retention_r30)  as new_r30_cnt
FROM warehouse.dw_now_day_anchor_live t
WHERE 
t.dt >= '{month}' AND t.dt <= LAST_DAY('{month}')
GROUP BY t.dt,
         t.backend_account_id,
         t.newold_state,
         t.active_state,
         t.revenue_level,
         t.city
ON DUPLICATE KEY UPDATE `new_r30_cnt`=values(new_r30_cnt);




-- 回写留存数据到guild month表


update  warehouse.dw_now_month_guild_live
	set new_r30_cnt = 0
WHERE dt >= '{month}'
  AND dt <= LAST_DAY('{month}');

INSERT INTO warehouse.dw_now_month_guild_live
(
  `dt`,
  `backend_account_id`,
  `newold_state`,
  `active_state`,
  `revenue_level`,
  `city`,
  `new_r30_cnt`
)
SELECT '{month}' AS dt,
       al.backend_account_id,
       al.newold_state AS newold_state,
       al.active_state,
       al.revenue_level,
       al.city,
       sum(retention_r30) as new_r30_cnt
FROM warehouse.dw_now_month_anchor_live al
WHERE  dt >= '{month}'
        AND dt <= LAST_DAY('{month}') and add_loss_state = 'new'

GROUP BY 
         al.backend_account_id,
         al.newold_state,
         al.active_state,
         al.revenue_level,
         al.city
ON DUPLICATE KEY UPDATE `new_r30_cnt`=values(new_r30_cnt);



