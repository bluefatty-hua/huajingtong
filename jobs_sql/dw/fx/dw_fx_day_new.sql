-- 2020年以后的数据按照前一天是否出现过来判断新用户

UPDATE warehouse.dw_fx_day_anchor_live
SET add_loss_state = null
where dt  >='{month}' and dt <= LAST_DAY('{month}');

UPDATE warehouse.dw_fx_day_anchor_live t1,
warehouse.dw_fx_day_anchor_live t2
SET t1.add_loss_state = 'old'
WHERE t1.anchor_no = t2.anchor_no
AND t1.dt = t2.dt + INTERVAL 1 DAY
AND t1.dt  >='{month}' and t1.dt <= LAST_DAY('{month}');


UPDATE warehouse.dw_fx_day_anchor_live
SET add_loss_state = 'new'
WHERE add_loss_state IS NULL
and dt  >='{month}' and dt <= LAST_DAY('{month}');


-- 2020年以前的数据按照首次直播时间作为新用户

-- UPDATE warehouse.dw_fx_day_anchor_live
-- SET add_loss_state = null
-- where  dt  >='{month}' and dt <= LAST_DAY('{month}') and dt<20200101;


-- -- 首次开播
-- update warehouse.dw_fx_day_anchor_live t1,
-- (SELECT anchor_no,MIN(dt)  as dt FROM warehouse.dw_fx_day_anchor_live
-- WHERE live_status = 1 AND  dt<20200101
-- GROUP BY anchor_no ) t2
-- SET t1.add_loss_state = 'new'
-- WHERE t1.anchor_no = t2.anchor_no
-- AND t1.dt = t2.dt 
-- and t1.dt<20200101 and t1.dt  >='{month}' and t1.dt <= LAST_DAY('{month}'); 

-- 首次开播后的算为老主播
-- update warehouse.dw_fx_day_anchor_live t1, warehouse.dw_fx_day_anchor_live t2
-- set t1.add_loss_state = 'old'
-- where  t1.anchor_no = t2.anchor_no and t2.add_loss_state = 'new'
-- and t1.dt > t2.dt  and t1.dt<20200101; 






