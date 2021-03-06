UPDATE warehouse.dw_now_month_anchor_live
SET add_loss_state = null
where dt  ='{month}' ;

UPDATE warehouse.dw_now_month_anchor_live t1,
warehouse.dw_now_month_anchor_live t2
SET t1.add_loss_state = 'old'
WHERE t1.anchor_no = t2.anchor_no
AND t1.dt = t2.dt + INTERVAL 1 MONTH
AND t1.dt  ='{month}' ;


UPDATE warehouse.dw_now_month_anchor_live
SET add_loss_state = 'new'
WHERE add_loss_state IS NULL
and dt  ='{month}' ;



-- UPDATE warehouse.dw_now_month_anchor_live
-- SET add_loss_state = null
-- where  dt  ='{month}'  and dt<20200101;


-- -- 首次开播
-- update warehouse.dw_now_month_anchor_live t1,
-- (SELECT anchor_no,concat(left(MIN(dt),7),'-01')  as dt FROM warehouse.dw_now_day_anchor_live
-- WHERE live_status = 1 AND  dt<20200101
-- GROUP BY anchor_no ) t2
-- SET t1.add_loss_state = 'new'
-- WHERE t1.anchor_no = t2.anchor_no
-- AND t1.dt = t2.dt 
-- and t1.dt<20200101 and t1.dt  >='{month}' and t1.dt <= LAST_DAY('{month}'); 

-- -- 首次开播后的算为老主播
-- update warehouse.dw_now_month_anchor_live t1, warehouse.dw_now_month_anchor_live t2
-- set t1.add_loss_state = 'old'
-- where  t1.anchor_no = t2.anchor_no and t2.add_loss_state = 'new'
-- and t1.dt > t2.dt  and t1.dt<20200101; 
