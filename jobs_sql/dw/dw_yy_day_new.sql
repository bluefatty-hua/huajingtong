UPDATE warehouse.dw_yy_day_anchor_live
SET add_loss_state = null
where dt  >='{month}' and dt <= LAST_DAY('{month}');

UPDATE warehouse.dw_yy_day_anchor_live t1,
warehouse.dw_yy_day_anchor_live t2
SET t1.add_loss_state = 'old'
WHERE t1.anchor_uid = t2.anchor_uid
AND t1.dt = t2.dt + INTERVAL 1 DAY
AND t1.dt  >='{month}' and t1.dt <= LAST_DAY('{month}');


UPDATE warehouse.dw_yy_day_anchor_live
SET add_loss_state = 'new'
WHERE add_loss_state IS NULL
and dt  >='{month}' and dt <= LAST_DAY('{month}');