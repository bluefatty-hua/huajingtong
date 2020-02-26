-- 主播 ----


INSERT INTO bireport.rpt_day_all_new_compare
(dt,idx,yy_val)
SELECT dt,'主播数',anchor_cnt FROM  bireport.rpt_day_all_new  
where dt >= '{start_date}' and dt<='{end_date}' and
platform = 'YY' and revenue_level = 'all' and newold_state = 'all' and active_state = 'all'
ON DUPLICATE KEY UPDATE yy_val=values(yy_val);


INSERT INTO bireport.rpt_day_all_new_compare
(dt,idx,hy_val)
SELECT dt,'主播数',anchor_cnt FROM  bireport.rpt_day_all_new  
where dt >= '{start_date}' and dt<='{end_date}' and
platform = 'HUYA' and revenue_level = 'all' and newold_state = 'all' and active_state = 'all'
ON DUPLICATE KEY UPDATE hy_val=values(hy_val);

INSERT INTO bireport.rpt_day_all_new_compare
(dt,idx,now_val)
SELECT dt,'主播数',anchor_cnt FROM  bireport.rpt_day_all_new  
where dt >= '{start_date}' and dt<='{end_date}' and
platform = 'NOW' and revenue_level = 'all' and newold_state = 'all' and active_state = 'all'
ON DUPLICATE KEY UPDATE now_val=values(now_val);

INSERT INTO bireport.rpt_day_all_new_compare
(dt,idx,bb_val)
SELECT dt,'主播数',anchor_cnt FROM  bireport.rpt_day_all_new  
where dt >= '{start_date}' and dt<='{end_date}' and
platform = 'bilibili' and revenue_level = 'all' and newold_state = 'all' and active_state = 'all'
ON DUPLICATE KEY UPDATE bb_val=values(bb_val);

INSERT INTO bireport.rpt_day_all_new_compare
(dt,idx,fx_val)
SELECT dt,'主播数',anchor_cnt FROM  bireport.rpt_day_all_new  
where dt >= '{start_date}' and dt<='{end_date}' and
platform = 'FanXing' and revenue_level = 'all' and newold_state = 'all' and active_state = 'all'
ON DUPLICATE KEY UPDATE fx_val=values(fx_val);

INSERT INTO bireport.rpt_day_all_new_compare
(dt,idx,dy_val)
SELECT dt,'主播数',anchor_cnt FROM  bireport.rpt_day_all_new  
where dt >= '{start_date}' and dt<='{end_date}' and
platform = 'DouYin' and revenue_level = 'all' and newold_state = 'all' and active_state = 'all'
ON DUPLICATE KEY UPDATE dy_val=values(dy_val);


-- 开播 ----


INSERT INTO bireport.rpt_day_all_new_compare
(dt,idx,yy_val)
SELECT dt,'开播数',live_cnt FROM  bireport.rpt_day_all_new  
where dt >= '{start_date}' and dt<='{end_date}' and
platform = 'YY' and revenue_level = 'all' and newold_state = 'all' and active_state = 'all'
ON DUPLICATE KEY UPDATE yy_val=values(yy_val);


INSERT INTO bireport.rpt_day_all_new_compare
(dt,idx,hy_val)
SELECT dt,'开播数',live_cnt FROM  bireport.rpt_day_all_new  
where dt >= '{start_date}' and dt<='{end_date}' and
platform = 'HUYA' and revenue_level = 'all' and newold_state = 'all' and active_state = 'all'
ON DUPLICATE KEY UPDATE hy_val=values(hy_val);

INSERT INTO bireport.rpt_day_all_new_compare
(dt,idx,now_val)
SELECT dt,'开播数',live_cnt FROM  bireport.rpt_day_all_new  
where dt >= '{start_date}' and dt<='{end_date}' and
platform = 'NOW' and revenue_level = 'all' and newold_state = 'all' and active_state = 'all'
ON DUPLICATE KEY UPDATE now_val=values(now_val);

INSERT INTO bireport.rpt_day_all_new_compare
(dt,idx,bb_val)
SELECT dt,'开播数',live_cnt FROM  bireport.rpt_day_all_new  
where dt >= '{start_date}' and dt<='{end_date}' and
platform = 'bilibili' and revenue_level = 'all' and newold_state = 'all' and active_state = 'all'
ON DUPLICATE KEY UPDATE bb_val=values(bb_val);

INSERT INTO bireport.rpt_day_all_new_compare
(dt,idx,fx_val)
SELECT dt,'开播数',live_cnt FROM  bireport.rpt_day_all_new  
where dt >= '{start_date}' and dt<='{end_date}' and
platform = 'FanXing' and revenue_level = 'all' and newold_state = 'all' and active_state = 'all'
ON DUPLICATE KEY UPDATE fx_val=values(fx_val);

INSERT INTO bireport.rpt_day_all_new_compare
(dt,idx,dy_val)
SELECT dt,'开播数',live_cnt FROM  bireport.rpt_day_all_new  
where dt >= '{start_date}' and dt<='{end_date}' and
platform = 'DouYin' and revenue_level = 'all' and newold_state = 'all' and active_state = 'all'
ON DUPLICATE KEY UPDATE dy_val=values(dy_val);



-- 流水 ----


INSERT INTO bireport.rpt_day_all_new_compare
(dt,idx,yy_val)
SELECT dt,'流水',revenue FROM  bireport.rpt_day_all_new  
where dt >= '{start_date}' and dt<='{end_date}' and
platform = 'YY' and revenue_level = 'all' and newold_state = 'all' and active_state = 'all'
ON DUPLICATE KEY UPDATE yy_val=values(yy_val);


INSERT INTO bireport.rpt_day_all_new_compare
(dt,idx,hy_val)
SELECT dt,'流水',revenue FROM  bireport.rpt_day_all_new  
where dt >= '{start_date}' and dt<='{end_date}' and
platform = 'HUYA' and revenue_level = 'all' and newold_state = 'all' and active_state = 'all'
ON DUPLICATE KEY UPDATE hy_val=values(hy_val);

INSERT INTO bireport.rpt_day_all_new_compare
(dt,idx,now_val)
SELECT dt,'流水',revenue FROM  bireport.rpt_day_all_new  
where dt >= '{start_date}' and dt<='{end_date}' and
platform = 'NOW' and revenue_level = 'all' and newold_state = 'all' and active_state = 'all'
ON DUPLICATE KEY UPDATE now_val=values(now_val);

INSERT INTO bireport.rpt_day_all_new_compare
(dt,idx,bb_val)
SELECT dt,'流水',revenue FROM  bireport.rpt_day_all_new  
where dt >= '{start_date}' and dt<='{end_date}' and
platform = 'bilibili' and revenue_level = 'all' and newold_state = 'all' and active_state = 'all'
ON DUPLICATE KEY UPDATE bb_val=values(bb_val);

INSERT INTO bireport.rpt_day_all_new_compare
(dt,idx,fx_val)
SELECT dt,'流水',revenue FROM  bireport.rpt_day_all_new  
where dt >= '{start_date}' and dt<='{end_date}' and
platform = 'FanXing' and revenue_level = 'all' and newold_state = 'all' and active_state = 'all'
ON DUPLICATE KEY UPDATE fx_val=values(fx_val);

INSERT INTO bireport.rpt_day_all_new_compare
(dt,idx,dy_val)
SELECT dt,'流水',revenue FROM  bireport.rpt_day_all_new  
where dt >= '{start_date}' and dt<='{end_date}' and
platform = 'DouYin' and revenue_level = 'all' and newold_state = 'all' and active_state = 'all'
ON DUPLICATE KEY UPDATE dy_val=values(dy_val);

-- 开播人均流水 ----


INSERT INTO bireport.rpt_day_all_new_compare
(dt,idx,yy_val)
SELECT dt,'开播人均流水',if(live_cnt>0,revenue/live_cnt,0) FROM  bireport.rpt_day_all_new  
where dt >= '{start_date}' and dt<='{end_date}' and
platform = 'YY' and revenue_level = 'all' and newold_state = 'all' and active_state = 'all'
ON DUPLICATE KEY UPDATE yy_val=values(yy_val);


INSERT INTO bireport.rpt_day_all_new_compare
(dt,idx,hy_val)
SELECT dt,'开播人均流水',if(live_cnt>0,revenue/live_cnt,0) FROM  bireport.rpt_day_all_new  
where dt >= '{start_date}' and dt<='{end_date}' and
platform = 'HUYA' and revenue_level = 'all' and newold_state = 'all' and active_state = 'all'
ON DUPLICATE KEY UPDATE hy_val=values(hy_val);

INSERT INTO bireport.rpt_day_all_new_compare
(dt,idx,now_val)
SELECT dt,'开播人均流水',if(live_cnt>0,revenue/live_cnt,0) FROM  bireport.rpt_day_all_new  
where dt >= '{start_date}' and dt<='{end_date}' and
platform = 'NOW' and revenue_level = 'all' and newold_state = 'all' and active_state = 'all'
ON DUPLICATE KEY UPDATE now_val=values(now_val);

INSERT INTO bireport.rpt_day_all_new_compare
(dt,idx,bb_val)
SELECT dt,'开播人均流水',if(live_cnt>0,revenue/live_cnt,0) FROM  bireport.rpt_day_all_new  
where dt >= '{start_date}' and dt<='{end_date}' and
platform = 'bilibili' and revenue_level = 'all' and newold_state = 'all' and active_state = 'all'
ON DUPLICATE KEY UPDATE bb_val=values(bb_val);

INSERT INTO bireport.rpt_day_all_new_compare
(dt,idx,fx_val)
SELECT dt,'开播人均流水',if(live_cnt>0,revenue/live_cnt,0) FROM  bireport.rpt_day_all_new  
where dt >= '{start_date}' and dt<='{end_date}' and
platform = 'FanXing' and revenue_level = 'all' and newold_state = 'all' and active_state = 'all'
ON DUPLICATE KEY UPDATE fx_val=values(fx_val);

INSERT INTO bireport.rpt_day_all_new_compare
(dt,idx,dy_val)
SELECT dt,'开播人均流水',if(live_cnt>0,revenue/live_cnt,0) FROM  bireport.rpt_day_all_new  
where dt >= '{start_date}' and dt<='{end_date}' and
platform = 'DouYin' and revenue_level = 'all' and newold_state = 'all' and active_state = 'all'
ON DUPLICATE KEY UPDATE dy_val=values(dy_val);


-- 时长 ----


INSERT INTO bireport.rpt_day_all_new_compare
(dt,idx,yy_val)
SELECT dt,'时长',duration FROM  bireport.rpt_day_all_new  
where dt >= '{start_date}' and dt<='{end_date}' and
platform = 'YY' and revenue_level = 'all' and newold_state = 'all' and active_state = 'all'
ON DUPLICATE KEY UPDATE yy_val=values(yy_val);


INSERT INTO bireport.rpt_day_all_new_compare
(dt,idx,hy_val)
SELECT dt,'时长',duration FROM  bireport.rpt_day_all_new  
where dt >= '{start_date}' and dt<='{end_date}' and
platform = 'HUYA' and revenue_level = 'all' and newold_state = 'all' and active_state = 'all'
ON DUPLICATE KEY UPDATE hy_val=values(hy_val);

INSERT INTO bireport.rpt_day_all_new_compare
(dt,idx,now_val)
SELECT dt,'时长',duration FROM  bireport.rpt_day_all_new  
where dt >= '{start_date}' and dt<='{end_date}' and
platform = 'NOW' and revenue_level = 'all' and newold_state = 'all' and active_state = 'all'
ON DUPLICATE KEY UPDATE now_val=values(now_val);

INSERT INTO bireport.rpt_day_all_new_compare
(dt,idx,bb_val)
SELECT dt,'时长',duration FROM  bireport.rpt_day_all_new  
where dt >= '{start_date}' and dt<='{end_date}' and
platform = 'bilibili' and revenue_level = 'all' and newold_state = 'all' and active_state = 'all'
ON DUPLICATE KEY UPDATE bb_val=values(bb_val);

INSERT INTO bireport.rpt_day_all_new_compare
(dt,idx,fx_val)
SELECT dt,'时长',duration FROM  bireport.rpt_day_all_new  
where dt >= '{start_date}' and dt<='{end_date}' and
platform = 'FanXing' and revenue_level = 'all' and newold_state = 'all' and active_state = 'all'
ON DUPLICATE KEY UPDATE fx_val=values(fx_val);

INSERT INTO bireport.rpt_day_all_new_compare
(dt,idx,dy_val)
SELECT dt,'时长',duration FROM  bireport.rpt_day_all_new  
where dt >= '{start_date}' and dt<='{end_date}' and
platform = 'DouYin' and revenue_level = 'all' and newold_state = 'all' and active_state = 'all'
ON DUPLICATE KEY UPDATE dy_val=values(dy_val);


-- 开播率 ----


INSERT INTO bireport.rpt_day_all_new_compare
(dt,idx,yy_val)
SELECT dt,'开播率',if(anchor_cnt>0,round(live_cnt*100/anchor_cnt,0),0) FROM  bireport.rpt_day_all_new  
where dt >= '{start_date}' and dt<='{end_date}' and
platform = 'YY' and revenue_level = 'all' and newold_state = 'all' and active_state = 'all'
ON DUPLICATE KEY UPDATE yy_val=values(yy_val);


INSERT INTO bireport.rpt_day_all_new_compare
(dt,idx,hy_val)
SELECT dt,'开播率',if(anchor_cnt>0,round(live_cnt*100/anchor_cnt,0),0) FROM  bireport.rpt_day_all_new  
where dt >= '{start_date}' and dt<='{end_date}' and
platform = 'HUYA' and revenue_level = 'all' and newold_state = 'all' and active_state = 'all'
ON DUPLICATE KEY UPDATE hy_val=values(hy_val);

INSERT INTO bireport.rpt_day_all_new_compare
(dt,idx,now_val)
SELECT dt,'开播率',if(anchor_cnt>0,round(live_cnt*100/anchor_cnt,0),0) FROM  bireport.rpt_day_all_new  
where dt >= '{start_date}' and dt<='{end_date}' and
platform = 'NOW' and revenue_level = 'all' and newold_state = 'all' and active_state = 'all'
ON DUPLICATE KEY UPDATE now_val=values(now_val);

INSERT INTO bireport.rpt_day_all_new_compare
(dt,idx,bb_val)
SELECT dt,'开播率',if(anchor_cnt>0,round(live_cnt*100/anchor_cnt,0),0) FROM  bireport.rpt_day_all_new  
where dt >= '{start_date}' and dt<='{end_date}' and
platform = 'bilibili' and revenue_level = 'all' and newold_state = 'all' and active_state = 'all'
ON DUPLICATE KEY UPDATE bb_val=values(bb_val);

INSERT INTO bireport.rpt_day_all_new_compare
(dt,idx,fx_val)
SELECT dt,'开播率',if(anchor_cnt>0,round(live_cnt*100/anchor_cnt,0),0) FROM  bireport.rpt_day_all_new  
where dt >= '{start_date}' and dt<='{end_date}' and
platform = 'FanXing' and revenue_level = 'all' and newold_state = 'all' and active_state = 'all'
ON DUPLICATE KEY UPDATE fx_val=values(fx_val);

INSERT INTO bireport.rpt_day_all_new_compare
(dt,idx,dy_val)
SELECT dt,'开播率',if(anchor_cnt>0,round(live_cnt*100/anchor_cnt,0),0) FROM  bireport.rpt_day_all_new  
where dt >= '{start_date}' and dt<='{end_date}' and
platform = 'DouYin' and revenue_level = 'all' and newold_state = 'all' and active_state = 'all'
ON DUPLICATE KEY UPDATE dy_val=values(dy_val);

-- 开播人均时长 ----


INSERT INTO bireport.rpt_day_all_new_compare
(dt,idx,yy_val)
SELECT dt,'开播人均时长',if(live_cnt>0,round(duration/live_cnt,0),0) FROM  bireport.rpt_day_all_new  
where dt >= '{start_date}' and dt<='{end_date}' and
platform = 'YY' and revenue_level = 'all' and newold_state = 'all' and active_state = 'all'
ON DUPLICATE KEY UPDATE yy_val=values(yy_val);


INSERT INTO bireport.rpt_day_all_new_compare
(dt,idx,hy_val)
SELECT dt,'开播人均时长',if(live_cnt>0,round(duration/live_cnt,0),0) FROM  bireport.rpt_day_all_new  
where dt >= '{start_date}' and dt<='{end_date}' and
platform = 'HUYA' and revenue_level = 'all' and newold_state = 'all' and active_state = 'all'
ON DUPLICATE KEY UPDATE hy_val=values(hy_val);

INSERT INTO bireport.rpt_day_all_new_compare
(dt,idx,now_val)
SELECT dt,'开播人均时长',if(live_cnt>0,round(duration/live_cnt,0),0) FROM  bireport.rpt_day_all_new  
where dt >= '{start_date}' and dt<='{end_date}' and
platform = 'NOW' and revenue_level = 'all' and newold_state = 'all' and active_state = 'all'
ON DUPLICATE KEY UPDATE now_val=values(now_val);

INSERT INTO bireport.rpt_day_all_new_compare
(dt,idx,bb_val)
SELECT dt,'开播人均时长',if(live_cnt>0,round(duration/live_cnt,0),0) FROM  bireport.rpt_day_all_new  
where dt >= '{start_date}' and dt<='{end_date}' and
platform = 'bilibili' and revenue_level = 'all' and newold_state = 'all' and active_state = 'all'
ON DUPLICATE KEY UPDATE bb_val=values(bb_val);

INSERT INTO bireport.rpt_day_all_new_compare
(dt,idx,fx_val)
SELECT dt,'开播人均时长',if(live_cnt>0,round(duration/live_cnt,0),0) FROM  bireport.rpt_day_all_new  
where dt >= '{start_date}' and dt<='{end_date}' and
platform = 'FanXing' and revenue_level = 'all' and newold_state = 'all' and active_state = 'all'
ON DUPLICATE KEY UPDATE fx_val=values(fx_val);

INSERT INTO bireport.rpt_day_all_new_compare
(dt,idx,dy_val)
SELECT dt,'开播人均时长',if(live_cnt>0,round(duration/live_cnt,0),0) FROM  bireport.rpt_day_all_new  
where dt >= '{start_date}' and dt<='{end_date}' and
platform = 'DouYin' and revenue_level = 'all' and newold_state = 'all' and active_state = 'all'
ON DUPLICATE KEY UPDATE dy_val=values(dy_val);