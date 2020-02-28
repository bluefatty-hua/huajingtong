-- 主播 ----


INSERT INTO bireport.rpt_month_all_new_compare
    (dt, idx, yy_val)
SELECT dt, '主播数', anchor_cnt
FROM bireport.rpt_month_all_new
WHERE dt = '{month}'
  AND platform = 'YY'
  AND revenue_level = 'all'
  AND newold_state = 'all'
  AND active_state = 'all'
ON DUPLICATE KEY UPDATE yy_val=VALUES(yy_val)
;



INSERT INTO bireport.rpt_month_all_new_compare
    (dt, idx, hy_val)
SELECT dt, '主播数', anchor_cnt
FROM bireport.rpt_month_all_new
WHERE dt = '{month}'
  AND platform = 'HUYA'
  AND revenue_level = 'all'
  AND newold_state = 'all'
  AND active_state = 'all'
ON DUPLICATE KEY UPDATE hy_val=VALUES(hy_val)
;


INSERT INTO bireport.rpt_month_all_new_compare
    (dt, idx, now_val)
SELECT dt, '主播数', anchor_cnt
FROM bireport.rpt_month_all_new
WHERE dt = '{month}'
  AND platform = 'NOW'
  AND revenue_level = 'all'
  AND newold_state = 'all'
  AND active_state = 'all'
ON DUPLICATE KEY UPDATE now_val=VALUES(now_val)
;


INSERT INTO bireport.rpt_month_all_new_compare
    (dt, idx, bb_val)
SELECT dt, '主播数', anchor_cnt
FROM bireport.rpt_month_all_new
WHERE dt = '{month}'
  AND platform = 'bilibili'
  AND revenue_level = 'all'
  AND newold_state = 'all'
  AND active_state = 'all'
ON DUPLICATE KEY UPDATE bb_val=VALUES(bb_val)
;


INSERT INTO bireport.rpt_month_all_new_compare
    (dt, idx, fx_val)
SELECT dt, '主播数', anchor_cnt
FROM bireport.rpt_month_all_new
WHERE dt = '{month}'
  AND platform = 'FanXing'
  AND revenue_level = 'all'
  AND newold_state = 'all'
  AND active_state = 'all'
ON DUPLICATE KEY UPDATE fx_val=VALUES(fx_val)
;


INSERT INTO bireport.rpt_month_all_new_compare
    (dt, idx, dy_val)
SELECT dt, '主播数', anchor_cnt
FROM bireport.rpt_month_all_new
WHERE dt = '{month}'
  AND platform = 'DouYin'
  AND revenue_level = 'all'
  AND newold_state = 'all'
  AND active_state = 'all'
ON DUPLICATE KEY UPDATE dy_val=VALUES(dy_val)
;



-- 开播 ----


INSERT INTO bireport.rpt_month_all_new_compare
    (dt, idx, yy_val)
SELECT dt, '开播数', live_cnt
FROM bireport.rpt_month_all_new
WHERE dt = '{month}'
  AND platform = 'YY'
  AND revenue_level = 'all'
  AND newold_state = 'all'
  AND active_state = 'all'
ON DUPLICATE KEY UPDATE yy_val=VALUES(yy_val)
;



INSERT INTO bireport.rpt_month_all_new_compare
    (dt, idx, hy_val)
SELECT dt, '开播数', live_cnt
FROM bireport.rpt_month_all_new
WHERE dt = '{month}'
  AND platform = 'HUYA'
  AND revenue_level = 'all'
  AND newold_state = 'all'
  AND active_state = 'all'
ON DUPLICATE KEY UPDATE hy_val=VALUES(hy_val)
;


INSERT INTO bireport.rpt_month_all_new_compare
    (dt, idx, now_val)
SELECT dt, '开播数', live_cnt
FROM bireport.rpt_month_all_new
WHERE dt = '{month}'
  AND platform = 'NOW'
  AND revenue_level = 'all'
  AND newold_state = 'all'
  AND active_state = 'all'
ON DUPLICATE KEY UPDATE now_val=VALUES(now_val)
;


INSERT INTO bireport.rpt_month_all_new_compare
    (dt, idx, bb_val)
SELECT dt, '开播数', live_cnt
FROM bireport.rpt_month_all_new
WHERE dt = '{month}'
  AND platform = 'bilibili'
  AND revenue_level = 'all'
  AND newold_state = 'all'
  AND active_state = 'all'
ON DUPLICATE KEY UPDATE bb_val=VALUES(bb_val)
;


INSERT INTO bireport.rpt_month_all_new_compare
    (dt, idx, fx_val)
SELECT dt, '开播数', live_cnt
FROM bireport.rpt_month_all_new
WHERE dt = '{month}'
  AND platform = 'FanXing'
  AND revenue_level = 'all'
  AND newold_state = 'all'
  AND active_state = 'all'
ON DUPLICATE KEY UPDATE fx_val=VALUES(fx_val)
;


INSERT INTO bireport.rpt_month_all_new_compare
    (dt, idx, dy_val)
SELECT dt, '开播数', live_cnt
FROM bireport.rpt_month_all_new
WHERE dt = '{month}'
  AND platform = 'DouYin'
  AND revenue_level = 'all'
  AND newold_state = 'all'
  AND active_state = 'all'
ON DUPLICATE KEY UPDATE dy_val=VALUES(dy_val)
;



-- 流水 ----


INSERT INTO bireport.rpt_month_all_new_compare
    (dt, idx, yy_val)
SELECT dt, '流水', revenue
FROM bireport.rpt_month_all_new
WHERE dt = '{month}'
  AND platform = 'YY'
  AND revenue_level = 'all'
  AND newold_state = 'all'
  AND active_state = 'all'
ON DUPLICATE KEY UPDATE yy_val=VALUES(yy_val)
;



INSERT INTO bireport.rpt_month_all_new_compare
    (dt, idx, hy_val)
SELECT dt, '流水', revenue
FROM bireport.rpt_month_all_new
WHERE dt = '{month}'
  AND platform = 'HUYA'
  AND revenue_level = 'all'
  AND newold_state = 'all'
  AND active_state = 'all'
ON DUPLICATE KEY UPDATE hy_val=VALUES(hy_val)
;


INSERT INTO bireport.rpt_month_all_new_compare
    (dt, idx, now_val)
SELECT dt, '流水', revenue
FROM bireport.rpt_month_all_new
WHERE dt = '{month}'
  AND platform = 'NOW'
  AND revenue_level = 'all'
  AND newold_state = 'all'
  AND active_state = 'all'
ON DUPLICATE KEY UPDATE now_val=VALUES(now_val)
;


INSERT INTO bireport.rpt_month_all_new_compare
    (dt, idx, bb_val)
SELECT dt, '流水', revenue
FROM bireport.rpt_month_all_new
WHERE dt = '{month}'
  AND platform = 'bilibili'
  AND revenue_level = 'all'
  AND newold_state = 'all'
  AND active_state = 'all'
ON DUPLICATE KEY UPDATE bb_val=VALUES(bb_val)
;


INSERT INTO bireport.rpt_month_all_new_compare
    (dt, idx, fx_val)
SELECT dt, '流水', revenue
FROM bireport.rpt_month_all_new
WHERE dt = '{month}'
  AND platform = 'FanXing'
  AND revenue_level = 'all'
  AND newold_state = 'all'
  AND active_state = 'all'
ON DUPLICATE KEY UPDATE fx_val=VALUES(fx_val)
;


INSERT INTO bireport.rpt_month_all_new_compare
    (dt, idx, dy_val)
SELECT dt, '流水', revenue
FROM bireport.rpt_month_all_new
WHERE dt = '{month}'
  AND platform = 'DouYin'
  AND revenue_level = 'all'
  AND newold_state = 'all'
  AND active_state = 'all'
ON DUPLICATE KEY UPDATE dy_val=VALUES(dy_val)
;


-- 开播人均流水 ----


INSERT INTO bireport.rpt_month_all_new_compare
    (dt, idx, yy_val)
SELECT dt, '开播人均流水', if(live_cnt > 0, revenue / live_cnt, 0)
FROM bireport.rpt_month_all_new
WHERE dt = '{month}'
  AND platform = 'YY'
  AND revenue_level = 'all'
  AND newold_state = 'all'
  AND active_state = 'all'
ON DUPLICATE KEY UPDATE yy_val=VALUES(yy_val)
;



INSERT INTO bireport.rpt_month_all_new_compare
    (dt, idx, hy_val)
SELECT dt, '开播人均流水', if(live_cnt > 0, revenue / live_cnt, 0)
FROM bireport.rpt_month_all_new
WHERE dt = '{month}'
  AND platform = 'HUYA'
  AND revenue_level = 'all'
  AND newold_state = 'all'
  AND active_state = 'all'
ON DUPLICATE KEY UPDATE hy_val=VALUES(hy_val)
;


INSERT INTO bireport.rpt_month_all_new_compare
    (dt, idx, now_val)
SELECT dt, '开播人均流水', if(live_cnt > 0, revenue / live_cnt, 0)
FROM bireport.rpt_month_all_new
WHERE dt = '{month}'
  AND platform = 'NOW'
  AND revenue_level = 'all'
  AND newold_state = 'all'
  AND active_state = 'all'
ON DUPLICATE KEY UPDATE now_val=VALUES(now_val)
;


INSERT INTO bireport.rpt_month_all_new_compare
    (dt, idx, bb_val)
SELECT dt, '开播人均流水', if(live_cnt > 0, revenue / live_cnt, 0)
FROM bireport.rpt_month_all_new
WHERE dt = '{month}'
  AND platform = 'bilibili'
  AND revenue_level = 'all'
  AND newold_state = 'all'
  AND active_state = 'all'
ON DUPLICATE KEY UPDATE bb_val=VALUES(bb_val)
;


INSERT INTO bireport.rpt_month_all_new_compare
    (dt, idx, fx_val)
SELECT dt, '开播人均流水', if(live_cnt > 0, revenue / live_cnt, 0)
FROM bireport.rpt_month_all_new
WHERE dt = '{month}'
  AND platform = 'FanXing'
  AND revenue_level = 'all'
  AND newold_state = 'all'
  AND active_state = 'all'
ON DUPLICATE KEY UPDATE fx_val=VALUES(fx_val)
;


INSERT INTO bireport.rpt_month_all_new_compare
    (dt, idx, dy_val)
SELECT dt, '开播人均流水', if(live_cnt > 0, revenue / live_cnt, 0)
FROM bireport.rpt_month_all_new
WHERE dt = '{month}'
  AND platform = 'DouYin'
  AND revenue_level = 'all'
  AND newold_state = 'all'
  AND active_state = 'all'
ON DUPLICATE KEY UPDATE dy_val=VALUES(dy_val)
;



-- 时长 ----


INSERT INTO bireport.rpt_month_all_new_compare
    (dt, idx, yy_val)
SELECT dt, '时长', duration
FROM bireport.rpt_month_all_new
WHERE dt = '{month}'
  AND platform = 'YY'
  AND revenue_level = 'all'
  AND newold_state = 'all'
  AND active_state = 'all'
ON DUPLICATE KEY UPDATE yy_val=VALUES(yy_val)
;



INSERT INTO bireport.rpt_month_all_new_compare
    (dt, idx, hy_val)
SELECT dt, '时长', duration
FROM bireport.rpt_month_all_new
WHERE dt = '{month}'
  AND platform = 'HUYA'
  AND revenue_level = 'all'
  AND newold_state = 'all'
  AND active_state = 'all'
ON DUPLICATE KEY UPDATE hy_val=VALUES(hy_val)
;


INSERT INTO bireport.rpt_month_all_new_compare
    (dt, idx, now_val)
SELECT dt, '时长', duration
FROM bireport.rpt_month_all_new
WHERE dt = '{month}'
  AND platform = 'NOW'
  AND revenue_level = 'all'
  AND newold_state = 'all'
  AND active_state = 'all'
ON DUPLICATE KEY UPDATE now_val=VALUES(now_val)
;


INSERT INTO bireport.rpt_month_all_new_compare
    (dt, idx, bb_val)
SELECT dt, '时长', duration
FROM bireport.rpt_month_all_new
WHERE dt = '{month}'
  AND platform = 'bilibili'
  AND revenue_level = 'all'
  AND newold_state = 'all'
  AND active_state = 'all'
ON DUPLICATE KEY UPDATE bb_val=VALUES(bb_val)
;


INSERT INTO bireport.rpt_month_all_new_compare
    (dt, idx, fx_val)
SELECT dt, '时长', duration
FROM bireport.rpt_month_all_new
WHERE dt = '{month}'
  AND platform = 'FanXing'
  AND revenue_level = 'all'
  AND newold_state = 'all'
  AND active_state = 'all'
ON DUPLICATE KEY UPDATE fx_val=VALUES(fx_val)
;


INSERT INTO bireport.rpt_month_all_new_compare
    (dt, idx, dy_val)
SELECT dt, '时长', duration
FROM bireport.rpt_month_all_new
WHERE dt = '{month}'
  AND platform = 'DouYin'
  AND revenue_level = 'all'
  AND newold_state = 'all'
  AND active_state = 'all'
ON DUPLICATE KEY UPDATE dy_val=VALUES(dy_val)
;



-- 开播率 ----


INSERT INTO bireport.rpt_month_all_new_compare
    (dt, idx, yy_val)
SELECT dt, '开播率', if(anchor_cnt > 0, round(live_cnt * 100 / anchor_cnt, 0), 0)
FROM bireport.rpt_month_all_new
WHERE dt = '{month}'
  AND platform = 'YY'
  AND revenue_level = 'all'
  AND newold_state = 'all'
  AND active_state = 'all'
ON DUPLICATE KEY UPDATE yy_val=VALUES(yy_val)
;



INSERT INTO bireport.rpt_month_all_new_compare
    (dt, idx, hy_val)
SELECT dt, '开播率', if(anchor_cnt > 0, round(live_cnt * 100 / anchor_cnt, 0), 0)
FROM bireport.rpt_month_all_new
WHERE dt = '{month}'
  AND platform = 'HUYA'
  AND revenue_level = 'all'
  AND newold_state = 'all'
  AND active_state = 'all'
ON DUPLICATE KEY UPDATE hy_val=VALUES(hy_val)
;


INSERT INTO bireport.rpt_month_all_new_compare
    (dt, idx, now_val)
SELECT dt, '开播率', if(anchor_cnt > 0, round(live_cnt * 100 / anchor_cnt, 0), 0)
FROM bireport.rpt_month_all_new
WHERE dt = '{month}'
  AND platform = 'NOW'
  AND revenue_level = 'all'
  AND newold_state = 'all'
  AND active_state = 'all'
ON DUPLICATE KEY UPDATE now_val=VALUES(now_val)
;


INSERT INTO bireport.rpt_month_all_new_compare
    (dt, idx, bb_val)
SELECT dt, '开播率', if(anchor_cnt > 0, round(live_cnt * 100 / anchor_cnt, 0), 0)
FROM bireport.rpt_month_all_new
WHERE dt = '{month}'
  AND platform = 'bilibili'
  AND revenue_level = 'all'
  AND newold_state = 'all'
  AND active_state = 'all'
ON DUPLICATE KEY UPDATE bb_val=VALUES(bb_val)
;


INSERT INTO bireport.rpt_month_all_new_compare
    (dt, idx, fx_val)
SELECT dt, '开播率', if(anchor_cnt > 0, round(live_cnt * 100 / anchor_cnt, 0), 0)
FROM bireport.rpt_month_all_new
WHERE dt = '{month}'
  AND platform = 'FanXing'
  AND revenue_level = 'all'
  AND newold_state = 'all'
  AND active_state = 'all'
ON DUPLICATE KEY UPDATE fx_val=VALUES(fx_val)
;


INSERT INTO bireport.rpt_month_all_new_compare
    (dt, idx, dy_val)
SELECT dt, '开播率', if(anchor_cnt > 0, round(live_cnt * 100 / anchor_cnt, 0), 0)
FROM bireport.rpt_month_all_new
WHERE dt = '{month}'
  AND platform = 'DouYin'
  AND revenue_level = 'all'
  AND newold_state = 'all'
  AND active_state = 'all'
ON DUPLICATE KEY UPDATE dy_val=VALUES(dy_val)
;


-- 开播人均时长 ----


INSERT INTO bireport.rpt_month_all_new_compare
    (dt, idx, yy_val)
SELECT dt, '开播人均时长', if(live_cnt > 0, round(duration / live_cnt, 0), 0)
FROM bireport.rpt_month_all_new
WHERE dt = '{month}'
  AND platform = 'YY'
  AND revenue_level = 'all'
  AND newold_state = 'all'
  AND active_state = 'all'
ON DUPLICATE KEY UPDATE yy_val=VALUES(yy_val)
;



INSERT INTO bireport.rpt_month_all_new_compare
    (dt, idx, hy_val)
SELECT dt, '开播人均时长', if(live_cnt > 0, round(duration / live_cnt, 0), 0)
FROM bireport.rpt_month_all_new
WHERE dt = '{month}'
  AND platform = 'HUYA'
  AND revenue_level = 'all'
  AND newold_state = 'all'
  AND active_state = 'all'
ON DUPLICATE KEY UPDATE hy_val=VALUES(hy_val)
;


INSERT INTO bireport.rpt_month_all_new_compare
    (dt, idx, now_val)
SELECT dt, '开播人均时长', if(live_cnt > 0, round(duration / live_cnt, 0), 0)
FROM bireport.rpt_month_all_new
WHERE dt = '{month}'
  AND platform = 'NOW'
  AND revenue_level = 'all'
  AND newold_state = 'all'
  AND active_state = 'all'
ON DUPLICATE KEY UPDATE now_val=VALUES(now_val)
;


INSERT INTO bireport.rpt_month_all_new_compare
    (dt, idx, bb_val)
SELECT dt, '开播人均时长', if(live_cnt > 0, round(duration / live_cnt, 0), 0)
FROM bireport.rpt_month_all_new
WHERE dt = '{month}'
  AND platform = 'bilibili'
  AND revenue_level = 'all'
  AND newold_state = 'all'
  AND active_state = 'all'
ON DUPLICATE KEY UPDATE bb_val=VALUES(bb_val)
;


INSERT INTO bireport.rpt_month_all_new_compare
    (dt, idx, fx_val)
SELECT dt, '开播人均时长', if(live_cnt > 0, round(duration / live_cnt, 0), 0)
FROM bireport.rpt_month_all_new
WHERE dt = '{month}'
  AND platform = 'FanXing'
  AND revenue_level = 'all'
  AND newold_state = 'all'
  AND active_state = 'all'
ON DUPLICATE KEY UPDATE fx_val=VALUES(fx_val)
;


INSERT INTO bireport.rpt_month_all_new_compare
    (dt, idx, dy_val)
SELECT dt, '开播人均时长', if(live_cnt > 0, round(duration / live_cnt, 0), 0)
FROM bireport.rpt_month_all_new
WHERE dt = '{month}'
  AND platform = 'DouYin'
  AND revenue_level = 'all'
  AND newold_state = 'all'
  AND active_state = 'all'
ON DUPLICATE KEY UPDATE dy_val=VALUES(dy_val)
;
