
-- ---------------------rpt_day_bb_guild_new-----------------------------
update bireport.rpt_day_bb_guild set 
  new_r60_cnt = 0
where dt  >='{month}' and dt <= LAST_DAY('{month}');

INSERT INTO bireport.rpt_day_bb_guild
(
  `dt`,
  `platform_id`,
  `platform`,
  `backend_account_id`,
  `remark`,
  `revenue_level`,
  `newold_state`,
  `active_state`,
  `new_r60_cnt`
  
)
SELECT t.dt,
       t.platform_id,
       t.platform_name                                                         AS platform,
       t.backend_account_id,
       ai.remark,
       t.revenue_level,
       t.newold_state,
       t.active_state,
       t.new_r60_cnt as new_r60_cnt
FROM warehouse.dw_bb_day_guild_live t
         LEFT JOIN spider_bb_backend.account_info ai ON t.backend_account_id = ai.backend_account_id
WHERE t.dt  >='{month}' and t.dt <= LAST_DAY('{month}')
ON DUPLICATE KEY UPDATE `new_r60_cnt`=values(new_r60_cnt);



insert INTO bireport.rpt_day_bb_guild 
(
  `dt`,
  `platform_id`,
  `platform`,
  `backend_account_id`,
  `remark`,
  `revenue_level`,
  `newold_state`,
  `active_state`,
  `new_r60_cnt`
)
SELECT t.dt,
       t.platform_id,
       t.platform,
       t.backend_account_id,
       IFNULL(ai.remark, 'all') AS remark,
       t.revenue_level,
       t.newold_state,
       t.active_state,
       t.new_r60_cnt
FROM (SELECT dt,
             MAX(platform_id)              AS platform_id,
             MAX(platform)                 AS platform,
             IFNULL(backend_account_id, 0) AS backend_account_id,
             IFNULL(revenue_level, 'all')  AS revenue_level,
             IFNULL(newold_state, 'all')   AS newold_state,
             IFNULL(active_state, 'all')   AS active_state,
             SUM(new_r60_cnt)              AS new_r60_cnt
      FROM bireport.rpt_day_bb_guild
      WHERE backend_account_id != 0
        AND revenue_level != 'all'
        AND newold_state != 'all'
        AND active_state != 'all'
        AND dt >= '{month}'
        AND dt <= LAST_DAY('{month}')
      GROUP BY dt, backend_account_id, revenue_level, newold_state, active_state
      WITH ROLLUP

      UNION

      SELECT dt,
             MAX(platform_id)              AS platform_id,
             MAX(platform)                 AS platform,
             IFNULL(backend_account_id, 0) AS backend_account_id,
             IFNULL(revenue_level, 'all')  AS revenue_level,
             IFNULL(newold_state, 'all')   AS newold_state,
             IFNULL(active_state, 'all')   AS active_state,
             SUM(new_r60_cnt)              AS new_r60_cnt
      FROM bireport.rpt_day_bb_guild
      WHERE backend_account_id != 0
        AND revenue_level != 'all'
        AND newold_state != 'all'
        AND active_state != 'all'
        AND dt >= '{month}'
        AND dt <= LAST_DAY('{month}')
      GROUP BY dt, revenue_level, newold_state, active_state, backend_account_id
      WITH ROLLUP

      UNION

      SELECT dt,
             MAX(platform_id)              AS platform_id,
             MAX(platform)                 AS platform,
             IFNULL(backend_account_id, 0) AS backend_account_id,
             IFNULL(revenue_level, 'all')  AS revenue_level,
             IFNULL(newold_state, 'all')   AS newold_state,
             IFNULL(active_state, 'all')   AS active_state,
             SUM(new_r60_cnt)                AS new_r60_cnt
      FROM bireport.rpt_day_bb_guild
      WHERE backend_account_id != 0
        AND revenue_level != 'all'
        AND newold_state != 'all'
        AND active_state != 'all'
        AND dt >= '{month}'
        AND dt <= LAST_DAY('{month}')
      GROUP BY dt, newold_state, active_state, backend_account_id, revenue_level
      WITH ROLLUP

      UNION

      SELECT dt,
             MAX(platform_id)              AS platform_id,
             MAX(platform)                 AS platform,
             IFNULL(backend_account_id, 0) AS backend_account_id,
             IFNULL(revenue_level, 'all')  AS revenue_level,
             IFNULL(newold_state, 'all')   AS newold_state,
             IFNULL(active_state, 'all')   AS active_state,
             SUM(new_r60_cnt)                AS new_r60_cnt
             
      FROM bireport.rpt_day_bb_guild
      WHERE backend_account_id != 0
        AND revenue_level != 'all'
        AND newold_state != 'all'
        AND active_state != 'all'
        AND dt >= '{month}'
        AND dt <= LAST_DAY('{month}')
      GROUP BY dt, active_state, backend_account_id, revenue_level, newold_state
      WITH ROLLUP

      UNION ALL
      SELECT dt,
             MAX(platform_id)              AS platform_id,
             MAX(platform)                 AS platform,
             IFNULL(backend_account_id, 0) AS backend_account_id,
             IFNULL(revenue_level, 'all')  AS revenue_level,
             IFNULL(newold_state, 'all')   AS newold_state,
             IFNULL(active_state, 'all')   AS active_state,
             SUM(new_r60_cnt)                AS new_r60_cnt
             
      FROM bireport.rpt_day_bb_guild
      WHERE backend_account_id != 0
        AND revenue_level != 'all'
        AND newold_state != 'all'
        AND active_state != 'all'
        AND dt >= '{month}'
        AND dt <= LAST_DAY('{month}')
      GROUP BY dt, backend_account_id, newold_state, revenue_level, active_state
      WITH ROLLUP
     ) t
         LEFT JOIN warehouse.ods_bb_account_info ai ON t.backend_account_id = ai.backend_account_id
WHERE dt IS NOT NULL
ON DUPLICATE KEY UPDATE `new_r60_cnt`=values(new_r60_cnt);



-- 报表用，计算上周、上月同期数据---
update bireport.rpt_day_bb_guild_view
  set new_r60_ratio = null
WHERE dt >= '{month}'
  AND dt <= LAST_DAY('{month}');
INSERT INTO bireport.rpt_day_bb_guild_view
(
  `dt`,
  `remark`,
  `revenue_level`,
  `newold_state`,
  `active_state`,
  `new_r60_ratio`
  
  )
SELECT t1.dt,
       t1.remark,
       t1.revenue_level,
       t1.newold_state,
       t1.active_state,
       if(t1.new_anchor_cnt=0,null,concat(ROUND(t1.new_r60_cnt/t1.new_anchor_cnt*100,1),'%'))  as new_r60_ratio
FROM bireport.rpt_day_bb_guild t1
WHERE t1.dt >= '{month}'
  AND t1.dt <= LAST_DAY('{month}')
ON DUPLICATE KEY UPDATE `new_r60_ratio`=values(new_r60_ratio);







update bireport.rpt_month_bb_guild set 
  new_r60_cnt = 0
where dt  ='{month}' ;

INSERT INTO bireport.rpt_month_bb_guild
(
  `dt`,
  `platform_id`,
  `platform`,
  `backend_account_id`,
  `remark`,
  `revenue_level`,
  `newold_state`,
  `active_state`,
  `new_r60_cnt`
  
 
)
SELECT gl.dt,
       gl.platform_id,
       pf.platform_name   AS platform,
       gl.backend_account_id,
       ai.remark,
       gl.revenue_level,
       gl.newold_state,
       gl.active_state,
       gl.new_r60_cnt as new_r60_cnt
 
FROM warehouse.dw_bb_month_guild_live gl
-- FROM stage.stage_rpt_bb_month_guild_live gl
         LEFT JOIN spider_bb_backend.account_info ai ON gl.backend_account_id = ai.backend_account_id
         lEFT JOIN warehouse.platform pf ON pf.id = gl.platform_id
WHERE dt = '{month}'
ON DUPLICATE KEY UPDATE `new_r60_cnt`=values(new_r60_cnt);







insert INTO bireport.rpt_month_bb_guild
(
  `dt`,
  `platform_id`,
  `platform`,
  `backend_account_id`,
  `remark`,
  `revenue_level`,
  `newold_state`,
  `active_state`,
  `new_r60_cnt`
  
)
SELECT t.dt,
       t.platform_id,
       t.platform,
       t.backend_account_id,
       CASE WHEN t.backend_account_id = 0 THEN 'all' ELSE ai.remark END AS remark,
       t.revenue_level,
       t.newold_state,
       t.active_state,
       t.`new_r60_cnt`
FROM (
         SELECT dt,
                MAX(platform_id)              AS platform_id,
                MAX(platform)                 AS platform,
                IFNULL(backend_account_id, 0) AS backend_account_id,
                IFNULL(revenue_level, 'all')  AS revenue_level,
                IFNULL(newold_state, 'all')   AS newold_state,
                IFNULL(active_state, 'all')   AS active_state,
                SUM(new_r60_cnt)                AS new_r60_cnt
         FROM bireport.rpt_month_bb_guild
         WHERE backend_account_id != 0
           AND revenue_level != 'all'
           AND newold_state != 'all'
           AND active_state != 'all'
           AND dt = '{month}'
         GROUP BY dt, backend_account_id, revenue_level, newold_state, active_state
         WITH ROLLUP

         UNION

         SELECT dt,
                MAX(platform_id)              AS platform_id,
                MAX(platform)                 AS platform,
                IFNULL(backend_account_id, 0) AS backend_account_id,
                IFNULL(revenue_level, 'all')  AS revenue_level,
                IFNULL(newold_state, 'all')   AS newold_state,
                IFNULL(active_state, 'all')   AS active_state,
                SUM(new_r60_cnt)                AS new_r60_cnt
         FROM bireport.rpt_month_bb_guild
         WHERE backend_account_id != 0
           AND revenue_level != 'all'
           AND newold_state != 'all'
           AND active_state != 'all'
           AND dt = '{month}'
         GROUP BY dt, revenue_level, newold_state, active_state, backend_account_id
         WITH ROLLUP

         UNION

         SELECT dt,
                MAX(platform_id)              AS platform_id,
                MAX(platform)                 AS platform,
                IFNULL(backend_account_id, 0) AS backend_account_id,
                IFNULL(revenue_level, 'all')  AS revenue_level,
                IFNULL(newold_state, 'all')   AS newold_state,
                IFNULL(active_state, 'all')   AS active_state,
                SUM(new_r60_cnt)                AS new_r60_cnt
         FROM bireport.rpt_month_bb_guild
         WHERE backend_account_id != 0
           AND revenue_level != 'all'
           AND newold_state != 'all'
           AND active_state != 'all'
           AND dt = '{month}'
         GROUP BY dt, newold_state, active_state, backend_account_id, revenue_level
         WITH ROLLUP

         UNION

         SELECT dt,
                MAX(platform_id)              AS platform_id,
                MAX(platform)                 AS platform,
                IFNULL(backend_account_id, 0) AS backend_account_id,
                IFNULL(revenue_level, 'all')  AS revenue_level,
                IFNULL(newold_state, 'all')   AS newold_state,
                IFNULL(active_state, 'all')   AS active_state,
                SUM(new_r60_cnt)                AS new_r60_cnt
         FROM bireport.rpt_month_bb_guild
         WHERE backend_account_id != 0
           AND revenue_level != 'all'
           AND newold_state != 'all'
           AND active_state != 'all'
           AND dt = '{month}'
         GROUP BY dt, active_state, backend_account_id, revenue_level, newold_state
         WITH ROLLUP
         UNION

         SELECT dt,
                MAX(platform_id)              AS platform_id,
                MAX(platform)                 AS platform,
                IFNULL(backend_account_id, 0) AS backend_account_id,
                IFNULL(revenue_level, 'all')  AS revenue_level,
                IFNULL(newold_state, 'all')   AS newold_state,
                IFNULL(active_state, 'all')   AS active_state,
                SUM(new_r60_cnt)                AS new_r60_cnt
         FROM bireport.rpt_month_bb_guild
         WHERE backend_account_id != 0
           AND revenue_level != 'all'
           AND newold_state != 'all'
           AND active_state != 'all'
           AND dt = '{month}'
         GROUP BY dt, backend_account_id, newold_state, revenue_level, active_state
         WITH ROLLUP) t
         LEFT JOIN spider_bb_backend.account_info ai
                   ON t.backend_account_id = ai.backend_account_id
WHERE dt IS NOT NULL
ON DUPLICATE KEY UPDATE `new_r60_cnt`=values(new_r60_cnt);

-- 报表用，计算上周、上月同期数据---
update bireport.rpt_month_bb_guild_view
  set new_r60_ratio = null
WHERE dt = '{month}';

INSERT INTO bireport.rpt_month_bb_guild_view
(
  `dt`,
  `remark`,
  `revenue_level`,
  `newold_state`,
  `active_state`,
  `new_r60_ratio`
)
SELECT t1.dt,
       t1.remark,
       t1.revenue_level,
       t1.newold_state,
       t1.active_state,
       if(t1.new_anchor_cnt=0,null,concat(ROUND(t1.new_r60_cnt/t1.new_anchor_cnt*100,1),'%'))  as new_r60_cnt
FROM bireport.rpt_month_bb_guild t1
WHERE t1.dt = '{month}'
ON DUPLICATE KEY UPDATE `new_r60_ratio`=values(new_r60_ratio);

