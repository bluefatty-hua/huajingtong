# -*- coding: utf8 -*-

MONITOR_TABLE = [
    'rpt_day_all_new',
]


MONITOR_DIC = {
    'rpt_day_all_new': {
        # 用于判断t-2的数据是否变化(数据刷新的最近7天的数据)
        'judge_sql': '''
                     SELECT n1.dt, 
                     m.platform, 
                     (n1.revenue = m.revenue AND n1.anchor_cnt = m.anchor_cnt AND n1.live_cnt = m.live_cnt) AS jud1,
                     (n2.dt IS NOT NULL AND n2.anchor_cnt > 0 AND n2.live_cnt > 0 AND n2.revenue) AS jud2
                     FROM bireport.rpt_day_all_new n1
                     INNER JOIN (SELECT * FROM stage.monitored WHERE type = 'all') m ON n1.dt = m.dt AND n1.platform = m.platform
                     LEFT JOIN (SELECT * 
                                FROM bireport.rpt_day_all_new
                                WHERE newold_state = 'all'
                                  AND active_state = 'all'
                                  AND revenue_level = 'all'
                                  AND dt = '2020-02-29') n2 ON n1.platform = n2.platform
                     WHERE n1.newold_state = 'all'
                       AND n1.active_state = 'all'
                       AND n1.revenue_level = 'all'
                        -- t-2
                       AND n1.dt = '2020-02-29' - INTERVAL 1 DAY;
    ''',
        'insert_sql': '''
    REPLACE INTO stage.monitored
            SELECT dt, platform, anchor_cnt, live_cnt, revenue, 'all' AS type
            FROM bireport.rpt_day_all_new
            WHERE newold_state = 'all'
              AND active_state = 'all'
              AND revenue_level = 'all'
              -- t-1
              AND dt = '2020-03-03';
    '''
    }
}
