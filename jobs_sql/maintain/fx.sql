# DROP TABLE IF EXISTS stage.stage_fx_star_list;
# CREATE TABLE stage.stage_fx_star_list AS
REPLACE INTO stage.stage_fx_star_list
SELECT sl.backend_account_id,
       sl.user_id,
       sl.nick_name,
       sl.avatar_url,
       sl.exclusive_str,
       sl.live_status,
       sl.gender,
       sl.level,
       sl.star_level,
       sl.star_level_icon,
       sl.sign_time,
       sl.latest_time,
       sl.location,
       sl.qq,
       sl.group_name,
       dd.dt,
       sl.timestamp
FROM stage.date_dict dd
         INNER JOIN spider_fx_backend.star_list sl ON dd.month_dt = DATE_FORMAT(sl.dt, '%Y-%m-01')
WHERE dd.dt < 20200209
;
