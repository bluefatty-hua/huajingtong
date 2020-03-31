# -*- coding: utf8 -*-
import logging
from datetime import datetime
import pymysql
from common.log import init_logging
from common.sent_email import send_email
from common.config import LOG_DIR
from common.config import XJL_ETL_DB
from common.config import RADAR_DB



source_db = pymysql.connect(
		host=RADAR_DB['host'],
		port=RADAR_DB['port'],
		user=RADAR_DB['user'],
		password=RADAR_DB['password'],
		charset=RADAR_DB['charset']
)
# 获取一个光标
source_cursor = source_db.cursor(cursor=pymysql.cursors.DictCursor)  # 返回字典数据类型
# pymysql.connections.DEBUG = True

source_sql = "SELECT\
  `id`,\
  `upload_id`,\
  `create_type`,\
  `valid_status`,\
  `valid_msg`,\
  `sheet_id`,\
  `contract_type`,\
  `contract_class`,\
  `anchor_id`,\
  `contract_no1`,\
  `contract_no2`,\
  `contract_no`,\
  `real_name`,\
  `nick_name`,\
  `cert_type`,\
  `id_card`,\
  `live_id`,\
  `yy_channel`,\
  `yy_account`,\
  `corp_subject`,\
  `contract_name`,\
  `sign_date`,\
  `contract_start_date`,\
  `contract_end_date`,\
  `pages`,\
  `contract_pos`,\
  `contract_link`,\
  `platform`,\
  `anchor_no`,\
  `broad_days`,\
  `broad_hours`,\
  `cost_type`,\
  `cost`,\
  `process_id`,\
  `remark`,\
  `dflag`,\
  `dflag_no`,\
  `tenant_id`,\
  `create_time`\
FROM `radar`.`anchor_contract_archive`"

source_cursor.execute(source_sql)
data = source_cursor.fetchall()



etl_db = pymysql.connect(
    host=XJL_ETL_DB['host'],
    port=XJL_ETL_DB['port'],
    user=XJL_ETL_DB['user'],
    password=XJL_ETL_DB['password'],
    charset=XJL_ETL_DB['charset']
)
etl_cursor = etl_db.cursor(cursor=pymysql.cursors.DictCursor)  # 返回字典数据类型
delete_sql = "delete from `warehouse`.`ods_radar_anchor_contract_archive` where dt = %s"
val=(datetime.now().strftime("%Y-%m-%d"))
etl_cursor.execute(delete_sql, val)

insert_sql = "INSERT INTO `warehouse`.`ods_radar_anchor_contract_archive`\
            (`dt`,\
             `id`,\
             `upload_id`,\
             `create_type`,\
             `valid_status`,\
             `valid_msg`,\
             `sheet_id`,\
             `contract_type`,\
             `contract_class`,\
             `anchor_id`,\
             `contract_no1`,\
             `contract_no2`,\
             `contract_no`,\
             `real_name`,\
             `nick_name`,\
             `cert_type`,\
             `id_card`,\
             `live_id`,\
             `yy_channel`,\
             `yy_account`,\
             `corp_subject`,\
             `contract_name`,\
             `sign_date`,\
             `contract_start_date`,\
             `contract_end_date`,\
             `pages`,\
             `contract_pos`,\
             `contract_link`,\
             `platform`,\
             `anchor_no`,\
             `broad_days`,\
             `broad_hours`,\
             `cost_type`,\
             `cost`,\
             `process_id`,\
             `remark`,\
             `tenant_id`,\
             `dflag`,\
             `dflag_no`,\
             `create_time`)\
VALUES (%s,\
        %s,\
        %s,\
        %s,\
        %s,\
        %s,\
        %s,\
        %s,\
        %s,\
        %s,\
        %s,\
        %s,\
        %s,\
        %s,\
        %s,\
        %s,\
        %s,\
        %s,\
        %s,\
        %s,\
        %s,\
        %s,\
        %s,\
        %s,\
        %s,\
        %s,\
        %s,\
        %s,\
        %s,\
        %s,\
        %s,\
        %s,\
        %s,\
        %s,\
        %s,\
        %s,\
        %s,\
        %s,\
        %s,\
        %s);"
vals = []
for row in data:
	val = (datetime.now().strftime("%Y-%m-%d"),
	row['id'],
	row['upload_id'],
	row['create_type'],
	row['valid_status'],
	row['valid_msg'],
	row['sheet_id'],
	row['contract_type'],
	row['contract_class'],
	row['anchor_id'],
	row['contract_no1'],
	row['contract_no2'],
	row['contract_no'],
	row['real_name'],
	row['nick_name'],
	row['cert_type'],
	row['id_card'],
	row['live_id'],
	row['yy_channel'],
	row['yy_account'],
	row['corp_subject'],
	row['contract_name'],
	row['sign_date'],
	row['contract_start_date'],
	row['contract_end_date'],
	row['pages'],
	row['contract_pos'],
	row['contract_link'],
	row['platform'],
	row['anchor_no'],
	row['broad_days'],
	row['broad_hours'],
	row['cost_type'],
	row['cost'],
	row['process_id'],
	row['remark'],
	row['tenant_id'],
	row['dflag'],
	row['dflag_no'],
	row['create_time'])
	vals.append(val)


etl_cursor.executemany(insert_sql, vals)
etl_db.commit()





source_sql = "select\
  `id`,\
  `contract_no`,\
  `real_name`,\
  `contract_name`,\
  `create_type`,\
  `contract_type`,\
  `use_ocr`,\
  `verified`,\
  `contract_content`,\
  `dflag`,\
  `dflag_no`,\
  `tenant_id`,\
  `create_time`\
from `radar`.`anchor_contract`"
source_cursor.execute(source_sql)
data = source_cursor.fetchall()


etl_cursor = etl_db.cursor(cursor=pymysql.cursors.DictCursor)  # 返回字典数据类型
delete_sql = "delete from `warehouse`.`ods_radar_anchor_contract` where dt = %s"
val=(datetime.now().strftime("%Y-%m-%d"))
etl_cursor.execute(delete_sql, val)

insert_sql = "INSERT INTO `warehouse`.`ods_radar_anchor_contract`\
            (`dt`,\
            `id`,\
            `contract_no`,\
            `real_name`,\
            `contract_name`,\
            `create_type`,\
            `contract_type`,\
            `use_ocr`,\
            `verified`,\
            `contract_content`,\
            `dflag`,\
            `dflag_no`,\
            `tenant_id`,\
            `create_time`)\
VALUES (%s,\
        %s,\
        %s,\
        %s,\
        %s,\
        %s,\
        %s,\
        %s,\
        %s,\
        %s,\
        %s,\
        %s,\
        %s,\
        %s\
        );"
vals = []
for row in data:
  val = (datetime.now().strftime("%Y-%m-%d"),
  row['id'],
  row['contract_no'],
  row['real_name'],
  row['contract_name'],
  row['create_type'],
  row['contract_type'],
  row['use_ocr'],
  row['verified'],
  row['contract_content'],
  row['dflag'],
  row['dflag_no'],
  row['tenant_id'],
  row['create_time'])
  vals.append(val)


etl_cursor.executemany(insert_sql, vals)
etl_db.commit()