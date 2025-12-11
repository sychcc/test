import mysql.connector
import json
import os
from dotenv import load_dotenv
# load enviroment variable
load_dotenv()
# create mysql database connection
import mysql.connector
def get_db_connection():
    return mysql.connector.connect(
        user=os.getenv('DB_USER'),
        password=os.getenv('DB_PASSWORD'),
        host=os.getenv('DB_HOST'),
        database=os.getenv('DB_DATABASE')
    )
# 【新增】只連線到伺服器的函數
def get_server_connection():
    return mysql.connector.connect(
        user=os.getenv('DB_USER'),
        password=os.getenv('DB_PASSWORD'),
        host=os.getenv('DB_HOST')
        # 沒有 database 參數，所以不會報 Unknown database 錯誤
    )

# 1. 取得資料庫名稱
DB_NAME = os.getenv('DB_DATABASE')
TABLE_NAME = 'attractions'

# 2. **【新增】** 連線到伺服器並創建資料庫
server_con = get_server_connection()
server_cursor = server_con.cursor()

try:
    # 創建資料庫
    server_cursor.execute(f"CREATE DATABASE IF NOT EXISTS {DB_NAME}")
    server_con.commit()
    print(f"INFO: Database '{DB_NAME}' checked/created successfully.")
except Exception as e:
    print(f"ERROR: Failed to create database: {e}")
finally:
    server_cursor.close()
    server_con.close()














# standalone python program to load raw data from json file
# 1. read json file
with open('data/taipei-attractions.json','r',encoding='utf-8')as f:
	data=json.load(f)
rows=data['result']['results']

# 2. coonect to database
con=get_db_connection()
cursor=con.cursor()

create_table_sql="""
CREATE TABLE attractions(
id INT PRIMARY KEY AUTO_INCREMENT NOT NULL,
_id INT,
name VARCHAR(255) NOT NULL,
CAT VARCHAR(255),
description TEXT,
rate INT,
date DATE,
direction TEXT,
MRT VARCHAR(255),
file TEXT,
latitude DECIMAL(9,6),
longitude DECIMAL(9,6),
address VARCHAR(255),
REF_WP INT,
avBegin DATE,
avEnd DATE,
rowNumber INT,
SERIAL_NO VARCHAR(255),
MEMO_TIME TEXT,
POI VARCHAR(255),
idpt VARCHAR(255)
);
"""
cursor.execute(create_table_sql)
con.commit() # 🚨 提交變更，讓表格真正生效！
print("INFO: Table 'attractions' created successfully.")
# 3. filter data to fit what I need

# for URLS(only keep the one which ends with jpg and png)
def filter_img_urls(file_str):
	if not file_str:
		return ""
	parts=file_str.split('https://')
	urls=[] # list
	for p in parts:
		if not p:
			continue
		url='https://'+p.strip()
		if url.lower().endswith(('.jpg','.png')):
			urls.append(url)
	return ",".join(urls) # string
# for date
def to_date(s):
	if not s:
		return None
	return s.replace('/','-')

# 4. insert data to attractions database
insert_sql="""
INSERT INTO attractions(_id, name, CAT, description, rate, date, direction, MRT, file,
 latitude, longitude, address, REF_WP, avBegin, avEnd,
 rowNumber, SERIAL_NO, MEMO_TIME, POI, idpt)VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
"""
for item in rows:
	cursor.execute(insert_sql,(
        item.get("_id"),
        item.get("name"),
        item.get("CAT"),
        item.get("description"),
        item.get("rate"),
        to_date(item.get("date")),
        item.get("direction"),
        item.get("MRT"),
        filter_img_urls(item.get("file", "")), #if no file, return empty string
        float(item["latitude"]) if item.get("latitude") else None,
		# if item.get('latitude'):
		# value=float(item['latitude'])
		# else:
		# value= None
        float(item["longitude"]) if item.get("longitude") else None,
        item.get("address"),
        int(item["REF_WP"]) if item.get("REF_WP") else None,
        to_date(item.get("avBegin")),
        to_date(item.get("avEnd")),
        int(item["RowNumber"]) if item.get("RowNumber") else None,
        item.get("SERIAL_NO"),
        item.get("MEMO_TIME"),
        item.get("POI"),
        item.get("idpt"),
	))
con.commit()
cursor.close()
con.close()
