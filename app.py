from fastapi import *
from fastapi.responses import FileResponse
from typing import Annotated
import mysql.connector
import os
from dotenv import load_dotenv
app=FastAPI()
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
# APIS

# Attraction
@app.get('/api/attractions')
async def show_attractions(
	page:int=Query(0,ge=0),
	category:str|None=None,
	keyword:str|None=None,
):
	con=get_db_connection()
	cursor=con.cursor(dictionary=True)
	page_size=8
	sql="""
        SELECT _id,name,CAT,description,address,direction,MRT,latitude,longitude,file
		FROM attractions    
    """
	params=[]

	#if category exits:
	if category:
		sql+="WHERE CAT=%s"
		params.append(category)
	
	if keyword:
		if category:
			sql+="AND (name LIKE %s OR MRT LIKE %s)"
		else:
			sql+="WHERE (name LIKE %s OR MRT LIKE %s)"

		kw = f"%{keyword}%"
		params.extend([kw, kw])
    	
    #page
	sql+="LIMIT %s OFFSET %s"
	params.extend([page_size,page*page_size])
	
	try:
		cursor.execute(sql,params)
		rows=cursor.fetchall()
	except Exception as e:
		return{"error": True, "message": "server database error"}
	finally:
		cursor.close()
		con.close()


	# return json format
	data=[]
	for row in rows:
		images=row['file'].split(',') if row['file'] else []
		mrt_str=row['MRT'] or ""
		# parts=mrt_str.split('/')
		# mrt_list=[]
		# for m in parts:
		# 	m=m.strip()
		# 	if m:
		# 		mrt_list.append(m)
		mrt_value = mrt_str.split('/')[0].strip() if mrt_str else None
		data.append({
			'id':row['_id'],
			'name':row['name'],
			"category":row['CAT'],
			"description":row['description'],
			"address":row['address'],
			"transport":row['direction'],
			'mrt':mrt_value,
			"lat":row['latitude'],
			'lng':row['longitude'],
			"images":images,
		}
		)
	next_page=page+1 if len(data)==page_size else None
	
	if data:
		return{
			'nextPage':next_page,
			'data':data
	}

@app.get('/api/attraction/{attractionId}')
async def attraction_id_data(attractionId:Annotated[int,None]):
	
	con=get_db_connection()
	cursor=con.cursor(dictionary=True)
	sql="""
        SELECT _id,name,CAT,description,address,direction,MRT,latitude,longitude,file
		FROM attractions WHERE _id=%s;
    """
	params=[attractionId]

	try:
		cursor.execute(sql,params)
		rows=cursor.fetchall()
	except Exception as e:
		return{"error": True, "message": "server database error"}
	finally:
		cursor.close()
		con.close()
		
	# return json format
	if not rows:
		return{
			"error": True,
            "message": "No this attraction id"
		}
	row=rows[0]
	images=row['file'].split(',') if row['file'] else []
	mrt_str=row['MRT'] or ""
	# parts=mrt_str.split('/')
	# mrt_list=[]
	# for m in parts:
	# 	m=m.strip()
	# 	if m:
	# 		mrt_list.append(m)
	mrt_value = mrt_str.split('/')[0].strip() if mrt_str else None
	data={
		"id": row["_id"],
        "name": row["name"],
        "category": row["CAT"],
        "description": row["description"],
        "address": row["address"],
        "transport": row["direction"],
        "mrt": mrt_value,
        "lat": row["latitude"],
        "lng": row["longitude"],
        "images": images
		}
	return {"data": data}

# Attraction Category
@app.get('/api/categories')
async def show_categories():
	con=get_db_connection()
	cursor=con.cursor(dictionary=True)
	sql="SELECT DISTINCT CAT FROM attractions"
	
	try:
		cursor.execute(sql)
		rows=cursor.fetchall()
	except Exception as e:
		return{"error": True, "message": "server database error"}
	finally:
		cursor.close()
		con.close()


	# return json format
	if rows:
		data=[]
		print(rows);
		for r in rows:
			data.append(r['CAT'])
	if data:
		return{"data":data}
	
# MRT station
@app.get('/api/mrts')
async def show_mrts():
	con=get_db_connection()
	cursor=con.cursor(dictionary=True)
	sql="""
	SELECT MRT, COUNT(*) AS attraction_count 
	FROM attractions WHERE MRT IS NOT NULL AND MRT !='' 
	GROUP BY MRT 
	ORDER BY attraction_count DESC
	"""
	
	try:
		cursor.execute(sql)
		rows=cursor.fetchall()
	except Exception as e:
		return{"error": True, "message": "server database error"}
	finally:
		cursor.close()
		con.close()
	if rows:
		mrt_list=[]
		for r in rows:
			mrt_list.append(r['MRT'])
		print(mrt_list)
	
	return{
		'data':mrt_list
	}	


# Static Pages (Never Modify Code in this Block)
@app.get("/", include_in_schema=False)
async def index(request: Request):
	return FileResponse("./static/index.html", media_type="text/html")
@app.get("/attraction/{id}", include_in_schema=False)
async def attraction(request: Request, id: int):
	return FileResponse("./static/attraction.html", media_type="text/html")
@app.get("/booking", include_in_schema=False)
async def booking(request: Request):
	return FileResponse("./static/booking.html", media_type="text/html")
@app.get("/thankyou", include_in_schema=False)
async def thankyou(request: Request):
	return FileResponse("./static/thankyou.html", media_type="text/html")