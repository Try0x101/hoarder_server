import fastapi,socketio,datetime,pytz,asyncio,json
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from fastapi import Request
from app.responses import PrettyJSONResponse
from app.db import init_db,get_latest_data
from app.routers import data,dashboard,history,telemetry,batch,export_import
from timezonefinder import TimezoneFinder

app=fastapi.FastAPI(default_response_class=PrettyJSONResponse)
templates=Jinja2Templates(directory="app/templates")
sio=socketio.AsyncServer(async_mode="asgi",cors_allowed_origins="*")
socket_app=socketio.ASGIApp(sio,other_asgi_app=app)
tf_sio=TimezoneFinder()
active_connections={}

@app.get("/")
async def root_endpoints(request:Request):
 return{
  "server":"hoarder_server IoT Telemetry API",
  "status":"active",
  "version":"1.0.0",
  "timestamp":datetime.datetime.now().isoformat(),
  "endpoints":{
   "GET /":"This endpoint - API documentation",
   "GET /data/latest":"Get latest data from all devices",
   "GET /data/history":"Get historical data with time filtering",
   "GET /data/gaps":"Get analysis of data gaps",
   "GET /data/summary":"Get statistical summary of device data",
   "POST /api/telemetry":"Submit IoT device telemetry data (binary/compressed)",
   "POST /api/batch":"Submit batch telemetry data (full objects)",
   "POST /api/batch-delta":"Submit batch telemetry data (delta objects)",
   "GET /dashboard/":"Web dashboard interface",
   "GET /static/*":"Static files",
   "WS /socket.io/":"Real-time updates via Socket.IO",
   "GET /export/database":"Export database to JSON",
   "POST /import/database":"Import database from JSON"
  },
  "urls":{
   "get":{
    "data_latest":"http://188.132.234.72:5000/data/latest",
    "data_history":"http://188.132.234.72:5000/data/history",
    "dashboard":"http://188.132.234.72:5000/dashboard/",
    "export_database":"http://188.132.234.72:5000/export/database"
   },
   "post":{
    "telemetry":"http://188.132.234.72:5000/api/telemetry",
    "batch":"http://188.132.234.72:5000/api/batch",
    "batch-delta":"http://188.132.234.72:5000/api/batch-delta",
    "import_database":"http://188.132.234.72:5000/import/database"
   },
   "websocket":"ws://188.132.234.72:5000/socket.io/"
  },
  "database":{
   "tables":["device_data","latest_device_states","timestamped_data"],
   "status":"connected",
   "config":{"host":"localhost","database":"database","user":"admin"}
  },
  "features":[
   "Real-time telemetry collection",
   "Weather data enrichment with Open-Meteo API",
   "GPS location tracking with timezone detection",
   "Socket.IO time updates",
   "PostgreSQL storage with JSONB",
   "Compressed data support (gzip/deflate)",
   "Device movement tracking",
   "Weather caching optimization",
   "Marine weather data",
   "Historical data streaming with delta detection",
   "Gap analysis",
   "Activity statistics",
   "Database export/import"
  ]
 }

@sio.event
async def connect(sid,environ):print(f"[{datetime.datetime.now()}] Socket.IO client connected: {sid}")

@sio.event
async def disconnect(sid):
 print(f"[{datetime.datetime.now()}] Socket.IO client disconnected: {sid}")
 if sid in active_connections:
  task=active_connections[sid]['task']
  if not task.done():task.cancel()
  del active_connections[sid]

@sio.on("register_device")
async def register_device(sid,device_id):
 print(f"[{datetime.datetime.now()}] Device {device_id} registered for time updates on {sid}")
 if sid in active_connections:
  task=active_connections[sid]['task']
  if not task.done():task.cancel()
  del active_connections[sid]
 task=sio.start_background_task(send_time_updates,sid,device_id)
 active_connections[sid]={'device_id':device_id,'task':task}

async def send_time_updates(sid,device_id):
 try:
  while True:
   all_latest_data=await get_latest_data()
   current_device_data=next((item for item in all_latest_data if item['device_id']==device_id),None)
   location_time_str=location_date_str=location_timezone_str="N/A"
   if current_device_data and 'payload' in current_device_data:
    payload=current_device_data['payload']
    lat,lon=payload.get('gps_latitude'),payload.get('gps_longitude')
    if lat is not None and lon is not None:
     try:
      tz_name=tf_sio.timezone_at(lng=lon,lat=lat)
      if tz_name:
       tz=pytz.timezone(tz_name)
       now_utc=datetime.datetime.utcnow()
       now_local=pytz.utc.localize(now_utc).astimezone(tz)
       offset=now_local.utcoffset()
       total_seconds=offset.total_seconds()
       hours=int(total_seconds//3600)
       minutes=int((abs(total_seconds)%3600)//60)
       sign='+'if hours>=0 else'-'
       location_timezone_str=f"UTC{sign}{abs(hours)}"if minutes==0 else f"UTC{sign}{abs(hours)}:{abs(minutes):02d}"
       location_date_str=now_local.strftime("%d.%m.%Y")
       location_time_str=now_local.strftime("%H:%M:%S")
     except Exception as e:print(f"[{datetime.datetime.now()}] Error calculating timezone for device {device_id}: {e}")
   await sio.emit("time_update",{"device_id":device_id,"location_date":location_date_str,"location_time":location_time_str,"location_timezone":location_timezone_str},room=sid)
   await asyncio.sleep(1)
 except asyncio.CancelledError:print(f"[{datetime.datetime.now()}] Time update task cancelled for device {device_id}")
 except Exception as e:print(f"[{datetime.datetime.now()}] Error in time update task for device {device_id}: {e}")

@app.on_event("startup")
async def startup():
 print(f"[{datetime.datetime.now()}] Starting hoarder_server...")
 await init_db()
 print(f"[{datetime.datetime.now()}] Database initialized successfully")
 print(f"[{datetime.datetime.now()}] Server ready at http://188.132.234.72:5000")

@app.on_event("shutdown")
async def shutdown():
 print(f"[{datetime.datetime.now()}] Shutting down hoarder_server...")
 for sid,connection in active_connections.items():
  task=connection['task']
  if not task.done():task.cancel()
 active_connections.clear()

app.mount("/static",StaticFiles(directory="app/static"),name="static")
app.include_router(data.router)
app.include_router(dashboard.router,prefix="/dashboard")
app.include_router(telemetry.router)
app.include_router(history.router)
app.include_router(batch.router)
app.include_router(export_import.router)