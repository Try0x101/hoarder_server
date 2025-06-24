import fastapi,socketio,datetime,pytz,asyncio,json
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from fastapi.responses import JSONResponse
from fastapi import Request
from app.db import init_db,get_latest_data
from app.routers import data,dashboard,telemetry
from timezonefinder import TimezoneFinder
class PrettyJSONResponse(JSONResponse):
 def render(self,content):return json.dumps(content,ensure_ascii=False,allow_nan=False,indent=2,separators=(",",": ")).encode("utf-8")
app=fastapi.FastAPI(default_response_class=PrettyJSONResponse)
templates=Jinja2Templates(directory="app/templates")
sio=socketio.AsyncServer(async_mode="asgi",cors_allowed_origins="*")
socket_app=socketio.ASGIApp(sio,other_asgi_app=app)
tf_sio=TimezoneFinder()
active_connections={}
@app.get("/")
async def root_endpoints(request:Request):
 return{"server":"hoarder_server IoT Telemetry API","status":"active","version":"1.0.0","timestamp":datetime.datetime.now().isoformat(),"endpoints":{"GET /":"This endpoint - API documentation","GET /data/latest":"Get latest data from all devices","POST /api/telemetry":"Submit IoT device telemetry data (binary/compressed)","GET /dashboard/":"Web dashboard interface","GET /static/*":"Static files","WS /socket.io/":"Real-time updates via Socket.IO"},"urls":{"data_latest":"http://188.132.234.72:5000/data/latest","telemetry":"http://188.132.234.72:5000/api/telemetry","dashboard":"http://188.132.234.72:5000/dashboard/","websocket":"ws://188.132.234.72:5000/socket.io/"},"database":{"tables":["device_data","latest_device_states"],"status":"connected","config":{"host":"localhost","database":"database","user":"admin"}},"features":["Real-time telemetry collection","Weather data enrichment with Open-Meteo API","GPS location tracking with timezone detection","Socket.IO time updates","PostgreSQL storage with JSONB","Compressed data support (gzip/deflate)","Device movement tracking","Weather caching optimization","Marine weather data"],"weather_api":{"primary":"https://api.open-meteo.com/v1/forecast","marine":"https://marine-api.open-meteo.com/v1/marine","fallback":"https://wttr.in","daily_limit":9000,"cache_duration":"1 hour","distance_threshold":"1.0 km"},"server_info":{"host":"0.0.0.0","port":5000,"workers":1,"framework":"FastAPI + Socket.IO","python":"3.10+","database":"PostgreSQL"}}
@sio.event
async def connect(sid,environ):pass
@sio.event
async def disconnect(sid):
 if sid in active_connections:
  task=active_connections[sid]['task']
  if not task.done():task.cancel()
  del active_connections[sid]
@sio.on("register_device")
async def register_device(sid,device_id):
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
     except:pass
   await sio.emit("time_update",{"device_id":device_id,"location_date":location_date_str,"location_time":location_time_str,"location_timezone":location_timezone_str},room=sid)
   await asyncio.sleep(1)
 except asyncio.CancelledError:pass
 except:pass
@app.on_event("startup")
async def startup():await init_db()
app.mount("/static",StaticFiles(directory="app/static"),name="static")
app.include_router(data.router)
app.include_router(dashboard.router,prefix="/dashboard")
app.include_router(telemetry.router)