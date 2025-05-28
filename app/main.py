import fastapi
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from app.db import init_db, get_latest_data
from app.routers import data, dashboard, telemetry
import socketio
import datetime
import pytz
from timezonefinder import TimezoneFinder
import asyncio

app = fastapi.FastAPI()

templates = Jinja2Templates(directory="app/templates")

# Инициализируем Socket.IO сервер
# Важно: 'cors_allowed_origins' настройте в соответствии с вашим фронтендом, если он на другом домене
sio = socketio.AsyncServer(async_mode="asgi", cors_allowed_origins="*")
# Создаем ASGI-приложение для Socket.IO, интегрируя его с FastAPI
socket_app = socketio.ASGIApp(sio, other_asgi_app=app)

# Инициализируем TimezoneFinder один раз
tf_sio = TimezoneFinder()

active_connections = {} # Словарь для отслеживания подключенных устройств и их задач

@sio.event
async def connect(sid, environ):
    print(f"[{datetime.datetime.now()}] Socket.IO: Client connected (SID: {sid})")
    pass

@sio.event
async def disconnect(sid):
    print(f"[{datetime.datetime.now()}] Socket.IO: Client disconnected (SID: {sid})")
    if sid in active_connections:
        task = active_connections[sid]['task']
        if not task.done():
            task.cancel()
        del active_connections[sid]
        print(f"[{datetime.datetime.now()}] Socket.IO: Removed time update task for SID {sid}")

@sio.on("register_device")
async def register_device(sid, device_id):
    print(f"[{datetime.datetime.now()}] Socket.IO: Registering device {device_id} for SID {sid}")
    if sid in active_connections:
        task = active_connections[sid]['task']
        if not task.done():
            task.cancel()
        del active_connections[sid]
        print(f"[{datetime.datetime.now()}] Socket.IO: Cancelled previous task for SID {sid}")

    task = sio.start_background_task(send_time_updates, sid, device_id)
    active_connections[sid] = {'device_id': device_id, 'task': task}
    print(f"[{datetime.datetime.now()}] Socket.IO: Started time update task for device {device_id} (SID: {sid})")

async def send_time_updates(sid, device_id):
    print(f"[{datetime.datetime.now()}] Socket.IO Task: send_time_updates started for device {device_id} (SID: {sid})")
    try:
        while True:
            all_latest_data = await get_latest_data()
            
            current_device_data = next((item for item in all_latest_data if item['device_id'] == device_id), None)

            location_time_str = "N/A"
            location_date_str = "N/A"
            location_timezone_str = "N/A"

            if current_device_data and 'payload' in current_device_data:
                payload = current_device_data['payload']
                lat = payload.get('gps_latitude')
                lon = payload.get('gps_longitude')

                if lat is not None and lon is not None:
                    try:
                        tz_name = tf_sio.timezone_at(lng=lon, lat=lat)
                        if tz_name:
                            tz = pytz.timezone(tz_name)
                            now_utc = datetime.datetime.utcnow()
                            now_local = pytz.utc.localize(now_utc).astimezone(tz)

                            offset = now_local.utcoffset()
                            total_seconds = offset.total_seconds()
                            hours = int(total_seconds // 3600)
                            minutes = int((abs(total_seconds) % 3600) // 60)

                            sign = '+' if hours >= 0 else '-'
                            if minutes == 0:
                                location_timezone_str = f"UTC{sign}{abs(hours)}"
                            else:
                                location_timezone_str = f"UTC{sign}{abs(hours)}:{abs(minutes):02d}"

                            location_date_str = now_local.strftime("%d.%m.%Y")
                            location_time_str = now_local.strftime("%H:%M:%S")
                        else:
                            print(f"[{datetime.datetime.now()}] Socket.IO Task: Timezone not found for device {device_id} at Lat {lat}, Lon {lon}")
                    except Exception as e:
                        print(f"[{datetime.datetime.now()}] Socket.IO Task: Error calculating time for device {device_id}: {e}")
                else:
                    print(f"[{datetime.datetime.now()}] Socket.IO Task: No valid Lat/Lon for device {device_id}. Lat: {lat}, Lon: {lon}")
            else:
                print(f"[{datetime.datetime.now()}] Socket.IO Task: No data found for device {device_id}")
            
            await sio.emit("time_update", {
                "device_id": device_id,
                "location_date": location_date_str,
                "location_time": location_time_str,
                "location_timezone": location_timezone_str
            }, room=sid)
            print(f"[{datetime.datetime.now()}] Socket.IO Task: Emitted time_update for device {device_id} (SID: {sid})")

            await asyncio.sleep(1) # Ждем 1 секунду
    except asyncio.CancelledError:
        print(f"[{datetime.datetime.now()}] Socket.IO Task: send_time_updates for SID {sid} cancelled.")
    except Exception as e:
        print(f"[{datetime.datetime.now()}] Socket.IO Task: send_time_updates encountered an unhandled error for SID {sid}: {e}")


@app.on_event("startup")
async def startup():
    print(f"[{datetime.datetime.now()}] FastAPI: Initializing database...")
    await init_db()
    print(f"[{datetime.datetime.now()}] FastAPI: Database initialized.")

app.mount("/static", StaticFiles(directory="app/static"), name="static")

# ИСПРАВЛЕНИЕ: Удаляем префикс "/data" здесь, чтобы эндпоинт был доступен по /data/latest
app.include_router(data.router) # ПУТЬ БУДЕТ /data/latest

app.include_router(dashboard.router, prefix="/dashboard")
app.include_router(telemetry.router) # Предполагается, что это роутер без префикса, т.е. /api/telemetry

# Важно: эта строка должна быть закомментирована или удалена.
# app.add_websocket_route("/socket.io/", socket_app)