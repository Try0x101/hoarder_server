import socketio
import datetime
import asyncio
import os
from fastapi import Request
from app.core.application import create_app
from app.core.startup import startup_handler, shutdown_handler, periodic_maintenance_task
from app.websocket.manager import ConnectionManager
from app.websocket.timezone_manager import SharedTimezoneManager
from app.monitoring.system_monitor import SystemMonitor
from app.monitoring.memory_manager import GlobalMemoryManager
from app.responses import PrettyJSONResponse
from app.db import (
    get_database_size, get_total_records_summary,
    get_top_devices_by_records, get_raw_latest_payload_for_device, get_pool_stats
)
from app.cache import get_redis_status
from app.middleware.rate_limiter import rate_limit_middleware, rate_limiter
from app.weather.cache import get_cache_stats
from app.transforms.device import safe_float

app = create_app()
app.middleware("http")(rate_limit_middleware)

sio = socketio.AsyncServer(async_mode="asgi", cors_allowed_origins="*", max_http_buffer_size=1024*1024)
socket_app = socketio.ASGIApp(sio, other_asgi_app=app)

connection_manager = ConnectionManager()
shared_timezone_manager = SharedTimezoneManager()
system_monitor = SystemMonitor()
global_memory = GlobalMemoryManager()

@app.middleware("http")
async def monitoring_middleware(request: Request, call_next):
    import time
    start_time = time.time()
    
    try:
        response = await call_next(request)
        process_time = time.time() - start_time
        
        endpoint = f"{request.method} {request.url.path}"
        system_monitor.record_request(endpoint, process_time, response.status_code)
        
        response.headers["X-Process-Time"] = f"{process_time:.4f}"
        response.headers["X-Server-Health"] = system_monitor.health_status
        
        return response
        
    except Exception as e:
        process_time = time.time() - start_time
        endpoint = f"{request.method} {request.url.path}"
        system_monitor.record_request(endpoint, process_time, 500)
        system_monitor.add_alert(f"Request failed: {endpoint} - {str(e)}")
        raise

@app.get("/", response_class=PrettyJSONResponse)
async def root_endpoints(request: Request):
    base_url = f"{request.url.scheme}://{request.url.netloc}"
    
    try:
        db_size = await get_database_size()
        records_summary = await get_total_records_summary()
        top_devices = await get_top_devices_by_records(limit=5)
    except Exception:
        db_size = "unavailable"
        records_summary = {"error": "database_unavailable"}
        top_devices = []

    system_health = system_monitor.get_system_health()
    connection_stats = connection_manager.get_stats()

    return {
        "server": "Hoarder Server - Advanced IoT Telemetry Platform",
        "version": "3.3.0",
        "status": system_health['health_status'],
        "timestamp_utc": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "system_health": {
            "status": system_health['health_status'],
            "score": system_health['health_score'],
            "uptime_seconds": system_health['uptime_seconds'],
            "memory_usage_mb": system_health['memory_usage_mb'],
            "total_requests": system_health['total_requests'],
            "error_rate_percent": system_health['error_rate_percent']
        },
        "statistics": {
            "average_response_time_ms": system_health['avg_response_time_ms'],
            "database_size": db_size,
            "total_records": records_summary,
            "top_devices_by_records": top_devices,
            "websocket_stats": connection_stats
        },
        "endpoints": {
            "self": f"{base_url}/",
            "system_health": f"{base_url}/api/system/health",
            "latest_data_all_devices": f"{base_url}/data/latest",
            "device_specific_latest_data": f"{base_url}/data/latest/{{device_id}}",
            "device_history": f"{base_url}/data/history?device_id={{device_id}}",
            "websocket": f"ws://{request.url.netloc}/socket.io/",
            "monitoring": f"{base_url}/api/system/monitor"
        }
    }

@app.get("/api/system/health")
async def system_health():
    try:
        redis_status = await get_redis_status()
        pool_stats = await get_pool_stats()
        cache_stats = await get_cache_stats()
        rate_limit_stats = rate_limiter.get_stats()
        
        system_health = system_monitor.get_system_health()
        
        return {
            "overall_health": system_health,
            "components": {
                "database": {
                    "status": "healthy" if pool_stats.get("healthy", False) else "degraded",
                    "pool_stats": pool_stats
                },
                "redis": {
                    "status": "healthy" if redis_status.get("redis_healthy", False) else "degraded",
                    "redis_stats": redis_status
                },
                "file_system": {
                    "status": "healthy" if not cache_stats.get("emergency_mode", False) else "critical",
                    "cache_stats": cache_stats
                },
                "rate_limiter": {"status": "active", "stats": rate_limit_stats},
                "websockets": {"status": "active", "stats": connection_manager.get_stats()},
                "memory_management": {"status": "active", "stats": global_memory.get_memory_stats()}
            }
        }
        
    except Exception as e:
        system_monitor.add_alert(f"Health check failed: {str(e)}")
        return {
            "overall_health": {"health_status": "critical", "error": str(e)},
            "components": {"error": "Health check failed"}
        }

@app.get("/api/system/monitor")
async def system_monitor_endpoint():
    health = system_monitor.get_system_health()
    endpoint_stats = system_monitor.get_endpoint_stats(20)
    
    return {
        "system_health": health,
        "top_endpoints": endpoint_stats,
        "shared_timezone_stats": shared_timezone_manager.get_stats(),
        "global_memory_stats": global_memory.get_memory_stats()
    }

@sio.event
async def connect(sid, environ):
    client_ip = connection_manager._get_client_ip(environ)
    
    if not await connection_manager.can_accept_connection(client_ip):
        await sio.disconnect(sid)
        return False
    
    connection_manager.add_connection(sid, environ)
    print(f"[{datetime.datetime.now()}] WebSocket connected: {sid} from {client_ip} ({len(connection_manager.connections)} total)")

@sio.event
async def disconnect(sid):
    await connection_manager.remove_connection(sid)
    print(f"[{datetime.datetime.now()}] WebSocket disconnected: {sid}")

@sio.event
async def ping(sid):
    connection_manager.update_ping(sid)

@sio.on("register_device")
async def register_device(sid, device_id):
    if not await connection_manager.register_device(sid, device_id):
        return
        
    print(f"[{datetime.datetime.now()}] Device {device_id} registered for updates on {sid}")
    
    try:
        payload = await get_raw_latest_payload_for_device(device_id)
        if payload:
            lat, lon = safe_float(payload.get('lat')), safe_float(payload.get('lon'))
            if lat is not None and lon is not None:
                coord_key = await shared_timezone_manager.subscribe_connection(sid, device_id, lat, lon)
                if coord_key and sid in connection_manager.connections:
                    connection_manager.connections[sid]['coord_key'] = coord_key
    except Exception as e:
        print(f"[{datetime.datetime.now()}] Error registering device timezone: {e}")

@app.on_event("startup")
async def startup():
    await startup_handler(system_monitor)
    asyncio.create_task(periodic_maintenance_task(connection_manager, shared_timezone_manager, system_monitor))

@app.on_event("shutdown")
async def shutdown():
    await shutdown_handler(connection_manager)
