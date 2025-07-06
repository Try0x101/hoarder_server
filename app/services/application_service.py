import socketio
import asyncio
from app.core.application import create_app
from app.core.startup import startup_handler, shutdown_handler, periodic_maintenance_task
from app.websocket.manager import ConnectionManager
from app.websocket.timezone_manager import SharedTimezoneManager
from app.monitoring.system_monitor import SystemMonitor
from app.monitoring.memory_manager import GlobalMemoryManager
from app.middleware.rate_limiter import rate_limit_middleware
from .websocket_handlers import setup_websocket_events
from .api_endpoints import setup_api_endpoints

def create_socket_app():
    app = create_app()
    app.middleware("http")(rate_limit_middleware)
    
    sio = socketio.AsyncServer(async_mode="asgi", cors_allowed_origins="*", max_http_buffer_size=1024*1024)
    socket_app = socketio.ASGIApp(sio, other_asgi_app=app)
    
    connection_manager = ConnectionManager()
    shared_timezone_manager = SharedTimezoneManager()
    system_monitor = SystemMonitor()
    global_memory = GlobalMemoryManager()
    
    setup_monitoring_middleware(app, system_monitor)
    setup_api_endpoints(app, system_monitor, connection_manager)
    setup_websocket_events(sio, connection_manager, shared_timezone_manager)
    setup_lifecycle_events(app, sio, connection_manager, shared_timezone_manager, system_monitor)
    
    return socket_app

def setup_monitoring_middleware(app, system_monitor):
    @app.middleware("http")
    async def monitoring_middleware(request, call_next):
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

def setup_lifecycle_events(app, sio, connection_manager, shared_timezone_manager, system_monitor):
    @app.on_event("startup")
    async def startup():
        await startup_handler(system_monitor)
        asyncio.create_task(periodic_maintenance_task(connection_manager, shared_timezone_manager, system_monitor))

    @app.on_event("shutdown")
    async def shutdown():
        await shutdown_handler(connection_manager)
