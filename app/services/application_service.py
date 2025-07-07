import socketio
import asyncio
from app.core.application import create_app
from app.core.startup import startup_handler, shutdown_handler, periodic_maintenance_task
from app.realtime.websocket.connection_manager import ConnectionManager
from app.realtime.timezone.manager import SharedTimezoneManager
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
    
    setup_api_endpoints(app, connection_manager)
    setup_websocket_events(sio, connection_manager, shared_timezone_manager)
    setup_lifecycle_events(app, sio, connection_manager, shared_timezone_manager)
    
    return socket_app

def setup_lifecycle_events(app, sio, connection_manager, shared_timezone_manager):
    @app.on_event("startup")
    async def startup():
        await startup_handler()
        asyncio.create_task(periodic_maintenance_task(sio, connection_manager, shared_timezone_manager))

    @app.on_event("shutdown")
    async def shutdown():
        await shutdown_handler(sio, connection_manager)
