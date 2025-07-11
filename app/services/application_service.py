import socketio
import asyncio
from core.application import create_app
from core.startup import startup_handler, shutdown_handler

def create_socket_app():
    app = create_app()
    
    sio = socketio.AsyncServer(async_mode="asgi", cors_allowed_origins="*", max_http_buffer_size=1024*1024)
    socket_app = socketio.ASGIApp(sio, other_asgi_app=app)
    
    @app.on_event("startup")
    async def startup():
        await startup_handler()

    @app.on_event("shutdown")
    async def shutdown():
        await shutdown_handler()
    
    return socket_app
