import asyncio
import datetime
from app.db import init_db
from app.cache import init_redis_pool
from app.realtime.websocket.cleanup import cleanup_stale_connections

async def startup_handler():
    print(f"[{datetime.datetime.now()}] Starting hoarder_server v3.3.0...")
    
    try:
        await init_db()
        print(f"[{datetime.datetime.now()}] Database initialized")
    except Exception as e:
        print(f"[{datetime.datetime.now()}] Database initialization failed: {e}")
    
    try:
        await init_redis_pool()
        print(f"[{datetime.datetime.now()}] Redis initialized")
    except Exception as e:
        print(f"[{datetime.datetime.now()}] Redis initialization failed: {e}")
    
    print(f"[{datetime.datetime.now()}] Server ready")

async def shutdown_handler(sio, connection_manager):
    print(f"[{datetime.datetime.now()}] Shutting down hoarder_server...")
    
    disconnect_tasks = []
    for sid in list(connection_manager.connections.keys()):
        disconnect_tasks.append(sio.disconnect(sid))
        
    if disconnect_tasks:
        await asyncio.gather(*disconnect_tasks, return_exceptions=True)
    
    print(f"[{datetime.datetime.now()}] Shutdown complete")

async def periodic_maintenance_task(sio, connection_manager, shared_timezone_manager):
    while True:
        try:
            await cleanup_stale_connections(connection_manager, sio)
            await shared_timezone_manager.broadcast_timezone_updates(sio)
            await asyncio.sleep(15)
        except Exception as e:
            print(f"[{datetime.datetime.now()}] Maintenance error: {e}")
            await asyncio.sleep(60)
