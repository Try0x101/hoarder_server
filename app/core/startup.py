import asyncio
import datetime
from app.db import init_db
from app.cache import init_redis_pool
from app.monitoring.system_monitor import SystemMonitor
from app.realtime.websocket.cleanup import cleanup_stale_connections

async def startup_handler(system_monitor: SystemMonitor):
    print(f"[{datetime.datetime.now()}] Starting hoarder_server v3.3.0 with memory management...")
    
    try:
        await init_db()
        print(f"[{datetime.datetime.now()}] Database initialized")
    except Exception as e:
        system_monitor.add_alert(f"Database initialization failed: {str(e)}")
        print(f"[{datetime.datetime.now()}] Database initialization failed: {e}")
    
    try:
        await init_redis_pool()
        print(f"[{datetime.datetime.now()}] Redis initialized")
    except Exception as e:
        system_monitor.add_alert(f"Redis initialization failed: {str(e)}")
        print(f"[{datetime.datetime.now()}] Redis initialization failed: {e}")
    
    print(f"[{datetime.datetime.now()}] Server ready with memory management and shared timezone computation")

async def shutdown_handler(sio, connection_manager):
    print(f"[{datetime.datetime.now()}] Shutting down hoarder_server...")
    
    disconnect_tasks = []
    for sid in list(connection_manager.connections.keys()):
        disconnect_tasks.append(sio.disconnect(sid))
        
    if disconnect_tasks:
        await asyncio.gather(*disconnect_tasks, return_exceptions=True)
    
    print(f"[{datetime.datetime.now()}] Shutdown complete")

async def periodic_maintenance_task(sio, connection_manager, shared_timezone_manager, system_monitor):
    while True:
        try:
            await cleanup_stale_connections(connection_manager, sio)
            await shared_timezone_manager.broadcast_timezone_updates(sio)
            
            import psutil
            memory_mb = psutil.Process().memory_info().rss / 1024 / 1024
            if memory_mb > 300:
                system_monitor.add_alert(f"High memory usage detected: {memory_mb:.1f}MB")
                import gc
                gc.collect()
            
            system_monitor.cleanup_old_response_times()
                
            await asyncio.sleep(15)
            
        except Exception as e:
            system_monitor.add_alert(f"Maintenance task error: {str(e)}")
            print(f"[{datetime.datetime.now()}] Maintenance error: {e}")
            await asyncio.sleep(60)
