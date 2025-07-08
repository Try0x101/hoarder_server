import asyncio
import datetime
from database.connection import init_db
from cache import init_redis_pool

async def startup_handler():
    print(f"[{datetime.datetime.now()}] Starting hoarder_api_server v2.0.0...")
    
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
    
    print(f"[{datetime.datetime.now()}] API Server ready")

async def shutdown_handler():
    print(f"[{datetime.datetime.now()}] Shutting down hoarder_api_server...")
    print(f"[{datetime.datetime.now()}] Shutdown complete")
