import asyncpg
import asyncio
from typing import Optional

DB_CONFIG = {
    "user": "admin",
    "password": "admin", 
    "database": "hoarder_api",
    "host": "localhost"
}

pool: Optional[asyncpg.Pool] = None
_init_lock = asyncio.Lock()

async def init_db():
    global pool
    async with _init_lock:
        if pool and not pool._closed:
            return
        try:
            pool = await asyncpg.create_pool(
                **DB_CONFIG,
                min_size=5,
                max_size=15,
                timeout=5,
                command_timeout=15
            )
            print("API database pool initialized")
        except Exception as e:
            print(f"Database initialization failed: {e}")
            pool = None

async def close_db():
    global pool
    if pool and not pool._closed:
        await pool.close()
        print("API database pool closed")

async def get_pool():
    if pool is None:
        await init_db()
    return pool

async def safe_db_operation(operation_func, *args, **kwargs):
    max_retries = 2
    for attempt in range(max_retries):
        try:
            pool_instance = await get_pool()
            if not pool_instance:
                raise Exception("Database pool not available")
            
            async with pool_instance.acquire() as conn:
                return await asyncio.wait_for(
                    operation_func(conn, *args, **kwargs),
                    timeout=15
                )
        except Exception as e:
            if attempt < max_retries - 1:
                await asyncio.sleep(0.2 * (attempt + 1))
                continue
            raise e
