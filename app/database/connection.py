import asyncpg
import datetime
import asyncio
from typing import Optional
from fastapi import HTTPException
from .config import (
    DB_CONFIG, POOL_MIN_SIZE, POOL_MAX_SIZE, POOL_MAX_QUERIES, 
    POOL_MAX_INACTIVE_TIME, CONNECTION_TIMEOUT, QUERY_TIMEOUT
)

pool = None
_init_lock = asyncio.Lock()
_initialized = False

async def get_pool():
    global pool
    if pool is None:
        await init_db()
    return pool

async def safe_db_operation(operation_func, *args, **kwargs):
    max_retries = 2
    for attempt in range(max_retries):
        conn = None
        try:
            pool_instance = await get_pool()
            if not pool_instance:
                raise Exception("Database pool not available")

            conn = await asyncio.wait_for(pool_instance.acquire(), timeout=CONNECTION_TIMEOUT)
            
            result = await asyncio.wait_for(
                operation_func(conn, *args, **kwargs), 
                timeout=QUERY_TIMEOUT
            )
            return result
            
        except asyncio.TimeoutError:
            if attempt < max_retries - 1:
                await asyncio.sleep(0.2 * (attempt + 1))
                continue
            raise HTTPException(503, "Database operation timeout")
            
        except Exception as e:
            if attempt < max_retries - 1:
                await asyncio.sleep(0.2 * (attempt + 1))
                continue
            raise HTTPException(503, f"Database error: {e}")
        
        finally:
            if conn and pool:
                try:
                    await pool.release(conn)
                except Exception:
                    pass

async def init_db():
    global pool, _initialized
    async with _init_lock:
        if _initialized and pool and not pool._closed:
            return
            
        try:
            if pool:
                await pool.close()
            
            pool = await asyncpg.create_pool(
                **DB_CONFIG,
                min_size=POOL_MIN_SIZE,
                max_size=POOL_MAX_SIZE,
                max_queries=POOL_MAX_QUERIES,
                max_inactive_connection_lifetime=POOL_MAX_INACTIVE_TIME,
                timeout=CONNECTION_TIMEOUT,
                command_timeout=QUERY_TIMEOUT
            )
            
            async def setup_database(conn):
                await conn.execute("""
                    CREATE OR REPLACE FUNCTION jsonb_recursive_merge(a JSONB, b JSONB)
                    RETURNS JSONB AS $$
                    BEGIN
                        IF a IS NULL THEN RETURN b; END IF;
                        IF b IS NULL THEN RETURN a; END IF;
                        RETURN a || b;
                    END;
                    $$ LANGUAGE plpgsql;
                """)
            
            await safe_db_operation(setup_database)
            _initialized = True
            
        except Exception as e:
            print(f"Database initialization failed: {e}")
            pool = None
            _initialized = False

async def close_pool():
    global pool, _initialized
    if pool:
        await pool.close()
        pool = None
        _initialized = False

async def get_simple_pool_stats():
    if not pool:
        return {"status": "not_initialized", "healthy": False}
    
    return {
        "size": pool.get_size(),
        "min_size": POOL_MIN_SIZE,
        "max_size": POOL_MAX_SIZE,
        "idle_connections": pool.get_idle_size(),
        "healthy": True
    }
