import asyncpg
import datetime
import asyncio
import time
from typing import Optional
from fastapi import HTTPException
from .config import (
    DB_CONFIG, POOL_MIN_SIZE, POOL_MAX_SIZE, POOL_MAX_QUERIES, 
    POOL_MAX_INACTIVE_TIME, CONNECTION_TIMEOUT, QUERY_TIMEOUT, CONNECTION_QUEUE_TIMEOUT
)
from .queue.monitor import queue_monitor
from .circuit_breaker import db_circuit_breaker
from .priority.manager import priority_manager
from .health.checker import check_pool_health, attempt_pool_recovery
from .partitions.manager import create_partition_for_date

pool = None
_init_lock = asyncio.Lock()
_initialized = False

async def get_pool():
    global pool
    
    if not db_circuit_breaker.can_execute():
        raise HTTPException(503, "Database circuit breaker is OPEN")
    
    if not await check_pool_health(pool, priority_manager):
        await attempt_pool_recovery(pool)
    
    if pool is None:
        await init_db()
    return pool

async def get_connection_with_timeout(timeout=CONNECTION_QUEUE_TIMEOUT, critical=False):
    pool_instance = await get_pool()
    if critical:
        return await priority_manager.acquire_critical_connection(pool_instance, timeout)
    else:
        return await priority_manager.acquire_general_connection(pool_instance, timeout)

async def release_connection_safe(conn, is_critical=False):
    await priority_manager.release_connection(pool, conn, is_critical)

async def safe_db_operation(operation_func, *args, critical=False, **kwargs):
    if not db_circuit_breaker.can_execute():
        raise HTTPException(503, "Database unavailable (circuit breaker open)")
    
    max_retries = 2 if critical else 1
    for attempt in range(max_retries):
        conn = None
        is_critical = False
        try:
            conn, is_critical = await get_connection_with_timeout(
                timeout=CONNECTION_QUEUE_TIMEOUT, 
                critical=critical
            )
            
            result = await asyncio.wait_for(
                operation_func(conn, *args, **kwargs), 
                timeout=QUERY_TIMEOUT * (2 if critical else 1)
            )
            db_circuit_breaker.record_success()
            return result
            
        except asyncio.TimeoutError:
            db_circuit_breaker.record_failure()
            if attempt < max_retries - 1:
                await asyncio.sleep(0.2 * (attempt + 1))
                continue
            raise HTTPException(503, "Database operation timeout")
            
        except HTTPException:
            raise
            
        except Exception as e:
            db_circuit_breaker.record_failure()
            raise HTTPException(503, f"Unexpected database error: {e}")
        
        finally:
            if conn:
                await release_connection_safe(conn, is_critical)

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
                
                now = datetime.datetime.now(datetime.timezone.utc)
                await create_partition_for_date(conn, now)
                await create_partition_for_date(conn, now + datetime.timedelta(days=32))
            
            await safe_db_operation(setup_database, critical=True)
            _initialized = True
            
        except Exception as e:
            print(f"Database initialization failed: {e}")
            raise

async def close_pool():
    global pool, _initialized
    if pool:
        await pool.close()
        pool = None
        _initialized = False