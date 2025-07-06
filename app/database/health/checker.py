import time
import datetime
import asyncio
from ..config import HEALTH_CHECK_INTERVAL, MAX_CONNECTION_FAILURES
from ..circuit_breaker import db_circuit_breaker

_pool_healthy = True
_last_health_check = 0
_connection_failures = 0

async def check_pool_health(pool, priority_manager):
    global _pool_healthy, _last_health_check, _connection_failures
    
    current_time = time.time()
    if current_time - _last_health_check < HEALTH_CHECK_INTERVAL:
        return _pool_healthy
    
    _last_health_check = current_time
    
    if not pool:
        _pool_healthy = False
        return False
    
    try:
        conn, is_critical = await priority_manager.acquire_general_connection(pool, timeout=2)
        try:
            await asyncio.wait_for(conn.fetchval("SELECT 1"), timeout=3)
            _pool_healthy = True
            _connection_failures = 0
            db_circuit_breaker.record_success()
        finally:
            await priority_manager.release_connection(pool, conn, is_critical)
        
    except Exception as e:
        _pool_healthy = False
        _connection_failures += 1
        db_circuit_breaker.record_failure()
        print(f"[{datetime.datetime.now(datetime.timezone.utc)}] Pool health check failed: {e}")
    
    return _pool_healthy

async def attempt_pool_recovery(pool):
    global _pool_healthy, _connection_failures
    
    if _connection_failures < MAX_CONNECTION_FAILURES:
        return
    
    print(f"[{datetime.datetime.now(datetime.timezone.utc)}] Attempting pool recovery after {_connection_failures} failures")
    
    try:
        if pool:
            await pool.close()
            pool = None
        
        await asyncio.sleep(min(_connection_failures, 10))
        from ..connection import init_db
        await init_db()
        _pool_healthy = True
        _connection_failures = 0
        
    except Exception as e:
        print(f"[{datetime.datetime.now(datetime.timezone.utc)}] Pool recovery failed: {e}")

def get_health_status():
    return {
        'healthy': _pool_healthy,
        'connection_failures': _connection_failures,
        'last_health_check': _last_health_check
    }