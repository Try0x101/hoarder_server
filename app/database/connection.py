import asyncpg
import datetime
import asyncio
import time
from typing import Optional
from fastapi import HTTPException

DB_CONFIG = {"user":"admin","password":"admin","database":"database","host":"localhost"}
pool = None
_init_lock = asyncio.Lock()
_partition_locks = {}
_partition_cache = set()
_initialized = False
_pool_healthy = True
_last_health_check = 0
_connection_failures = 0

POOL_MIN_SIZE = 5
POOL_MAX_SIZE = 12
POOL_MAX_QUERIES = 10000
POOL_MAX_INACTIVE_TIME = 300
CONNECTION_TIMEOUT = 3
QUERY_TIMEOUT = 15
MAX_PARTITION_RETRIES = 3
PARTITION_RETRY_DELAY = 0.1
HEALTH_CHECK_INTERVAL = 15
MAX_CONNECTION_FAILURES = 3
CIRCUIT_BREAKER_TIMEOUT = 30
CONNECTION_QUEUE_TIMEOUT = 3

class ConnectionQueueMonitor:
    def __init__(self):
        self.pending_requests = 0
        self.total_requests = 0
        self.timeouts = 0
        self.queue_full_rejections = 0
        self.max_pending_seen = 0
        
    def request_started(self):
        self.pending_requests += 1
        self.total_requests += 1
        self.max_pending_seen = max(self.max_pending_seen, self.pending_requests)
        
    def request_completed(self):
        self.pending_requests = max(0, self.pending_requests - 1)
        
    def request_timeout(self):
        self.timeouts += 1
        self.request_completed()
        
    def queue_full(self):
        self.queue_full_rejections += 1
        
    def get_stats(self):
        return {
            'pending_requests': self.pending_requests,
            'total_requests': self.total_requests,
            'timeouts': self.timeouts,
            'queue_full_rejections': self.queue_full_rejections,
            'max_pending_seen': self.max_pending_seen,
            'queue_pressure': min(1.0, self.pending_requests / 10.0)
        }

queue_monitor = ConnectionQueueMonitor()

class DatabaseCircuitBreaker:
    def __init__(self):
        self.failure_count = 0
        self.last_failure_time = 0
        self.state = "CLOSED"
    
    def can_execute(self):
        if self.state == "CLOSED":
            return True
        elif self.state == "OPEN":
            if time.time() - self.last_failure_time > CIRCUIT_BREAKER_TIMEOUT:
                self.state = "HALF_OPEN"
                return True
            return False
        elif self.state == "HALF_OPEN":
            return True
        return False
    
    def record_success(self):
        self.failure_count = 0
        self.state = "CLOSED"
    
    def record_failure(self):
        self.failure_count += 1
        self.last_failure_time = time.time()
        if self.failure_count >= 2:
            self.state = "OPEN"

db_circuit_breaker = DatabaseCircuitBreaker()

class PriorityConnectionManager:
    def __init__(self):
        self.critical_pool_size = 3
        self.critical_connections_used = 0
        self.critical_lock = asyncio.Lock()
        
    async def acquire_critical_connection(self, timeout=CONNECTION_QUEUE_TIMEOUT):
        async with self.critical_lock:
            if self.critical_connections_used >= self.critical_pool_size:
                stats = queue_monitor.get_stats()
                if stats['queue_pressure'] > 0.8:
                    raise HTTPException(503, "Critical database pool exhausted")
            
            try:
                queue_monitor.request_started()
                conn = await asyncio.wait_for(pool.acquire(), timeout=timeout)
                self.critical_connections_used += 1
                return conn, True
            except asyncio.TimeoutError:
                queue_monitor.request_timeout()
                raise HTTPException(503, "Database connection timeout (critical)")
            except Exception as e:
                queue_monitor.request_completed()
                raise HTTPException(503, f"Database connection failed: {str(e)}")
    
    async def acquire_general_connection(self, timeout=CONNECTION_QUEUE_TIMEOUT):
        try:
            queue_monitor.request_started()
            
            stats = queue_monitor.get_stats()
            if stats['pending_requests'] > 8:
                queue_monitor.queue_full()
                raise HTTPException(503, "Database overloaded, try again")
            
            adjusted_timeout = max(1, timeout - (stats['queue_pressure'] * 2))
            conn = await asyncio.wait_for(pool.acquire(), timeout=adjusted_timeout)
            return conn, False
        except asyncio.TimeoutError:
            queue_monitor.request_timeout()
            raise HTTPException(503, "Database connection timeout")
        except Exception as e:
            queue_monitor.request_completed()
            raise HTTPException(503, f"Database connection failed: {str(e)}")
    
    async def release_connection(self, conn, is_critical=False):
        try:
            await pool.release(conn)
            if is_critical:
                async with self.critical_lock:
                    self.critical_connections_used = max(0, self.critical_connections_used - 1)
            queue_monitor.request_completed()
        except Exception as e:
            print(f"[{datetime.datetime.now(datetime.timezone.utc)}] Connection release error: {e}")

priority_manager = PriorityConnectionManager()

async def get_pool():
    global pool, _pool_healthy, _last_health_check, _connection_failures
    
    if not db_circuit_breaker.can_execute():
        raise HTTPException(503, "Database circuit breaker is OPEN")
    
    current_time = time.time()
    if current_time - _last_health_check > HEALTH_CHECK_INTERVAL:
        await check_pool_health()
        _last_health_check = current_time
    
    if not _pool_healthy:
        await attempt_pool_recovery()
    
    if pool is None:
        await init_db()
    return pool

async def get_connection_with_timeout(timeout=CONNECTION_QUEUE_TIMEOUT, critical=False):
    if critical:
        return await priority_manager.acquire_critical_connection(timeout)
    else:
        return await priority_manager.acquire_general_connection(timeout)

async def release_connection_safe(conn, is_critical=False):
    await priority_manager.release_connection(conn, is_critical)

async def check_pool_health():
    global _pool_healthy, _connection_failures
    
    if not pool:
        _pool_healthy = False
        return
    
    try:
        conn, is_critical = await get_connection_with_timeout(timeout=2, critical=False)
        try:
            await asyncio.wait_for(conn.fetchval("SELECT 1"), timeout=3)
            _pool_healthy = True
            _connection_failures = 0
            db_circuit_breaker.record_success()
        finally:
            await release_connection_safe(conn, is_critical)
        
    except Exception as e:
        _pool_healthy = False
        _connection_failures += 1
        db_circuit_breaker.record_failure()
        print(f"[{datetime.datetime.now(datetime.timezone.utc)}] Pool health check failed: {e}")

async def attempt_pool_recovery():
    global pool, _pool_healthy, _connection_failures
    
    if _connection_failures < MAX_CONNECTION_FAILURES:
        return
    
    print(f"[{datetime.datetime.now(datetime.timezone.utc)}] Attempting pool recovery after {_connection_failures} failures")
    
    try:
        if pool:
            await pool.close()
            pool = None
        
        await asyncio.sleep(min(_connection_failures, 10))
        await init_db()
        _pool_healthy = True
        _connection_failures = 0
        
    except Exception as e:
        print(f"[{datetime.datetime.now(datetime.timezone.utc)}] Pool recovery failed: {e}")

async def safe_db_operation(operation_func, *args, critical=False, **kwargs):
    if not db_circuit_breaker.can_execute():
        raise HTTPException(503, "Database unavailable (circuit breaker open)")
    
    max_retries = 2 if critical else 1
    for attempt in range(max_retries):
        conn = None
        is_critical = False
        try:
            pool_instance = await get_pool()
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
            
        except (asyncpg.PostgresError, asyncpg.InterfaceError) as e:
            db_circuit_breaker.record_failure()
            if attempt < max_retries - 1:
                await asyncio.sleep(0.5 * (attempt + 1))
                continue
            raise HTTPException(503, f"Database error: {e}")
            
        except Exception as e:
            db_circuit_breaker.record_failure()
            raise HTTPException(503, f"Unexpected database error: {e}")
        
        finally:
            if conn:
                await release_connection_safe(conn, is_critical)

def _get_partition_name(target_date):
    partition_start = target_date.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    return f"timestamped_data_y{partition_start.strftime('%Y')}m{partition_start.strftime('%m')}"

async def _get_partition_lock(partition_name):
    if partition_name not in _partition_locks:
        _partition_locks[partition_name] = asyncio.Lock()
    return _partition_locks[partition_name]

async def create_partition_for_date(conn, target_date):
    partition_start = target_date.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    partition_end = (partition_start + datetime.timedelta(days=32)).replace(day=1)
    partition_name = _get_partition_name(target_date)
    
    if partition_name in _partition_cache:
        return
    
    partition_lock = await _get_partition_lock(partition_name)
    
    async with partition_lock:
        if partition_name in _partition_cache:
            return
            
        for attempt in range(MAX_PARTITION_RETRIES):
            try:
                exists = await asyncio.wait_for(
                    conn.fetchval("SELECT EXISTS(SELECT 1 FROM pg_tables WHERE tablename = $1)", partition_name),
                    timeout=5
                )
                
                if exists:
                    _partition_cache.add(partition_name)
                    return
                
                async with conn.transaction():
                    await asyncio.wait_for(conn.execute(f"""
                        CREATE TABLE {partition_name} PARTITION OF timestamped_data
                        FOR VALUES FROM ('{partition_start.isoformat()}') TO ('{partition_end.isoformat()}');
                    """), timeout=15)
                    
                    await asyncio.wait_for(conn.execute(f'CREATE INDEX ON {partition_name} (device_id, data_timestamp DESC);'), timeout=30)
                    await asyncio.wait_for(conn.execute(f'CREATE INDEX ON {partition_name} (data_timestamp DESC);'), timeout=30)
                
                _partition_cache.add(partition_name)
                print(f"[{datetime.datetime.now(datetime.timezone.utc)}] Created partition {partition_name}")
                return
                
            except asyncpg.exceptions.DuplicateTableError:
                _partition_cache.add(partition_name)
                return
                
            except Exception as e:
                if attempt < MAX_PARTITION_RETRIES - 1:
                    wait_time = PARTITION_RETRY_DELAY * (2 ** attempt)
                    print(f"[{datetime.datetime.now(datetime.timezone.utc)}] Partition creation attempt {attempt + 1} failed, retrying in {wait_time}s: {e}")
                    await asyncio.sleep(wait_time)
                    continue
                print(f"[{datetime.datetime.now(datetime.timezone.utc)}] CRITICAL: Failed to create partition {partition_name}: {e}")
                raise

async def init_db():
    global pool, _initialized, _pool_healthy
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
                command_timeout=QUERY_TIMEOUT,
                server_settings={
                    'jit': 'off',
                    'application_name': 'hoarder_server_v3_optimized',
                    'statement_timeout': '15s',
                    'idle_in_transaction_session_timeout': '10s'
                }
            )
            
            async def setup_database(conn):
                await conn.execute("""
                    CREATE OR REPLACE FUNCTION safe_cast_to_float(p_text TEXT)
                    RETURNS FLOAT AS
                    $BODY$
                    BEGIN
                        RETURN p_text::FLOAT;
                    EXCEPTION
                        WHEN invalid_text_representation THEN
                            RETURN NULL;
                    END;
                    $BODY$
                    LANGUAGE plpgsql IMMUTABLE;
                """)

                await conn.execute("""
                    CREATE OR REPLACE FUNCTION jsonb_recursive_merge(a JSONB, b JSONB)
                    RETURNS JSONB AS $$
                    DECLARE
                        key TEXT;
                        value JSONB;
                    BEGIN
                        IF a IS NULL THEN RETURN b; END IF;
                        IF b IS NULL THEN RETURN a; END IF;
                        
                        a := a || b;
                        FOR key, value IN SELECT * FROM jsonb_each(b)
                        LOOP
                            IF a->key IS NOT NULL AND jsonb_typeof(a->key) = 'object' AND jsonb_typeof(value) = 'object' THEN
                                a := jsonb_set(a, ARRAY[key], jsonb_recursive_merge(a->key, value));
                            END IF;
                        END LOOP;
                        RETURN a;
                    END;
                    $$ LANGUAGE plpgsql;
                """)

                is_partitioned = await conn.fetchval("""
                    SELECT EXISTS (
                        SELECT 1 FROM pg_class WHERE relname = 'timestamped_data' AND relkind = 'p'
                    );
                """)

                if not is_partitioned:
                    await conn.execute("DROP TABLE IF EXISTS timestamped_data CASCADE;")
                    await conn.execute("""
                        CREATE TABLE timestamped_data (
                            device_id TEXT NOT NULL,
                            payload JSONB NOT NULL,
                            data_timestamp TIMESTAMPTZ NOT NULL,
                            received_at TIMESTAMPTZ DEFAULT now(),
                            data_type TEXT DEFAULT 'delta',
                            is_offline BOOLEAN DEFAULT false,
                            batch_id TEXT NULL
                        ) PARTITION BY RANGE (data_timestamp);
                    """)

                existing_partitions = await conn.fetch("""
                    SELECT tablename FROM pg_tables 
                    WHERE tablename LIKE 'timestamped_data_y%'
                """)
                for row in existing_partitions:
                    _partition_cache.add(row['tablename'])

                now = datetime.datetime.now(datetime.timezone.utc)
                await create_partition_for_date(conn, now)
                await create_partition_for_date(conn, now + datetime.timedelta(days=32))

                await conn.execute("""
                    CREATE TABLE IF NOT EXISTS device_data(id SERIAL PRIMARY KEY,device_id TEXT NOT NULL,payload JSONB NOT NULL,received_at TIMESTAMPTZ DEFAULT now());
                    CREATE TABLE IF NOT EXISTS latest_device_states(device_id TEXT PRIMARY KEY,payload JSONB NOT NULL,received_at TIMESTAMPTZ DEFAULT now());
                    CREATE INDEX IF NOT EXISTS idx_device_data_device_id ON device_data(device_id);
                    CREATE INDEX IF NOT EXISTS idx_device_data_received_at ON device_data(received_at DESC);
                """)
            
            await safe_db_operation(setup_database, critical=True)
            
            _initialized = True
            _pool_healthy = True
            db_circuit_breaker.record_success()
            
            print(f"[{datetime.datetime.now(datetime.timezone.utc)}] Database pool initialized: {POOL_MIN_SIZE}-{POOL_MAX_SIZE} connections with queue monitoring")
            
        except Exception as e:
            _pool_healthy = False
            db_circuit_breaker.record_failure()
            print(f"[{datetime.datetime.now(datetime.timezone.utc)}] CRITICAL: Database initialization failed: {e}")
            raise

async def ensure_partition_exists(data_timestamp):
    partition_name = _get_partition_name(data_timestamp)
    if partition_name in _partition_cache:
        return
    
    async def create_partition_op(conn):
        await create_partition_for_date(conn, data_timestamp)
    
    await safe_db_operation(create_partition_op, critical=True)

async def get_pool_stats():
    global _pool_healthy, _connection_failures
    
    if not pool:
        return {"status": "not_initialized", "healthy": False}
    
    queue_stats = queue_monitor.get_stats()
    
    return {
        "size": pool.get_size(),
        "min_size": POOL_MIN_SIZE,
        "max_size": POOL_MAX_SIZE,
        "idle_connections": pool.get_idle_size(),
        "max_queries": POOL_MAX_QUERIES,
        "healthy": _pool_healthy,
        "connection_failures": _connection_failures,
        "circuit_breaker_state": db_circuit_breaker.state,
        "partitions_cached": len(_partition_cache),
        "query_timeout": QUERY_TIMEOUT,
        "queue_stats": queue_stats,
        "critical_pool_used": priority_manager.critical_connections_used,
        "critical_pool_size": priority_manager.critical_pool_size
    }

async def close_pool():
    global pool, _initialized
    if pool:
        await pool.close()
        pool = None
        _initialized = False
        print(f"[{datetime.datetime.now(datetime.timezone.utc)}] Database pool closed")
