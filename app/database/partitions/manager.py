import asyncio
import datetime
from ..config import MAX_PARTITION_RETRIES, PARTITION_RETRY_DELAY

_partition_locks = {}
_partition_cache = set()

async def _get_partition_lock(partition_name):
    if partition_name not in _partition_locks:
        _partition_locks[partition_name] = asyncio.Lock()
    return _partition_locks[partition_name]

def _get_partition_name(target_date):
    partition_start = target_date.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    return f"timestamped_data_y{partition_start.strftime('%Y')}m{partition_start.strftime('%m')}"

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
                
            except Exception as e:
                if attempt < MAX_PARTITION_RETRIES - 1:
                    wait_time = PARTITION_RETRY_DELAY * (2 ** attempt)
                    print(f"[{datetime.datetime.now(datetime.timezone.utc)}] Partition creation attempt {attempt + 1} failed, retrying in {wait_time}s: {e}")
                    await asyncio.sleep(wait_time)
                    continue
                print(f"[{datetime.datetime.now(datetime.timezone.utc)}] CRITICAL: Failed to create partition {partition_name}: {e}")
                raise

async def ensure_partition_exists(data_timestamp):
    partition_name = _get_partition_name(data_timestamp)
    if partition_name in _partition_cache:
        return
    
    from ..connection import safe_db_operation
    
    async def create_partition_op(conn):
        await create_partition_for_date(conn, data_timestamp)
    
    await safe_db_operation(create_partition_op, critical=True)