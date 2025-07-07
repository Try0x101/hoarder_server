import redis.asyncio as redis
import orjson
import datetime
import asyncio
import time
from typing import Set, List, Optional

CACHE_KEY_LATEST_DATA = "latest_data"
redis_client = None
redis_healthy = False

CACHE_DEPENDENCIES = {
    CACHE_KEY_LATEST_DATA: ["latest_data_raw_*"],
    "device_position_*": ["weather_rate_*"],
    "weather_cache_*": ["latest_data_raw_*"]
}

_invalidation_lock = asyncio.Lock()
_invalidation_queue = asyncio.Queue(maxsize=1000)
_invalidation_worker_running = False

async def init_redis_pool():
    global redis_client, redis_healthy, _invalidation_worker_running
    try:
        redis_client = redis.from_url(
            "redis://localhost", 
            encoding="utf-8", 
            decode_responses=True,
            socket_timeout=3,
            socket_connect_timeout=3,
            retry_on_timeout=True,
            health_check_interval=30
        )
        await redis_client.ping()
        redis_healthy = True
        
        if not _invalidation_worker_running:
            asyncio.create_task(_invalidation_worker())
            _invalidation_worker_running = True
            
        print(f"[{datetime.datetime.now()}] Redis connection initialized with invalidation worker")
    except Exception as e:
        redis_healthy = False
        print(f"[{datetime.datetime.now()}] Redis unavailable: {e}")
        redis_client = None

async def get_cached_data(key: str):
    if not redis_client or not redis_healthy:
        return None
    try:
        cached = await asyncio.wait_for(redis_client.get(key), timeout=2)
        if cached:
            return orjson.loads(cached)
    except Exception:
        pass
    return None

async def set_cached_data(key: str, data: any, ttl: int = 60):
    if not redis_client or not redis_healthy:
        return
    try:
        serialized = orjson.dumps(data)
        await asyncio.wait_for(
            redis_client.set(key, serialized, ex=ttl), 
            timeout=2
        )
    except Exception:
        pass

async def invalidate_cache_atomic(primary_key: str, device_id: Optional[str] = None):
    if not redis_client or not redis_healthy:
        return
    
    invalidation_task = {
        'primary_key': primary_key,
        'device_id': device_id,
        'timestamp': time.time(),
        'retries': 0
    }
    
    try:
        _invalidation_queue.put_nowait(invalidation_task)
    except asyncio.QueueFull:
        print(f"[{datetime.datetime.now()}] Invalidation queue full, dropping cache invalidation")

async def _invalidation_worker():
    while True:
        try:
            task = await _invalidation_queue.get()
            await _process_invalidation_task(task)
            _invalidation_queue.task_done()
        except Exception as e:
            print(f"[{datetime.datetime.now()}] Invalidation worker error: {e}")
            await asyncio.sleep(1)

async def _process_invalidation_task(task: dict):
    primary_key = task['primary_key']
    device_id = task['device_id']
    retries = task['retries']
    
    if retries > 2:
        print(f"[{datetime.datetime.now()}] Cache invalidation failed after retries: {primary_key}")
        return
    
    try:
        keys_to_invalidate = await _collect_invalidation_keys(primary_key, device_id)
        
        if keys_to_invalidate:
            async with _invalidation_lock:
                pipeline = redis_client.pipeline()
                for key in keys_to_invalidate:
                    pipeline.delete(key)
                results = await asyncio.wait_for(pipeline.execute(), timeout=3)
                
                invalidated_count = sum(1 for r in results if r > 0)
                if invalidated_count > 0:
                    print(f"[{datetime.datetime.now()}] Cache invalidated: {invalidated_count} keys for {primary_key}")
                    
    except Exception as e:
        print(f"[{datetime.datetime.now()}] Cache invalidation error for {primary_key}: {e}")
        
        task['retries'] = retries + 1
        try:
            _invalidation_queue.put_nowait(task)
        except asyncio.QueueFull:
            pass

async def _collect_invalidation_keys(primary_key: str, device_id: Optional[str] = None) -> Set[str]:
    keys_to_invalidate = {primary_key}
    
    if primary_key == CACHE_KEY_LATEST_DATA:
        keys_to_invalidate.update([
            CACHE_KEY_LATEST_DATA,
            "latest_data_transformed",
            "device_stats_summary"
        ])
        
        if device_id:
            keys_to_invalidate.update([
                f"latest_data_raw_{device_id}",
                f"device_transform_{device_id}",
                f"device_history_{device_id}"
            ])
    
    elif primary_key.startswith("latest_data_raw_"):
        if device_id:
            keys_to_invalidate.update([
                CACHE_KEY_LATEST_DATA,
                f"latest_data_raw_{device_id}",
                f"device_transform_{device_id}"
            ])
    
    elif primary_key.startswith("device_position_"):
        if device_id:
            keys_to_invalidate.update([
                f"device_position_{device_id}",
                f"weather_rate_{device_id}",
                f"latest_data_raw_{device_id}"
            ])
    
    try:
        existing_keys = []
        for key_pattern in keys_to_invalidate:
            if '*' in key_pattern:
                pattern_keys = await redis_client.keys(key_pattern)
                existing_keys.extend(pattern_keys)
            else:
                exists = await redis_client.exists(key_pattern)
                if exists:
                    existing_keys.append(key_pattern)
        
        return set(existing_keys)
    except Exception:
        return keys_to_invalidate

async def invalidate_cache(key: str):
    await invalidate_cache_atomic(key)

async def invalidate_device_cache(device_id: str):
    await invalidate_cache_atomic(CACHE_KEY_LATEST_DATA, device_id)

async def bulk_invalidate_cache(keys: List[str]):
    for key in keys:
        await invalidate_cache_atomic(key)

async def get_cache_health():
    if not redis_client:
        return {"healthy": False, "error": "not_initialized"}
    
    try:
        start_time = time.time()
        await asyncio.wait_for(redis_client.ping(), timeout=2)
        latency = (time.time() - start_time) * 1000
        
        info = await asyncio.wait_for(redis_client.info('memory'), timeout=2)
        memory_usage = info.get('used_memory_human', 'unknown')
        
        return {
            "healthy": True,
            "latency_ms": f"{latency:.1f}",
            "memory_usage": memory_usage,
            "invalidation_queue_size": _invalidation_queue.qsize()
        }
    except Exception as e:
        global redis_healthy
        redis_healthy = False
        return {"healthy": False, "error": str(e)}

async def cleanup_expired_cache():
    if not redis_client or not redis_healthy:
        return
    
    try:
        expired_patterns = [
            "latest_data_raw_*",
            "device_transform_*",
            "device_position_*",
            "weather_rate_*"
        ]
        
        for pattern in expired_patterns:
            try:
                keys = await redis_client.keys(pattern)
                if len(keys) > 100:
                    oldest_keys = keys[:50]
                    if oldest_keys:
                        await redis_client.delete(*oldest_keys)
                        print(f"[{datetime.datetime.now()}] Cleaned {len(oldest_keys)} old cache keys")
            except Exception:
                continue
                
    except Exception as e:
        print(f"[{datetime.datetime.now()}] Cache cleanup error: {e}")
