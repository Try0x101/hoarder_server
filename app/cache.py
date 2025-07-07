import redis.asyncio as redis
import orjson
import datetime
import asyncio
import time
from typing import Set, List, Optional, Dict

CACHE_KEY_LATEST_DATA = "latest_data"
redis_client = None
_invalidation_queue = asyncio.Queue(maxsize=1000)
_invalidation_worker_task = None

async def init_redis_pool():
    global redis_client, _invalidation_worker_task
    try:
        redis_client = redis.from_url(
            "redis://localhost", decode_responses=True, socket_timeout=2, health_check_interval=30
        )
        await redis_client.ping()
        if not _invalidation_worker_task:
            _invalidation_worker_task = asyncio.create_task(_invalidation_worker())
        print(f"[{datetime.datetime.now()}] Redis connection initialized.")
    except Exception as e:
        redis_client = None
        print(f"[{datetime.datetime.now()}] Redis unavailable: {e}")

async def get_cached_data(key: str):
    if not redis_client: return None
    try:
        cached = await asyncio.wait_for(redis_client.get(key), timeout=1)
        return orjson.loads(cached) if cached else None
    except Exception: return None

async def set_cached_data(key: str, data: any, ttl: int = 60):
    if not redis_client: return
    try:
        await asyncio.wait_for(redis_client.set(key, orjson.dumps(data), ex=ttl), timeout=1)
    except Exception: pass

async def invalidate_cache_atomic(primary_key: str, device_id: Optional[str] = None):
    try:
        await _invalidation_queue.put({'primary_key': primary_key, 'device_id': device_id})
    except asyncio.QueueFull:
        print(f"[{datetime.datetime.now()}] Invalidation queue full, dropping task for {primary_key}")

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
    primary_key, device_id = task['primary_key'], task.get('device_id')
    keys_to_invalidate = {primary_key}
    
    if primary_key == CACHE_KEY_LATEST_DATA:
        keys_to_invalidate.add("device_stats_summary")
        if device_id:
            keys_to_invalidate.add(f"latest_data_raw_{device_id}")
            keys_to_invalidate.add(f"device_history_{device_id}")
    elif primary_key.startswith("device_position_") and device_id:
        keys_to_invalidate.add(f"weather_rate_{device_id}")

    if keys_to_invalidate and redis_client:
        try:
            await redis_client.delete(*list(keys_to_invalidate))
        except Exception as e:
            print(f"[{datetime.datetime.now()}] Failed to invalidate keys {keys_to_invalidate}: {e}")

async def invalidate_device_cache(device_id: str):
    await invalidate_cache_atomic(CACHE_KEY_LATEST_DATA, device_id)

async def get_cache_health():
    if not redis_client: return {"healthy": False, "error": "not_initialized"}
    try:
        start_time = time.time()
        await asyncio.wait_for(redis_client.ping(), timeout=1)
        latency = (time.time() - start_time) * 1000
        info = await asyncio.wait_for(redis_client.info('memory'), timeout=1)
        return {
            "healthy": True, "latency_ms": f"{latency:.1f}",
            "memory_usage": info.get('used_memory_human', 'unknown'),
            "invalidation_queue_size": _invalidation_queue.qsize()
        }
    except Exception as e:
        return {"healthy": False, "error": str(e)}
