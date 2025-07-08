import redis.asyncio as redis
import orjson
import datetime
import asyncio
import time
from typing import Set, List, Optional, Dict

CACHE_KEY_LATEST_DATA = "latest_data"
redis_client = None

async def init_redis_pool():
    global redis_client
    try:
        redis_client = redis.from_url(
            "redis://localhost", decode_responses=True, socket_timeout=2, health_check_interval=30
        )
        await redis_client.ping()
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

async def invalidate_device_cache(device_id: str):
    if not redis_client: return
    try:
        keys_to_delete = [CACHE_KEY_LATEST_DATA, f"latest_data_raw_{device_id}"]
        await redis_client.delete(*keys_to_delete)
    except Exception: pass
