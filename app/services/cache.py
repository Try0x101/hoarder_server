import redis.asyncio as redis
import orjson
import asyncio
from typing import Optional, Any

redis_client: Optional[redis.Redis] = None

async def init_redis():
    global redis_client
    try:
        redis_client = redis.from_url("redis://localhost", decode_responses=False, socket_timeout=2)
        await redis_client.ping()
        print("Redis cache initialized")
    except Exception as e:
        print(f"Redis initialization failed: {e}")
        redis_client = None

async def get_cached_data(key: str) -> Optional[Any]:
    if not redis_client:
        return None
    try:
        cached = await asyncio.wait_for(redis_client.get(key), timeout=1)
        return orjson.loads(cached) if cached else None
    except Exception:
        return None

async def set_cached_data(key: str, data: Any, ttl: int = 60):
    if not redis_client:
        return
    try:
        await asyncio.wait_for(redis_client.set(key, orjson.dumps(data), ex=ttl), timeout=1)
    except Exception:
        pass

async def invalidate_cache(pattern: str):
    if not redis_client:
        return
    try:
        keys = await redis_client.keys(pattern)
        if keys:
            await redis_client.delete(*keys)
    except Exception:
        pass
