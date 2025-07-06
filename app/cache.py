import redis.asyncio as redis
import orjson
import datetime
import asyncio
import time

CACHE_KEY_LATEST_DATA = "latest_data"
redis_client = None
redis_healthy = False

async def init_redis_pool():
    global redis_client, redis_healthy
    try:
        redis_client = redis.from_url(
            "redis://localhost", 
            encoding="utf-8", 
            decode_responses=True,
            socket_timeout=5,
            socket_connect_timeout=5
        )
        await redis_client.ping()
        redis_healthy = True
        print(f"[{datetime.datetime.now()}] Redis connection initialized")
    except Exception as e:
        redis_healthy = False
        print(f"[{datetime.datetime.now()}] Redis unavailable: {e}")
        redis_client = None

async def get_cached_data(key: str):
    if not redis_client or not redis_healthy:
        return None
    try:
        cached = await redis_client.get(key)
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
        await redis_client.set(key, serialized, ex=ttl)
    except Exception:
        pass

async def invalidate_cache(key: str):
    if not redis_client or not redis_healthy:
        return
    try:
        await redis_client.delete(key)
        print(f"[{datetime.datetime.now()}] Cache invalidated: {key}")
    except Exception:
        pass
