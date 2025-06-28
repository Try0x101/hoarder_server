import redis.asyncio as redis
import orjson
import datetime

CACHE_KEY_LATEST_DATA = "latest_data"
redis_client = None

async def init_redis_pool():
    global redis_client
    if redis_client is None:
        try:
            redis_client = redis.from_url("redis://localhost", encoding="utf-8", decode_responses=True)
            await redis_client.ping()
            print(f"[{datetime.datetime.now()}] Redis connection pool initialized.")
        except Exception as e:
            print(f"[{datetime.datetime.now()}] CRITICAL: Could not connect to Redis: {e}")
            redis_client = None

async def get_cached_data(key: str):
    if not redis_client:
        return None
    try:
        cached = await redis_client.get(key)
        if cached:
            return orjson.loads(cached)
    except Exception as e:
        print(f"[{datetime.datetime.now()}] ERROR: Redis GET failed for key '{key}': {e}")
    return None

async def set_cached_data(key: str, data: any, ttl: int = 60):
    if not redis_client:
        return
    try:
        await redis_client.set(key, orjson.dumps(data), ex=ttl)
    except Exception as e:
        print(f"[{datetime.datetime.now()}] ERROR: Redis SET failed for key '{key}': {e}")

async def invalidate_cache(key: str):
    if not redis_client:
        return
    try:
        await redis_client.delete(key)
        print(f"[{datetime.datetime.now()}] Cache invalidated for key: {key}")
    except Exception as e:
        print(f"[{datetime.datetime.now()}] ERROR: Redis DELETE failed for key '{key}': {e}")
