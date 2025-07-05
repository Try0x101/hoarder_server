import redis.asyncio as redis
import orjson
import datetime
import asyncio
import time

CACHE_KEY_LATEST_DATA = "latest_data"
redis_client = None
redis_healthy = False
last_health_check = 0
last_reconnect_attempt = 0
connection_failures = 0

HEALTH_CHECK_INTERVAL = 30
RECONNECT_INTERVAL = 60
MAX_CONNECTION_FAILURES = 5
REDIS_TIMEOUT = 5
REDIS_RETRY_DELAY = 1

async def init_redis_pool():
    global redis_client, redis_healthy
    try:
        if redis_client:
            try:
                await redis_client.close()
            except:
                pass
            redis_client = None
        
        redis_client = redis.from_url(
            "redis://localhost", 
            encoding="utf-8", 
            decode_responses=True,
            socket_timeout=REDIS_TIMEOUT,
            socket_connect_timeout=REDIS_TIMEOUT,
            retry_on_timeout=True,
            health_check_interval=HEALTH_CHECK_INTERVAL
        )
        
        await asyncio.wait_for(redis_client.ping(), timeout=REDIS_TIMEOUT)
        redis_healthy = True
        global connection_failures
        connection_failures = 0
        print(f"[{datetime.datetime.now()}] Redis connection pool initialized successfully.")
        
    except Exception as e:
        redis_healthy = False
        connection_failures += 1
        print(f"[{datetime.datetime.now()}] CRITICAL: Could not connect to Redis: {e}")
        redis_client = None

async def check_redis_health():
    global redis_healthy, last_health_check, connection_failures
    
    current_time = time.time()
    if current_time - last_health_check < HEALTH_CHECK_INTERVAL:
        return redis_healthy
    
    last_health_check = current_time
    
    if not redis_client:
        redis_healthy = False
        return False
    
    try:
        await asyncio.wait_for(redis_client.ping(), timeout=REDIS_TIMEOUT)
        if not redis_healthy:
            print(f"[{datetime.datetime.now()}] Redis connection restored.")
        redis_healthy = True
        connection_failures = 0
        return True
        
    except Exception as e:
        if redis_healthy:
            print(f"[{datetime.datetime.now()}] Redis health check failed: {e}")
        redis_healthy = False
        connection_failures += 1
        
        if connection_failures >= MAX_CONNECTION_FAILURES:
            await attempt_reconnect()
        
        return False

async def attempt_reconnect():
    global last_reconnect_attempt, connection_failures
    
    current_time = time.time()
    if current_time - last_reconnect_attempt < RECONNECT_INTERVAL:
        return
        
    last_reconnect_attempt = current_time
    print(f"[{datetime.datetime.now()}] Attempting Redis reconnection (failures: {connection_failures})...")
    
    try:
        await init_redis_pool()
    except Exception as e:
        print(f"[{datetime.datetime.now()}] Redis reconnection failed: {e}")

async def safe_redis_operation(operation, *args, **kwargs):
    if not await check_redis_health():
        return None
    
    try:
        if operation == 'get':
            return await asyncio.wait_for(redis_client.get(*args), timeout=REDIS_TIMEOUT)
        elif operation == 'set':
            return await asyncio.wait_for(redis_client.set(*args, **kwargs), timeout=REDIS_TIMEOUT)
        elif operation == 'delete':
            return await asyncio.wait_for(redis_client.delete(*args), timeout=REDIS_TIMEOUT)
        elif operation == 'hgetall':
            return await asyncio.wait_for(redis_client.hgetall(*args), timeout=REDIS_TIMEOUT)
        elif operation == 'hset':
            return await asyncio.wait_for(redis_client.hset(*args, **kwargs), timeout=REDIS_TIMEOUT)
        elif operation == 'expire':
            return await asyncio.wait_for(redis_client.expire(*args), timeout=REDIS_TIMEOUT)
        else:
            return None
            
    except asyncio.TimeoutError:
        print(f"[{datetime.datetime.now()}] Redis operation timeout: {operation}")
        return None
    except redis.ConnectionError as e:
        global redis_healthy, connection_failures
        redis_healthy = False
        connection_failures += 1
        print(f"[{datetime.datetime.now()}] Redis connection error during {operation}: {e}")
        return None
    except Exception as e:
        print(f"[{datetime.datetime.now()}] Redis operation failed ({operation}): {e}")
        return None

async def get_cached_data(key: str):
    try:
        cached = await safe_redis_operation('get', key)
        if cached:
            return orjson.loads(cached)
    except Exception as e:
        print(f"[{datetime.datetime.now()}] ERROR: Redis GET failed for key '{key}': {e}")
    return None

async def set_cached_data(key: str, data: any, ttl: int = 60):
    try:
        serialized = orjson.dumps(data)
        await safe_redis_operation('set', key, serialized, ex=ttl)
    except Exception as e:
        print(f"[{datetime.datetime.now()}] ERROR: Redis SET failed for key '{key}': {e}")

async def invalidate_cache(key: str):
    try:
        result = await safe_redis_operation('delete', key)
        if result:
            print(f"[{datetime.datetime.now()}] Cache invalidated for key: {key}")
    except Exception as e:
        print(f"[{datetime.datetime.now()}] ERROR: Redis DELETE failed for key '{key}': {e}")

async def get_redis_status():
    healthy = await check_redis_health()
    return {
        "redis_healthy": healthy,
        "connection_failures": connection_failures,
        "last_health_check": datetime.datetime.fromtimestamp(last_health_check).isoformat(),
        "last_reconnect_attempt": datetime.datetime.fromtimestamp(last_reconnect_attempt).isoformat() if last_reconnect_attempt else None,
        "redis_available": redis_client is not None
    }

async def redis_hgetall(key: str):
    return await safe_redis_operation('hgetall', key)

async def redis_hset_with_expire(key: str, mapping: dict, ttl: int):
    try:
        await safe_redis_operation('hset', key, mapping=mapping)
        await safe_redis_operation('expire', key, ttl)
    except Exception as e:
        print(f"[{datetime.datetime.now()}] ERROR: Redis HSET with expire failed for key '{key}': {e}")
