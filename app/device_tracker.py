import datetime
from typing import Optional, Dict, Tuple

from app.cache import redis_client
from app.transforms.geo import calculate_distance_km

MOVEMENT_THRESHOLD_KM = 1.0
DEVICE_POSITION_KEY_PREFIX = "device:position"
DEVICE_POSITION_TTL_SECONDS = 30 * 24 * 3600
WEATHER_FETCH_COOLDOWN_SECONDS = 30
MAX_WEATHER_FETCHES_PER_MINUTE = 8
BURST_WEATHER_FETCHES_LIMIT = 12
WEATHER_QUOTA_RESET_INTERVAL = 60

REDIS_RATE_LIMIT_SCRIPT = """
local key = KEYS[1]
local limit = tonumber(ARGV[1])
local burst_limit = tonumber(ARGV[2])
local ttl = tonumber(ARGV[3])

local current = redis.call('GET', key)
if current == false then 
    current = 0 
else 
    current = tonumber(current) 
end

local burst_key = key .. ':burst'
local burst_current = redis.call('GET', burst_key)
if burst_current == false then 
    burst_current = 0 
else 
    burst_current = tonumber(burst_current) 
end

if current >= limit then
    return {0, current, burst_current, 'rate_limit_exceeded'}
elseif burst_current >= burst_limit then
    return {0, current, burst_current, 'burst_limit_exceeded'}
else
    local new_count = redis.call('INCR', key)
    local new_burst = redis.call('INCR', burst_key)
    
    redis.call('EXPIRE', key, ttl)
    redis.call('EXPIRE', burst_key, 300)
    
    return {1, new_count, new_burst, 'allowed'}
end
"""

class WeatherRateLimiter:
    def __init__(self):
        self.script_sha = None
        self.fallback_counts = {}
        self.last_fallback_reset = 0
        
    async def _ensure_script_loaded(self):
        if not redis_client:
            return False
            
        if not self.script_sha:
            try:
                self.script_sha = await redis_client.script_load(REDIS_RATE_LIMIT_SCRIPT)
                print(f"[{datetime.datetime.now(datetime.timezone.utc)}] Weather rate limit script loaded: {self.script_sha}")
                return True
            except Exception as e:
                print(f"[{datetime.datetime.now(datetime.timezone.utc)}] Failed to load rate limit script: {e}")
                return False
        return True
        
    async def check_global_rate_limit(self) -> Tuple[bool, str, Dict]:
        if not redis_client:
            return self._fallback_rate_limit()
            
        script_loaded = await self._ensure_script_loaded()
        if not script_loaded:
            return self._fallback_rate_limit()
            
        try:
            current_minute = int(datetime.datetime.now(datetime.timezone.utc).timestamp() // 60)
            global_key = f"global:weather_rate:{current_minute}"
            
            result = await redis_client.evalsha(
                self.script_sha,
                1,
                global_key,
                str(MAX_WEATHER_FETCHES_PER_MINUTE),
                str(BURST_WEATHER_FETCHES_LIMIT),
                str(WEATHER_QUOTA_RESET_INTERVAL)
            )
            
            allowed, current_count, burst_count, reason = result
            
            stats = {
                'current_count': current_count,
                'burst_count': burst_count,
                'limit': MAX_WEATHER_FETCHES_PER_MINUTE,
                'burst_limit': BURST_WEATHER_FETCHES_LIMIT,
                'reason': reason,
                'method': 'redis_atomic'
            }
            
            success = bool(allowed)
            message = f"Weather API: {reason} ({current_count}/{MAX_WEATHER_FETCHES_PER_MINUTE}, burst: {burst_count}/{BURST_WEATHER_FETCHES_LIMIT})"
            
            if success:
                print(f"[{datetime.datetime.now(datetime.timezone.utc)}] {message}")
            else:
                print(f"[{datetime.datetime.now(datetime.timezone.utc)}] RATE LIMITED: {message}")
                
            return success, message, stats
            
        except Exception as e:
            print(f"[{datetime.datetime.now(datetime.timezone.utc)}] Redis rate limit check failed: {e}")
            return self._fallback_rate_limit()
    
    def _fallback_rate_limit(self) -> Tuple[bool, str, Dict]:
        current_time = datetime.datetime.now(datetime.timezone.utc).timestamp()
        current_minute = int(current_time // 60)
        
        if self.last_fallback_reset != current_minute:
            self.fallback_counts = {}
            self.last_fallback_reset = current_minute
            
        current_count = self.fallback_counts.get(current_minute, 0)
        
        if current_count >= MAX_WEATHER_FETCHES_PER_MINUTE:
            stats = {
                'current_count': current_count,
                'limit': MAX_WEATHER_FETCHES_PER_MINUTE,
                'reason': 'fallback_rate_limited',
                'method': 'fallback_memory'
            }
            return False, f"Fallback rate limit exceeded ({current_count}/{MAX_WEATHER_FETCHES_PER_MINUTE})", stats
        
        self.fallback_counts[current_minute] = current_count + 1
        
        stats = {
            'current_count': current_count + 1,
            'limit': MAX_WEATHER_FETCHES_PER_MINUTE,
            'reason': 'fallback_allowed',
            'method': 'fallback_memory'
        }
        
        return True, f"Fallback rate limit OK ({current_count + 1}/{MAX_WEATHER_FETCHES_PER_MINUTE})", stats

weather_rate_limiter = WeatherRateLimiter()

def _get_redis_key(device_id: str) -> str:
    return f"{DEVICE_POSITION_KEY_PREFIX}:{device_id}"

async def get_device_position(device_id: str) -> Optional[Dict]:
    if not redis_client: return None
    redis_key = _get_redis_key(device_id)
    
    try:
        pos_data = await redis_client.hgetall(redis_key)
        if not pos_data: return None
        
        typed_pos_data = {}
        for key, value in pos_data.items():
            if key in ['lat', 'lon', 'current_lat', 'current_lon']:
                try: typed_pos_data[key] = float(value)
                except (ValueError, TypeError): typed_pos_data[key] = None
            elif key == 'weather_update_count':
                try: typed_pos_data[key] = int(value)
                except (ValueError, TypeError): typed_pos_data[key] = 0
            else:
                typed_pos_data[key] = value
                
        return typed_pos_data
    except Exception as e:
        print(f"[{datetime.datetime.now(datetime.timezone.utc)}] Error getting device position: {e}")
        return None

async def _save_device_position(device_id: str, position_data: Dict):
    if not redis_client: return
    redis_key = _get_redis_key(device_id)
    
    try:
        save_data = {k: v for k, v in position_data.items() if v is not None}
        if not save_data: return
        
        async with redis_client.pipeline(transaction=True) as pipe:
            pipe.hset(redis_key, mapping=save_data)
            pipe.expire(redis_key, DEVICE_POSITION_TTL_SECONDS)
            await pipe.execute()
    except Exception as e:
        print(f"[{datetime.datetime.now(datetime.timezone.utc)}] Error saving device position: {e}")

async def should_force_weather_update(device_id: str, current_lat: float, current_lon: float) -> Tuple[bool, str]:
    global_rate_ok, rate_message, rate_stats = await weather_rate_limiter.check_global_rate_limit()
    if not global_rate_ok:
        return False, f"global_rate_limit: {rate_stats.get('reason', 'exceeded')}"

    now_utc = datetime.datetime.now(datetime.timezone.utc)
    last_position = await get_device_position(device_id)

    if not last_position:
        print(f"[{now_utc}] Device {device_id} - first weather request (global: {rate_stats})")
        await _save_device_position(device_id, {
            'lat': current_lat, 
            'lon': current_lon, 
            'last_weather_update': now_utc.isoformat(), 
            'weather_update_count': 1,
            'rate_limit_stats': rate_message
        })
        return True, "first_request"

    last_lat = last_position.get('lat')
    last_lon = last_position.get('lon')
    last_update_iso = last_position.get('last_weather_update')

    if last_update_iso:
        try:
            last_update_time = datetime.datetime.fromisoformat(last_update_iso)
            time_since_update = now_utc - last_update_time
            if time_since_update.total_seconds() < WEATHER_FETCH_COOLDOWN_SECONDS:
                print(f"[{now_utc}] Device {device_id} - cooldown active ({time_since_update.total_seconds():.1f}s < {WEATHER_FETCH_COOLDOWN_SECONDS}s)")
                update_payload = {
                    'current_lat': current_lat,
                    'current_lon': current_lon,
                    'last_seen': now_utc.isoformat()
                }
                merged_data = last_position.copy()
                merged_data.update(update_payload)
                await _save_device_position(device_id, merged_data)
                return False, f"cooldown_active_{time_since_update.total_seconds():.1f}s"
        except Exception as e:
            print(f"[{now_utc}] Error parsing last update time: {e}")

    if last_lat is None or last_lon is None:
        print(f"[{now_utc}] Device {device_id} - invalid last position (global: {rate_stats})")
        last_position.update({
            'lat': current_lat, 
            'lon': current_lon, 
            'last_weather_update': now_utc.isoformat(), 
            'weather_update_count': last_position.get('weather_update_count', 0) + 1,
            'rate_limit_stats': rate_message
        })
        await _save_device_position(device_id, last_position)
        return True, "invalid_last_position"

    distance = calculate_distance_km(current_lat, current_lon, last_lat, last_lon)
    print(f"[{now_utc}] Device {device_id} - distance from last weather update: {distance:.2f}km (global: {rate_stats})")

    if distance >= MOVEMENT_THRESHOLD_KM:
        print(f"[{now_utc}] Device {device_id} - significant movement detected ({distance:.2f}km >= {MOVEMENT_THRESHOLD_KM}km)")
        last_position.update({
            'lat': current_lat, 
            'lon': current_lon, 
            'last_weather_update': now_utc.isoformat(), 
            'weather_update_count': last_position.get('weather_update_count', 0) + 1,
            'rate_limit_stats': rate_message
        })
        await _save_device_position(device_id, last_position)
        return True, f"moved_{distance:.2f}km"

    if last_update_iso:
        try:
            last_update_time = datetime.datetime.fromisoformat(last_update_iso)
            time_since_update = now_utc - last_update_time
            if time_since_update.total_seconds() > 3600:
                print(f"[{now_utc}] Device {device_id} - weather data expired ({time_since_update}) (global: {rate_stats})")
                last_position.update({
                    'lat': current_lat, 
                    'lon': current_lon, 
                    'last_weather_update': now_utc.isoformat(), 
                    'weather_update_count': last_position.get('weather_update_count', 0) + 1,
                    'rate_limit_stats': rate_message
                })
                await _save_device_position(device_id, last_position)
                return True, f"expired_{int(time_since_update.total_seconds())}s"
        except Exception as e:
            print(f"[{now_utc}] Error parsing last update time: {e}")

    update_payload = {
        'current_lat': current_lat,
        'current_lon': current_lon,
        'last_seen': now_utc.isoformat()
    }
    merged_data = last_position.copy()
    merged_data.update(update_payload)
    await _save_device_position(device_id, merged_data)
    
    print(f"[{now_utc}] Device {device_id} - using cached weather (distance: {distance:.2f}km, global: {rate_stats})")
    return False, f"cached_distance_{distance:.2f}km"

async def get_device_stats() -> Dict[str, any]:
    if not redis_client: return {'total_devices': 0, 'devices': {}}
    
    try:
        stats = {'devices': {}}
        async for key in redis_client.scan_iter(match=f"{DEVICE_POSITION_KEY_PREFIX}:*"):
            device_id = key.decode('utf-8').split(':')[-1]
            position = await get_device_position(device_id)
            if position:
                stats['devices'][device_id] = position

        stats['total_devices'] = len(stats['devices'])
        
        rate_limit_stats = {
            'max_per_minute': MAX_WEATHER_FETCHES_PER_MINUTE,
            'burst_limit': BURST_WEATHER_FETCHES_LIMIT,
            'cooldown_seconds': WEATHER_FETCH_COOLDOWN_SECONDS,
            'script_loaded': weather_rate_limiter.script_sha is not None
        }
        
        stats['rate_limiting'] = rate_limit_stats
        return stats
    except Exception as e:
        print(f"[{datetime.datetime.now(datetime.timezone.utc)}] Error getting device stats: {e}")
        return {'total_devices': 0, 'devices': {}, 'error': str(e)}

def cleanup_old_device_data(days_threshold: int = 7):
    print(f"[{datetime.datetime.now(datetime.timezone.utc)}] Device data cleanup is handled automatically by Redis key TTLs ({DEVICE_POSITION_TTL_SECONDS}s).")
