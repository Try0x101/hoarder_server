import datetime
from typing import Tuple, Dict
from app.cache import redis_client

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
