import datetime
from typing import Dict
from app.cache import redis_client
from .position_manager import get_device_position, DEVICE_POSITION_KEY_PREFIX, DEVICE_POSITION_TTL_SECONDS
from .rate_limiter import MAX_WEATHER_FETCHES_PER_MINUTE, BURST_WEATHER_FETCHES_LIMIT, weather_rate_limiter

WEATHER_FETCH_COOLDOWN_SECONDS = 30

async def get_device_stats() -> Dict[str, any]:
    if not redis_client: 
        return {'total_devices': 0, 'devices': {}}
    
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
