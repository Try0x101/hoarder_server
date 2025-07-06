import datetime
from typing import Optional, Dict
from app.cache import redis_client

DEVICE_POSITION_KEY_PREFIX = "device:position"
DEVICE_POSITION_TTL_SECONDS = 30 * 24 * 3600

def _get_redis_key(device_id: str) -> str:
    return f"{DEVICE_POSITION_KEY_PREFIX}:{device_id}"

async def get_device_position(device_id: str) -> Optional[Dict]:
    if not redis_client: 
        return None
    redis_key = _get_redis_key(device_id)
    
    try:
        pos_data = await redis_client.hgetall(redis_key)
        if not pos_data: 
            return None
        
        typed_pos_data = {}
        for key, value in pos_data.items():
            if key in ['lat', 'lon', 'current_lat', 'current_lon']:
                try: 
                    typed_pos_data[key] = float(value)
                except (ValueError, TypeError): 
                    typed_pos_data[key] = None
            elif key == 'weather_update_count':
                try: 
                    typed_pos_data[key] = int(value)
                except (ValueError, TypeError): 
                    typed_pos_data[key] = 0
            else:
                typed_pos_data[key] = value
                
        return typed_pos_data
    except Exception as e:
        print(f"[{datetime.datetime.now(datetime.timezone.utc)}] Error getting device position: {e}")
        return None

async def save_device_position(device_id: str, position_data: Dict):
    if not redis_client: 
        return
    redis_key = _get_redis_key(device_id)
    
    try:
        save_data = {k: v for k, v in position_data.items() if v is not None}
        if not save_data: 
            return
        
        async with redis_client.pipeline(transaction=True) as pipe:
            pipe.hset(redis_key, mapping=save_data)
            pipe.expire(redis_key, DEVICE_POSITION_TTL_SECONDS)
            await pipe.execute()
    except Exception as e:
        print(f"[{datetime.datetime.now(datetime.timezone.utc)}] Error saving device position: {e}")
