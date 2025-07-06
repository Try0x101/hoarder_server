from app.cache import get_cached_data, set_cached_data, CACHE_KEY_LATEST_DATA
from app.db import get_raw_latest_data_for_all_devices, get_raw_latest_payload_for_device

async def get_cached_latest_data():
    cached_raw_data = await get_cached_data(CACHE_KEY_LATEST_DATA)
    
    if cached_raw_data and isinstance(cached_raw_data, list):
        return cached_raw_data
    else:
        raw_data = await get_raw_latest_data_for_all_devices()
        if not isinstance(raw_data, list):
            raw_data = []
        
        await set_cached_data(CACHE_KEY_LATEST_DATA, raw_data, ttl=5)
        return raw_data

async def get_cached_device_data(device_id: str):
    cache_key = f"latest_data_raw_{device_id}"
    cached_raw_payload = await get_cached_data(cache_key)
    
    if cached_raw_payload and isinstance(cached_raw_payload, dict):
        return cached_raw_payload
    else:
        raw_payload = await get_raw_latest_payload_for_device(device_id)
        if raw_payload is None:
            return None
        
        await set_cached_data(cache_key, raw_payload, ttl=5)
        return raw_payload
