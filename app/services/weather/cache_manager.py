import os
import json
import time
import datetime
import asyncio
import aiofiles
from typing import Optional, Dict, Any
from app.transforms.geo import calculate_distance_km

CACHE_DIR = "/tmp/weather_cache"
CACHE_DURATION = 3600
DISTANCE_THRESHOLD_KM = 1.0
MAX_CACHE_FILES = 500

WEATHER_KEYS = {
    'weather_temp', 'weather_humidity', 'weather_apparent_temp',
    'precipitation', 'weather_code', 'pressure_msl', 'cloud_cover',
    'wind_speed_10m', 'wind_direction_10m', 'wind_gusts_10m',
    'weather_observation_time', 'marine_wave_height', 'marine_wave_direction',
    'marine_wave_period', 'marine_swell_wave_height', 'marine_swell_wave_direction',
    'marine_swell_wave_period'
}

def safe_ensure_cache_dir():
    try:
        os.makedirs(CACHE_DIR, exist_ok=True)
        return True
    except Exception:
        return False

def get_cache_key(lat: float, lon: float) -> str:
    import hashlib
    rounded_lat, rounded_lon = round(lat, 3), round(lon, 3)
    return hashlib.md5(f"{rounded_lat}_{rounded_lon}".encode()).hexdigest()

async def safe_scan_cache_files() -> list:
    try:
        def scan_files():
            try:
                return [f for f in os.listdir(CACHE_DIR) if f.endswith('.json')][:MAX_CACHE_FILES]
            except:
                return []
        return await asyncio.to_thread(scan_files)
    except Exception:
        return []

async def find_cached_weather(lat: float, lon: float) -> Optional[Dict[str, Any]]:
    if not safe_ensure_cache_dir():
        return None
        
    cache_files = await safe_scan_cache_files()
    
    for cache_file in cache_files:
        cache_path = os.path.join(CACHE_DIR, cache_file)
        
        try:
            file_age = await asyncio.to_thread(lambda: time.time() - os.path.getmtime(cache_path))
            if file_age > CACHE_DURATION:
                await asyncio.to_thread(os.remove, cache_path)
                continue
        except Exception:
            continue
        
        try:
            async with aiofiles.open(cache_path, 'r') as f:
                content = await f.read()
            cached_data = await asyncio.to_thread(json.loads, content)
        except Exception:
            continue
        
        cached_lat = cached_data.get('_cache_lat')
        cached_lon = cached_data.get('_cache_lon')
        
        if cached_lat is None or cached_lon is None:
            continue
        
        try:
            distance = calculate_distance_km(lat, lon, cached_lat, cached_lon)
            if distance <= DISTANCE_THRESHOLD_KM:
                return {k: v for k, v in cached_data.items() if k in WEATHER_KEYS}
        except Exception:
            continue
    
    return None

async def save_weather_cache(lat: float, lon: float, data: Dict[str, Any]):
    if not safe_ensure_cache_dir():
        return
        
    try:
        cache_key = get_cache_key(lat, lon)
        cache_file = os.path.join(CACHE_DIR, f"{cache_key}.json")
        
        cache_data = {k: v for k, v in data.items() if k in WEATHER_KEYS}
        cache_data.update({
            '_cache_lat': lat,
            '_cache_lon': lon,
            '_cache_time': datetime.datetime.now(datetime.timezone.utc).isoformat()
        })
        
        json_content = await asyncio.to_thread(json.dumps, cache_data)
        async with aiofiles.open(cache_file, 'w') as f:
            await f.write(json_content)
    except Exception as e:
        print(f"Cache write error: {e}")
