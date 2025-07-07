import os
import json
import time
import datetime
import asyncio
import aiofiles
from typing import Optional, Dict, Any, List
from app.transforms.geo import calculate_distance_km

CACHE_DIR = "/tmp/weather_cache"
CACHE_DURATION_S = 3600
DISTANCE_THRESHOLD_KM = 1.0
MAX_CACHE_FILES = 100
MAX_CACHE_SIZE_MB = 50
_cleanup_lock = asyncio.Lock()

WEATHER_KEYS = {
    'weather_temp', 'weather_humidity', 'weather_apparent_temp', 'precipitation', 'weather_code',
    'pressure_msl', 'cloud_cover', 'wind_speed_10m', 'wind_direction_10m', 'wind_gusts_10m',
    'weather_observation_time', 'marine_wave_height', 'marine_wave_direction', 'marine_wave_period',
    'marine_swell_wave_height', 'marine_swell_wave_direction', 'marine_swell_wave_period'
}

def get_cache_key(lat: float, lon: float) -> str:
    import hashlib
    return hashlib.md5(f"{round(lat, 3)},{round(lon, 3)}".encode()).hexdigest()

async def find_cached_weather(lat: float, lon: float) -> Optional[Dict[str, Any]]:
    if not os.path.exists(CACHE_DIR): return None
    try:
        files = await asyncio.to_thread(lambda: [os.path.join(CACHE_DIR, f) for f in os.listdir(CACHE_DIR) if f.endswith('.json')])
        for path in files:
            try:
                if time.time() - os.path.getmtime(path) > CACHE_DURATION_S: continue
                async with aiofiles.open(path, 'r') as f: content = await f.read()
                cached_data = json.loads(content)
                if calculate_distance_km(lat, lon, cached_data.get('_cache_lat', 0), cached_data.get('_cache_lon', 0)) <= DISTANCE_THRESHOLD_KM:
                    return {k: v for k, v in cached_data.items() if k in WEATHER_KEYS}
            except Exception: continue
    except Exception: pass
    return None

async def save_weather_cache(lat: float, lon: float, data: Dict[str, Any]):
    os.makedirs(CACHE_DIR, exist_ok=True)
    cache_file = os.path.join(CACHE_DIR, f"{get_cache_key(lat, lon)}.json")
    cache_data = {k: v for k, v in data.items() if k in WEATHER_KEYS}
    cache_data.update({'_cache_lat': lat, '_cache_lon': lon})
    try:
        async with aiofiles.open(cache_file, 'w') as f:
            await f.write(json.dumps(cache_data, separators=(',', ':')))
        await enforce_cache_limits()
    except Exception as e: print(f"Cache write error: {e}")

async def enforce_cache_limits():
    async with _cleanup_lock:
        try:
            files = await asyncio.to_thread(lambda: [os.path.join(CACHE_DIR, f) for f in os.listdir(CACHE_DIR) if f.endswith('.json')])
            if not files: return

            files_with_meta = [(p, os.path.getsize(p), os.path.getmtime(p)) for p in files]
            total_size_mb = sum(size for _, size, _ in files_with_meta) / (1024*1024)
            
            if len(files_with_meta) <= MAX_CACHE_FILES and total_size_mb <= MAX_CACHE_SIZE_MB: return
            
            files_with_meta.sort(key=lambda x: x[2]) # sort by modification time
            
            removed_count = 0
            while len(files_with_meta) > MAX_CACHE_FILES or sum(f[1] for f in files_with_meta)/(1024*1024) > MAX_CACHE_SIZE_MB:
                file_to_remove = files_with_meta.pop(0)
                os.remove(file_to_remove[0])
                removed_count += 1
            if removed_count > 0: print(f"[{datetime.datetime.now()}] Cache cleanup: removed {removed_count} files.")
        except Exception: pass
