import os
import json
import datetime
import asyncio
import aiofiles
import time
from typing import Optional, Dict, Any
from app.transforms.geo import calculate_distance_km
from .constants import CACHE_DIR, WEATHER_CACHE_DURATION, WEATHER_KEYS, DISTANCE_THRESHOLD_KM, EMERGENCY_DISK_THRESHOLD_MB
from .helpers import ensure_cache_dir, get_cache_key
from .disk_monitor import get_disk_usage
from . import state

async def find_nearby_cached_weather(lat: float, lon: float) -> Optional[Dict[str, Any]]:
    try:
        disk_stats = await get_disk_usage()
        
        if state._emergency_mode:
            return None
            
        ensure_cache_dir()
        
        cache_files = []
        async with state._file_list_lock:
            current_time = time.time()
            if state._file_list_cache is not None and current_time - state._file_list_cache_time < 5:
                cache_files = state._file_list_cache
            else:
                def _scan_cache_files():
                    try:
                        return [f for f in os.listdir(CACHE_DIR) if f.endswith('.json')][:100]
                    except:
                        return []
                
                cache_files = await asyncio.to_thread(_scan_cache_files)
                state._file_list_cache = cache_files
                state._file_list_cache_time = current_time
        
        for cache_file in cache_files:
            cache_path = os.path.join(CACHE_DIR, cache_file)
            
            try:
                def _check_file_age():
                    return time.time() - os.path.getmtime(cache_path)
                
                file_age = await asyncio.to_thread(_check_file_age)
                if file_age > WEATHER_CACHE_DURATION:
                    try:
                        await asyncio.to_thread(os.remove, cache_path)
                    except:
                        pass
                    continue
                
                async with aiofiles.open(cache_path, 'r') as f:
                    content = await f.read()
                    cached_data = await asyncio.to_thread(json.loads, content)
                
                cached_lat = cached_data.get('_cache_lat')
                cached_lon = cached_data.get('_cache_lon')
                
                if cached_lat is None or cached_lon is None:
                    continue
                
                distance = calculate_distance_km(lat, lon, cached_lat, cached_lon)
                if distance <= DISTANCE_THRESHOLD_KM:
                    return {k: v for k, v in cached_data.items() if k in WEATHER_KEYS}
                    
            except Exception:
                try:
                    await asyncio.to_thread(os.remove, cache_path)
                except:
                    pass
                continue
                
    except Exception as e:
        print(f"[{datetime.datetime.now()}] Cache lookup error: {e}")
    
    return None

async def save_weather_to_cache(lat: float, lon: float, data: Dict[str, Any]):
    try:
        if state._emergency_mode:
            return
            
        disk_stats = await get_disk_usage()
        if disk_stats['available_mb'] < EMERGENCY_DISK_THRESHOLD_MB:
            return
            
        ensure_cache_dir()
        
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
        print(f"[{datetime.datetime.now()}] Cache write error: {e}")
