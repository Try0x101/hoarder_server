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
MAX_CACHE_FILES = 100
MAX_CACHE_SIZE_MB = 50
EMERGENCY_CLEANUP_THRESHOLD = 80

WEATHER_KEYS = {
    'weather_temp', 'weather_humidity', 'weather_apparent_temp',
    'precipitation', 'weather_code', 'pressure_msl', 'cloud_cover',
    'wind_speed_10m', 'wind_direction_10m', 'wind_gusts_10m',
    'weather_observation_time', 'marine_wave_height', 'marine_wave_direction',
    'marine_wave_period', 'marine_swell_wave_height', 'marine_swell_wave_direction',
    'marine_swell_wave_period'
}

_cleanup_lock = asyncio.Lock()
_last_size_check = 0

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

async def get_cache_size_mb() -> float:
    try:
        def calculate_size():
            total_size = 0
            for file in os.listdir(CACHE_DIR):
                if file.endswith('.json'):
                    try:
                        total_size += os.path.getsize(os.path.join(CACHE_DIR, file))
                    except:
                        continue
            return total_size / 1024 / 1024
        return await asyncio.to_thread(calculate_size)
    except Exception:
        return 0.0

async def safe_scan_cache_files() -> list:
    try:
        def scan_files():
            try:
                files = [f for f in os.listdir(CACHE_DIR) if f.endswith('.json')]
                return sorted(files, key=lambda x: os.path.getmtime(os.path.join(CACHE_DIR, x)), reverse=True)
            except:
                return []
        return await asyncio.to_thread(scan_files)
    except Exception:
        return []

async def emergency_cleanup():
    async with _cleanup_lock:
        try:
            cache_files = await safe_scan_cache_files()
            if len(cache_files) <= MAX_CACHE_FILES // 2:
                return
            
            files_to_remove = cache_files[MAX_CACHE_FILES // 2:]
            removed_count = 0
            
            for cache_file in files_to_remove:
                try:
                    cache_path = os.path.join(CACHE_DIR, cache_file)
                    await asyncio.to_thread(os.remove, cache_path)
                    removed_count += 1
                except:
                    continue
            
            print(f"[{datetime.datetime.now()}] Emergency cache cleanup: removed {removed_count} files")
        except Exception as e:
            print(f"[{datetime.datetime.now()}] Emergency cleanup failed: {e}")

async def enforce_cache_limits():
    global _last_size_check
    current_time = time.time()
    
    if current_time - _last_size_check < 300:
        return
    
    async with _cleanup_lock:
        try:
            cache_size = await get_cache_size_mb()
            cache_files = await safe_scan_cache_files()
            
            if cache_size > EMERGENCY_CLEANUP_THRESHOLD or len(cache_files) > MAX_CACHE_FILES:
                await emergency_cleanup()
            elif len(cache_files) > MAX_CACHE_FILES:
                files_to_remove = cache_files[MAX_CACHE_FILES:]
                for cache_file in files_to_remove:
                    try:
                        cache_path = os.path.join(CACHE_DIR, cache_file)
                        await asyncio.to_thread(os.remove, cache_path)
                    except:
                        continue
            
            _last_size_check = current_time
        except Exception as e:
            print(f"[{datetime.datetime.now()}] Cache limit enforcement failed: {e}")

async def find_cached_weather(lat: float, lon: float) -> Optional[Dict[str, Any]]:
    if not safe_ensure_cache_dir():
        return None
    
    await enforce_cache_limits()
    cache_files = await safe_scan_cache_files()
    
    for cache_file in cache_files[:50]:
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
            try:
                await asyncio.to_thread(os.remove, cache_path)
            except:
                pass
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
    
    await enforce_cache_limits()
    
    try:
        cache_key = get_cache_key(lat, lon)
        cache_file = os.path.join(CACHE_DIR, f"{cache_key}.json")
        
        cache_data = {k: v for k, v in data.items() if k in WEATHER_KEYS}
        cache_data.update({
            '_cache_lat': lat,
            '_cache_lon': lon,
            '_cache_time': datetime.datetime.now(datetime.timezone.utc).isoformat()
        })
        
        json_content = await asyncio.to_thread(json.dumps, cache_data, separators=(',', ':'))
        async with aiofiles.open(cache_file, 'w') as f:
            await f.write(json_content)
    except Exception as e:
        print(f"Cache write error: {e}")

async def cleanup_old_cache():
    async with _cleanup_lock:
        try:
            cache_files = await safe_scan_cache_files()
            current_time = time.time()
            removed_count = 0
            
            for cache_file in cache_files:
                cache_path = os.path.join(CACHE_DIR, cache_file)
                try:
                    file_age = current_time - os.path.getmtime(cache_path)
                    if file_age > CACHE_DURATION:
                        os.remove(cache_path)
                        removed_count += 1
                except:
                    continue
            
            if removed_count > 0:
                print(f"[{datetime.datetime.now()}] Cleaned {removed_count} expired cache files")
        except Exception as e:
            print(f"[{datetime.datetime.now()}] Cache cleanup error: {e}")

async def get_cache_stats() -> Dict[str, Any]:
    if not safe_ensure_cache_dir():
        return {"error": "cache_directory_unavailable"}
    
    try:
        cache_files = await safe_scan_cache_files()
        cache_size = await get_cache_size_mb()
        
        valid_files = 0
        current_time = time.time()
        
        for cache_file in cache_files[:20]:
            cache_path = os.path.join(CACHE_DIR, cache_file)
            try:
                file_age = current_time - os.path.getmtime(cache_path)
                if file_age <= CACHE_DURATION:
                    valid_files += 1
            except:
                continue
        
        return {
            "total_files": len(cache_files),
            "valid_files": valid_files,
            "max_files": MAX_CACHE_FILES,
            "cache_size_mb": f"{cache_size:.1f}",
            "max_size_mb": MAX_CACHE_SIZE_MB,
            "cache_duration_hours": CACHE_DURATION / 3600,
            "distance_threshold_km": DISTANCE_THRESHOLD_KM,
            "emergency_threshold_mb": EMERGENCY_CLEANUP_THRESHOLD
        }
    except Exception as e:
        return {"error": f"cache_stats_failed: {e}"}
