import os
import json
import hashlib
import datetime
import shutil
import asyncio
import aiofiles
import time
from typing import Optional, Dict, Any, Tuple, List
from app.transforms.geo import calculate_distance_km

CACHE_DIR = "/tmp/weather_cache_optimized"
REQUEST_LOG_FILE = "/tmp/weather_requests.log"
EMERGENCY_CLEANUP_LOG = "/tmp/weather_emergency_cleanup.log"

WEATHER_CACHE_DURATION = 3600
DISTANCE_THRESHOLD_KM = 1.0
MAX_CACHE_SIZE_MB = 50
MAX_LOG_SIZE_MB = 5
EMERGENCY_DISK_THRESHOLD_MB = 500
CRITICAL_DISK_THRESHOLD_MB = 200
CLEANUP_INTERVAL_HOURS = 0.5
MAX_CACHE_FILES = 1000
LOG_ROTATION_SIZE_MB = 2

WEATHER_KEYS = {
    'weather_temp', 'weather_humidity', 'weather_apparent_temp',
    'precipitation', 'weather_code', 'pressure_msl', 'cloud_cover',
    'wind_speed_10m', 'wind_direction_10m', 'wind_gusts_10m',
    'weather_observation_time', 'marine_wave_height', 'marine_wave_direction',
    'marine_wave_period', 'marine_swell_wave_height', 'marine_swell_wave_direction',
    'marine_swell_wave_period'
}

_last_cleanup = 0
_last_disk_check = 0
_cleanup_lock = asyncio.Lock()
_emergency_mode = False
_disk_stats = {'total_mb': 0, 'available_mb': 0, 'used_mb': 0}

_file_list_cache: Optional[List[str]] = None
_file_list_cache_time: float = 0
_file_list_lock = asyncio.Lock()

class DiskMonitor:
    def __init__(self):
        self.critical_callbacks = []
        self.warning_callbacks = []
        self.last_alert = 0
        
    def add_critical_callback(self, callback):
        self.critical_callbacks.append(callback)
        
    def add_warning_callback(self, callback):
        self.warning_callbacks.append(callback)
        
    async def check_and_alert(self, available_mb: float):
        current_time = time.time()
        
        if available_mb < CRITICAL_DISK_THRESHOLD_MB:
            if current_time - self.last_alert > 300:
                for callback in self.critical_callbacks:
                    try:
                        await callback(available_mb)
                    except Exception as e:
                        print(f"[{datetime.datetime.now()}] Critical callback failed: {e}")
                self.last_alert = current_time
                        
        elif available_mb < EMERGENCY_DISK_THRESHOLD_MB:
            if current_time - self.last_alert > 600:
                for callback in self.warning_callbacks:
                    try:
                        await callback(available_mb)
                    except Exception as e:
                        print(f"[{datetime.datetime.now()}] Warning callback failed: {e}")
                self.last_alert = current_time

disk_monitor = DiskMonitor()

async def get_disk_usage() -> Dict[str, float]:
    try:
        def _get_disk_stats():
            statvfs = os.statvfs('/tmp')
            total_bytes = statvfs.f_frsize * statvfs.f_blocks
            available_bytes = statvfs.f_frsize * statvfs.f_bavail
            used_bytes = total_bytes - available_bytes
            return {
                'total_mb': total_bytes / (1024 * 1024),
                'available_mb': available_bytes / (1024 * 1024),
                'used_mb': used_bytes / (1024 * 1024),
                'usage_percent': (used_bytes / total_bytes) * 100
            }
        
        return await asyncio.to_thread(_get_disk_stats)
    except Exception as e:
        print(f"[{datetime.datetime.now()}] Error getting disk usage: {e}")
        return {'total_mb': 0, 'available_mb': 0, 'used_mb': 0, 'usage_percent': 0}

async def get_directory_size_mb(path: str) -> float:
    if not os.path.exists(path):
        return 0.0
    
    try:
        def _calculate_size():
            if os.path.isfile(path):
                return os.path.getsize(path) / (1024 * 1024)
            
            total_size = 0
            for dirpath, dirnames, filenames in os.walk(path):
                for filename in filenames:
                    filepath = os.path.join(dirpath, filename)
                    try:
                        total_size += os.path.getsize(filepath)
                    except (OSError, IOError):
                        continue
            return total_size / (1024 * 1024)
        
        return await asyncio.to_thread(_calculate_size)
    except Exception:
        return 0.0

async def emergency_disk_cleanup():
    global _emergency_mode
    
    emergency_log_entry = {
        'timestamp': datetime.datetime.now().isoformat(),
        'action': 'emergency_cleanup_started',
        'disk_stats': await get_disk_usage()
    }
    
    try:
        _emergency_mode = True
        cleanup_results = {'cache_files_removed': 0, 'log_files_truncated': 0, 'space_freed_mb': 0}
        
        def _cleanup_cache_files():
            if not os.path.exists(CACHE_DIR):
                return 0, 0
                
            cache_files = []
            for cache_file in os.listdir(CACHE_DIR):
                if cache_file.endswith('.json'):
                    cache_path = os.path.join(CACHE_DIR, cache_file)
                    try:
                        mtime = os.path.getmtime(cache_path)
                        size = os.path.getsize(cache_path)
                        cache_files.append((cache_path, mtime, size))
                    except:
                        continue
            
            cache_files.sort(key=lambda x: x[1])
            
            space_freed = 0
            files_removed = 0
            for cache_path, mtime, size in cache_files:
                try:
                    os.remove(cache_path)
                    files_removed += 1
                    space_freed += size
                    
                    if space_freed > 50 * 1024 * 1024:
                        break
                except:
                    continue
            
            return files_removed, space_freed
        
        files_removed, space_freed = await asyncio.to_thread(_cleanup_cache_files)
        cleanup_results['cache_files_removed'] = files_removed
        cleanup_results['space_freed_mb'] = space_freed / (1024 * 1024)
        
        if os.path.exists(REQUEST_LOG_FILE):
            try:
                def _truncate_log():
                    log_size = os.path.getsize(REQUEST_LOG_FILE)
                    if log_size > 1024 * 1024:
                        with open(REQUEST_LOG_FILE, 'w') as f:
                            f.write(f"{datetime.datetime.now().isoformat()}\n")
                        return 1
                    return 0
                
                truncated = await asyncio.to_thread(_truncate_log)
                cleanup_results['log_files_truncated'] = truncated
            except:
                pass
        
        emergency_log_entry.update({
            'action': 'emergency_cleanup_completed',
            'results': cleanup_results,
            'final_disk_stats': await get_disk_usage()
        })
        
        print(f"[{datetime.datetime.now()}] EMERGENCY: Disk cleanup completed - {cleanup_results}")
        
    except Exception as e:
        emergency_log_entry.update({
            'action': 'emergency_cleanup_failed',
            'error': str(e)
        })
        print(f"[{datetime.datetime.now()}] CRITICAL: Emergency cleanup failed: {e}")
    finally:
        _emergency_mode = False
        
        try:
            async with aiofiles.open(EMERGENCY_CLEANUP_LOG, 'a') as f:
                await f.write(json.dumps(emergency_log_entry) + '\n')
        except:
            pass

async def rotate_log_file(log_path: str, max_size_mb: float):
    try:
        if not os.path.exists(log_path):
            return
        
        def _rotate_log():
            size_mb = os.path.getsize(log_path) / (1024 * 1024)
            if size_mb <= max_size_mb:
                return False
                
            backup_path = f"{log_path}.{int(time.time())}"
            
            cutoff_date = datetime.date.today() - datetime.timedelta(days=1)
            recent_lines = []
            
            with open(log_path, 'r') as f:
                for line in f:
                    try:
                        timestamp_str = line.strip()
                        request_time = datetime.datetime.fromisoformat(timestamp_str)
                        if request_time.date() >= cutoff_date:
                            recent_lines.append(line)
                    except:
                        continue
            
            shutil.move(log_path, backup_path)
            
            with open(log_path, 'w') as f:
                f.writelines(recent_lines[-1000:])
            
            if os.path.exists(backup_path):
                os.remove(backup_path)
            return True
        
        rotated = await asyncio.to_thread(_rotate_log)
        if rotated:
            print(f"[{datetime.datetime.now()}] Log rotated: {log_path}")
            
    except Exception as e:
        print(f"[{datetime.datetime.now()}] Log rotation failed for {log_path}: {e}")

async def intelligent_cache_cleanup():
    global _last_cleanup
    
    async with _cleanup_lock:
        try:
            if not os.path.exists(CACHE_DIR):
                return {'removed': 0, 'size_freed_mb': 0}
            
            def _analyze_cache_files():
                current_time = time.time()
                cache_files = []
                total_cache_size = 0
                
                for cache_file in os.listdir(CACHE_DIR):
                    if not cache_file.endswith('.json'):
                        continue
                        
                    cache_path = os.path.join(CACHE_DIR, cache_file)
                    try:
                        stat = os.stat(cache_path)
                        file_age = current_time - stat.st_mtime
                        file_size = stat.st_size
                        total_cache_size += file_size
                        
                        cache_files.append({
                            'path': cache_path,
                            'age': file_age,
                            'size': file_size,
                            'priority': file_age + (file_size / 1024)
                        })
                    except:
                        try:
                            os.remove(cache_path)
                        except:
                            pass
                        continue
                
                return cache_files, total_cache_size
            
            cache_files, total_cache_size = await asyncio.to_thread(_analyze_cache_files)
            total_cache_size_mb = total_cache_size / (1024 * 1024)
            
            def _cleanup_files(files_to_remove):
                removed_count = 0
                size_freed = 0
                
                for cache_info in files_to_remove:
                    try:
                        os.remove(cache_info['path'])
                        removed_count += 1
                        size_freed += cache_info['size']
                    except:
                        continue
                        
                return removed_count, size_freed
            
            cache_files.sort(key=lambda x: x['priority'], reverse=True)
            
            target_removal_count = 0
            if total_cache_size_mb > MAX_CACHE_SIZE_MB:
                target_removal_count = len(cache_files) // 3
            elif len(cache_files) > MAX_CACHE_FILES:
                target_removal_count = len(cache_files) - MAX_CACHE_FILES
            
            files_to_remove = []
            for cache_info in cache_files:
                if cache_info['age'] > WEATHER_CACHE_DURATION or len(files_to_remove) < target_removal_count:
                    files_to_remove.append(cache_info)
                    
                if len(files_to_remove) >= target_removal_count and total_cache_size_mb - (sum(f['size'] for f in files_to_remove) / (1024 * 1024)) <= MAX_CACHE_SIZE_MB:
                    break
            
            removed_count, size_freed = await asyncio.to_thread(_cleanup_files, files_to_remove)
            
            _last_cleanup = time.time()
            
            result = {
                'removed': removed_count,
                'size_freed_mb': size_freed / (1024 * 1024),
                'remaining_files': len(cache_files) - removed_count,
                'remaining_size_mb': (total_cache_size - size_freed) / (1024 * 1024)
            }
            
            if removed_count > 0:
                print(f"[{datetime.datetime.now()}] Cache cleanup: {result}")
            
            return result
            
        except Exception as e:
            print(f"[{datetime.datetime.now()}] Cache cleanup failed: {e}")
            return {'removed': 0, 'size_freed_mb': 0}

async def monitor_disk_usage():
    global _last_disk_check, _disk_stats
    
    current_time = time.time()
    if current_time - _last_disk_check < 60:
        return _disk_stats
    
    _last_disk_check = current_time
    _disk_stats = await get_disk_usage()
    
    await disk_monitor.check_and_alert(_disk_stats['available_mb'])
    
    if _disk_stats['available_mb'] < CRITICAL_DISK_THRESHOLD_MB:
        await emergency_disk_cleanup()
    elif _disk_stats['available_mb'] < EMERGENCY_DISK_THRESHOLD_MB:
        await intelligent_cache_cleanup()
    
    return _disk_stats

def round_coordinates(lat: float, lon: float, precision: int = 3) -> Tuple[float, float]:
    return round(lat, precision), round(lon, precision)

def ensure_cache_dir():
    if not os.path.exists(CACHE_DIR):
        os.makedirs(CACHE_DIR, exist_ok=True)

def get_cache_key(lat: float, lon: float) -> str:
    rounded_lat, rounded_lon = round_coordinates(lat, lon)
    return hashlib.md5(f"{rounded_lat}_{rounded_lon}".encode()).hexdigest()

async def find_nearby_cached_weather(lat: float, lon: float) -> Optional[Dict[str, Any]]:
    global _file_list_cache, _file_list_cache_time
    try:
        await monitor_disk_usage()
        
        if _emergency_mode:
            return None
            
        ensure_cache_dir()
        
        cache_files = []
        async with _file_list_lock:
            current_time = time.time()
            if _file_list_cache is not None and current_time - _file_list_cache_time < 5:
                cache_files = _file_list_cache
            else:
                def _scan_cache_files():
                    try:
                        return [f for f in os.listdir(CACHE_DIR) if f.endswith('.json')][:100]
                    except:
                        return []
                
                cache_files = await asyncio.to_thread(_scan_cache_files)
                _file_list_cache = cache_files
                _file_list_cache_time = current_time
        
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
        if _emergency_mode:
            return
            
        disk_stats = await monitor_disk_usage()
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

async def cleanup_old_request_logs():
    try:
        await rotate_log_file(REQUEST_LOG_FILE, LOG_ROTATION_SIZE_MB)
        
        if os.path.exists(EMERGENCY_CLEANUP_LOG):
            log_size_mb = await get_directory_size_mb(EMERGENCY_CLEANUP_LOG)
            if log_size_mb > MAX_LOG_SIZE_MB:
                await rotate_log_file(EMERGENCY_CLEANUP_LOG, MAX_LOG_SIZE_MB)
                
    except Exception as e:
        print(f"[{datetime.datetime.now()}] Log cleanup error: {e}")

async def get_cache_stats():
    try:
        disk_stats = await get_disk_usage()
        cache_size_mb = await get_directory_size_mb(CACHE_DIR)
        log_size_mb = await get_directory_size_mb(REQUEST_LOG_FILE)
        
        def _count_cache_files():
            if os.path.exists(CACHE_DIR):
                return len([f for f in os.listdir(CACHE_DIR) if f.endswith('.json')])
            return 0
        
        cache_file_count = await asyncio.to_thread(_count_cache_files)
        
        return {
            'disk_stats': disk_stats,
            'cache_size_mb': cache_size_mb,
            'cache_file_count': cache_file_count,
            'log_size_mb': log_size_mb,
            'emergency_mode': _emergency_mode,
            'last_cleanup': datetime.datetime.fromtimestamp(_last_cleanup).isoformat() if _last_cleanup else None,
            'thresholds': {
                'max_cache_size_mb': MAX_CACHE_SIZE_MB,
                'emergency_threshold_mb': EMERGENCY_DISK_THRESHOLD_MB,
                'critical_threshold_mb': CRITICAL_DISK_THRESHOLD_MB
            }
        }
        
    except Exception as e:
        return {'error': str(e)}

async def periodic_maintenance():
    while True:
        try:
            await intelligent_cache_cleanup()
            await cleanup_old_request_logs()
            await monitor_disk_usage()
            await asyncio.sleep(1800)
        except Exception as e:
            print(f"[{datetime.datetime.now()}] Maintenance task error: {e}")
            await asyncio.sleep(3600)

disk_monitor.add_critical_callback(emergency_disk_cleanup)
disk_monitor.add_warning_callback(lambda mb: print(f"[{datetime.datetime.now()}] WARNING: Low disk space: {mb:.1f}MB"))
