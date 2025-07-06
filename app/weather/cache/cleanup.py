import os
import datetime
import asyncio
import time
from .disk_monitor import get_disk_usage

CACHE_DIR = "/tmp/weather_cache_optimized"
REQUEST_LOG_FILE = "/tmp/weather_requests.log"
EMERGENCY_CLEANUP_LOG = "/tmp/weather_emergency_cleanup.log"
MAX_CACHE_SIZE_MB = 50
MAX_CACHE_FILES = 1000
WEATHER_CACHE_DURATION = 3600
LOG_ROTATION_SIZE_MB = 2

_last_cleanup = 0
_cleanup_lock = asyncio.Lock()

async def cleanup_old_request_logs():
    try:
        await rotate_log_file(REQUEST_LOG_FILE, LOG_ROTATION_SIZE_MB)
        
        if os.path.exists(EMERGENCY_CLEANUP_LOG):
            from .statistics import get_directory_size_mb
            log_size_mb = await get_directory_size_mb(EMERGENCY_CLEANUP_LOG)
            if log_size_mb > 5:
                await rotate_log_file(EMERGENCY_CLEANUP_LOG, 5)
                
    except Exception as e:
        print(f"[{datetime.datetime.now()}] Log cleanup error: {e}")

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
            
            os.rename(log_path, backup_path)
            
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
            
            cache_files, total_cache_size = await _analyze_cache_files()
            total_cache_size_mb = total_cache_size / (1024 * 1024)
            
            files_to_remove = await _determine_files_to_remove(cache_files, total_cache_size_mb)
            removed_count, size_freed = await _cleanup_files(files_to_remove)
            
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

async def _analyze_cache_files():
    def _scan_files():
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
    
    return await asyncio.to_thread(_scan_files)

async def _determine_files_to_remove(cache_files, total_cache_size_mb):
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
    
    return files_to_remove

async def _cleanup_files(files_to_remove):
    def _remove_files():
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
    
    return await asyncio.to_thread(_remove_files)
