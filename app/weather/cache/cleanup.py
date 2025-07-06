import os
import time
import datetime
import asyncio
from . import state
from .disk import get_disk_usage, disk_monitor, emergency_disk_cleanup
from .log_manager import cleanup_old_request_logs
from .constants import (
    CACHE_DIR, WEATHER_CACHE_DURATION, MAX_CACHE_SIZE_MB, MAX_CACHE_FILES,
    EMERGENCY_DISK_THRESHOLD_MB, CRITICAL_DISK_THRESHOLD_MB
)

async def intelligent_cache_cleanup():
    async with state._cleanup_lock:
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
            
            state._last_cleanup = time.time()
            
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
    current_time = time.time()
    if current_time - state._last_disk_check < 60:
        return state._disk_stats
    
    state._last_disk_check = current_time
    state._disk_stats = await get_disk_usage()
    
    await disk_monitor.check_and_alert(state._disk_stats['available_mb'])
    
    if state._disk_stats['available_mb'] < CRITICAL_DISK_THRESHOLD_MB:
        await emergency_disk_cleanup()
    elif state._disk_stats['available_mb'] < EMERGENCY_DISK_THRESHOLD_MB:
        await intelligent_cache_cleanup()
    
    return state._disk_stats

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