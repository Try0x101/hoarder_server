import os
import time
import datetime
import asyncio
import json
import aiofiles
from typing import Dict
from . import state
from .constants import (
    CRITICAL_DISK_THRESHOLD_MB, EMERGENCY_DISK_THRESHOLD_MB, 
    EMERGENCY_CLEANUP_LOG, CACHE_DIR, REQUEST_LOG_FILE
)

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
    emergency_log_entry = {
        'timestamp': datetime.datetime.now().isoformat(),
        'action': 'emergency_cleanup_started',
        'disk_stats': await get_disk_usage()
    }
    
    try:
        state._emergency_mode = True
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
        state._emergency_mode = False
        
        try:
            async with aiofiles.open(EMERGENCY_CLEANUP_LOG, 'a') as f:
                await f.write(json.dumps(emergency_log_entry) + '\n')
        except:
            pass

disk_monitor.add_critical_callback(emergency_disk_cleanup)
disk_monitor.add_warning_callback(lambda mb: print(f"[{datetime.datetime.now()}] WARNING: Low disk space: {mb:.1f}MB"))