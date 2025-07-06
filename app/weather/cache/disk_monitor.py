import os
import time
import datetime
import asyncio
from typing import Dict

CACHE_DIR = "/tmp/weather_cache_optimized"
EMERGENCY_DISK_THRESHOLD_MB = 500
CRITICAL_DISK_THRESHOLD_MB = 200

_last_disk_check = 0
_disk_stats = {'total_mb': 0, 'available_mb': 0, 'used_mb': 0}
_emergency_mode = False

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

async def _basic_cache_cleanup():
    try:
        def _remove_old_files():
            if not os.path.exists(CACHE_DIR):
                return 0, 0
            
            files_removed = 0
            space_freed = 0
            current_time = time.time()
            
            for cache_file in os.listdir(CACHE_DIR):
                if cache_file.endswith('.json'):
                    file_path = os.path.join(CACHE_DIR, cache_file)
                    try:
                        file_age = current_time - os.path.getmtime(file_path)
                        if file_age > 3600:
                            size = os.path.getsize(file_path)
                            os.remove(file_path)
                            files_removed += 1
                            space_freed += size
                    except:
                        continue
            
            return files_removed, space_freed
        
        return await asyncio.to_thread(_remove_old_files)
    except Exception:
        return 0, 0

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
    
    return _disk_stats

async def emergency_disk_cleanup():
    global _emergency_mode
    
    try:
        _emergency_mode = True
        files_removed, space_freed = await _basic_cache_cleanup()
        print(f"[{datetime.datetime.now()}] EMERGENCY: Cleanup completed - removed {files_removed} files")
    except Exception as e:
        print(f"[{datetime.datetime.now()}] CRITICAL: Emergency cleanup failed: {e}")
    finally:
        _emergency_mode = False

def is_emergency_mode():
    return _emergency_mode
