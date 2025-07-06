import os
import time
import datetime
import asyncio
from typing import Dict

CRITICAL_DISK_THRESHOLD_MB = 200
EMERGENCY_DISK_THRESHOLD_MB = 500

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

disk_monitor = DiskMonitor()

def is_emergency_mode():
    from . import state
    return state._emergency_mode

_last_disk_check = 0
_disk_stats = {'total_mb': 0, 'available_mb': 0, 'used_mb': 0}

async def monitor_disk_usage():
    global _last_disk_check, _disk_stats
    
    current_time = time.time()
    if current_time - _last_disk_check < 60:
        return _disk_stats
    
    _last_disk_check = current_time
    _disk_stats = await get_disk_usage()
    
    await disk_monitor.check_and_alert(_disk_stats['available_mb'])
    
    if _disk_stats['available_mb'] < CRITICAL_DISK_THRESHOLD_MB:
        from .disk_operations import emergency_disk_cleanup
        await emergency_disk_cleanup()
    
    return _disk_stats
