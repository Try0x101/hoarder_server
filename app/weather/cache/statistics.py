import os
import datetime
import asyncio
from .disk_monitor import get_disk_usage, is_emergency_mode

CACHE_DIR = "/tmp/weather_cache_optimized"
REQUEST_LOG_FILE = "/tmp/weather_requests.log"
MAX_CACHE_SIZE_MB = 50
EMERGENCY_DISK_THRESHOLD_MB = 500
CRITICAL_DISK_THRESHOLD_MB = 200

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
            'emergency_mode': is_emergency_mode(),
            'thresholds': {
                'max_cache_size_mb': MAX_CACHE_SIZE_MB,
                'emergency_threshold_mb': EMERGENCY_DISK_THRESHOLD_MB,
                'critical_threshold_mb': CRITICAL_DISK_THRESHOLD_MB
            }
        }
        
    except Exception as e:
        return {'error': str(e)}
