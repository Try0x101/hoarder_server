import os
import datetime
from . import state
from .disk import get_disk_usage, get_directory_size_mb
from .constants import (
    CACHE_DIR, REQUEST_LOG_FILE, MAX_CACHE_SIZE_MB, 
    EMERGENCY_DISK_THRESHOLD_MB, CRITICAL_DISK_THRESHOLD_MB
)

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
            'emergency_mode': state._emergency_mode,
            'last_cleanup': datetime.datetime.fromtimestamp(state._last_cleanup).isoformat() if state._last_cleanup else None,
            'thresholds': {
                'max_cache_size_mb': MAX_CACHE_SIZE_MB,
                'emergency_threshold_mb': EMERGENCY_DISK_THRESHOLD_MB,
                'critical_threshold_mb': CRITICAL_DISK_THRESHOLD_MB
            }
        }
        
    except Exception as e:
        return {'error': str(e)}