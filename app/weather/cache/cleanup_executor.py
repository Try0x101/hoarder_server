import os
import datetime
import asyncio
import time
from .constants import CACHE_DIR
from .cleanup_strategy import analyze_cache_files, determine_files_to_remove, cleanup_files

_last_cleanup = 0
_cleanup_lock = asyncio.Lock()

async def intelligent_cache_cleanup():
    global _last_cleanup
    
    async with _cleanup_lock:
        try:
            if not os.path.exists(CACHE_DIR):
                return {'removed': 0, 'size_freed_mb': 0}
            
            cache_files, total_size = await analyze_cache_files()
            files_to_remove = await determine_files_to_remove(cache_files, total_size)
            removed_count, size_freed = await cleanup_files(files_to_remove)
            
            _last_cleanup = time.time()
            
            if removed_count > 0:
                print(f"[{datetime.datetime.now()}] Cache cleanup: removed {removed_count} files, freed {size_freed / (1024*1024):.2f}MB")
            
            return {'removed': removed_count, 'size_freed_mb': size_freed / (1024*1024)}
            
        except Exception as e:
            print(f"[{datetime.datetime.now()}] Cache cleanup failed: {e}")
            return {'removed': 0, 'size_freed_mb': 0}

def get_last_cleanup_time():
    return _last_cleanup
