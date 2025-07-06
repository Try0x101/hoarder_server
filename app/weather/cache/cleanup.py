import os
import datetime
import asyncio
import time

from .constants import CACHE_DIR, MAX_CACHE_SIZE_MB, MAX_CACHE_FILES, WEATHER_CACHE_DURATION

_last_cleanup = 0
_cleanup_lock = asyncio.Lock()

async def intelligent_cache_cleanup():
    global _last_cleanup
    
    async with _cleanup_lock:
        try:
            if not os.path.exists(CACHE_DIR):
                return {'removed': 0, 'size_freed_mb': 0}
            
            cache_files, total_size = await _analyze_cache_files()
            files_to_remove = await _determine_files_to_remove(cache_files, total_size)
            removed_count, size_freed = await _cleanup_files(files_to_remove)
            
            _last_cleanup = time.time()
            
            if removed_count > 0:
                print(f"[{datetime.datetime.now()}] Cache cleanup: removed {removed_count} files, freed {size_freed / (1024*1024):.2f}MB")
            
            return {'removed': removed_count, 'size_freed_mb': size_freed / (1024*1024)}
            
        except Exception as e:
            print(f"[{datetime.datetime.now()}] Cache cleanup failed: {e}")
            return {'removed': 0, 'size_freed_mb': 0}

async def _analyze_cache_files():
    def _scan():
        files, total_size = [], 0
        now = time.time()
        for f_name in os.listdir(CACHE_DIR):
            if not f_name.endswith('.json'): continue
            path = os.path.join(CACHE_DIR, f_name)
            try:
                stat = os.stat(path)
                total_size += stat.st_size
                files.append({'path': path, 'age': now - stat.st_mtime, 'size': stat.st_size})
            except OSError:
                continue
        return files, total_size
    return await asyncio.to_thread(_scan)

async def _determine_files_to_remove(cache_files, total_size_mb):
    cache_files.sort(key=lambda x: x['age'], reverse=True)
    
    to_remove = []
    size_to_free = (total_size_mb - MAX_CACHE_SIZE_MB) * 1024 * 1024
    files_over_limit = len(cache_files) - MAX_CACHE_FILES
    
    freed_size = 0
    for i, f_info in enumerate(cache_files):
        is_old = f_info['age'] > WEATHER_CACHE_DURATION
        is_over_count = i < files_over_limit
        is_over_size = freed_size < size_to_free
        
        if is_old or is_over_count or is_over_size:
            to_remove.append(f_info)
            freed_size += f_info['size']
            
    return to_remove

async def _cleanup_files(files_to_remove):
    def _remove():
        removed, freed = 0, 0
        for f_info in files_to_remove:
            try:
                os.remove(f_info['path'])
                removed += 1
                freed += f_info['size']
            except OSError:
                continue
        return removed, freed
    return await asyncio.to_thread(_remove)
