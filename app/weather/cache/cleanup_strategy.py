import os
import time
import asyncio
from .constants import CACHE_DIR, MAX_CACHE_SIZE_MB, MAX_CACHE_FILES, WEATHER_CACHE_DURATION

async def analyze_cache_files():
    def _scan():
        files, total_size = [], 0
        now = time.time()
        for f_name in os.listdir(CACHE_DIR):
            if not f_name.endswith('.json'): 
                continue
            path = os.path.join(CACHE_DIR, f_name)
            try:
                stat = os.stat(path)
                total_size += stat.st_size
                files.append({'path': path, 'age': now - stat.st_mtime, 'size': stat.st_size})
            except OSError:
                continue
        return files, total_size
    return await asyncio.to_thread(_scan)

async def determine_files_to_remove(cache_files, total_size_mb):
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

async def cleanup_files(files_to_remove):
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
