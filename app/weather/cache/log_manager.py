import os
import time
import datetime
import asyncio
import shutil
from .statistics import get_directory_size_mb
from .constants import REQUEST_LOG_FILE, LOG_ROTATION_SIZE_MB, EMERGENCY_CLEANUP_LOG, MAX_LOG_SIZE_MB

async def rotate_log_file(log_path: str, max_size_mb: float):
    try:
        if not os.path.exists(log_path):
            return
        
        def _rotate_log():
            if os.path.getsize(log_path) / (1024 * 1024) <= max_size_mb:
                return False
            
            backup_path = f"{log_path}.{int(time.time())}"
            shutil.move(log_path, backup_path)
            
            with open(backup_path, 'r') as old_f, open(log_path, 'w') as new_f:
                lines = old_f.readlines()
                new_f.writelines(lines[-1000:])
            
            os.remove(backup_path)
            return True
        
        if await asyncio.to_thread(_rotate_log):
            print(f"[{datetime.datetime.now()}] Log rotated: {log_path}")
            
    except Exception as e:
        print(f"[{datetime.datetime.now()}] Log rotation failed for {log_path}: {e}")

async def cleanup_old_request_logs():
    try:
        await rotate_log_file(REQUEST_LOG_FILE, LOG_ROTATION_SIZE_MB)
        
        if os.path.exists(EMERGENCY_CLEANUP_LOG):
            log_size_mb = await get_directory_size_mb(EMERGENCY_CLEANUP_LOG)
            if log_size_mb > MAX_LOG_SIZE_MB:
                await rotate_log_file(EMERGENCY_CLEANUP_LOG, MAX_LOG_SIZE_MB)
                
    except Exception as e:
        print(f"[{datetime.datetime.now()}] Log cleanup error: {e}")
