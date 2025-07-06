import os
import time
import datetime
import shutil
import asyncio
from .disk import get_directory_size_mb
from .constants import REQUEST_LOG_FILE, LOG_ROTATION_SIZE_MB, EMERGENCY_CLEANUP_LOG, MAX_LOG_SIZE_MB

async def rotate_log_file(log_path: str, max_size_mb: float):
    try:
        if not os.path.exists(log_path):
            return
        
        def _rotate_log():
            size_mb = os.path.getsize(log_path) / (1024 * 1024)
            if size_mb <= max_size_mb:
                return False
                
            backup_path = f"{log_path}.{int(time.time())}"
            
            cutoff_date = datetime.date.today() - datetime.timedelta(days=1)
            recent_lines = []
            
            with open(log_path, 'r') as f:
                for line in f:
                    try:
                        timestamp_str = line.strip()
                        request_time = datetime.datetime.fromisoformat(timestamp_str)
                        if request_time.date() >= cutoff_date:
                            recent_lines.append(line)
                    except:
                        continue
            
            shutil.move(log_path, backup_path)
            
            with open(log_path, 'w') as f:
                f.writelines(recent_lines[-1000:])
            
            if os.path.exists(backup_path):
                os.remove(backup_path)
            return True
        
        rotated = await asyncio.to_thread(_rotate_log)
        if rotated:
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