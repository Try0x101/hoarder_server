import datetime
import os

async def get_device_stats(device_id: str = None):
    return {
        'total_devices': 0,
        'active_devices_24h': 0,
        'data_points_today': 0,
        'weather_requests_today': 0
    }

def cleanup_old_device_data():
    try:
        cache_dirs = ['/tmp/weather_cache', '/tmp/hoarder_exports', '/tmp/hoarder_imports']
        cleaned = 0
        
        for cache_dir in cache_dirs:
            if not os.path.exists(cache_dir):
                continue
                
            cutoff_time = datetime.datetime.now().timestamp() - (7 * 24 * 3600)
            
            for filename in os.listdir(cache_dir):
                file_path = os.path.join(cache_dir, filename)
                if os.path.isfile(file_path) and os.path.getmtime(file_path) < cutoff_time:
                    try:
                        os.remove(file_path)
                        cleaned += 1
                    except OSError:
                        pass
        
        if cleaned > 0:
            print(f"[{datetime.datetime.now()}] Cleaned {cleaned} old cache files")
    except Exception as e:
        print(f"[{datetime.datetime.now()}] Cache cleanup error: {e}")
