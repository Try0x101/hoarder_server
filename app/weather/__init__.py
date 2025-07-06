from app.services.weather.coordinator import get_weather_data, WEATHER_CODE_DESCRIPTIONS
from app.services.weather.cache_manager import find_cached_weather, save_weather_cache
from app.weather.utils import enrich_with_weather_data

async def cleanup_old_cache():
    from app.services.weather.cache_manager import CACHE_DIR, MAX_CACHE_FILES
    import os
    import asyncio
    
    try:
        def cleanup_files():
            try:
                files = os.listdir(CACHE_DIR)
                if len(files) <= MAX_CACHE_FILES:
                    return 0
                
                file_times = []
                for f in files:
                    if f.endswith('.json'):
                        path = os.path.join(CACHE_DIR, f)
                        try:
                            mtime = os.path.getmtime(path)
                            file_times.append((path, mtime))
                        except:
                            continue
                
                file_times.sort(key=lambda x: x[1])
                to_remove = file_times[:-MAX_CACHE_FILES]
                
                removed = 0
                for path, _ in to_remove:
                    try:
                        os.remove(path)
                        removed += 1
                    except:
                        continue
                
                return removed
            except:
                return 0
        
        removed = await asyncio.to_thread(cleanup_files)
        if removed > 0:
            print(f"Cleaned up {removed} old cache files")
    except Exception as e:
        print(f"Cache cleanup error: {e}")

__all__ = [
    'get_weather_data',
    'find_cached_weather',
    'save_weather_cache', 
    'cleanup_old_cache',
    'enrich_with_weather_data',
    'WEATHER_CODE_DESCRIPTIONS'
]
