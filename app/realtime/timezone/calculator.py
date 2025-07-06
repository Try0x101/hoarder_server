import asyncio
import datetime
import pytz
import time
from typing import Dict, Optional
from timezonefinder import TimezoneFinder

MAX_COORDINATE_GROUPS = 50
COORDINATE_PRECISION = 3

class TimezoneCalculator:
    def __init__(self):
        self.coordinate_groups = {}
        self.timezone_results = {}
        self.tf = TimezoneFinder()
        self.last_calculation = {}
        self.calculation_lock = asyncio.Lock()
        self.max_groups = MAX_COORDINATE_GROUPS
        
    def _normalize_coordinate_key(self, lat: float, lon: float) -> Optional[str]:
        try:
            if not (-90 <= lat <= 90) or not (-180 <= lon <= 180):
                return None
            if lat == 0.0 and lon == 0.0:
                return None
            rounded_lat = round(float(lat), COORDINATE_PRECISION)
            rounded_lon = round(float(lon), COORDINATE_PRECISION)
            return f"{rounded_lat},{rounded_lon}"
        except (ValueError, TypeError, OverflowError):
            return None
    
    async def calculate_timezone(self, coord_key: str, lat: float, lon: float):
        async with self.calculation_lock:
            if coord_key in self.timezone_results:
                return
                
            try:
                def get_timezone_data():
                    tz_name = self.tf.timezone_at(lng=lon, lat=lat)
                    if not tz_name:
                        return None
                    
                    tz = pytz.timezone(tz_name)
                    now_utc = datetime.datetime.now(datetime.timezone.utc)
                    now_local = now_utc.replace(tzinfo=pytz.utc).astimezone(tz)
                    
                    offset = now_local.utcoffset()
                    total_seconds = offset.total_seconds()
                    hours = int(total_seconds // 3600)
                    minutes = int((abs(total_seconds) % 3600) // 60)
                    sign = '+' if hours >= 0 else '-'
                    
                    if minutes == 0:
                        timezone_str = f"UTC{sign}{abs(hours)}"
                    else:
                        timezone_str = f"UTC{sign}{abs(hours)}:{abs(minutes):02d}"
                    
                    return {
                        'timezone_str': timezone_str,
                        'date_str': now_local.strftime("%d.%m.%Y"),
                        'time_str': now_local.strftime("%H:%M:%S"),
                        'tz_obj': tz
                    }
                
                result = await asyncio.to_thread(get_timezone_data)
                if result:
                    self.timezone_results[coord_key] = result
                    self.last_calculation[coord_key] = time.time()
                    
            except Exception as e:
                print(f"[{datetime.datetime.now()}] Timezone calculation error for {coord_key}: {e}")

    async def get_timezone_update(self, coord_key: str) -> Optional[Dict]:
        if coord_key not in self.timezone_results:
            return None
            
        tz_data = self.timezone_results[coord_key]
        if not tz_data or 'tz_obj' not in tz_data:
            return tz_data
            
        try:
            def update_time():
                now_utc = datetime.datetime.now(datetime.timezone.utc)
                now_local = now_utc.replace(tzinfo=pytz.utc).astimezone(tz_data['tz_obj'])
                return {
                    'timezone_str': tz_data['timezone_str'],
                    'date_str': now_local.strftime("%d.%m.%Y"),
                    'time_str': now_local.strftime("%H:%M:%S")
                }
            
            return await asyncio.to_thread(update_time)
        except Exception as e:
            print(f"[{datetime.datetime.now()}] Timezone update error for {coord_key}: {e}")
            return tz_data

    def cleanup_old_calculations(self):
        if len(self.coordinate_groups) >= self.max_groups:
            oldest_key = min(self.last_calculation.keys(), 
                           key=lambda k: self.last_calculation[k], 
                           default=None)
            if oldest_key:
                self.coordinate_groups.pop(oldest_key, None)
                self.timezone_results.pop(oldest_key, None)
                self.last_calculation.pop(oldest_key, None)
