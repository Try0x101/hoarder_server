import pytz
from typing import Optional, Tuple
from timezonefinder import TimezoneFinder
import datetime

tf = TimezoneFinder()

def safe_coordinate_lookup(lat: float, lon: float) -> Tuple[Optional[str], Optional[pytz.BaseTzInfo]]:
    try:
        if not (-90 <= lat <= 90) or not (-180 <= lon <= 180):
            return None, None
        timezone_str = tf.timezone_at(lat=lat, lng=lon)
        if timezone_str:
            return timezone_str, pytz.timezone(timezone_str)
    except Exception:
        pass
    return None, None

def get_location_time_info(lat: float, lon: float) -> Tuple[Optional[str], Optional[str], Optional[str], Optional[pytz.BaseTzInfo]]:
    timezone_str, tz = safe_coordinate_lookup(lat, lon)
    if not tz:
        return None, None, None, None
    
    try:
        current_time = datetime.datetime.now(tz)
        location_date = current_time.strftime("%d.%m.%Y")
        location_time = current_time.strftime("%H:%M:%S")
        
        utc_offset = current_time.utcoffset()
        total_seconds = int(utc_offset.total_seconds())
        hours = total_seconds // 3600
        minutes = abs(total_seconds % 3600) // 60
        
        if minutes == 0:
            location_timezone = f"UTC{'+' if hours >= 0 else ''}{hours}"
        else:
            location_timezone = f"UTC{'+' if hours >= 0 else ''}{hours}:{minutes:02d}"
        
        return location_date, location_time, location_timezone, tz
    except Exception:
        return None, None, None, None

def get_timezone_info_from_coordinates(lat, lon) -> Tuple[Optional[str], Optional[str], Optional[str], Optional[pytz.BaseTzInfo]]:
    if lat is None or lon is None:
        return None, None, None, None
    
    try:
        lat_float = float(lat) if lat is not None else None
        lon_float = float(lon) if lon is not None else None
        
        if lat_float is not None and lon_float is not None:
            return get_location_time_info(lat_float, lon_float)
    except (ValueError, TypeError):
        pass
    
    return None, None, None, None
