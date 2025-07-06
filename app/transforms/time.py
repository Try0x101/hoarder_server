import datetime
import pytz
import re
from typing import Optional, Tuple
from timezonefinder import TimezoneFinder

tf = TimezoneFinder()

def get_current_location_time(tz) -> Optional[str]:
    if not tz:
        return None
    try:
        current_time = datetime.datetime.now(tz)
        return current_time.strftime("%H:%M:%S")
    except Exception:
        return None

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

def format_last_refresh_time(received_data: dict, location_tz, location_timezone) -> Tuple[Optional[str], Optional[str]]:
    received_time_raw = received_data.get('received_at')
    if not received_time_raw:
        now_utc = datetime.datetime.now(datetime.timezone.utc)
        utc_ref = now_utc.strftime("%Y-%m-%d %H:%M:%S UTC")
        if location_tz:
            now_local = now_utc.astimezone(location_tz)
            local_time = now_local.strftime("%Y-%m-%d %H:%M:%S ") + (location_timezone or "UTC")
        else:
            local_time = utc_ref
        return local_time, utc_ref
    
    try:
        if isinstance(received_time_raw, str):
            received_time = datetime.datetime.fromisoformat(received_time_raw.replace('Z', '+00:00'))
        else:
            received_time = received_time_raw
        
        if received_time.tzinfo is None:
            received_time_utc = received_time.replace(tzinfo=datetime.timezone.utc)
        else:
            received_time_utc = received_time.astimezone(datetime.timezone.utc)
        
        utc_ref = received_time_utc.strftime("%Y-%m-%d %H:%M:%S UTC")
        
        if location_tz:
            received_time_local = received_time_utc.astimezone(location_tz)
            local_time = received_time_local.strftime("%Y-%m-%d %H:%M:%S ") + (location_timezone or "UTC")
        else:
            local_time = utc_ref
        
        return local_time, utc_ref
    except Exception:
        return None, None

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
