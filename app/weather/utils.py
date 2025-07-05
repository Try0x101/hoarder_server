import datetime
from typing import Optional, Dict, Any

def _safe_float(value):
    if value is None:
        return None
    try:
        return float(value)
    except (ValueError, TypeError):
        return None

async def enrich_with_weather_data(data:dict)->dict:
    from app.device_tracker import should_force_weather_update,cleanup_old_device_data
    from .client import get_weather_data

    print(f"[{datetime.datetime.now(datetime.timezone.utc)}] DEBUG: Starting optimized weather enrichment")
    lat=data.get('lat')
    lon=data.get('lon')
    device_id=data.get('id') or data.get('device_id')
    print(f"[{datetime.datetime.now(datetime.timezone.utc)}] DEBUG: Device {device_id} - coordinates: lat={lat}, lon={lon}")

    lat_float = _safe_float(lat)
    lon_float = _safe_float(lon)

    if lat_float is None or lon_float is None:
        print(f"[{datetime.datetime.now(datetime.timezone.utc)}] DEBUG: No valid coordinates found, skipping weather enrichment")
        return data
    if not device_id:
        print(f"[{datetime.datetime.now(datetime.timezone.utc)}] DEBUG: No device_id found, skipping weather enrichment")
        return data

    try:
        force_update,reason=await should_force_weather_update(device_id,lat_float,lon_float)
        print(f"[{datetime.datetime.now(datetime.timezone.utc)}] DEBUG: Device {device_id} - force_update: {force_update} (reason: {reason})")

        if hash(str(device_id))%100==0:
            cleanup_old_device_data()

        print(f"[{datetime.datetime.now(datetime.timezone.utc)}] DEBUG: Getting weather data...")
        weather_data=await get_weather_data(lat_float,lon_float)
        if weather_data:
            data.update(weather_data)
            weather_keys_added=[k for k in weather_data.keys() if weather_data[k] is not None]
            print(f"[{datetime.datetime.now(datetime.timezone.utc)}] SUCCESS: Weather data added for device {device_id}")
            print(f"[{datetime.datetime.now(datetime.timezone.utc)}] DEBUG: Added keys: {weather_keys_added}")
        else:
            print(f"[{datetime.datetime.now(datetime.timezone.utc)}] WARNING: No weather data for device {device_id}")
            
    except (ValueError,TypeError) as e:
        print(f"[{datetime.datetime.now(datetime.timezone.utc)}] ERROR: Invalid coordinates: lat={lat}, lon={lon}, error={e}")
    except Exception as e:
        print(f"[{datetime.datetime.now(datetime.timezone.utc)}] ERROR: Unexpected error in weather enrichment: {e}")
        import traceback
        print(f"[{datetime.datetime.now(datetime.timezone.utc)}] DEBUG: Traceback: {traceback.format_exc()}")

    return data
