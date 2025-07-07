import datetime
from typing import Optional, Dict, Any

from app.validation import robust_coordinate_validation

def safe_device_id_extraction(data: dict) -> Optional[str]:
    id_fields = ['id', 'device_id', 'deviceId', 'device', 'dev_id']
    
    for field in id_fields:
        if field in data and data[field]:
            device_id = str(data[field]).strip()
            if device_id and device_id.lower() not in ['null', 'none', 'undefined', '']:
                return device_id
    
    return None

async def enrich_with_weather_data(data: dict) -> dict:
    from app.device_tracker import should_force_weather_update, cleanup_old_device_data
    from app.services.weather.coordinator import get_weather_data

    print(f"[{datetime.datetime.now(datetime.timezone.utc)}] DEBUG: Starting weather enrichment")
    
    lat = data.get('lat')
    lon = data.get('lon')
    device_id = safe_device_id_extraction(data)
    
    print(f"[{datetime.datetime.now(datetime.timezone.utc)}] DEBUG: Device {device_id} - raw coordinates: lat={lat}, lon={lon}")

    lat_float, lon_float, validation_msg = robust_coordinate_validation(lat, lon)
    
    if lat_float is None or lon_float is None:
        print(f"[{datetime.datetime.now(datetime.timezone.utc)}] DEBUG: Coordinate validation failed: {validation_msg}")
        return data
    
    if not device_id:
        print(f"[{datetime.datetime.now(datetime.timezone.utc)}] DEBUG: No valid device_id found, skipping weather enrichment")
        return data

    print(f"[{datetime.datetime.now(datetime.timezone.utc)}] DEBUG: Device {device_id} - validated coordinates: lat={lat_float}, lon={lon_float}")

    try:
        force_update, reason = await should_force_weather_update(device_id, lat_float, lon_float)
        print(f"[{datetime.datetime.now(datetime.timezone.utc)}] DEBUG: Device {device_id} - force_update: {force_update} (reason: {reason})")

        if hash(str(device_id)) % 100 == 0:
            cleanup_old_device_data()

        if force_update:
            print(f"[{datetime.datetime.now(datetime.timezone.utc)}] DEBUG: Fetching weather data for {lat_float}, {lon_float}")
            weather_data = await get_weather_data(lat_float, lon_float)
            
            if weather_data and isinstance(weather_data, dict):
                valid_weather_keys = [k for k, v in weather_data.items() if v is not None and str(v).strip()]
                if valid_weather_keys:
                    data.update(weather_data)
                    print(f"[{datetime.datetime.now(datetime.timezone.utc)}] SUCCESS: Weather data added for device {device_id}")
                    print(f"[{datetime.datetime.now(datetime.timezone.utc)}] DEBUG: Added keys: {valid_weather_keys}")
                else:
                    print(f"[{datetime.datetime.now(datetime.timezone.utc)}] WARNING: Weather data returned but all values null/empty")
            else:
                print(f"[{datetime.datetime.now(datetime.timezone.utc)}] WARNING: No valid weather data returned for device {device_id}")
        else:
            print(f"[{datetime.datetime.now(datetime.timezone.utc)}] DEBUG: Using cached weather data (reason: {reason})")
            
    except (ValueError, TypeError) as e:
        print(f"[{datetime.datetime.now(datetime.timezone.utc)}] ERROR: Coordinate processing error: lat={lat}, lon={lon}, error={e}")
    except Exception as e:
        print(f"[{datetime.datetime.now(datetime.timezone.utc)}] ERROR: Unexpected error in weather enrichment: {e}")
        import traceback
        print(f"[{datetime.datetime.now(datetime.timezone.utc)}] DEBUG: Traceback: {traceback.format_exc()}")

    return data
