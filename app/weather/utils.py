import datetime
from typing import Optional, Dict, Any
from app.processing.validation.validators import robust_coordinate_validation

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

    lat = data.get('lat')
    lon = data.get('lon')
    device_id = safe_device_id_extraction(data)
    
    lat_float, lon_float, validation_msg = robust_coordinate_validation(lat, lon)
    
    if lat_float is None or lon_float is None or not device_id:
        return data

    try:
        force_update, reason = await should_force_weather_update(device_id, lat_float, lon_float)

        if hash(str(device_id)) % 100 == 0:
            cleanup_old_device_data()

        if force_update:
            weather_data = await get_weather_data(lat_float, lon_float)
            
            if weather_data and isinstance(weather_data, dict):
                valid_weather_keys = [k for k, v in weather_data.items() if v is not None and str(v).strip()]
                if valid_weather_keys:
                    data.update(weather_data)
                    print(f"[{datetime.datetime.now(datetime.timezone.utc)}] SUCCESS: Weather data added for device {device_id} (reason: {reason})")
                else:
                    print(f"[{datetime.datetime.now(datetime.timezone.utc)}] WARNING: Weather data for {device_id} returned empty")
            else:
                print(f"[{datetime.datetime.now(datetime.timezone.utc)}] WARNING: No valid weather data for {device_id} (reason: {reason})")
        
    except (ValueError, TypeError) as e:
        print(f"[{datetime.datetime.now(datetime.timezone.utc)}] ERROR: Coordinate processing error: lat={lat}, lon={lon}, error={e}")
    except Exception as e:
        print(f"[{datetime.datetime.now(datetime.timezone.utc)}] ERROR: Unexpected error in weather enrichment for {device_id}: {e}")
        import traceback
        print(f"[{datetime.datetime.now(datetime.timezone.utc)}] Traceback: {traceback.format_exc()}")

    return data
