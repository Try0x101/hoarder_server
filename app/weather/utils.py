import datetime
import re
from typing import Optional, Dict, Any, Tuple

def robust_coordinate_validation(lat, lon) -> Tuple[Optional[float], Optional[float], str]:
    def normalize_coordinate(coord_val, coord_type="coordinate"):
        if coord_val is None:
            return None, f"Missing {coord_type}"
        
        if isinstance(coord_val, (int, float)):
            if coord_val != coord_val:
                return None, f"NaN {coord_type}"
            if abs(coord_val) == float('inf'):
                return None, f"Infinite {coord_type}"
            return float(coord_val), "valid"
        
        str_val = str(coord_val).strip()
        if not str_val or str_val.lower() in ['null', 'none', 'undefined', 'n/a', '', '0.0', '0']:
            return None, f"Empty/null {coord_type}"
        
        str_val = re.sub(r'[^\d\-\+\.]', '', str_val)
        if not str_val or str_val in ['-', '+', '.']:
            return None, f"Invalid {coord_type} format"
        
        try:
            val = float(str_val)
            if val != val or abs(val) == float('inf'):
                return None, f"Invalid {coord_type} value"
            return val, "valid"
        except (ValueError, TypeError, OverflowError):
            return None, f"Parse error for {coord_type}"
    
    lat_val, lat_msg = normalize_coordinate(lat, "latitude")
    lon_val, lon_msg = normalize_coordinate(lon, "longitude")
    
    if lat_val is None and lon_val is None:
        return None, None, "No coordinates provided"
    
    if lat_val is None:
        return None, None, lat_msg
    
    if lon_val is None:
        return None, None, lon_msg
    
    if not (-90 <= lat_val <= 90):
        return None, None, f"Latitude {lat_val} out of range (-90 to 90)"
    
    if not (-180 <= lon_val <= 180):
        return None, None, f"Longitude {lon_val} out of range (-180 to 180)"
    
    if lat_val == 0.0 and lon_val == 0.0:
        return None, None, "Null Island coordinates (0,0) not valid"
    
    return lat_val, lon_val, "valid"

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
