import zlib
import gzip
import json
import io
import asyncio
import re
from typing import Dict, Any

async def decode_raw_data(raw: bytes) -> dict:
    if not raw:
        return {"error": "Empty payload"}
    
    try:
        decompressed_data = await asyncio.to_thread(zlib.decompress, raw, wbits=-15)
        return await asyncio.to_thread(json.loads, decompressed_data)
    except (zlib.error, json.JSONDecodeError):
        pass

    try:
        def decompress_and_load_gzip():
            with gzip.GzipFile(fileobj=io.BytesIO(raw)) as f:
                decompressed_data = f.read()
            return json.loads(decompressed_data)
        return await asyncio.to_thread(decompress_and_load_gzip)
    except (OSError, json.JSONDecodeError):
        pass

    try:
        return await asyncio.to_thread(json.loads, raw)
    except json.JSONDecodeError:
        pass
    
    try:
        text_data = raw.decode('utf-8', errors='ignore').strip()
        if text_data.startswith('{') and text_data.endswith('}'):
            return await asyncio.to_thread(json.loads, text_data)
    except Exception:
        pass

    return {"error": "Failed to decode", "raw_size": len(raw), "raw_preview": raw[:100].hex()}

def deep_merge(source: Dict[str, Any], destination: Dict[str, Any]) -> Dict[str, Any]:
    for key, value in source.items():
        if isinstance(value, dict) and key in destination and isinstance(destination[key], dict):
            destination[key] = deep_merge(value, destination[key])
        else:
            destination[key] = value
    return destination

def normalize_coordinate_value(coord_val):
    if coord_val is None:
        return None
    
    if isinstance(coord_val, (int, float)):
        return float(coord_val)
    
    str_val = str(coord_val).strip()
    if not str_val or str_val.lower() in ['null', 'none', 'undefined', 'n/a', '']:
        return None
    
    str_val = re.sub(r'[^\d\-\+\.]', '', str_val)
    
    try:
        return float(str_val)
    except (ValueError, TypeError):
        return None

def validate_coordinates(lat, lon):
    lat_float = normalize_coordinate_value(lat)
    lon_float = normalize_coordinate_value(lon)
    
    if lat_float is None and lon_float is None:
        return True, "No coordinates provided"
    
    if lat_float is None or lon_float is None:
        return False, "Incomplete coordinates (missing lat or lon)"

    if not (-90 <= lat_float <= 90):
        return False, f"Latitude {lat_float} out of range (-90 to 90)"

    if not (-180 <= lon_float <= 180):
        return False, f"Longitude {lon_float} out of range (-180 to 180)"

    return True, "Valid coordinates"

def validate_device_data(data: dict):
    errors = []
    warnings = []

    if not isinstance(data, dict):
        errors.append("Data must be a JSON object")
        return {'is_valid': False, 'errors': errors, 'warnings': warnings}

    device_id = data.get('id') or data.get('device_id')
    if not device_id:
        warnings.append("Missing device identifier (id or device_id) - will generate automatic ID")

    lat = data.get('lat')
    lon = data.get('lon')
    if lat is not None or lon is not None:
        is_valid, message = validate_coordinates(lat, lon)
        if not is_valid:
            warnings.append(f"Coordinate validation: {message}")

    numeric_fields = {
        'perc': (0, 100, "Battery percentage"),
        'cap': (0, 50000, "Battery capacity (mAh)"),
        'rssi': (-150, 0, "Signal strength (dBm)"),
        'acc': (0, 10000, "GPS accuracy (m)"),
        'spd': (0, 500, "Speed (km/h)"),
        'alt': (-1000, 10000, "Altitude (m)")
    }
    
    for field, (min_val, max_val, desc) in numeric_fields.items():
        if field in data:
            try:
                val = float(data[field])
                if not (min_val <= val <= max_val):
                    warnings.append(f"{desc} value {val} outside expected range ({min_val}-{max_val})")
            except (ValueError, TypeError):
                warnings.append(f"Invalid {desc} format: {data[field]}")

    return {
        'is_valid': len(errors) == 0,
        'errors': errors,
        'warnings': warnings
    }

def sanitize_payload_for_cache(payload: dict):
    cache_whitelist = {
        'weather_temp', 'weather_humidity', 'weather_apparent_temp',
        'precipitation', 'weather_code', 'pressure_msl', 'cloud_cover',
        'wind_speed_10m', 'wind_direction_10m', 'wind_gusts_10m',
        'weather_observation_time', 'marine_wave_height', 'marine_wave_direction',
        'marine_wave_period', 'marine_swell_wave_height', 'marine_swell_wave_direction',
        'marine_swell_wave_period'
    }

    return {k: v for k, v in payload.items() if k in cache_whitelist and v is not None}

def extract_metadata_from_payload(payload: dict):
    metadata = {}

    if 'source_ip' in payload:
        metadata['source_ip'] = str(payload['source_ip'])

    if 'server_received_at' in payload:
        metadata['server_received_at'] = str(payload['server_received_at'])

    if 'batch_id' in payload:
        metadata['batch_id'] = str(payload['batch_id'])

    if 'timestamp' in payload:
        metadata['data_timestamp'] = str(payload['timestamp'])

    return metadata
