import zlib
import gzip
import json
import io
import asyncio
from typing import Dict, Any

async def decode_raw_data(raw: bytes) -> dict:
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
    except Exception as e:
        return {"error": "Failed to decode", "raw": raw.hex(), "exception": str(e)}

def deep_merge(source: Dict[str, Any], destination: Dict[str, Any]) -> Dict[str, Any]:
    for key, value in source.items():
        if isinstance(value, dict) and key in destination and isinstance(destination[key], dict):
            destination[key] = deep_merge(value, destination[key])
        else:
            destination[key] = value
    return destination

def validate_coordinates(lat, lon):
    if lat is None or lon is None:
        return False, "Missing coordinates"

    try:
        lat_float = float(lat)
        lon_float = float(lon)

        if not (-90 <= lat_float <= 90):
            return False, "Latitude out of range (-90 to 90)"

        if not (-180 <= lon_float <= 180):
            return False, "Longitude out of range (-180 to 180)"

        return True, "Valid coordinates"
    except (ValueError, TypeError):
        return False, "Invalid coordinate format"

def validate_device_data(data: dict):
    errors = []
    warnings = []

    if not data.get('id') and not data.get('device_id'):
        errors.append("Missing device identifier (id or device_id)")

    lat = data.get('lat')
    lon = data.get('lon')
    if lat is not None or lon is not None:
        is_valid, message = validate_coordinates(lat, lon)
        if not is_valid:
            warnings.append(f"Coordinate validation: {message}")

    if 'perc' in data:
        try:
            perc = float(data['perc'])
            if not (0 <= perc <= 100):
                warnings.append("Battery percentage out of range (0-100)")
        except (ValueError, TypeError):
            warnings.append("Invalid battery percentage format")

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

    return {k: v for k, v in payload.items() if k in cache_whitelist}

def extract_metadata_from_payload(payload: dict):
    metadata = {}

    if 'source_ip' in payload:
        metadata['source_ip'] = payload['source_ip']

    if 'server_received_at' in payload:
        metadata['server_received_at'] = payload['server_received_at']

    if 'batch_id' in payload:
        metadata['batch_id'] = payload['batch_id']

    if 'timestamp' in payload:
        metadata['data_timestamp'] = payload['timestamp']

    return metadata
