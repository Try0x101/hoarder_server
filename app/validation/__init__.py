from app.processing.validation.decoders import decode_raw_data
from app.processing.validation.binary_decoder import decode_maximum_compression
from app.processing.validation.validators import validate_coordinates, validate_device_data, robust_coordinate_validation

def deep_merge(source: dict, destination: dict) -> dict:
    for key, value in source.items():
        if isinstance(value, dict) and key in destination and isinstance(destination[key], dict):
            destination[key] = deep_merge(value, destination[key])
        else:
            destination[key] = value
    return destination

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

__all__ = [
    'decode_raw_data',
    'decode_maximum_compression',
    'deep_merge',
    'validate_coordinates',
    'validate_device_data',
    'sanitize_payload_for_cache',
    'extract_metadata_from_payload',
    'robust_coordinate_validation'
]
