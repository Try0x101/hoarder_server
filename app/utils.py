import hashlib
from app.weather import enrich_with_weather_data, WEATHER_CODE_DESCRIPTIONS
from app.transforms import (
    safe_int, safe_float, get_wind_direction_compass, get_network_active,
    format_weather_observation_time, get_weather_fetch_formatted,
    get_current_location_time, format_last_refresh_time, get_timezone_info_from_coordinates,
    calculate_weather_data_age
)
from app.validation import decode_raw_data, deep_merge
from app.transforms.geo import calculate_distance_km

def safe_string(value):
    if value is None or value == "":
        return None
    return str(value)

def create_device_fingerprint(source_ip, user_agent, payload_data):
    fingerprint_parts = [source_ip or "unknown_ip"]

    if user_agent:
        ua_hash = hashlib.md5(user_agent.encode()).hexdigest()[:8]
        fingerprint_parts.append(f"ua_{ua_hash}")

    device_characteristics = []
    if payload_data.get('cap'):
        device_characteristics.append(f"cap{safe_int(payload_data.get('cap'))}")
    if payload_data.get('nt'):
        device_characteristics.append(f"nt{str(payload_data.get('nt'))[:3]}")
    if payload_data.get('op'):
        device_characteristics.append(f"op{str(payload_data.get('op'))[:3]}")
    if payload_data.get('mcc'):
        device_characteristics.append(f"mcc{safe_int(payload_data.get('mcc'))}")

    if device_characteristics:
        char_hash = hashlib.md5("_".join(device_characteristics).encode()).hexdigest()[:6]
        fingerprint_parts.append(char_hash)

    fingerprint = "_".join(fingerprint_parts)

    if len(fingerprint) > 50:
        fingerprint = hashlib.md5(fingerprint.encode()).hexdigest()[:16]

    return f"auto_{fingerprint}"

def safe_device_id(device_id_value, source_ip=None, user_agent=None, payload_data=None):
    if device_id_value and str(device_id_value).strip():
        return str(device_id_value)

    if source_ip and payload_data:
        return create_device_fingerprint(source_ip, user_agent, payload_data)

    fallback_id = f"unknown_{hash(str(device_id_value) + str(source_ip))%10000:04d}"
    return fallback_id

async def transform_device_data(received_data):
    location_date, location_time, location_timezone, location_tz = get_timezone_info_from_coordinates(
        received_data.get('lat'), received_data.get('lon')
    )

    current_time = None
    if location_tz:
        current_time = get_current_location_time(location_tz)

    location_time_to_use = current_time if current_time else location_time
    wind_direction_compass = get_wind_direction_compass(received_data.get('wind_direction_10m'))

    weather_observation_formatted = format_weather_observation_time(
        received_data.get('weather_observation_time'), location_tz, location_timezone
    )

    device_id = safe_device_id(
        received_data.get('id') or received_data.get('device_id'),
        received_data.get('source_ip'),
        received_data.get('user_agent'),
        received_data
    )

    weather_fetch_formatted = await get_weather_fetch_formatted(device_id, location_tz, location_timezone)
    weather_fetch_data_age = calculate_weather_data_age(weather_fetch_formatted)

    _, last_refresh_time_utc_reference, _ = format_last_refresh_time(
        received_data, location_tz, location_timezone
    )

    network_active = get_network_active(received_data)

    return {
        'device_id': device_id,
        'device_name': safe_string(received_data.get('n')),
        'battery_percent': f"{safe_int(received_data.get('perc'))}%" if safe_int(received_data.get('perc')) is not None else None,
        'battery_total_capacity': f"{safe_int(received_data.get('cap'))} mAh" if safe_int(received_data.get('cap')) is not None else None,
        'battery_leftover_calculated': f"{safe_int(safe_int(received_data.get('cap', 0)) * safe_int(received_data.get('perc', 0)) / 100)} mAh" if safe_int(received_data.get('cap')) is not None and safe_int(received_data.get('perc')) is not None else None,
        'cell_id': safe_string(safe_int(received_data.get('ci'))),
        'cell_mcc': safe_string(safe_int(received_data.get('mcc'))),
        'cell_mnc': safe_string(safe_int(received_data.get('mnc'))),
        'cell_tac': safe_string(received_data.get('tac')),
        'cell_operator': safe_string(received_data.get('op')),
        'network_type': safe_string(received_data.get('nt')),
        'network_active': network_active,
        'cell_signal_strength': f"{safe_int(received_data.get('rssi'))} dBm" if 'rssi' in received_data and received_data.get('rssi') is not None else None,
        'gps_accuracy': f"{safe_int(received_data.get('acc'))} m" if 'acc' in received_data and received_data.get('acc') is not None else None,
        'gps_altitude': f"{safe_int(received_data.get('alt'))} m" if safe_int(received_data.get('alt')) is not None else None,
        'gps_speed': f"{safe_int(received_data.get('spd'))} km/h" if safe_int(received_data.get('spd')) is not None else None,
        'wifi_bssid': safe_string(received_data.get('bssid')),
        'gps_latitude': safe_string(safe_float(received_data.get('lat'))),
        'gps_longitude': safe_string(safe_float(received_data.get('lon'))),
        'network_download_capacity': f"{safe_int(received_data.get('dn'))} Mbps" if 'dn' in received_data and received_data.get('dn') is not None else None,
        'network_upload_capacity': f"{safe_int(received_data.get('up'))} Mbps" if 'up' in received_data and received_data.get('up') is not None else None,
        'weather_temperature': f"{safe_int(received_data.get('weather_temp'))}°C" if 'weather_temp' in received_data and received_data.get('weather_temp') is not None else None,
        'weather_description': WEATHER_CODE_DESCRIPTIONS.get(safe_int(received_data.get('weather_code')), "Unknown"),
        'weather_humidity': f"{safe_int(received_data.get('weather_humidity'))}%" if 'weather_humidity' in received_data and received_data.get('weather_humidity') is not None else None,
        'weather_apparent_temp': f"{safe_int(received_data.get('weather_apparent_temp'))}°C" if 'weather_apparent_temp' in received_data and received_data.get('weather_apparent_temp') is not None else None,
        'weather_precipitation': f"{safe_int(received_data.get('precipitation'))} mm" if 'precipitation' in received_data and received_data.get('precipitation') is not None else None,
        'weather_pressure_msl': f"{safe_int(received_data.get('pressure_msl'))} hPa" if 'pressure_msl' in received_data and received_data.get('pressure_msl') is not None else None,
        'weather_cloud_cover': f"{safe_int(received_data.get('cloud_cover'))}%" if 'cloud_cover' in received_data and received_data.get('cloud_cover') is not None else None,
        'weather_wind_speed': f"{safe_int(received_data.get('wind_speed_10m'))} m/s" if 'wind_speed_10m' in received_data and received_data.get('wind_speed_10m') is not None else None,
        'weather_wind_direction': wind_direction_compass if wind_direction_compass else None,
        'weather_wind_gusts': f"{safe_int(received_data.get('wind_gusts_10m'))} m/s" if 'wind_gusts_10m' in received_data and received_data.get('wind_gusts_10m') is not None else None,
        'weather_observation_time': weather_observation_formatted,
        'weather_last_fetch_request_time': weather_fetch_formatted,
        'weather_fetch_data_age': weather_fetch_data_age,
        'marine_wave_height': f"{safe_int(received_data.get('marine_wave_height'))} m" if 'marine_wave_height' in received_data and received_data.get('marine_wave_height') is not None else None,
        'marine_wave_direction': f"{safe_int(received_data.get('marine_wave_direction'))}°" if 'marine_wave_direction' in received_data and received_data.get('marine_wave_direction') is not None else None,
        'marine_wave_period': f"{safe_int(received_data.get('marine_wave_period'))} s" if 'marine_wave_period' in received_data and received_data.get('marine_wave_period') is not None else None,
        'marine_swell_wave_height': f"{safe_int(received_data.get('marine_swell_wave_height'))} m" if 'marine_swell_wave_height' in received_data and received_data.get('marine_swell_wave_height') is not None else None,
        'marine_swell_wave_direction': f"{safe_int(received_data.get('marine_swell_wave_direction'))}°" if 'marine_swell_wave_direction' in received_data and received_data.get('marine_swell_wave_direction') is not None else None,
        'marine_swell_wave_period': f"{safe_int(received_data.get('marine_swell_wave_period'))} s" if 'marine_swell_wave_period' in received_data and received_data.get('marine_swell_wave_period') is not None else None,
        'gps_date_time': {'location_time': location_time_to_use, 'location_timezone': location_timezone, 'location_date': location_date},
        'source_ip': safe_string(received_data.get('source_ip')),
        'last_refresh_time_utc_reference': last_refresh_time_utc_reference
    }
