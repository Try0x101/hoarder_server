from app.weather import WEATHER_CODE_DESCRIPTIONS
from app.transforms import (
    safe_int, safe_float, get_wind_direction_compass, get_network_active,
    format_weather_observation_time, get_weather_fetch_formatted,
    get_current_location_time, format_last_refresh_time, normalize_bssid
)
from app.shared.time.timezone_lookup import get_timezone_info_from_coordinates
from .helpers import safe_string
from .fingerprint import safe_device_id

async def transform_device_data(received_data):
    try:
        lat = received_data.get('lat')
        lon = received_data.get('lon')
        
        loc_date, loc_time, loc_tz_str, loc_tz = get_timezone_info_from_coordinates(lat, lon)
        current_time = get_current_location_time(loc_tz) if loc_tz else None
        
        device_id = safe_device_id(
            received_data.get('id') or received_data.get('device_id'),
            received_data.get('source_ip'), received_data.get('user_agent'), received_data
        )
        
        weather_fetch_fmt = await get_weather_fetch_formatted(device_id, loc_tz, loc_tz_str)
        _, last_refresh_utc = format_last_refresh_time(received_data, loc_tz, loc_tz_str)
        
        wind_dir_compass = get_wind_direction_compass(received_data.get('wind_direction_10m'))
        weather_obs_fmt = format_weather_observation_time(received_data.get('weather_observation_time'), loc_tz, loc_tz_str)

        transformed = {
            'device_id': device_id,
            'device_name': safe_string(received_data.get('n')),
            'cell_id': safe_string(safe_int(received_data.get('ci'))),
            'cell_mcc': safe_string(safe_int(received_data.get('mcc'))),
            'cell_mnc': safe_string(safe_int(received_data.get('mnc'))),
            'cell_tac': safe_string(received_data.get('tac')),
            'cell_operator': safe_string(received_data.get('op')),
            'network_type': safe_string(received_data.get('nt')),
            'network_active': get_network_active(received_data),
            'wifi_bssid': normalize_bssid(received_data.get('bssid')),
            'gps_latitude': safe_string(safe_float(lat)),
            'gps_longitude': safe_string(safe_float(lon)),
            'weather_description': WEATHER_CODE_DESCRIPTIONS.get(safe_int(received_data.get('weather_code')), "Unknown"),
            'weather_observation_time': weather_obs_fmt,
            'weather_last_fetch_request_time': weather_fetch_fmt,
            'gps_date_time': {'location_time': current_time or loc_time, 'location_timezone': loc_tz_str, 'location_date': loc_date},
            'source_ip': safe_string(received_data.get('source_ip')),
            'last_refresh_time_utc_reference': last_refresh_utc
        }

        # Numeric fields with units, correctly handling 0
        numeric_fields = {
            'perc': 'battery_percent', 'cap': 'battery_total_capacity', 'rssi': 'cell_signal_strength',
            'acc': 'gps_accuracy', 'alt': 'gps_altitude', 'spd': 'gps_speed', 'dn': 'network_download_capacity',
            'up': 'network_upload_capacity', 'weather_temp': 'weather_temperature', 'weather_humidity': 'weather_humidity',
            'weather_apparent_temp': 'weather_apparent_temp', 'precipitation': 'weather_precipitation',
            'pressure_msl': 'weather_pressure_msl', 'cloud_cover': 'weather_cloud_cover',
            'wind_speed_10m': 'weather_wind_speed', 'wind_gusts_10m': 'weather_wind_gusts',
            'marine_wave_height': 'marine_wave_height', 'marine_wave_direction': 'marine_wave_direction',
            'marine_wave_period': 'marine_wave_period', 'marine_swell_wave_height': 'marine_swell_wave_height',
            'marine_swell_wave_direction': 'marine_swell_wave_direction', 'marine_swell_wave_period': 'marine_swell_wave_period',
        }
        units = { 'perc': '%', 'cap': ' mAh', 'rssi': ' dBm', 'acc': ' m', 'alt': ' m', 'spd': ' km/h', 'dn': ' Mbps', 'up': ' Mbps', 'weather_temp': '°C', 'weather_humidity': '%', 'weather_apparent_temp': '°C', 'precipitation': ' mm', 'pressure_msl': ' hPa', 'cloud_cover': '%', 'wind_speed_10m': ' m/s', 'wind_gusts_10m': ' m/s', 'marine_wave_height': ' m', 'marine_wave_direction': '°', 'marine_wave_period': ' s', 'marine_swell_wave_height': ' m', 'marine_swell_wave_direction': '°', 'marine_swell_wave_period': ' s' }

        for key, new_key in numeric_fields.items():
            val = safe_int(received_data.get(key))
            if val is not None:
                transformed[new_key] = f"{val}{units.get(key, '')}"
        
        if (cap := safe_int(received_data.get('cap'))) is not None and (perc := safe_int(received_data.get('perc'))) is not None:
            transformed['battery_leftover_calculated'] = f"{safe_int(cap * perc / 100)} mAh"

        if wind_dir := get_wind_direction_compass(received_data.get('wind_direction_10m')):
            transformed['weather_wind_direction'] = wind_dir

        return {k: v for k, v in transformed.items() if v is not None and v != ""}
        
    except Exception as e:
        print(f"Transform error details: {str(e)}")
        import traceback
        traceback.print_exc()
        raise e
