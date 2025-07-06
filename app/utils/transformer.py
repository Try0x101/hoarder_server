from app.weather import WEATHER_CODE_DESCRIPTIONS
from app.transforms import (
    safe_int, safe_float, get_wind_direction_compass, get_network_active,
    format_weather_observation_time, get_weather_fetch_formatted,
    get_current_location_time, format_last_refresh_time,
    calculate_weather_data_age
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
        _, last_refresh_utc, _ = format_last_refresh_time(received_data, loc_tz, loc_tz_str)
        
        wind_dir_compass = get_wind_direction_compass(received_data.get('wind_direction_10m'))
        weather_obs_fmt = format_weather_observation_time(received_data.get('weather_observation_time'), loc_tz, loc_tz_str)

        return {
            'device_id': device_id,
            'device_name': safe_string(received_data.get('n')),
            'battery_percent': f"{safe_int(received_data.get('perc'))}%" if received_data.get('perc') is not None else None,
            'battery_total_capacity': f"{safe_int(received_data.get('cap'))} mAh" if received_data.get('cap') is not None else None,
            'battery_leftover_calculated': f"{safe_int(safe_int(received_data.get('cap', 0)) * safe_int(received_data.get('perc', 0)) / 100)} mAh" if received_data.get('cap') is not None and received_data.get('perc') is not None else None,
            'cell_id': safe_string(safe_int(received_data.get('ci'))),
            'cell_mcc': safe_string(safe_int(received_data.get('mcc'))),
            'cell_mnc': safe_string(safe_int(received_data.get('mnc'))),
            'cell_tac': safe_string(received_data.get('tac')),
            'cell_operator': safe_string(received_data.get('op')),
            'network_type': safe_string(received_data.get('nt')),
            'network_active': get_network_active(received_data),
            'cell_signal_strength': f"{safe_int(received_data.get('rssi'))} dBm" if received_data.get('rssi') is not None else None,
            'gps_accuracy': f"{safe_int(received_data.get('acc'))} m" if received_data.get('acc') is not None else None,
            'gps_altitude': f"{safe_int(received_data.get('alt'))} m" if received_data.get('alt') is not None else None,
            'gps_speed': f"{safe_int(received_data.get('spd'))} km/h" if received_data.get('spd') is not None else None,
            'wifi_bssid': safe_string(received_data.get('bssid')),
            'gps_latitude': safe_string(safe_float(lat)),
            'gps_longitude': safe_string(safe_float(lon)),
            'network_download_capacity': f"{safe_int(received_data.get('dn'))} Mbps" if received_data.get('dn') is not None else None,
            'network_upload_capacity': f"{safe_int(received_data.get('up'))} Mbps" if received_data.get('up') is not None else None,
            'weather_temperature': f"{safe_int(received_data.get('weather_temp'))}°C" if received_data.get('weather_temp') is not None else None,
            'weather_description': WEATHER_CODE_DESCRIPTIONS.get(safe_int(received_data.get('weather_code')), "Unknown"),
            'weather_humidity': f"{safe_int(received_data.get('weather_humidity'))}%" if received_data.get('weather_humidity') is not None else None,
            'weather_apparent_temp': f"{safe_int(received_data.get('weather_apparent_temp'))}°C" if received_data.get('weather_apparent_temp') is not None else None,
            'weather_precipitation': f"{safe_int(received_data.get('precipitation'))} mm" if received_data.get('precipitation') is not None else None,
            'weather_pressure_msl': f"{safe_int(received_data.get('pressure_msl'))} hPa" if received_data.get('pressure_msl') is not None else None,
            'weather_cloud_cover': f"{safe_int(received_data.get('cloud_cover'))}%" if received_data.get('cloud_cover') is not None else None,
            'weather_wind_speed': f"{safe_int(received_data.get('wind_speed_10m'))} m/s" if received_data.get('wind_speed_10m') is not None else None,
            'weather_wind_direction': wind_dir_compass if wind_dir_compass else None,
            'weather_wind_gusts': f"{safe_int(received_data.get('wind_gusts_10m'))} m/s" if received_data.get('wind_gusts_10m') is not None else None,
            'weather_observation_time': weather_obs_fmt,
            'weather_last_fetch_request_time': weather_fetch_fmt,
            'marine_wave_height': f"{safe_int(received_data.get('marine_wave_height'))} m" if received_data.get('marine_wave_height') is not None else None,
            'marine_wave_direction': f"{safe_int(received_data.get('marine_wave_direction'))}°" if received_data.get('marine_wave_direction') is not None else None,
            'marine_wave_period': f"{safe_int(received_data.get('marine_wave_period'))} s" if received_data.get('marine_wave_period') is not None else None,
            'marine_swell_wave_height': f"{safe_int(received_data.get('marine_swell_wave_height'))} m" if received_data.get('marine_swell_wave_height') is not None else None,
            'marine_swell_wave_direction': f"{safe_int(received_data.get('marine_swell_wave_direction'))}°" if received_data.get('marine_swell_wave_direction') is not None else None,
            'marine_swell_wave_period': f"{safe_int(received_data.get('marine_swell_wave_period'))} s" if received_data.get('marine_swell_wave_period') is not None else None,
            'gps_date_time': {'location_time': current_time or loc_time, 'location_timezone': loc_tz_str, 'location_date': loc_date},
            'source_ip': safe_string(received_data.get('source_ip')),
            'last_refresh_time_utc_reference': last_refresh_utc
        }
    except Exception as e:
        print(f"Transform error details: {str(e)}")
        import traceback
        traceback.print_exc()
        raise e
