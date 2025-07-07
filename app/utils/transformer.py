from app.transforms import (
    safe_int, safe_float, get_wind_direction_compass, get_network_active,
    format_weather_observation_time, get_weather_fetch_formatted,
    get_current_location_time, format_last_refresh_time, normalize_bssid,
    WEATHER_CODE_DESCRIPTIONS
)
from app.shared.time.timezone_lookup import get_timezone_info_from_coordinates
from .helpers import safe_string
from .fingerprint import safe_device_id

async def transform_device_data(received_data):
    try:
        lat = received_data.get('lat')
        lon = received_data.get('lon')
        
        loc_date, loc_time, loc_tz_str, loc_tz = get_timezone_info_from_coordinates(lat, lon)
        
        device_id_val = safe_device_id(
            received_data.get('id') or received_data.get('device_id'),
            received_data.get('source_ip'), received_data.get('user_agent'), received_data
        )
        
        weather_fetch_fmt = await get_weather_fetch_formatted(device_id_val, loc_tz, loc_tz_str)
        _, last_refresh_utc = format_last_refresh_time(received_data, loc_tz, loc_tz_str)
        
        def f(key, unit):
            val = safe_int(received_data.get(key))
            return f"{val}{unit}" if val is not None else None

        transformed = {
            "identity": {
                "device_id": device_id_val,
                "device_name": safe_string(received_data.get('n'))
            },
            "network": {
                "cellular": {
                    "operator": safe_string(received_data.get('op')),
                    "mcc": safe_string(safe_int(received_data.get('mcc'))),
                    "mnc": safe_string(safe_int(received_data.get('mnc'))),
                    "cell_id": safe_string(safe_int(received_data.get('ci'))),
                    "tac": safe_string(received_data.get('tac')),
                    "signal_strength": f('rssi', ' dBm'),
                    "type": safe_string(received_data.get('nt'))
                },
                "wifi": {"active": get_network_active(received_data), "bssid": normalize_bssid(received_data.get('bssid'))},
                "bandwidth": {"download_capacity": f('dn', ' Mbps'), "upload_capacity": f('up', ' Mbps')},
                "source_ip": safe_string(received_data.get('source_ip'))
            },
            "location": {
                "coordinates": {
                    "latitude": safe_string(safe_float(lat)), "longitude": safe_string(safe_float(lon)),
                    "accuracy": f('acc', ' m'), "altitude": f('alt', ' m'), "speed": f('spd', ' km/h'),
                },
                "gps_date_time": {"date": loc_date, "time": loc_time, "timezone": loc_tz_str}
            },
            "environment": {
                "weather": {
                    "description": WEATHER_CODE_DESCRIPTIONS.get(safe_int(received_data.get('weather_code')), "Unknown"),
                    "temperature": f('weather_temp', '°C'), "apparent_temp": f('weather_apparent_temp', '°C'),
                    "humidity": f('weather_humidity', '%'), "precipitation": f('precipitation', ' mm'),
                    "pressure_msl": f('pressure_msl', ' hPa'), "cloud_cover": f('cloud_cover', '%'),
                    "wind": {
                        "speed": f('wind_speed_10m', ' m/s'), "gusts": f('wind_gusts_10m', ' m/s'),
                        "direction": get_wind_direction_compass(received_data.get('wind_direction_10m'))
                    },
                    "observation_time": format_weather_observation_time(received_data.get('weather_observation_time'), loc_tz, loc_tz_str),
                    "last_fetch_request_time": weather_fetch_fmt
                },
                "marine": {
                    "wave": {"height": f('marine_wave_height', ' m'), "direction": f('marine_wave_direction', '°'), "period": f('marine_wave_period', ' s')},
                    "swell_wave": {"height": f('marine_swell_wave_height', ' m'), "direction": f('marine_swell_wave_direction', '°'), "period": f('marine_swell_wave_period', ' s')}
                }
            },
            "power": {"battery": {"percent": f('perc', '%'), "total_capacity": f('cap', ' mAh')}},
            "timestamps": {"last_refresh_time_utc": last_refresh_utc}
        }
        
        if (cap := safe_int(received_data.get('cap'))) is not None and (perc := safe_int(received_data.get('perc'))) is not None:
            transformed['power']['battery']['leftover_calculated'] = f"{safe_int(cap * perc / 100)} mAh"

        def cleanup_empty(d):
            if not isinstance(d, dict): return d
            cleaned_dict = {}
            for k, v in d.items():
                if isinstance(v, dict): v = cleanup_empty(v)
                if v is not None and v != {} and v != '' and v != []: cleaned_dict[k] = v
            return cleaned_dict
        
        return cleanup_empty(transformed)

    except Exception as e:
        import traceback
        print(f"Transform error details: {str(e)}")
        traceback.print_exc()
        raise e
