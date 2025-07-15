import datetime
import pytz

WEATHER_CODE_DESCRIPTIONS = {
    0: "Clear sky", 1: "Mainly clear", 2: "Partly cloudy", 3: "Overcast",
    45: "Fog", 48: "Depositing rime fog", 51: "Light drizzle", 53: "Moderate drizzle",
    55: "Dense drizzle", 56: "Light freezing drizzle", 57: "Dense freezing drizzle",
    61: "Slight rain", 63: "Moderate rain", 65: "Heavy rain",
    66: "Light freezing rain", 67: "Heavy freezing rain", 71: "Slight snow fall",
    73: "Moderate snow fall", 75: "Heavy snow fall", 77: "Snow grains",
    80: "Slight rain showers", 81: "Moderate rain showers", 82: "Violent rain showers",
    85: "Slight snow showers", 86: "Heavy snow showers", 95: "Thunderstorm",
    96: "Thunderstorm with slight hail", 99: "Thunderstorm with heavy hail"
}

def safe_int(value):
 if value is None: return None
 try: return int(float(value))
 except (ValueError, TypeError): return None

def safe_float(value):
 if value is None: return None
 try: return float(value)
 except (ValueError, TypeError): return None

def normalize_bssid(bssid_value):
    if bssid_value is None: return None
    str_value = str(bssid_value).strip()
    if str_value.lower() in ['', 'error', 'null', 'none', 'undefined', 'n/a', '0']:
        return None
    return str_value

def get_network_active(received_data):
    bssid = normalize_bssid(received_data.get('bssid'))
    if bssid is not None and bssid != "0":
        return 'Wi-Fi'
    return received_data.get('nt')

def get_wind_direction_compass(wind_direction_10m):
    if wind_direction_10m is None: return ""
    try:
        direction = int(float(wind_direction_10m))
        if 337.5 <= direction <= 360 or 0 <= direction < 22.5: return "N"
        elif 22.5 <= direction < 67.5: return "NE"
        elif 67.5 <= direction < 112.5: return "E"
        elif 112.5 <= direction < 157.5: return "SE"
        elif 157.5 <= direction < 202.5: return "S"
        elif 202.5 <= direction < 247.5: return "SW"
        elif 247.5 <= direction < 292.5: return "W"
        elif 292.5 <= direction < 337.5: return "NW"
    except (ValueError, TypeError): pass
    return ""

def format_weather_observation_time(weather_observation_time, location_tz, location_timezone):
    if not weather_observation_time: return None
    try:
        obs_time = datetime.datetime.fromisoformat(str(weather_observation_time).strip().replace("Z", "+00:00"))
        if location_tz:
            local_time = obs_time.astimezone(location_tz)
            return f"{local_time.strftime('%d.%m.%Y %H:%M')} {location_timezone or ''}".strip()
        return f"{obs_time.strftime('%d.%m.%Y %H:%M')} UTC"
    except Exception:
        return str(weather_observation_time)

async def get_weather_fetch_formatted(device_id, location_tz, location_timezone):
    if not device_id: return None
    try:
        from app.device_tracker import get_device_position
        position = await get_device_position(device_id)
        if not position or 'last_weather_update' not in position: return None
        
        weather_time_utc = datetime.datetime.fromisoformat(position['last_weather_update'].replace('Z', '+00:00'))
        if location_tz:
            local_time = weather_time_utc.astimezone(location_tz)
            return f"{local_time.strftime('%d.%m.%Y %H:%M:%S')} {location_timezone or ''}".strip()
        return f"{weather_time_utc.strftime('%d.%m.%Y %H:%M:%S')} UTC"
    except Exception:
        return None
