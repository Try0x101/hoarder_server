import datetime
import pytz
from app.weather import WEATHER_CODE_DESCRIPTIONS

def safe_int(value):
 if value is None:return None
 try:return int(float(value))
 except (ValueError,TypeError):return None

def safe_float(value):
 if value is None:return None
 try:return float(value)
 except (ValueError,TypeError):return None

def get_wind_direction_compass(wind_direction_10m):
    if wind_direction_10m is None:
        return ""

    try:
        direction = int(float(wind_direction_10m))
        if 337.5 <= direction < 360 or 0 <= direction < 22.5:
            return "N"
        elif 22.5 <= direction < 67.5:
            return "NE"
        elif 67.5 <= direction < 112.5:
            return "E"
        elif 112.5 <= direction < 157.5:
            return "SE"
        elif 157.5 <= direction < 202.5:
            return "S"
        elif 202.5 <= direction < 247.5:
            return "SW"
        elif 247.5 <= direction < 292.5:
            return "W"
        elif 292.5 <= direction < 337.5:
            return "NW"
        return ""
    except (ValueError, TypeError):
        return ""

def get_network_active(received_data):
    return 'Wi-Fi' if received_data.get('bssid') and received_data.get('bssid') not in ['0', '', 'error'] else received_data.get('nt')

def get_barometric_data(received_data):
    if 'bar' in received_data and received_data.get('bar') is not None:
        bar_value = safe_float(received_data.get('bar'))
        if bar_value is not None:
            if bar_value < 0:
                return f"{abs(bar_value)} hPa"
            else:
                return f"{bar_value} m"
    return None

def format_weather_observation_time(weather_observation_time, location_tz, location_timezone):
    if not weather_observation_time:
        return None

    try:
        obs_time = None
        if 'T' in str(weather_observation_time):
            obs_time = datetime.datetime.fromisoformat(weather_observation_time)
        elif ':' in str(weather_observation_time):
            obs_str = str(weather_observation_time).strip()
            try:
                if 'AM' in obs_str.upper() or 'PM' in obs_str.upper():
                    obs_time = datetime.datetime.strptime(obs_str, '%I:%M %p')
                else:
                    obs_time = datetime.datetime.strptime(obs_str, '%H:%M')
                obs_time = obs_time.replace(year=datetime.datetime.now().year, month=datetime.datetime.now().month, day=datetime.datetime.now().day)
            except:
                pass

        if obs_time:
            if location_tz:
                if obs_time.tzinfo is None:
                    obs_time = obs_time.replace(tzinfo=pytz.utc)
                obs_time_local = obs_time.astimezone(location_tz)
                return f"{obs_time_local.strftime('%d.%m.%Y %H:%M')} {location_timezone}"
            else:
                if obs_time.tzinfo is None:
                    obs_time = obs_time.replace(tzinfo=pytz.utc)
                return f"{obs_time.strftime('%d.%m.%Y %H:%M')} UTC"
        else:
            return f"{weather_observation_time} (local time)"
    except Exception as e:
        print(f"Error formatting weather observation time: {e}")
        return f"{weather_observation_time} (format error)"

async def get_weather_fetch_formatted(device_id, location_tz, location_timezone):
    weather_fetch_formatted = None
    if device_id:
        try:
            from app.device_tracker import get_device_position
            position = await get_device_position(device_id)
            if position and 'last_weather_update' in position:
                weather_last_fetched_iso = position['last_weather_update']
                weather_time_utc = None
                try:
                    weather_time_utc = datetime.datetime.fromisoformat(weather_last_fetched_iso)
                except ValueError:
                    try:
                        weather_time_utc = datetime.datetime.strptime(weather_last_fetched_iso, '%d.%m.%Y %H:%M:%S')
                    except Exception as strptime_err:
                        print(f"Could not parse weather fetch timestamp '{weather_last_fetched_iso}': {strptime_err}")
                        weather_fetch_formatted = weather_last_fetched_iso
                if weather_time_utc:
                    if weather_time_utc.tzinfo is None:
                        weather_time_utc = pytz.utc.localize(weather_time_utc)
                    if location_tz:
                        weather_time_local = weather_time_utc.astimezone(location_tz)
                        weather_fetch_formatted = f"{weather_time_local.strftime('%d.%m.%Y %H:%M:%S')} {location_timezone}"
                    else:
                        weather_fetch_formatted = f"{weather_time_utc.strftime('%d.%m.%Y %H:%M:%S')} UTC"
        except Exception as e:
            print(f"Error getting weather fetch time from device tracker: {e}")

    if not weather_fetch_formatted:
        now_utc = datetime.datetime.now(datetime.timezone.utc)
        if location_tz:
            now_local = now_utc.astimezone(location_tz)
            weather_fetch_formatted = f"{now_local.strftime('%d.%m.%Y %H:%M:%S')} {location_timezone}"
        else:
            weather_fetch_formatted = f"{now_utc.strftime('%d.%m.%Y %H:%M:%S')} UTC"

    return weather_fetch_formatted
