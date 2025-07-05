import datetime
import pytz
from timezonefinder import TimezoneFinder

tf = TimezoneFinder()

def calculate_weather_data_age(fetch_time_str):
    if not fetch_time_str or not isinstance(fetch_time_str, str):
        return None
    try:
        dt = None
        if 'T' in fetch_time_str and ('Z' in fetch_time_str or '+' in fetch_time_str):
            try:
                dt = datetime.datetime.fromisoformat(fetch_time_str.replace('Z', '+00:00'))
            except ValueError:
                dt = None
        
        if dt is None:
            parts = fetch_time_str.strip().split()
            if len(parts) >= 2:
                date_time_str = " ".join(parts[:-1])
                tz_str = parts[-1]

                if "." in date_time_str:
                    dt_naive = datetime.datetime.strptime(date_time_str, "%d.%m.%Y %H:%M:%S")
                else:
                    dt_naive = datetime.datetime.strptime(date_time_str, "%Y-%m-%d %H:%M:%S")

                if tz_str.startswith("UTC"):
                    if tz_str == "UTC":
                        offset = datetime.timedelta(hours=0)
                    else:
                        offset_str = tz_str[3:]
                        sign = 1 if offset_str[0] == "+" else -1
                        if ":" in offset_str:
                            hours, minutes = map(int, offset_str[1:].split(":"))
                        else:
                            hours, minutes = int(offset_str[1:]), 0
                        offset = datetime.timedelta(hours=sign * hours, minutes=minutes)
                    
                    tz = datetime.timezone(offset)
                    dt = dt_naive.replace(tzinfo=tz)
    except Exception as e:
        print(f"Error parsing time '{fetch_time_str}': {e}")
        return None

    if dt is None: return None
    if dt.tzinfo is None: dt = dt.replace(tzinfo=datetime.timezone.utc)

    now = datetime.datetime.now(datetime.timezone.utc)
    time_diff = now - dt
    seconds_diff = int(time_diff.total_seconds())

    if seconds_diff < 0: return "in the future"
    if seconds_diff < 60: return f"{seconds_diff} sec"
    if seconds_diff < 3600:
        minutes = seconds_diff // 60
        return f"{minutes} minute{'s' if minutes > 1 else ''}"
    if seconds_diff < 86400:
        hours = seconds_diff // 3600
        return f"{hours} hour{'s' if hours > 1 else ''}"
    
    days = seconds_diff // 86400
    return f"{days} day{'s' if days > 1 else ''}"

def get_current_location_time(tz):
    if not tz:
        return None
    try:
        current_time = datetime.datetime.now(tz)
        return current_time.strftime("%H:%M:%S")
    except Exception as e:
        print(f"Error getting current location time: {e}")
        return None

def get_location_time_info(lat, lon):
    try:
        timezone_str = tf.timezone_at(lat=lat, lng=lon)
        if not timezone_str:
            return None, None, None, None

        tz = pytz.timezone(timezone_str)
        current_time = datetime.datetime.now(tz)
        location_date = current_time.strftime("%d.%m.%Y")
        location_time = current_time.strftime("%H:%M:%S")

        utc_offset = current_time.utcoffset()
        total_seconds = int(utc_offset.total_seconds())
        hours = total_seconds // 3600
        minutes = abs(total_seconds % 3600) // 60

        if minutes == 0:
            location_timezone = f"UTC{'+' if hours >= 0 else ''}{hours}"
        else:
            location_timezone = f"UTC{'+' if hours >= 0 else ''}{hours}:{minutes:02d}"

        return location_date, location_time, location_timezone, tz
    except Exception as e:
        print(f"Error getting location time info: {e}")
        return None, None, None, None

def format_last_refresh_time(received_data, location_tz, location_timezone):
    last_refresh_time = None
    last_refresh_time_utc_reference = None

    if 'received_at' in received_data and received_data.get('received_at'):
        try:
            received_time = received_data.get('received_at')
            if isinstance(received_time, str):
                received_time = datetime.datetime.fromisoformat(received_time.replace('Z', '+00:00'))

            if isinstance(received_time, datetime.datetime):
                if received_time.tzinfo is None:
                    received_time_utc = received_time.replace(tzinfo=datetime.timezone.utc)
                else:
                    received_time_utc = received_time.astimezone(datetime.timezone.utc)

                last_refresh_time_utc_reference = received_time_utc.strftime("%Y-%m-%d %H:%M:%S UTC")

                if location_tz:
                    received_time_local = received_time_utc.astimezone(location_tz)
                    last_refresh_time = received_time_local.strftime("%Y-%m-%d %H:%M:%S ") + (location_timezone or "UTC")
                else:
                    last_refresh_time = last_refresh_time_utc_reference
        except Exception as e:
            print(f"Error formatting last_refresh_time: {e}")

    if not last_refresh_time_utc_reference:
        now_utc = datetime.datetime.now(datetime.timezone.utc)
        last_refresh_time_utc_reference = now_utc.strftime("%Y-%m-%d %H:%M:%S UTC")

        if location_tz:
            now_local = now_utc.astimezone(location_tz)
            last_refresh_time = now_local.strftime("%Y-%m-%d %H:%M:%S ") + (location_timezone or "UTC")
        else:
            last_refresh_time = last_refresh_time_utc_reference

    last_refresh_time_data_age = calculate_weather_data_age(last_refresh_time)

    return last_refresh_time, last_refresh_time_utc_reference, last_refresh_time_data_age

def get_timezone_info_from_coordinates(lat, lon):
    location_date, location_time, location_timezone, location_tz = None, None, None, None

    if lat is not None and lon is not None:
        try:
            from .device import safe_float
            lat_float, lon_float = safe_float(lat), safe_float(lon)
            if lat_float is not None and lon_float is not None:
                location_date, location_time, location_timezone, location_tz = get_location_time_info(lat_float, lon_float)
        except Exception as e:
            print(f"Error processing coordinates: lat={lat}, lon={lon}, error={e}")

    return location_date, location_time, location_timezone, location_tz
