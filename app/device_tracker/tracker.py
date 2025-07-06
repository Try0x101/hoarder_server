import datetime
from typing import Tuple
from app.transforms.geo import calculate_distance_km
from .rate_limiter import weather_rate_limiter
from .position_manager import get_device_position, save_device_position

MOVEMENT_THRESHOLD_KM = 1.0
WEATHER_FETCH_COOLDOWN_SECONDS = 30

async def should_force_weather_update(device_id: str, current_lat: float, current_lon: float) -> Tuple[bool, str]:
    global_rate_ok, rate_message, rate_stats = await weather_rate_limiter.check_global_rate_limit()
    if not global_rate_ok:
        return False, f"global_rate_limit: {rate_stats.get('reason', 'exceeded')}"

    now_utc = datetime.datetime.now(datetime.timezone.utc)
    last_position = await get_device_position(device_id)

    if not last_position:
        print(f"[{now_utc}] Device {device_id} - first weather request (global: {rate_stats})")
        await save_device_position(device_id, {
            'lat': current_lat, 
            'lon': current_lon, 
            'last_weather_update': now_utc.isoformat(), 
            'weather_update_count': 1,
            'rate_limit_stats': rate_message
        })
        return True, "first_request"

    last_lat = last_position.get('lat')
    last_lon = last_position.get('lon')
    last_update_iso = last_position.get('last_weather_update')

    if last_update_iso:
        try:
            last_update_time = datetime.datetime.fromisoformat(last_update_iso)
            time_since_update = now_utc - last_update_time
            if time_since_update.total_seconds() < WEATHER_FETCH_COOLDOWN_SECONDS:
                print(f"[{now_utc}] Device {device_id} - cooldown active ({time_since_update.total_seconds():.1f}s < {WEATHER_FETCH_COOLDOWN_SECONDS}s)")
                update_payload = {
                    'current_lat': current_lat,
                    'current_lon': current_lon,
                    'last_seen': now_utc.isoformat()
                }
                merged_data = last_position.copy()
                merged_data.update(update_payload)
                await save_device_position(device_id, merged_data)
                return False, f"cooldown_active_{time_since_update.total_seconds():.1f}s"
        except Exception as e:
            print(f"[{now_utc}] Error parsing last update time: {e}")

    if last_lat is None or last_lon is None:
        print(f"[{now_utc}] Device {device_id} - invalid last position (global: {rate_stats})")
        last_position.update({
            'lat': current_lat, 
            'lon': current_lon, 
            'last_weather_update': now_utc.isoformat(), 
            'weather_update_count': last_position.get('weather_update_count', 0) + 1,
            'rate_limit_stats': rate_message
        })
        await save_device_position(device_id, last_position)
        return True, "invalid_last_position"

    distance = calculate_distance_km(current_lat, current_lon, last_lat, last_lon)
    print(f"[{now_utc}] Device {device_id} - distance from last weather update: {distance:.2f}km (global: {rate_stats})")

    if distance >= MOVEMENT_THRESHOLD_KM:
        print(f"[{now_utc}] Device {device_id} - significant movement detected ({distance:.2f}km >= {MOVEMENT_THRESHOLD_KM}km)")
        last_position.update({
            'lat': current_lat, 
            'lon': current_lon, 
            'last_weather_update': now_utc.isoformat(), 
            'weather_update_count': last_position.get('weather_update_count', 0) + 1,
            'rate_limit_stats': rate_message
        })
        await save_device_position(device_id, last_position)
        return True, f"moved_{distance:.2f}km"

    if last_update_iso:
        try:
            last_update_time = datetime.datetime.fromisoformat(last_update_iso)
            time_since_update = now_utc - last_update_time
            if time_since_update.total_seconds() > 3600:
                print(f"[{now_utc}] Device {device_id} - weather data expired ({time_since_update}) (global: {rate_stats})")
                last_position.update({
                    'lat': current_lat, 
                    'lon': current_lon, 
                    'last_weather_update': now_utc.isoformat(), 
                    'weather_update_count': last_position.get('weather_update_count', 0) + 1,
                    'rate_limit_stats': rate_message
                })
                await save_device_position(device_id, last_position)
                return True, f"expired_{int(time_since_update.total_seconds())}s"
        except Exception as e:
            print(f"[{now_utc}] Error parsing last update time: {e}")

    update_payload = {
        'current_lat': current_lat,
        'current_lon': current_lon,
        'last_seen': now_utc.isoformat()
    }
    merged_data = last_position.copy()
    merged_data.update(update_payload)
    await save_device_position(device_id, merged_data)
    
    print(f"[{now_utc}] Device {device_id} - using cached weather (distance: {distance:.2f}km, global: {rate_stats})")
    return False, f"cached_distance_{distance:.2f}km"
