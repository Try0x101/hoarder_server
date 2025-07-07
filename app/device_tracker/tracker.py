import datetime
from typing import Tuple
from app.transforms.geo import calculate_distance_km
from .rate_limiter import weather_rate_limiter
from .position_manager import get_device_position, save_device_position

MOVEMENT_THRESHOLD_KM = 1.0
WEATHER_FETCH_COOLDOWN_SECONDS = 30
WEATHER_EXPIRATION_SECONDS = 3600

async def should_force_weather_update(device_id: str, current_lat: float, current_lon: float) -> Tuple[bool, str]:
    now_utc = datetime.datetime.now(datetime.timezone.utc)
    last_position = await get_device_position(device_id)

    update_needed = False
    reason = "cached"

    if not last_position:
        update_needed = True
        reason = "first_request"
    else:
        last_update_iso = last_position.get('last_weather_update')
        if not last_update_iso:
            update_needed = True
            reason = "invalid_last_position"
        else:
            try:
                last_update_time = datetime.datetime.fromisoformat(last_update_iso)
                time_since_update = (now_utc - last_update_time).total_seconds()

                if time_since_update < WEATHER_FETCH_COOLDOWN_SECONDS:
                    return False, f"cooldown_active_{time_since_update:.1f}s"

                if time_since_update > WEATHER_EXPIRATION_SECONDS:
                    update_needed = True
                    reason = f"expired_{time_since_update:.0f}s"
                else:
                    last_lat = last_position.get('lat')
                    last_lon = last_position.get('lon')
                    distance = calculate_distance_km(current_lat, current_lon, last_lat, last_lon) if last_lat and last_lon else float('inf')
                    if distance >= MOVEMENT_THRESHOLD_KM:
                        update_needed = True
                        reason = f"moved_{distance:.2f}km"
                    else:
                        reason = f"cached_distance_{distance:.2f}km"
            except (ValueError, TypeError):
                update_needed = True
                reason = "invalid_timestamp_format"

    if not update_needed:
        return False, reason

    global_rate_ok, rate_message, rate_stats = await weather_rate_limiter.check_global_rate_limit()

    if global_rate_ok:
        print(f"[{now_utc}] Device {device_id} - proceeding with fetch (reason: {reason})")
        update_payload = {
            'lat': current_lat, 'lon': current_lon, 'last_weather_update': now_utc.isoformat(),
            'weather_update_count': (last_position.get('weather_update_count', 0) if last_position else 0) + 1,
            'rate_limit_stats': rate_message
        }
        await save_device_position(device_id, {**(last_position or {}), **update_payload})
        return True, reason
    else:
        print(f"[{now_utc}] Device {device_id} - update needed ({reason}) but hit global rate limit.")
        update_payload = {
            'lat': current_lat, 'lon': current_lon, 'last_seen': now_utc.isoformat(),
            'rate_limit_stats': rate_message
        }
        current_state = {**(last_position or {})}
        current_state.update(update_payload)
        await save_device_position(device_id, current_state)
        return False, f"global_rate_limit_{rate_stats.get('reason', 'exceeded')}"
