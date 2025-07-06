import os
import datetime
import asyncio
from typing import Optional, Dict, Any
from collections import defaultdict
from .openmeteo_client import fetch_openmeteo_weather
from .wttr_client import get_weather_from_wttr
from .simple_breaker import weather_breaker, wttr_breaker

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

DAILY_API_LIMIT = 9000
REQUEST_LOG_FILE = "/tmp/weather_requests.log"
_api_locks = defaultdict(asyncio.Lock)

def safe_log_api_request():
    try:
        timestamp = datetime.datetime.now().isoformat()
        with open(REQUEST_LOG_FILE, 'a') as f:
            f.write(f"{timestamp}\n")
    except Exception:
        pass

def get_today_request_count() -> int:
    try:
        if not os.path.exists(REQUEST_LOG_FILE):
            return 0
        today = datetime.date.today()
        count = 0
        with open(REQUEST_LOG_FILE, 'r') as f:
            for line in f:
                try:
                    request_time = datetime.datetime.fromisoformat(line.strip())
                    if request_time.date() == today:
                        count += 1
                except:
                    continue
        return count
    except Exception:
        return 0

def can_make_api_request() -> bool:
    today_count = get_today_request_count()
    if today_count >= DAILY_API_LIMIT:
        print(f"[{datetime.datetime.now()}] WARNING: Daily API limit reached ({today_count}/{DAILY_API_LIMIT})")
        return False
    return True

def create_cache_key(lat: float, lon: float) -> str:
    return f"{round(lat, 3)},{round(lon, 3)}"

async def get_weather_data(lat: float, lon: float) -> Optional[Dict[str, Any]]:
    from .simple_cache import find_cached_weather, save_weather_cache

    print(f"[{datetime.datetime.now()}] Weather request for {lat:.4f}, {lon:.4f}")
    
    try:
        cached_data = await find_cached_weather(lat, lon)
        if cached_data:
            print(f"[{datetime.datetime.now()}] Using cached weather data")
            return cached_data
    except Exception as e:
        print(f"[{datetime.datetime.now()}] Cache lookup failed: {e}")

    cache_key = create_cache_key(lat, lon)
    async with _api_locks[cache_key]:
        try:
            cached_data = await find_cached_weather(lat, lon)
            if cached_data:
                return cached_data
        except Exception:
            pass

        if not can_make_api_request():
            print(f"[{datetime.datetime.now()}] Daily API limit reached, trying WTTR fallback")
            return await get_weather_from_wttr(lat, lon)

        primary_result = await try_primary_weather_api(lat, lon)
        if primary_result:
            try:
                await save_weather_cache(lat, lon, primary_result)
            except Exception as e:
                print(f"[{datetime.datetime.now()}] Cache save failed: {e}")
            return primary_result

        fallback_result = await try_fallback_weather_api(lat, lon)
        if fallback_result:
            try:
                await save_weather_cache(lat, lon, fallback_result)
            except Exception as e:
                print(f"[{datetime.datetime.now()}] Fallback cache save failed: {e}")
            return fallback_result

    print(f"[{datetime.datetime.now()}] All weather APIs failed")
    return None

async def try_primary_weather_api(lat: float, lon: float) -> Optional[Dict[str, Any]]:
    try:
        result = await weather_breaker.call(fetch_openmeteo_weather, lat, lon)
        safe_log_api_request()
        
        if result and any(v is not None for v in result.values()):
            print(f"[{datetime.datetime.now()}] OpenMeteo API success")
            return result
    except Exception as e:
        print(f"[{datetime.datetime.now()}] OpenMeteo circuit breaker failed: {e}")
    
    return None

async def try_fallback_weather_api(lat: float, lon: float) -> Optional[Dict[str, Any]]:
    try:
        result = await wttr_breaker.call(get_weather_from_wttr, lat, lon)
        if result:
            print(f"[{datetime.datetime.now()}] Fallback WTTR success")
            return result
    except Exception as e:
        print(f"[{datetime.datetime.now()}] Fallback WTTR also failed: {e}")
    
    return None

async def get_weather_service_status():
    try:
        from .simple_breaker import get_breaker_status
        circuit_status = await get_breaker_status()
    except Exception:
        circuit_status = {"error": "circuit_status_unavailable"}
    
    return {
        **circuit_status,
        "system_health": {
            "daily_requests": get_today_request_count(),
            "daily_limit": DAILY_API_LIMIT,
            "can_make_requests": can_make_api_request()
        },
        "weather_codes": len(WEATHER_CODE_DESCRIPTIONS),
        "services": ["OpenMeteo", "WTTR"]
    }

def get_weather_code_description(code: int) -> str:
    return WEATHER_CODE_DESCRIPTIONS.get(code, "Unknown")
