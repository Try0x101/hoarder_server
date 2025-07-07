import os
import datetime
import asyncio
from typing import Optional, Dict, Any
from collections import defaultdict
from .openmeteo_client import fetch_openmeteo_weather
from .wttr_client import get_weather_from_wttr
from .simple_breaker import weather_breaker, wttr_breaker, get_breaker_status
from .simple_cache import find_cached_weather, save_weather_cache

WEATHER_CODE_DESCRIPTIONS = {0: "Clear", 1: "Mainly clear", 2: "Partly cloudy", 3: "Overcast", 45: "Fog", 48: "Rime fog", 51: "Light drizzle", 53: "Drizzle", 55: "Dense drizzle", 56: "Light freezing drizzle", 57: "Dense freezing drizzle", 61: "Slight rain", 63: "Rain", 65: "Heavy rain", 66: "Light freezing rain", 67: "Heavy freezing rain", 71: "Slight snow", 73: "Snow", 75: "Heavy snow", 77: "Snow grains", 80: "Slight showers", 81: "Showers", 82: "Violent showers", 85: "Slight snow showers", 86: "Heavy snow showers", 95: "Thunderstorm", 96: "Thunderstorm+hail", 99: "Thunderstorm+heavy hail"}
DAILY_API_LIMIT = 9000
_api_locks = defaultdict(asyncio.Lock)

def get_today_request_count() -> int:
    # This is a simplified stub for demonstration. A robust implementation would use a persistent store like Redis.
    return 0 

async def get_weather_data(lat: float, lon: float) -> Optional[Dict[str, Any]]:
    if cached := await find_cached_weather(lat, lon): return cached
    
    cache_key = f"{round(lat, 3)},{round(lon, 3)}"
    async with _api_locks[cache_key]:
        if cached := await find_cached_weather(lat, lon): return cached

        if get_today_request_count() >= DAILY_API_LIMIT:
            print(f"[{datetime.datetime.now()}] Daily API limit reached, trying WTTR fallback")
            return await get_weather_from_wttr(lat, lon)

        try:
            result = await weather_breaker.call(fetch_openmeteo_weather, lat, lon)
            if result and any(v is not None for v in result.values()):
                await save_weather_cache(lat, lon, result)
                return result
        except Exception as e:
            print(f"[{datetime.datetime.now()}] OpenMeteo failed: {e}")

        try:
            result = await wttr_breaker.call(get_weather_from_wttr, lat, lon)
            if result:
                await save_weather_cache(lat, lon, result)
                return result
        except Exception as e:
            print(f"[{datetime.datetime.now()}] Fallback WTTR also failed: {e}")

    return None

async def get_weather_service_status():
    return {
        "circuit_breakers": await get_breaker_status(),
        "daily_requests": get_today_request_count(), "daily_limit": DAILY_API_LIMIT
    }
def get_weather_code_description(code: int) -> str:
    return WEATHER_CODE_DESCRIPTIONS.get(code, "Unknown")
