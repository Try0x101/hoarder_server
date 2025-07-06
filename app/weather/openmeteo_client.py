from typing import Optional, Dict, Any
from .openmeteo.http_client import fetch_weather_data
from .openmeteo.response_processor import combine_responses
from .openmeteo.config import get_client_info

async def fetch_openmeteo_weather(lat: float, lon: float) -> Optional[Dict[str, Any]]:
    try:
        weather_data, marine_data = await fetch_weather_data(lat, lon)
        return combine_responses(weather_data, marine_data)
    except Exception as e:
        raise e

def get_openmeteo_client_info():
    return get_client_info()
