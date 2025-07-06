import asyncio
import httpx
import datetime
from typing import Optional, Dict, Any, Tuple
from .config import (
    WEATHER_API_URL, MARINE_API_URL, TIMEOUT_CONFIG, TOTAL_TIMEOUT,
    get_weather_params, get_marine_params
)

async def fetch_weather_data(lat: float, lon: float) -> Tuple[Optional[Dict], Optional[Dict]]:
    weather_params = get_weather_params(lat, lon)
    marine_params = get_marine_params(lat, lon)
    
    async with httpx.AsyncClient(timeout=TIMEOUT_CONFIG) as client:
        try:
            weather_task = client.get(WEATHER_API_URL, params=weather_params)
            marine_task = client.get(MARINE_API_URL, params=marine_params)
            
            weather_response, marine_response = await asyncio.wait_for(
                asyncio.gather(weather_task, marine_task, return_exceptions=True), 
                timeout=TOTAL_TIMEOUT
            )

            weather_data = None
            marine_data = None
            
            if not isinstance(weather_response, Exception):
                weather_response.raise_for_status()
                weather_data = weather_response.json()

            if not isinstance(marine_response, Exception):
                marine_response.raise_for_status()
                marine_data = marine_response.json()
            
            return weather_data, marine_data
            
        except asyncio.TimeoutError:
            raise asyncio.TimeoutError("OpenMeteo API timeout")
        except httpx.HTTPStatusError as e:
            raise e
        except Exception as e:
            raise Exception(f"OpenMeteo API error: {str(e)}")

async def test_api_connectivity() -> Dict[str, bool]:
    try:
        async with httpx.AsyncClient(timeout=TIMEOUT_CONFIG) as client:
            weather_task = asyncio.create_task(client.get(WEATHER_API_URL, params={'latitude': 0, 'longitude': 0}))
            marine_task = asyncio.create_task(client.get(MARINE_API_URL, params={'latitude': 0, 'longitude': 0}))
            
            results = await asyncio.gather(weather_task, marine_task, return_exceptions=True)
            
            return {
                'weather_api': not isinstance(results[0], Exception),
                'marine_api': not isinstance(results[1], Exception)
            }
    except Exception:
        return {'weather_api': False, 'marine_api': False}
