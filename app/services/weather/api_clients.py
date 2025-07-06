import asyncio
import httpx
import datetime
from typing import Optional, Dict, Any

async def fetch_openmeteo_weather(lat: float, lon: float) -> Optional[Dict[str, Any]]:
    from app.weather.openmeteo.http_client import fetch_weather_data
    from app.weather.openmeteo.response_processor import combine_responses
    
    try:
        weather_data, marine_data = await fetch_weather_data(lat, lon)
        return combine_responses(weather_data, marine_data)
    except Exception as e:
        raise e

async def fetch_wttr_weather(lat: float, lon: float) -> Optional[Dict[str, Any]]:
    timeout_config = httpx.Timeout(connect=1.5, read=2.5, write=1.5, pool=4.0)
    
    async with httpx.AsyncClient(timeout=timeout_config) as client:
        try:
            response = await asyncio.wait_for(
                client.get(f'https://wttr.in/{lat},{lon}?format=j1'), 
                timeout=3.0
            )
            response.raise_for_status()
            
            data = response.json()
            current = data.get('current_condition', [{}])[0]
            
            result = {
                'weather_temp': float(current.get('temp_C', 0)) if current.get('temp_C') else None,
                'weather_humidity': int(current.get('humidity', 0)) if current.get('humidity') else None,
                'weather_apparent_temp': float(current.get('FeelsLikeC', 0)) if current.get('FeelsLikeC') else None,
                'precipitation': float(current.get('precipMM', 0)) if current.get('precipMM') else None,
                'pressure_msl': float(current.get('pressure', 0)) if current.get('pressure') else None,
                'cloud_cover': int(current.get('cloudcover', 0)) if current.get('cloudcover') else None,
                'wind_speed_10m': float(current.get('windspeedKmph', 0))/3.6 if current.get('windspeedKmph') else None,
                'wind_direction_10m': int(current.get('winddirDegree', 0)) if current.get('winddirDegree') else None,
                'weather_observation_time': current.get('observation_time')
            }
            
            return result
            
        except asyncio.TimeoutError:
            raise asyncio.TimeoutError("WTTR API timeout")
        except httpx.HTTPStatusError as e:
            raise e
        except Exception as e:
            raise Exception(f"WTTR API error: {str(e)}")
