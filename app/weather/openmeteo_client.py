import asyncio
import httpx
import datetime
from typing import Optional, Dict, Any

async def fetch_openmeteo_weather(lat: float, lon: float) -> Optional[Dict[str, Any]]:
    weather_params = {
        'latitude': lat,
        'longitude': lon,
        'current': [
            'temperature_2m', 'relative_humidity_2m', 'apparent_temperature',
            'precipitation', 'weather_code', 'pressure_msl', 'cloud_cover',
            'wind_speed_10m', 'wind_direction_10m', 'wind_gusts_10m'
        ],
        'timezone': 'auto'
    }
    
    marine_params = {
        'latitude': lat,
        'longitude': lon,
        'current': [
            'wave_height', 'wave_direction', 'wave_period',
            'swell_wave_height', 'swell_wave_direction', 'swell_wave_period'
        ],
        'timezone': 'auto'
    }

    timeout_config = httpx.Timeout(connect=2.0, read=3.0, write=2.0, pool=5.0)
    
    async with httpx.AsyncClient(timeout=timeout_config) as client:
        try:
            weather_task = client.get('https://api.open-meteo.com/v1/forecast', params=weather_params)
            marine_task = client.get('https://marine-api.open-meteo.com/v1/marine', params=marine_params)
            
            weather_response, marine_response = await asyncio.wait_for(
                asyncio.gather(weather_task, marine_task, return_exceptions=True), 
                timeout=4.0
            )

            result = {}
            weather_success = False
            marine_success = False
            
            if not isinstance(weather_response, Exception):
                weather_response.raise_for_status()
                weather_data = weather_response.json()
                current = weather_data.get('current', {})
                result.update({
                    'weather_temp': current.get('temperature_2m'),
                    'weather_humidity': current.get('relative_humidity_2m'),
                    'weather_apparent_temp': current.get('apparent_temperature'),
                    'precipitation': current.get('precipitation'),
                    'weather_code': current.get('weather_code'),
                    'pressure_msl': current.get('pressure_msl'),
                    'cloud_cover': current.get('cloud_cover'),
                    'wind_speed_10m': current.get('wind_speed_10m'),
                    'wind_direction_10m': current.get('wind_direction_10m'),
                    'wind_gusts_10m': current.get('wind_gusts_10m'),
                    'weather_observation_time': current.get('time')
                })
                weather_success = True

            if not isinstance(marine_response, Exception):
                marine_response.raise_for_status()
                marine_data = marine_response.json()
                current = marine_data.get('current', {})
                result.update({
                    'marine_wave_height': current.get('wave_height'),
                    'marine_wave_direction': current.get('wave_direction'),
                    'marine_wave_period': current.get('wave_period'),
                    'marine_swell_wave_height': current.get('swell_wave_height'),
                    'marine_swell_wave_direction': current.get('swell_wave_direction'),
                    'marine_swell_wave_period': current.get('swell_wave_period')
                })
                marine_success = True

            if not weather_success and not marine_success:
                raise Exception("Both weather and marine API calls failed")
            elif not weather_success:
                print(f"[{datetime.datetime.now()}] OpenMeteo: Marine data only (weather failed)")
            elif not marine_success:
                print(f"[{datetime.datetime.now()}] OpenMeteo: Weather data only (marine failed)")
                
            return result

        except asyncio.TimeoutError:
            raise asyncio.TimeoutError("OpenMeteo API timeout")
        except httpx.HTTPStatusError as e:
            raise e
        except Exception as e:
            raise Exception(f"OpenMeteo API error: {str(e)}")

def get_openmeteo_client_info():
    return {
        'service': 'OpenMeteo',
        'endpoints': {
            'weather': 'https://api.open-meteo.com/v1/forecast',
            'marine': 'https://marine-api.open-meteo.com/v1/marine'
        },
        'timeout_config': {
            'connect': 2.0,
            'read': 3.0,
            'write': 2.0,
            'pool': 5.0,
            'total': 4.0
        },
        'data_types': ['weather', 'marine']
    }
