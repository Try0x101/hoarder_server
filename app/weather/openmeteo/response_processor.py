import datetime
from typing import Optional, Dict, Any

def process_weather_response(weather_data: Dict) -> Dict[str, Any]:
    if not weather_data:
        return {}
        
    current = weather_data.get('current', {})
    return {
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
    }

def process_marine_response(marine_data: Dict) -> Dict[str, Any]:
    if not marine_data:
        return {}
        
    current = marine_data.get('current', {})
    return {
        'marine_wave_height': current.get('wave_height'),
        'marine_wave_direction': current.get('wave_direction'),
        'marine_wave_period': current.get('wave_period'),
        'marine_swell_wave_height': current.get('swell_wave_height'),
        'marine_swell_wave_direction': current.get('swell_wave_direction'),
        'marine_swell_wave_period': current.get('swell_wave_period')
    }

def combine_responses(weather_data: Optional[Dict], marine_data: Optional[Dict]) -> Optional[Dict[str, Any]]:
    result = {}
    weather_success = False
    marine_success = False
    
    if weather_data:
        try:
            weather_result = process_weather_response(weather_data)
            result.update(weather_result)
            weather_success = True
        except Exception as e:
            print(f"[{datetime.datetime.now()}] Weather processing failed: {e}")
    
    if marine_data:
        try:
            marine_result = process_marine_response(marine_data)
            result.update(marine_result)
            marine_success = True
        except Exception as e:
            print(f"[{datetime.datetime.now()}] Marine processing failed: {e}")
    
    if not weather_success and not marine_success:
        raise Exception("Both weather and marine API calls failed")
    elif not weather_success:
        print(f"[{datetime.datetime.now()}] OpenMeteo: Marine data only (weather failed)")
    elif not marine_success:
        print(f"[{datetime.datetime.now()}] OpenMeteo: Weather data only (marine failed)")
    
    return result if result else None
