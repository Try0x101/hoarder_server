import httpx

WEATHER_PARAMS = [
    'temperature_2m', 'relative_humidity_2m', 'apparent_temperature',
    'precipitation', 'weather_code', 'pressure_msl', 'cloud_cover',
    'wind_speed_10m', 'wind_direction_10m', 'wind_gusts_10m'
]

MARINE_PARAMS = [
    'wave_height', 'wave_direction', 'wave_period',
    'swell_wave_height', 'swell_wave_direction', 'swell_wave_period'
]

WEATHER_API_URL = 'https://api.open-meteo.com/v1/forecast'
MARINE_API_URL = 'https://marine-api.open-meteo.com/v1/marine'

TIMEOUT_CONFIG = httpx.Timeout(connect=2.0, read=3.0, write=2.0, pool=5.0)
TOTAL_TIMEOUT = 4.0

def get_weather_params(lat: float, lon: float) -> dict:
    return {
        'latitude': lat,
        'longitude': lon,
        'current': WEATHER_PARAMS,
        'timezone': 'auto'
    }

def get_marine_params(lat: float, lon: float) -> dict:
    return {
        'latitude': lat,
        'longitude': lon,
        'current': MARINE_PARAMS,
        'timezone': 'auto'
    }

def get_client_info():
    return {
        'service': 'OpenMeteo',
        'endpoints': {
            'weather': WEATHER_API_URL,
            'marine': MARINE_API_URL
        },
        'timeout_config': {
            'connect': 2.0,
            'read': 3.0,
            'write': 2.0,
            'pool': 5.0,
            'total': TOTAL_TIMEOUT
        },
        'data_types': ['weather', 'marine']
    }
