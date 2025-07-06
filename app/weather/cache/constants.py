CACHE_DIR = "/tmp/weather_cache_optimized"
REQUEST_LOG_FILE = "/tmp/weather_requests.log"
EMERGENCY_CLEANUP_LOG = "/tmp/weather_emergency_cleanup.log"

WEATHER_CACHE_DURATION = 3600
DISTANCE_THRESHOLD_KM = 1.0
MAX_CACHE_SIZE_MB = 50
MAX_LOG_SIZE_MB = 5
EMERGENCY_DISK_THRESHOLD_MB = 500
CRITICAL_DISK_THRESHOLD_MB = 200
CLEANUP_INTERVAL_HOURS = 0.5
MAX_CACHE_FILES = 1000
LOG_ROTATION_SIZE_MB = 2

WEATHER_KEYS = {
    'weather_temp', 'weather_humidity', 'weather_apparent_temp',
    'precipitation', 'weather_code', 'pressure_msl', 'cloud_cover',
    'wind_speed_10m', 'wind_direction_10m', 'wind_gusts_10m',
    'weather_observation_time', 'marine_wave_height', 'marine_wave_direction',
    'marine_wave_period', 'marine_swell_wave_height', 'marine_swell_wave_direction',
    'marine_swell_wave_period'
}