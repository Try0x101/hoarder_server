import zlib,gzip,json,io,datetime,httpx,asyncio
from typing import Optional,Dict,Any,Tuple,List
import hashlib,os,math
from timezonefinder import TimezoneFinder
import pytz

AIS_STREAM_API_KEY = "69411d27bff7498f6ed24487136d918aae504c2d"
AIS_STREAM_API_URL = "https://aisstream.io/v1/live"
WIGLE_API_HEADER = {"Authorization": "Basic QUlEMmQ3ZGIzOWZkMDNkY2UxYTllMGNjYjJmYWFiMTE1Njk6ZGYzNDZjYjMyNTBhY2ZjZDg3MDA4YTE0MWYxNDZiOTg="}
WIGLE_WIFI_API_URL = "https://api.wigle.net/api/v2/network/detail"
WIGLE_CELL_API_URL = "https://api.wigle.net/api/v2/cell/search"
WIGLE_REQUEST_LOG_FILE = "/tmp/wigle_requests.log"
MIN_WIGLE_REQUEST_INTERVAL = 60

WEATHER_CODE_DESCRIPTIONS={0:"Clear sky",1:"Mainly clear",2:"Partly cloudy",3:"Overcast",45:"Fog",48:"Depositing rime fog",51:"Light drizzle",53:"Moderate drizzle",55:"Dense drizzle",56:"Light freezing drizzle",57:"Dense freezing drizzle",61:"Slight rain",63:"Moderate rain",65:"Heavy rain",66:"Light freezing rain",67:"Heavy freezing rain",71:"Slight snow fall",73:"Moderate snow fall",75:"Heavy snow fall",77:"Snow grains",80:"Slight rain showers",81:"Moderate rain showers",82:"Violent rain showers",85:"Slight snow showers",86:"Heavy snow showers",95:"Thunderstorm",96:"Thunderstorm with slight hail",99:"Thunderstorm with heavy hail"}
tf=TimezoneFinder()
CACHE_DIR="/tmp/weather_cache_optimized"
WEATHER_CACHE_DURATION=3900
DISTANCE_THRESHOLD_KM=1.0
MIN_REQUEST_INTERVAL=300
DAILY_API_LIMIT=9000
REQUEST_LOG_FILE="/tmp/weather_requests.log"

def format_database_size(size_bytes: int) -> str:
    if size_bytes is None:
        return "N/A"
    if size_bytes == 0:
        return "0 B"
    size_name = ("B", "KB", "MB", "GB", "TB", "PB", "EB", "ZB", "YB")
    i = int(math.floor(math.log(size_bytes, 1024)))
    p = math.pow(1024, i)
    s = round(size_bytes / p, 2)
    return f"{s} {size_name[i]}"

def calculate_distance_km(lat1:float,lon1:float,lat2:float,lon2:float)->float:
 R=6371.0
 lat1_rad=math.radians(lat1)
 lon1_rad=math.radians(lon1)
 lat2_rad=math.radians(lat2)
 lon2_rad=math.radians(lon2)
 dlat=lat2_rad-lat1_rad
 dlon=lon2_rad-lon1_rad
 a=math.sin(dlat/2)**2+math.cos(lat1_rad)*math.cos(lat2_rad)*math.sin(dlon/2)**2
 c=2*math.atan2(math.sqrt(a),math.sqrt(1-a))
 return R*c

def round_coordinates(lat:float,lon:float,precision:int=3)->Tuple[float,float]:
 return round(lat,precision),round(lon,precision)

def log_api_request():
 try:
  timestamp=datetime.datetime.now().isoformat()
  with open(REQUEST_LOG_FILE,'a') as f:f.write(f"{timestamp}\n")
 except Exception as e:print(f"[{datetime.datetime.now()}] DEBUG: Failed to log API request: {e}")

def get_today_request_count()->int:
 try:
  if not os.path.exists(REQUEST_LOG_FILE):return 0
  today=datetime.date.today()
  count=0
  with open(REQUEST_LOG_FILE,'r') as f:
   for line in f:
    try:
     request_time=datetime.datetime.fromisoformat(line.strip())
     if request_time.date()==today:count+=1
    except:continue
  return count
 except Exception as e:
  print(f"[{datetime.datetime.now()}] DEBUG: Failed to count requests: {e}")
  return 0

def get_last_request_time()->Optional[datetime.datetime]:
 try:
  if not os.path.exists(REQUEST_LOG_FILE):return None
  with open(REQUEST_LOG_FILE,'r') as f:
   lines=f.readlines()
   if not lines:return None
   last_line=lines[-1].strip()
   return datetime.datetime.fromisoformat(last_line)
 except Exception as e:
  print(f"[{datetime.datetime.now()}] DEBUG: Failed to get last request time: {e}")
  return None

def cleanup_old_request_logs():
 try:
  if not os.path.exists(REQUEST_LOG_FILE):return
  cutoff_date=datetime.date.today()-datetime.timedelta(days=2)
  new_lines=[]
  with open(REQUEST_LOG_FILE,'r') as f:
   for line in f:
    try:
     request_time=datetime.datetime.fromisoformat(line.strip())
     if request_time.date()>=cutoff_date:new_lines.append(line)
    except:continue
  with open(REQUEST_LOG_FILE,'w') as f:f.writelines(new_lines)
 except Exception as e:print(f"[{datetime.datetime.now()}] DEBUG: Failed to cleanup request logs: {e}")

def can_make_api_request()->bool:
 cleanup_old_request_logs()
 today_count=get_today_request_count()
 if today_count>=DAILY_API_LIMIT:
  print(f"[{datetime.datetime.now()}] WARNING: Daily API limit reached ({today_count}/{DAILY_API_LIMIT})")
  return False
 last_request=get_last_request_time()
 if last_request:
  time_since_last=(datetime.datetime.now()-last_request).total_seconds()
  if time_since_last<MIN_REQUEST_INTERVAL:
   print(f"[{datetime.datetime.now()}] DEBUG: Rate limit - last request {int(time_since_last)}s ago (min {MIN_REQUEST_INTERVAL}s)")
   return False
 print(f"[{datetime.datetime.now()}] DEBUG: API requests today: {today_count}/{DAILY_API_LIMIT}")
 return True

def ensure_cache_dir():
 if not os.path.exists(CACHE_DIR):os.makedirs(CACHE_DIR)

def get_cache_key(lat:float,lon:float)->str:
 rounded_lat,rounded_lon=round_coordinates(lat,lon)
 return hashlib.md5(f"{rounded_lat}_{rounded_lon}".encode()).hexdigest()

def find_nearby_cached_weather(lat:float,lon:float)->Optional[Dict[str,Any]]:
 try:
  ensure_cache_dir()
  for cache_file in os.listdir(CACHE_DIR):
   if not cache_file.endswith('.json'):continue
   cache_path=os.path.join(CACHE_DIR,cache_file)
   file_age=datetime.datetime.now().timestamp()-os.path.getmtime(cache_path)
   if file_age>WEATHER_CACHE_DURATION:
    try:os.remove(cache_path)
    except:pass
    continue
   try:
    with open(cache_path,'r') as f:cached_data=json.load(f)
    cached_lat=cached_data.get('_cache_lat')
    cached_lon=cached_data.get('_cache_lon')
    if cached_lat is None or cached_lon is None:continue
    distance=calculate_distance_km(lat,lon,cached_lat,cached_lon)
    if distance<=DISTANCE_THRESHOLD_KM:
     print(f"[{datetime.datetime.now()}] DEBUG: Using nearby cached weather (distance: {distance:.2f}km, age: {int(file_age)}s)")
     return {k:v for k,v in cached_data.items() if not k.startswith('_cache_')}
   except Exception as e:
    print(f"[{datetime.datetime.now()}] DEBUG: Error reading cache file {cache_file}: {e}")
    continue
 except Exception as e:print(f"[{datetime.datetime.now()}] DEBUG: Error in find_nearby_cached_weather: {e}")
 return None

def save_weather_to_cache(lat:float,lon:float,data:Dict[str,Any]):
 try:
  ensure_cache_dir()
  cache_key=get_cache_key(lat,lon)
  cache_file=os.path.join(CACHE_DIR,f"{cache_key}.json")
  cache_data=data.copy()
  cache_data['_cache_lat']=lat
  cache_data['_cache_lon']=lon
  cache_data['_cache_time']=datetime.datetime.now(datetime.timezone.utc).isoformat()
  with open(cache_file,'w') as f:json.dump(cache_data,f)
  print(f"[{datetime.datetime.now()}] DEBUG: Weather data cached for coordinates {lat}, {lon}")
 except Exception as e:print(f"[{datetime.datetime.now()}] DEBUG: Cache write error: {e}")

async def get_weather_from_wttr(lat:float,lon:float)->Optional[Dict[str,Any]]:
 try:
  print(f"[{datetime.datetime.now()}] DEBUG: Trying wttr.in API for {lat}, {lon}")
  async with httpx.AsyncClient(timeout=10.0) as client:
   response=await client.get(f'https://wttr.in/{lat},{lon}?format=j1')
   if response.status_code==200:
    data=response.json()
    current=data.get('current_condition',[{}])[0]
    result={'weather_temp':float(current.get('temp_C',0)) if current.get('temp_C') else None,'weather_humidity':int(current.get('humidity',0)) if current.get('humidity') else None,'weather_apparent_temp':float(current.get('FeelsLikeC',0)) if current.get('FeelsLikeC') else None,'precipitation':float(current.get('precipMM',0)) if current.get('precipMM') else None,'pressure_msl':float(current.get('pressure',0)) if current.get('pressure') else None,'cloud_cover':int(current.get('cloudcover',0)) if current.get('cloudcover') else None,'wind_speed_10m':float(current.get('windspeedKmph',0))/3.6 if current.get('windspeedKmph') else None,'wind_direction_10m':int(current.get('winddirDegree',0)) if current.get('winddirDegree') else None,'weather_observation_time':current.get('observation_time')}
    print(f"[{datetime.datetime.now()}] SUCCESS: wttr.in data parsed")
    return result
   else:print(f"[{datetime.datetime.now()}] ERROR: wttr.in returned {response.status_code}")
 except Exception as e:print(f"[{datetime.datetime.now()}] ERROR: wttr.in API error: {e}")
 return None

async def get_weather_data(lat:float,lon:float,force_update:bool=False)->Optional[Dict[str,Any]]:
 print(f"[{datetime.datetime.now()}] DEBUG: Weather request for {lat:.4f}, {lon:.4f} (force: {force_update})")
 if not force_update:
  cached_data=find_nearby_cached_weather(lat,lon)
  if cached_data:return cached_data
 if not can_make_api_request():
  print(f"[{datetime.datetime.now()}] WARNING: API rate limited, trying cached data...")
  cached_data=find_nearby_cached_weather(lat,lon)
  if cached_data:
   print(f"[{datetime.datetime.now()}] DEBUG: Using cached data due to rate limit")
   return cached_data
  wttr_data=await get_weather_from_wttr(lat,lon)
  if wttr_data:
   save_weather_to_cache(lat,lon,wttr_data)
   return wttr_data
  return None
 result={}
 try:
  weather_params={'latitude':lat,'longitude':lon,'current':['temperature_2m','relative_humidity_2m','apparent_temperature','precipitation','weather_code','pressure_msl','cloud_cover','wind_speed_10m','wind_direction_10m','wind_gusts_10m'],'timezone':'auto'}
  marine_params={'latitude':lat,'longitude':lon,'current':['wave_height','wave_direction','wave_period','swell_wave_height','swell_wave_direction','swell_wave_period'],'timezone':'auto'}
  print(f"[{datetime.datetime.now()}] DEBUG: Making API requests to Open-Meteo...")
  async with httpx.AsyncClient(timeout=10.0) as client:
   weather_task=client.get('https://api.open-meteo.com/v1/forecast',params=weather_params)
   marine_task=client.get('https://marine-api.open-meteo.com/v1/marine',params=marine_params)
   weather_response,marine_response=await asyncio.gather(weather_task,marine_task,return_exceptions=True)
   if isinstance(weather_response,Exception):print(f"[{datetime.datetime.now()}] ERROR: Weather API exception: {weather_response}")
   else:
    print(f"[{datetime.datetime.now()}] DEBUG: Weather API status: {weather_response.status_code}")
    if weather_response.status_code==200:
     log_api_request()
     try:
      weather_data=weather_response.json()
      current=weather_data.get('current',{})
      weather_result={'weather_temp':current.get('temperature_2m'),'weather_humidity':current.get('relative_humidity_2m'),'weather_apparent_temp':current.get('apparent_temperature'),'precipitation':current.get('precipitation'),'weather_code':current.get('weather_code'),'pressure_msl':current.get('pressure_msl'),'cloud_cover':current.get('cloud_cover'),'wind_speed_10m':current.get('wind_speed_10m'),'wind_direction_10m':current.get('wind_direction_10m'),'wind_gusts_10m':current.get('wind_gusts_10m'),'weather_observation_time':current.get('time')}
      result.update(weather_result)
      print(f"[{datetime.datetime.now()}] SUCCESS: Open-Meteo weather data received")
     except Exception as json_error:print(f"[{datetime.datetime.now()}] ERROR: Failed to parse weather JSON: {json_error}")
    elif weather_response.status_code==429:print(f"[{datetime.datetime.now()}] WARNING: Open-Meteo rate limited")
    else:print(f"[{datetime.datetime.now()}] ERROR: Weather API status {weather_response.status_code}. Response: {weather_response.text}")
   if isinstance(marine_response,Exception):print(f"[{datetime.datetime.now()}] ERROR: Marine API exception: {marine_response}")
   else:
    print(f"[{datetime.datetime.now()}] DEBUG: Marine API status: {marine_response.status_code}")
    if marine_response.status_code==200:
     log_api_request()
     try:
      marine_data=marine_response.json()
      current=marine_data.get('current',{})
      marine_result={'marine_wave_height':current.get('wave_height'),'marine_wave_direction':current.get('wave_direction'),'marine_wave_period':current.get('wave_period'),'marine_swell_wave_height':current.get('swell_wave_height'),'marine_swell_wave_direction':current.get('swell_wave_direction'),'marine_swell_wave_period':current.get('swell_wave_period')}
      result.update(marine_result)
      print(f"[{datetime.datetime.now()}] SUCCESS: Marine data received")
     except Exception as json_error:print(f"[{datetime.datetime.now()}] ERROR: Failed to parse marine JSON: {json_error}")
    else:
     print(f"[{datetime.datetime.now()}] ERROR: Marine API returned status {marine_response.status_code}. Response: {marine_response.text}")
 except Exception as e:print(f"[{datetime.datetime.now()}] ERROR: Exception in Open-Meteo requests: {e}")
 if not result or all(v is None for k,v in result.items() if k.startswith('weather_')):
  print(f"[{datetime.datetime.now()}] DEBUG: Trying fallback wttr.in API...")
  wttr_data=await get_weather_from_wttr(lat,lon)
  if wttr_data:result.update(wttr_data)
 if result and any(v is not None for v in result.values()):
  save_weather_to_cache(lat,lon,result)
  print(f"[{datetime.datetime.now()}] SUCCESS: Weather data cached and returned")
  return result
 print(f"[{datetime.datetime.now()}] WARNING: No weather data obtained")
 return None

async def enrich_with_weather_data(data:dict)->dict:
 from app.device_tracker import should_force_weather_update,cleanup_old_device_data
 print(f"[{datetime.datetime.now()}] DEBUG: Starting optimized weather enrichment")
 lat=data.get('lat')
 lon=data.get('lon')
 device_id=data.get('id') or data.get('device_id')
 print(f"[{datetime.datetime.now()}] DEBUG: Device {device_id} - coordinates: lat={lat}, lon={lon}")
 if lat is None or lon is None:
  print(f"[{datetime.datetime.now()}] DEBUG: No coordinates found, skipping weather enrichment")
  return data
 if not device_id:
  print(f"[{datetime.datetime.now()}] DEBUG: No device_id found, skipping weather enrichment")
  return data
 try:
  lat_float=float(lat)
  lon_float=float(lon)
  force_update,reason=should_force_weather_update(device_id,lat_float,lon_float)
  print(f"[{datetime.datetime.now()}] DEBUG: Device {device_id} - force_update: {force_update} (reason: {reason})")
  if hash(str(device_id))%100==0:cleanup_old_device_data()
  print(f"[{datetime.datetime.now()}] DEBUG: Getting weather data...")
  weather_data=await get_weather_data(lat_float,lon_float,force_update)
  if weather_data:
   data.update(weather_data)
   weather_keys_added=[k for k in weather_data.keys() if weather_data[k] is not None]
   print(f"[{datetime.datetime.now()}] SUCCESS: Weather data added for device {device_id}")
   print(f"[{datetime.datetime.now()}] DEBUG: Added keys: {weather_keys_added}")
  else:print(f"[{datetime.datetime.now()}] WARNING: No weather data for device {device_id}")
 except (ValueError,TypeError) as e:print(f"[{datetime.datetime.now()}] ERROR: Invalid coordinates: lat={lat}, lon={lon}, error={e}")
 except Exception as e:
  print(f"[{datetime.datetime.now()}] ERROR: Unexpected error in weather enrichment: {e}")
  import traceback
  print(f"[{datetime.datetime.now()}] DEBUG: Traceback: {traceback.format_exc()}")
 return data

def get_location_time_info(lat,lon):
 try:
  timezone_str=tf.timezone_at(lat=lat,lng=lon)
  if not timezone_str:return None,None,None,None
  tz=pytz.timezone(timezone_str)
  current_time=datetime.datetime.now(tz)
  location_date=current_time.strftime("%d.%m.%Y")
  location_time=current_time.strftime("%H:%M:%S")
  utc_offset=current_time.utcoffset()
  total_seconds=int(utc_offset.total_seconds())
  hours=total_seconds//3600
  minutes=abs(total_seconds%3600)//60
  if minutes==0:location_timezone=f"UTC{'+' if hours>=0 else ''}{hours}"
  else:location_timezone=f"UTC{'+' if hours>=0 else ''}{hours}:{minutes:02d}"
  return location_date,location_time,location_timezone,tz
 except Exception as e:
  print(f"Error getting location time info: {e}")
  return None,None,None,None

async def decode_raw_data(raw:bytes)->dict:
 try:
  data=zlib.decompress(raw,wbits=-15)
  return json.loads(data)
 except zlib.error:pass
 except json.JSONDecodeError:pass
 try:
  with gzip.GzipFile(fileobj=io.BytesIO(raw)) as f:
   data=f.read()
   return json.loads(data)
 except OSError:pass
 except json.JSONDecodeError:pass
 try:return json.loads(raw)
 except Exception as e:return{"error":"Failed to decode","raw":raw.hex(),"exception":str(e)}

def deep_merge(source,destination):
 for key,value in source.items():
  if isinstance(value,dict) and key in destination and isinstance(destination[key],dict):destination[key]=deep_merge(value,destination[key])
  elif value is not None:destination[key]=value
 return destination

def safe_int(value):
 if value is None:return None
 try:return int(float(value))
 except (ValueError,TypeError):return None

def safe_float(value):
 if value is None:return None
 try:return float(value)
 except (ValueError,TypeError):return None

async def get_nearby_vessels(lat: float, lon: float) -> Optional[List[Dict[str, Any]]]:
    """Get nearby vessels using AISStream API - simplified implementation"""
    print(f"[{datetime.datetime.now()}] DEBUG: AISStream requires WebSocket - returning empty list")
    return [] # Return empty list as placeholder

def transform_device_data(received_data):
    location_date, location_time, location_timezone, location_tz = None, None, None, None
    lat, lon = received_data.get('lat'), received_data.get('lon')
    if lat is not None and lon is not None:
        try:
            lat_float, lon_float = safe_float(lat), safe_float(lon)
            if lat_float is not None and lon_float is not None:
                location_date, location_time, location_timezone, location_tz = get_location_time_info(lat_float, lon_float)
        except Exception as e:
            print(f"Error processing coordinates: lat={lat}, lon={lon}, error={e}")

    wind_direction_compass = ""
    if 'wind_direction_10m' in received_data and received_data.get('wind_direction_10m') is not None:
        direction = int(float(received_data.get('wind_direction_10m')))
        if 337.5 <= direction < 360 or 0 <= direction < 22.5:
            wind_direction_compass = "N"
        elif 22.5 <= direction < 67.5:
            wind_direction_compass = "NE"
        elif 67.5 <= direction < 112.5:
            wind_direction_compass = "E"
        elif 112.5 <= direction < 157.5:
            wind_direction_compass = "SE"
        elif 157.5 <= direction < 202.5:
            wind_direction_compass = "S"
        elif 202.5 <= direction < 247.5:
            wind_direction_compass = "SW"
        elif 247.5 <= direction < 292.5:
            wind_direction_compass = "W"
        elif 292.5 <= direction < 337.5:
            wind_direction_compass = "NW"

    weather_observation_formatted = None
    weather_observation_time = received_data.get('weather_observation_time')
    if weather_observation_time:
        try:
            obs_time = None
            if 'T' in str(weather_observation_time):
                obs_time = datetime.datetime.fromisoformat(weather_observation_time)
            elif ':' in str(weather_observation_time):
                obs_str = str(weather_observation_time).strip()
                try:
                    if 'AM' in obs_str.upper() or 'PM' in obs_str.upper():
                        obs_time = datetime.datetime.strptime(obs_str, '%I:%M %p')
                    else:
                        obs_time = datetime.datetime.strptime(obs_str, '%H:%M')
                    obs_time = obs_time.replace(year=datetime.datetime.now().year, month=datetime.datetime.now().month, day=datetime.datetime.now().day)
                except:
                    pass
            if obs_time:
                if location_tz:
                    if obs_time.tzinfo is None:
                        obs_time = obs_time.replace(tzinfo=pytz.utc)
                    obs_time_local = obs_time.astimezone(location_tz)
                    weather_observation_formatted = f"{obs_time_local.strftime('%d.%m.%Y %H:%M')} {location_timezone}"
                else:
                    if obs_time.tzinfo is None:
                        obs_time = obs_time.replace(tzinfo=pytz.utc)
                    weather_observation_formatted = f"{obs_time.strftime('%d.%m.%Y %H:%M')} UTC"
            else:
                weather_observation_formatted = f"{weather_observation_time} (local time)"
        except Exception as e:
            print(f"Error formatting weather observation time: {e}")
            weather_observation_formatted = f"{weather_observation_time} (format error)"

    weather_fetch_formatted = None
    device_id = received_data.get('id') or received_data.get('device_id')
    if device_id:
        try:
            from app.device_tracker import load_device_positions
            positions = load_device_positions()
            device_key = str(device_id)
            if device_key in positions and 'last_weather_update' in positions[device_key]:
                weather_last_fetched_iso = positions[device_key]['last_weather_update']
                weather_time_utc = None
                try:
                    weather_time_utc = datetime.datetime.fromisoformat(weather_last_fetched_iso)
                except ValueError:
                    try:
                        weather_time_utc = datetime.datetime.strptime(weather_last_fetched_iso, '%d.%m.%Y %H:%M:%S')
                    except Exception as strptime_err:
                        print(f"Could not parse weather fetch timestamp '{weather_last_fetched_iso}': {strptime_err}")
                        weather_fetch_formatted = weather_last_fetched_iso
                if weather_time_utc:
                    if weather_time_utc.tzinfo is None:
                        weather_time_utc = pytz.utc.localize(weather_time_utc)
                    if location_tz:
                        weather_time_local = weather_time_utc.astimezone(location_tz)
                        weather_fetch_formatted = f"{weather_time_local.strftime('%d.%m.%Y %H:%M:%S')} {location_timezone}"
                    else:
                        weather_fetch_formatted = f"{weather_time_utc.strftime('%d.%m.%Y %H:%M:%S')} UTC"
        except Exception as e:
            print(f"Error getting weather fetch time from device tracker: {e}")
    if not weather_fetch_formatted:
        weather_fetch_formatted = datetime.datetime.now(datetime.timezone.utc).strftime("%d.%m.%Y %H:%M:%S") + " UTC"

    network_active = 'Wi-Fi' if received_data.get('bssid') and str(received_data.get('bssid')) not in ['0', '', 'error'] else received_data.get('nt')
    rssi_val = received_data.get('rssi')
    cell_signal_strength_val = None
    if rssi_val is not None:
        rssi_int = safe_int(rssi_val)
        if rssi_int is not None and rssi_int not in [0, 1]:
            cell_signal_strength_val = f"{rssi_int} dBm"
    
    barometric_data = None
    if 'bar' in received_data and received_data.get('bar') is not None:
        bar_value = safe_float(received_data.get('bar'))
        if bar_value is not None:
            if bar_value < 0:
                barometric_data = f"{abs(bar_value)} hPa"
            else:
                barometric_data = f"{bar_value} m"

    result = {
        'device_name': received_data.get('n'),
        'battery_percent': f"{safe_int(received_data.get('perc'))}%" if received_data.get('perc') is not None else None,
        'battery_total_capacity': f"{safe_int(received_data.get('cap'))} mAh" if received_data.get('cap') is not None else None,
        'battery_leftover_calculated': f"{safe_int(safe_int(received_data.get('cap', 0)) * safe_int(received_data.get('perc', 0)) / 100)} mAh" if received_data.get('cap') is not None and received_data.get('perc') is not None else None,
        'cell_id': safe_int(received_data.get('ci')),
        'cell_mcc': safe_int(received_data.get('mcc')),
        'cell_mnc': safe_int(received_data.get('mnc')),
        'cell_tac': received_data.get('tac'),
        'cell_operator': received_data.get('op'),
        'network_type': received_data.get('nt'),
        'network_active': network_active,
        'cell_signal_strength': cell_signal_strength_val,
        'gps_accuracy': f"{safe_int(received_data.get('acc'))} m" if 'acc' in received_data and received_data.get('acc') is not None else None,
        'gps_barometer_kalman_filter_altitude': f"{safe_int(received_data.get('alt'))} m" if safe_int(received_data.get('alt')) is not None else None,
        'gps_speed': f"{safe_int(received_data.get('spd'))} km/h" if safe_int(received_data.get('spd')) is not None else None,
        'wifi_bssid': received_data.get('bssid'),
        'wifi_ssid': received_data.get('wifi_ssid'),
        'wifi_channel': received_data.get('wifi_channel'),
        'wifi_encryption': received_data.get('wifi_encryption'),
        'wifi_trilaterated_latitude': received_data.get('wifi_trilaterated_latitude'),
        'wifi_trilaterated_longitude': received_data.get('wifi_trilaterated_longitude'),
        'wifi_location': ', '.join(filter(None, [received_data.get('wifi_road'), received_data.get('wifi_city'), received_data.get('wifi_region'), received_data.get('wifi_country')])) or None,
        'cell_trilaterated_latitude': received_data.get('cell_trilaterated_latitude'),
        'cell_trilaterated_longitude': received_data.get('cell_trilaterated_longitude'),
        'cell_location': ', '.join(filter(None, [received_data.get('cell_road'), received_data.get('cell_city'), received_data.get('cell_region'), received_data.get('cell_country')])) or None,
        'gps_latitude': received_data.get('lat'),
        'gps_longitude': received_data.get('lon'),
        'network_download_capacity': f"{safe_int(received_data.get('dn'))} Mbps" if 'dn' in received_data and received_data.get('dn') is not None else None,
        'network_upload_capacity': f"{safe_int(received_data.get('up'))} Mbps" if 'up' in received_data and received_data.get('up') is not None else None,
        'barometric_data': barometric_data,
        'weather_temperature': f"{safe_int(received_data.get('weather_temp'))}°C" if 'weather_temp' in received_data and received_data.get('weather_temp') is not None else None,
        'weather_description': WEATHER_CODE_DESCRIPTIONS.get(received_data.get('weather_code'), "Unknown"),
        'weather_humidity': f"{safe_int(received_data.get('weather_humidity'))}%" if 'weather_humidity' in received_data and received_data.get('weather_humidity') is not None else None,
        'weather_apparent_temp': f"{safe_int(received_data.get('weather_apparent_temp'))}°C" if 'weather_apparent_temp' in received_data and received_data.get('weather_apparent_temp') is not None else None,
        'weather_precipitation': f"{safe_int(received_data.get('precipitation'))} mm" if 'precipitation' in received_data and received_data.get('precipitation') is not None else None,
        'weather_pressure_msl': f"{safe_int(received_data.get('pressure_msl'))} hPa" if 'pressure_msl' in received_data and received_data.get('pressure_msl') is not None else None,
        'weather_cloud_cover': f"{safe_int(received_data.get('cloud_cover'))}%" if 'cloud_cover' in received_data and received_data.get('cloud_cover') is not None else None,
        'weather_wind_speed': f"{safe_int(received_data.get('wind_speed_10m'))} m/s" if 'wind_speed_10m' in received_data and received_data.get('wind_speed_10m') is not None else None,
        'weather_wind_direction': wind_direction_compass if wind_direction_compass else None,
        'weather_wind_gusts': f"{safe_int(received_data.get('wind_gusts_10m'))} m/s" if 'wind_gusts_10m' in received_data and received_data.get('wind_gusts_10m') is not None else None,
        'weather_observation_time': weather_observation_formatted,
        'weather_last_fetch_request_time': weather_fetch_formatted,
        'marine_wave_height': f"{safe_int(received_data.get('marine_wave_height'))} m" if 'marine_wave_height' in received_data and received_data.get('marine_wave_height') is not None else None,
        'marine_wave_direction': f"{safe_int(received_data.get('marine_wave_direction'))}°" if 'marine_wave_direction' in received_data and received_data.get('marine_wave_direction') is not None else None,
        'marine_wave_period': f"{safe_int(received_data.get('marine_wave_period'))} s" if 'marine_wave_period' in received_data and received_data.get('marine_wave_period') is not None else None,
        'marine_swell_wave_height': f"{safe_int(received_data.get('marine_swell_wave_height'))} m" if 'marine_swell_wave_height' in received_data and received_data.get('marine_swell_wave_height') is not None else None,
        'marine_swell_wave_direction': f"{safe_int(received_data.get('marine_swell_wave_direction'))}°" if 'marine_swell_wave_direction' in received_data and received_data.get('marine_swell_wave_direction') is not None else None,
        'marine_swell_wave_period': f"{safe_int(received_data.get('marine_swell_wave_period'))} s" if 'marine_swell_wave_period' in received_data and received_data.get('marine_swell_wave_period') is not None else None,
        'gps_date_time': {'location_time': location_time, 'location_timezone': location_timezone, 'location_date': location_date},
        'source_ip': received_data.get('source_ip'),
        'nearby_vessels': received_data.get('nearby_vessels')
    }
    return result

def log_wigle_api_request():
    try:
        with open(WIGLE_REQUEST_LOG_FILE, 'a') as f:
            f.write(f"{datetime.datetime.now().isoformat()}\n")
    except Exception as e:
        print(f"[{datetime.datetime.now()}] DEBUG: Failed to log Wigle API request: {e}")

def can_make_wigle_api_request() -> bool:
    if not os.path.exists(WIGLE_REQUEST_LOG_FILE):
        return True
    try:
        with open(WIGLE_REQUEST_LOG_FILE, 'r') as f:
            lines = f.readlines()
            if not lines:
                return True
            last_request_time = datetime.datetime.fromisoformat(lines[-1].strip())
            if (datetime.datetime.now() - last_request_time).total_seconds() < MIN_WIGLE_REQUEST_INTERVAL:
                print(f"[{datetime.datetime.now()}] DEBUG: Wigle API rate limit active.")
                return False
    except Exception as e:
        print(f"[{datetime.datetime.now()}] DEBUG: Error checking Wigle rate limit: {e}")
        return True
    return True

async def get_wifi_details_from_wigle(bssid: str) -> Optional[Dict[str, Any]]:
    if not can_make_wigle_api_request():
        return None
    
    print(f"[{datetime.datetime.now()}] DEBUG: Querying Wigle API for BSSID {bssid}")
    bssid = bssid.replace(":", "")
    try:
        async with httpx.AsyncClient(timeout=15.0) as client:
            params = {'netid': bssid}
            response = await client.get(WIGLE_WIFI_API_URL, headers=WIGLE_API_HEADER, params=params)
            log_wigle_api_request()

            if response.status_code == 200:
                wigle_data = response.json()
                if wigle_data.get("success") and wigle_data.get("results"):
                    result = wigle_data["results"][0]
                    wifi_details = {
                        "wifi_ssid": result.get("ssid"),
                        "wifi_channel": result.get("channel"),
                        "wifi_encryption": result.get("encryption"),
                        "wifi_trilaterated_latitude": result.get("trilat"),
                        "wifi_trilaterated_longitude": result.get("trilong"),
                        "wifi_country": result.get("country"),
                        "wifi_region": result.get("region"),
                        "wifi_city": result.get("city"),
                        "wifi_road": result.get("road"),
                    }
                    print(f"[{datetime.datetime.now()}] SUCCESS: Wigle data received for {bssid}")
                    return wifi_details
                else:
                    print(f"[{datetime.datetime.now()}] WARNING: Wigle API returned success=false for {bssid}. Response: {wigle_data.get('message')}")
            else:
                print(f"[{datetime.datetime.now()}] ERROR: Wigle API returned status {response.status_code}. Response: {response.text}")
    except Exception as e:
        print(f"[{datetime.datetime.now()}] ERROR: Exception during Wigle API call: {e}")
    return None

async def enrich_with_wifi_data(data: dict) -> dict:
    bssid = data.get('bssid')
    if bssid and str(bssid) not in ['0', '', 'error']:
        wifi_details = await get_wifi_details_from_wigle(bssid)
        if wifi_details:
            data.update(wifi_details)
            print(f"[{datetime.datetime.now()}] SUCCESS: Wi-Fi data added for device {data.get('id') or data.get('device_id')}")
    return data

async def get_cell_details_from_wigle(mcc: int, mnc: int, lac: int, cid: int) -> Optional[Dict[str, Any]]:
    if not can_make_wigle_api_request():
        return None

    print(f"[{datetime.datetime.now()}] DEBUG: Querying Wigle API for cell tower {mcc}-{mnc}-{lac}-{cid}")
    try:
        async with httpx.AsyncClient(timeout=15.0) as client:
            params = {'cellId': cid, 'locationAreaCode': lac, 'mobileCountryCode': mcc, 'mobileNetworkCode': mnc}
            response = await client.get(WIGLE_CELL_API_URL, headers=WIGLE_API_HEADER, params=params)
            log_wigle_api_request()

            if response.status_code == 200:
                wigle_data = response.json()
                if wigle_data.get("success") and wigle_data.get("results"):
                    result = wigle_data["results"][0]
                    cell_details = {
                        "cell_trilaterated_latitude": result.get("trilat"),
                        "cell_trilaterated_longitude": result.get("trilong"),
                        "cell_country": result.get("country"),
                        "cell_region": result.get("region"),
                        "cell_city": result.get("city"),
                        "cell_road": result.get("road"),
                    }
                    print(f"[{datetime.datetime.now()}] SUCCESS: Wigle cell data received for {mcc}-{mnc}-{lac}-{cid}")
                    return cell_details
                else:
                    print(f"[{datetime.datetime.now()}] WARNING: Wigle cell API returned success=false. Response: {wigle_data.get('message')}")
            else:
                print(f"[{datetime.datetime.now()}] ERROR: Wigle cell API returned status {response.status_code}. Response: {response.text}")
    except Exception as e:
        print(f"[{datetime.datetime.now()}] ERROR: Exception during Wigle cell API call: {e}")
    return None

async def enrich_with_cell_data(data: dict) -> dict:
    cid = data.get('ci')
    mcc = data.get('mcc')
    mnc = data.get('mnc')
    lac = data.get('tac')

    if all([cid, mcc, mnc, lac]):
        cell_details = await get_cell_details_from_wigle(mcc=mcc, mnc=mnc, lac=lac, cid=cid)
        if cell_details:
            data.update(cell_details)
            print(f"[{datetime.datetime.now()}] SUCCESS: Cell data added for device {data.get('id') or data.get('device_id')}")
    return data

async def enrich_with_ais_data(data: dict) -> dict:
    from app.device_tracker import should_force_ais_update, update_device_timestamp
    
    lat = data.get('lat')
    lon = data.get('lon')
    device_id = data.get('id') or data.get('device_id')

    if not all([lat, lon, device_id]):
        return data

    force_update, reason = should_force_ais_update(device_id)
    print(f"[{datetime.datetime.now()}] DEBUG: Device {device_id} - check AIS update: {force_update} (reason: {reason})")

    if force_update:
        nearby_vessels = await get_nearby_vessels(lat, lon)
        if nearby_vessels is not None:
            data['nearby_vessels'] = nearby_vessels
            print(f"[{datetime.datetime.now()}] SUCCESS: AIS data fetched for device {device_id}, found {len(nearby_vessels)} vessels.")
            update_device_timestamp(device_id, 'last_ais_update')
        else:
            print(f"[{datetime.datetime.now()}] WARNING: AIS API call failed for device {device_id}. Will retry on next packet.")
    return data
