import zlib,gzip,json,io,datetime,httpx,asyncio,hashlib,os,math,pytz
from typing import Optional,Dict,Any,Tuple
from timezonefinder import TimezoneFinder
WEATHER_CODE_DESCRIPTIONS={0:"Clear sky",1:"Mainly clear",2:"Partly cloudy",3:"Overcast",45:"Fog",48:"Depositing rime fog",51:"Light drizzle",53:"Moderate drizzle",55:"Dense drizzle",56:"Light freezing drizzle",57:"Dense freezing drizzle",61:"Slight rain",63:"Moderate rain",65:"Heavy rain",66:"Light freezing rain",67:"Heavy freezing rain",71:"Slight snow fall",73:"Moderate snow fall",75:"Heavy snow fall",77:"Snow grains",80:"Slight rain showers",81:"Moderate rain showers",82:"Violent rain showers",85:"Slight snow showers",86:"Heavy snow showers",95:"Thunderstorm",96:"Thunderstorm with slight hail",99:"Thunderstorm with heavy hail"}
tf=TimezoneFinder()
CACHE_DIR="/tmp/weather_cache_optimized"
WEATHER_CACHE_DURATION=3600
DISTANCE_THRESHOLD_KM=1.0
DAILY_API_LIMIT=9000
REQUEST_LOG_FILE="/tmp/weather_requests.log"
def calculate_distance_km(lat1:float,lon1:float,lat2:float,lon2:float)->float:
 R=6371.0
 lat1_rad,lon1_rad,lat2_rad,lon2_rad=math.radians(lat1),math.radians(lon1),math.radians(lat2),math.radians(lon2)
 dlat,dlon=lat2_rad-lat1_rad,lon2_rad-lon1_rad
 a=math.sin(dlat/2)**2+math.cos(lat1_rad)*math.cos(lat2_rad)*math.sin(dlon/2)**2
 c=2*math.atan2(math.sqrt(a),math.sqrt(1-a))
 return R*c
def round_coordinates(lat:float,lon:float,precision:int=3)->Tuple[float,float]:return round(lat,precision),round(lon,precision)
def log_api_request():
 try:
  with open(REQUEST_LOG_FILE,'a')as f:f.write(f"{datetime.datetime.now().isoformat()}\n")
 except:pass
def get_today_request_count()->int:
 try:
  if not os.path.exists(REQUEST_LOG_FILE):return 0
  today,count=datetime.date.today(),0
  with open(REQUEST_LOG_FILE,'r')as f:
   for line in f:
    try:
     if datetime.datetime.fromisoformat(line.strip()).date()==today:count+=1
    except:continue
  return count
 except:return 0
def cleanup_old_request_logs():
 try:
  if not os.path.exists(REQUEST_LOG_FILE):return
  cutoff_date=datetime.date.today()-datetime.timedelta(days=2)
  new_lines=[]
  with open(REQUEST_LOG_FILE,'r')as f:
   for line in f:
    try:
     if datetime.datetime.fromisoformat(line.strip()).date()>=cutoff_date:new_lines.append(line)
    except:continue
  with open(REQUEST_LOG_FILE,'w')as f:f.writelines(new_lines)
 except:pass
def can_make_api_request()->bool:
 cleanup_old_request_logs()
 today_count=get_today_request_count()
 return today_count<DAILY_API_LIMIT
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
    try:os.remove(cache_path);continue
    except:pass
   try:
    with open(cache_path,'r')as f:cached_data=json.load(f)
    cached_lat,cached_lon=cached_data.get('_cache_lat'),cached_data.get('_cache_lon')
    if cached_lat is None or cached_lon is None:continue
    distance=calculate_distance_km(lat,lon,cached_lat,cached_lon)
    if distance<=DISTANCE_THRESHOLD_KM:return{k:v for k,v in cached_data.items()if not k.startswith('_cache_')}
   except:continue
 except:pass
 return None
def save_weather_to_cache(lat:float,lon:float,data:Dict[str,Any]):
 try:
  ensure_cache_dir()
  cache_key=get_cache_key(lat,lon)
  cache_file=os.path.join(CACHE_DIR,f"{cache_key}.json")
  cache_data=data.copy()
  cache_data.update({'_cache_lat':lat,'_cache_lon':lon,'_cache_time':datetime.datetime.now().isoformat()})
  with open(cache_file,'w')as f:json.dump(cache_data,f)
 except:pass
async def get_weather_from_wttr(lat:float,lon:float)->Optional[Dict[str,Any]]:
 try:
  async with httpx.AsyncClient(timeout=10.0)as client:
   response=await client.get(f'https://wttr.in/{lat},{lon}?format=j1')
   if response.status_code==200:
    data=response.json()
    current=data.get('current_condition',[{}])[0]
    return{'weather_temp':float(current.get('temp_C',0))if current.get('temp_C')else None,'weather_humidity':int(current.get('humidity',0))if current.get('humidity')else None,'weather_apparent_temp':float(current.get('FeelsLikeC',0))if current.get('FeelsLikeC')else None,'precipitation':float(current.get('precipMM',0))if current.get('precipMM')else None,'pressure_msl':float(current.get('pressure',0))if current.get('pressure')else None,'cloud_cover':int(current.get('cloudcover',0))if current.get('cloudcover')else None,'wind_speed_10m':float(current.get('windspeedKmph',0))/3.6 if current.get('windspeedKmph')else None,'wind_direction_10m':int(current.get('winddirDegree',0))if current.get('winddirDegree')else None}
 except:pass
 return None
async def get_weather_data(lat:float,lon:float,force_update:bool=False)->Optional[Dict[str,Any]]:
 if not force_update:
  cached_data=find_nearby_cached_weather(lat,lon)
  if cached_data:return cached_data
 if not can_make_api_request():
  wttr_data=await get_weather_from_wttr(lat,lon)
  if wttr_data:save_weather_to_cache(lat,lon,wttr_data);return wttr_data
  return None
 result={}
 try:
  weather_params={'latitude':lat,'longitude':lon,'current':['temperature_2m','relative_humidity_2m','apparent_temperature','precipitation','weather_code','pressure_msl','cloud_cover','wind_speed_10m','wind_direction_10m','wind_gusts_10m'],'timezone':'auto'}
  marine_params={'latitude':lat,'longitude':lon,'current':['wave_height','wave_direction','wave_period','swell_wave_height','swell_wave_direction','swell_wave_period'],'timezone':'auto'}
  async with httpx.AsyncClient(timeout=10.0)as client:
   weather_task=client.get('https://api.open-meteo.com/v1/forecast',params=weather_params)
   marine_task=client.get('https://marine-api.open-meteo.com/v1/marine',params=marine_params)
   weather_response,marine_response=await asyncio.gather(weather_task,marine_task,return_exceptions=True)
   log_api_request()
   if not isinstance(marine_response,Exception):log_api_request()
   if not isinstance(weather_response,Exception)and weather_response.status_code==200:
    try:
     weather_data=weather_response.json()
     current=weather_data.get('current',{})
     weather_result={'weather_temp':current.get('temperature_2m'),'weather_humidity':current.get('relative_humidity_2m'),'weather_apparent_temp':current.get('apparent_temperature'),'precipitation':current.get('precipitation'),'weather_code':current.get('weather_code'),'pressure_msl':current.get('pressure_msl'),'cloud_cover':current.get('cloud_cover'),'wind_speed_10m':current.get('wind_speed_10m'),'wind_direction_10m':current.get('wind_direction_10m'),'wind_gusts_10m':current.get('wind_gusts_10m')}
     result.update(weather_result)
    except:pass
   if not isinstance(marine_response,Exception)and marine_response.status_code==200:
    try:
     marine_data=marine_response.json()
     current=marine_data.get('current',{})
     marine_result={'marine_wave_height':current.get('wave_height'),'marine_wave_direction':current.get('wave_direction'),'marine_wave_period':current.get('wave_period'),'marine_swell_wave_height':current.get('swell_wave_height'),'marine_swell_wave_direction':current.get('swell_wave_direction'),'marine_swell_wave_period':current.get('swell_wave_period')}
     result.update(marine_result)
    except:pass
 except:pass
 if not result or all(v is None for k,v in result.items()if k.startswith('weather_')):
  wttr_data=await get_weather_from_wttr(lat,lon)
  if wttr_data:result.update(wttr_data)
 if result and any(v is not None for v in result.values()):save_weather_to_cache(lat,lon,result);return result
 return None
async def enrich_with_weather_data(data:dict)->dict:
 from app.device_tracker import should_force_weather_update,cleanup_old_device_data
 lat,lon=data.get('lat'),data.get('lon')
 device_id=data.get('id')or data.get('device_id')
 if lat is None or lon is None or not device_id:return data
 try:
  lat_float,lon_float=float(lat),float(lon)
  force_update,reason=should_force_weather_update(device_id,lat_float,lon_float)
  if hash(str(device_id))%100==0:cleanup_old_device_data()
  weather_data=await get_weather_data(lat_float,lon_float,force_update)
  if weather_data:data.update(weather_data)
 except:pass
 return data
def get_location_time_info(lat,lon):
 try:
  timezone_str=tf.timezone_at(lat=lat,lng=lon)
  if not timezone_str:return None,None,None
  tz=pytz.timezone(timezone_str)
  current_time=datetime.datetime.now(tz)
  location_date,location_time=current_time.strftime("%d.%m.%Y"),current_time.strftime("%H:%M:%S")
  utc_offset=current_time.utcoffset()
  total_seconds=int(utc_offset.total_seconds())
  hours,minutes=total_seconds//3600,abs(total_seconds%3600)//60
  location_timezone=f"UTC{'+' if hours>=0 else ''}{hours}"if minutes==0 else f"UTC{'+' if hours>=0 else ''}{hours}:{minutes:02d}"
  return location_date,location_time,location_timezone
 except:return None,None,None
async def decode_raw_data(raw:bytes)->dict:
 try:return json.loads(zlib.decompress(raw,wbits=-15))
 except:pass
 try:
  with gzip.GzipFile(fileobj=io.BytesIO(raw))as f:return json.loads(f.read())
 except:pass
 try:return json.loads(raw)
 except Exception as e:return{"error":"Failed to decode","raw":raw.hex(),"exception":str(e)}
def deep_merge(source,destination):
 for key,value in source.items():
  if isinstance(value,dict)and key in destination and isinstance(destination[key],dict):destination[key]=deep_merge(value,destination[key])
  elif value is not None:destination[key]=value
 return destination
def safe_int(value):
 if value is None:return None
 try:return int(float(value))
 except:return None
def safe_float(value):
 if value is None:return None
 try:return float(value)
 except:return None
def transform_device_data(received_data):
 timestamp_unix=received_data.get('timestamp')
 transformed_timestamp=None
 if timestamp_unix is not None:
  try:transformed_timestamp=datetime.datetime.fromtimestamp(timestamp_unix).isoformat()
  except:transformed_timestamp=None
 location_date=location_time=location_timezone=None
 lat,lon=received_data.get('lat'),received_data.get('lon')
 if lat is not None and lon is not None:
  try:
   lat_float,lon_float=safe_float(lat),safe_float(lon)
   if lat_float is not None and lon_float is not None:location_date,location_time,location_timezone=get_location_time_info(lat_float,lon_float)
  except:pass
 return{'device_id':received_data.get('id'),'device_name':received_data.get('n'),'battery_percent':f"{safe_int(received_data.get('perc'))}%"if safe_int(received_data.get('perc'))is not None else None,'battery_status':received_data.get('stat'),'battery_total_capacity':f"{safe_int(received_data.get('cap'))} mAh"if safe_int(received_data.get('cap'))is not None else None,'battery_leftover_calculated':f"{safe_int(safe_int(received_data.get('cap',0))*safe_int(received_data.get('perc',0))/100)} mAh"if safe_int(received_data.get('cap'))is not None and safe_int(received_data.get('perc'))is not None else None,'download_bandwidth':f"{received_data.get('dn')} Mbps"if'dn'in received_data and received_data.get('dn')is not None else None,'upload_bandwidth':f"{received_data.get('up')} Mbps"if'up'in received_data and received_data.get('up')is not None else None,'cell_id':safe_int(received_data.get('ci')),'cell_mcc':safe_int(received_data.get('mcc')),'cell_mnc':safe_int(received_data.get('mnc')),'cell_tac':received_data.get('tac'),'cell_operator':received_data.get('op'),'network_type':received_data.get('nt'),'network_active':'Wi-Fi'if received_data.get('ssid')and received_data.get('ssid')not in['0','','error']else received_data.get('nt'),'cell_signal_strength':f"{received_data.get('rssi')} dBm"if'rssi'in received_data and received_data.get('rssi')is not None else None,'gps_accuracy':f"{received_data.get('acc')} m"if'acc'in received_data and received_data.get('acc')is not None else None,'gps_altitude':f"{safe_int(received_data.get('alt'))} m"if safe_int(received_data.get('alt'))is not None else None,'gps_bearing':f"{safe_int(received_data.get('bear'))} deg"if safe_int(received_data.get('bear'))is not None else None,'gps_speed':f"{safe_int(received_data.get('spd'))} km/h"if safe_int(received_data.get('spd'))is not None else None,'wifi_name':received_data.get('ssid'),'gps_latitude':received_data.get('lat'),'gps_longitude':received_data.get('lon'),'timestamp':transformed_timestamp,'weather_temperature':f"{received_data.get('weather_temp')}C"if'weather_temp'in received_data and received_data.get('weather_temp')is not None else None,'weather_description':WEATHER_CODE_DESCRIPTIONS.get(received_data.get('weather_code'),"Unknown"),'weather_humidity':f"{received_data.get('weather_humidity')}%"if'weather_humidity'in received_data and received_data.get('weather_humidity')is not None else None,'weather_apparent_temp':f"{received_data.get('weather_apparent_temp')}C"if'weather_apparent_temp'in received_data and received_data.get('weather_apparent_temp')is not None else None,'weather_precipitation':f"{received_data.get('precipitation')} mm"if'precipitation'in received_data and received_data.get('precipitation')is not None else None,'weather_pressure_msl':f"{received_data.get('pressure_msl')} hPa"if'pressure_msl'in received_data and received_data.get('pressure_msl')is not None else None,'weather_cloud_cover':f"{received_data.get('cloud_cover')}%"if'cloud_cover'in received_data and received_data.get('cloud_cover')is not None else None,'weather_wind_speed':f"{received_data.get('wind_speed_10m')} m/s"if'wind_speed_10m'in received_data and received_data.get('wind_speed_10m')is not None else None,'weather_wind_direction':f"{received_data.get('wind_direction_10m')} deg"if'wind_direction_10m'in received_data and received_data.get('wind_direction_10m')is not None else None,'weather_wind_gusts':f"{received_data.get('wind_gusts_10m')} m/s"if'wind_gusts_10m'in received_data and received_data.get('wind_gusts_10m')is not None else None,'marine_wave_height':f"{received_data.get('marine_wave_height')} m"if'marine_wave_height'in received_data and received_data.get('marine_wave_height')is not None else None,'marine_wave_direction':f"{received_data.get('marine_wave_direction')} deg"if'marine_wave_direction'in received_data and received_data.get('marine_wave_direction')is not None else None,'marine_wave_period':f"{received_data.get('marine_wave_period')} s"if'marine_wave_period'in received_data and received_data.get('marine_wave_period')is not None else None,'marine_swell_wave_height':f"{received_data.get('marine_swell_wave_height')} m"if'marine_swell_wave_height'in received_data and received_data.get('marine_swell_wave_height')is not None else None,'marine_swell_wave_direction':f"{received_data.get('marine_swell_wave_direction')} deg"if'marine_swell_wave_direction'in received_data and received_data.get('marine_swell_wave_direction')is not None else None,'marine_swell_wave_period':f"{received_data.get('marine_swell_wave_period')} s"if'marine_swell_wave_period'in received_data and received_data.get('marine_swell_wave_period')is not None else None,'gps_date_time':{'location_time':location_time,'location_timezone':location_timezone,'location_date':location_date},'source_ip':received_data.get('source_ip')}
