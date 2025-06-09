import json,os,datetime,math
from typing import Dict,Tuple
DEVICE_POSITIONS_FILE="/tmp/device_positions.json"
MOVEMENT_THRESHOLD_KM=1.0
def calculate_distance_km(lat1:float,lon1:float,lat2:float,lon2:float)->float:
 R=6371.0
 lat1_rad,lon1_rad,lat2_rad,lon2_rad=math.radians(lat1),math.radians(lon1),math.radians(lat2),math.radians(lon2)
 dlat,dlon=lat2_rad-lat1_rad,lon2_rad-lon1_rad
 a=math.sin(dlat/2)**2+math.cos(lat1_rad)*math.cos(lat2_rad)*math.sin(dlon/2)**2
 c=2*math.atan2(math.sqrt(a),math.sqrt(1-a))
 return R*c
def load_device_positions()->Dict[str,Dict]:
 try:
  if os.path.exists(DEVICE_POSITIONS_FILE):
   with open(DEVICE_POSITIONS_FILE,'r')as f:return json.load(f)
 except:pass
 return{}
def save_device_positions(positions:Dict[str,Dict]):
 try:
  with open(DEVICE_POSITIONS_FILE,'w')as f:json.dump(positions,f,indent=2)
 except:pass
def should_force_weather_update(device_id:str,current_lat:float,current_lon:float)->Tuple[bool,str]:
 positions=load_device_positions()
 device_key=str(device_id)
 if device_key not in positions:
  positions[device_key]={'lat':current_lat,'lon':current_lon,'last_weather_update':datetime.datetime.now().isoformat(),'weather_update_count':1}
  save_device_positions(positions)
  return True,"first_request"
 last_position=positions[device_key]
 last_lat,last_lon,last_update=last_position.get('lat'),last_position.get('lon'),last_position.get('last_weather_update')
 if last_lat is None or last_lon is None:
  positions[device_key].update({'lat':current_lat,'lon':current_lon,'last_weather_update':datetime.datetime.now().isoformat(),'weather_update_count':positions[device_key].get('weather_update_count',0)+1})
  save_device_positions(positions)
  return True,"invalid_last_position"
 distance=calculate_distance_km(current_lat,current_lon,last_lat,last_lon)
 if distance>=MOVEMENT_THRESHOLD_KM:
  positions[device_key].update({'lat':current_lat,'lon':current_lon,'last_weather_update':datetime.datetime.now().isoformat(),'weather_update_count':positions[device_key].get('weather_update_count',0)+1})
  save_device_positions(positions)
  return True,f"moved_{distance:.2f}km"
 if last_update:
  try:
   last_update_time=datetime.datetime.fromisoformat(last_update)
   time_since_update=datetime.datetime.now()-last_update_time
   if time_since_update.total_seconds()>3600:
    positions[device_key].update({'lat':current_lat,'lon':current_lon,'last_weather_update':datetime.datetime.now().isoformat(),'weather_update_count':positions[device_key].get('weather_update_count',0)+1})
    save_device_positions(positions)
    return True,f"expired_{int(time_since_update.total_seconds())}s"
  except:pass
 positions[device_key].update({'current_lat':current_lat,'current_lon':current_lon,'last_seen':datetime.datetime.now().isoformat()})
 save_device_positions(positions)
 return False,f"cached_distance_{distance:.2f}km"
def get_device_stats()->Dict[str,any]:
 positions=load_device_positions()
 stats={'total_devices':len(positions),'devices':{}}
 for device_id,position in positions.items():
  stats['devices'][device_id]={'last_lat':position.get('lat'),'last_lon':position.get('lon'),'current_lat':position.get('current_lat'),'current_lon':position.get('current_lon'),'last_weather_update':position.get('last_weather_update'),'weather_update_count':position.get('weather_update_count',0),'last_seen':position.get('last_seen')}
 return stats
def cleanup_old_device_data(days_threshold:int=7):
 try:
  positions=load_device_positions()
  cutoff_time=datetime.datetime.now()-datetime.timedelta(days=days_threshold)
  devices_to_remove=[]
  for device_id,position in positions.items():
   last_seen=position.get('last_seen')or position.get('last_weather_update')
   if last_seen:
    try:
     if datetime.datetime.fromisoformat(last_seen)<cutoff_time:devices_to_remove.append(device_id)
    except:devices_to_remove.append(device_id)
   else:devices_to_remove.append(device_id)
  for device_id in devices_to_remove:del positions[device_id]
  if devices_to_remove:save_device_positions(positions)
 except:pass
