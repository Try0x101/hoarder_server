import json,os,datetime,math
from typing import Optional,Dict,Tuple

DEVICE_POSITIONS_FILE="/tmp/device_positions.json"
MOVEMENT_THRESHOLD_KM=1.0

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

def load_device_positions()->Dict[str,Dict]:
    try:
        if os.path.exists(DEVICE_POSITIONS_FILE):
            with open(DEVICE_POSITIONS_FILE,'r') as f:
                return json.load(f)
    except Exception as e:
        print(f"[{datetime.datetime.now()}] DEBUG: Error loading device positions: {e}")
    return {}

def save_device_positions(positions:Dict[str,Dict]):
    try:
        with open(DEVICE_POSITIONS_FILE,'w') as f:
            json.dump(positions,f,indent=2)
    except Exception as e:
        print(f"[{datetime.datetime.now()}] DEBUG: Error saving device positions: {e}")

def update_device_timestamp(device_id: str, key: str):
    positions = load_device_positions()
    device_key = str(device_id)
    if device_key not in positions:
        positions[device_key] = {}
    positions[device_key][key] = datetime.datetime.now(datetime.timezone.utc).isoformat()
    save_device_positions(positions)

def should_force_weather_update(device_id:str,current_lat:float,current_lon:float)->Tuple[bool,str]:
    positions=load_device_positions()
    device_key=str(device_id)
    now_utc=datetime.datetime.now(datetime.timezone.utc)
    if device_key not in positions:
        print(f"[{datetime.datetime.now()}] DEBUG: Device {device_id} - first weather request")
        positions[device_key]={'lat':current_lat,'lon':current_lon,'last_weather_update':now_utc.isoformat(),'weather_update_count':1, 'last_ais_update':None}
        save_device_positions(positions)
        return True,"first_request"
    
    last_position=positions[device_key]
    last_lat=last_position.get('lat')
    last_lon=last_position.get('lon')
    last_update_iso=last_position.get('last_weather_update')

    if last_lat is None or last_lon is None:
        positions[device_key].update({'lat':current_lat,'lon':current_lon,'last_weather_update':now_utc.isoformat(),'weather_update_count':positions[device_key].get('weather_update_count',0)+1})
        save_device_positions(positions)
        return True,"invalid_last_position"

    distance=calculate_distance_km(current_lat,current_lon,last_lat,last_lon)
    if distance>=MOVEMENT_THRESHOLD_KM:
        positions[device_key].update({'lat':current_lat,'lon':current_lon,'last_weather_update':now_utc.isoformat(),'weather_update_count':positions[device_key].get('weather_update_count',0)+1})
        save_device_positions(positions)
        return True,f"moved_{distance:.2f}km"

    if last_update_iso:
        try:
            last_update_time=datetime.datetime.fromisoformat(last_update_iso)
            time_since_update=now_utc-last_update_time
            if time_since_update.total_seconds()>3600:
                positions[device_key].update({'lat':current_lat,'lon':current_lon,'last_weather_update':now_utc.isoformat(),'weather_update_count':positions[device_key].get('weather_update_count',0)+1})
                save_device_positions(positions)
                return True,f"expired_{int(time_since_update.total_seconds())}s"
        except Exception as e:
            print(f"[{datetime.datetime.now()}] DEBUG: Error parsing last update time: {e}")
    
    positions[device_key].update({'current_lat':current_lat,'current_lon':current_lon,'last_seen':now_utc.isoformat()})
    save_device_positions(positions)
    return False,f"cached_distance_{distance:.2f}km"

def should_force_ais_update(device_id:str)->Tuple[bool,str]:
    positions = load_device_positions()
    device_key = str(device_id)
    if device_key not in positions:
        return True, "first_request_new_device"

    last_update_iso = positions[device_key].get('last_ais_update')
    if not last_update_iso:
        return True, "first_ais_request"

    try:
        last_update_time = datetime.datetime.fromisoformat(last_update_iso)
        time_since_update = (datetime.datetime.now(datetime.timezone.utc) - last_update_time).total_seconds()
        print(f"[{datetime.datetime.now()}] DEBUG: Time since last AIS update for {device_id}: {time_since_update:.2f}s")
        if time_since_update > 120:
            return True, f"expired_{int(time_since_update)}s"
    except (ValueError, TypeError):
        return True, "invalid_timestamp"
    
    return False, "rate_limited"

def cleanup_old_device_data(days_threshold:int=7):
    try:
        positions=load_device_positions()
        cutoff_time=datetime.datetime.now(datetime.timezone.utc)-datetime.timedelta(days=days_threshold)
        devices_to_remove=[]
        for device_id,position in positions.items():
            last_seen=position.get('last_seen') or position.get('last_weather_update')
            if last_seen:
                try:
                    last_seen_time=datetime.datetime.fromisoformat(last_seen)
                    if last_seen_time.tzinfo is None:
                        last_seen_time=last_seen_time.replace(tzinfo=datetime.timezone.utc)
                    if last_seen_time<cutoff_time:
                        devices_to_remove.append(device_id)
                except:
                    devices_to_remove.append(device_id)
            else:
                devices_to_remove.append(device_id)
        
        for device_id in devices_to_remove:
            del positions[device_id]
            print(f"[{datetime.datetime.now()}] DEBUG: Removed old device data for {device_id}")
        
        if devices_to_remove:
            save_device_positions(positions)
            print(f"[{datetime.datetime.now()}] DEBUG: Cleaned up {len(devices_to_remove)} old devices")
    except Exception as e:
        print(f"[{datetime.datetime.now()}] DEBUG: Error during cleanup: {e}")
