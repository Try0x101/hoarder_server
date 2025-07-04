import asyncio
import datetime
import pytz
import time
from typing import Dict, Optional
from collections import defaultdict
from timezonefinder import TimezoneFinder

MAX_COORDINATE_GROUPS = 50
COORDINATE_PRECISION = 3

class SharedTimezoneManager:
    def __init__(self):
        self.coordinate_groups = {}
        self.group_subscribers = defaultdict(set)
        self.timezone_results = {}
        self.tf = TimezoneFinder()
        self.last_calculation = {}
        self.calculation_lock = asyncio.Lock()
        self.max_groups = MAX_COORDINATE_GROUPS
        
    def _get_coordinate_key(self, lat, lon):
        rounded_lat = round(lat, COORDINATE_PRECISION)
        rounded_lon = round(lon, COORDINATE_PRECISION)
        return f"{rounded_lat},{rounded_lon}"
    
    async def subscribe_connection(self, sid, device_id, lat, lon):
        if lat is None or lon is None:
            return None
            
        coord_key = self._get_coordinate_key(lat, lon)
        
        if len(self.coordinate_groups) >= self.max_groups:
            oldest_key = min(self.last_calculation.keys(), 
                           key=lambda k: self.last_calculation[k], 
                           default=None)
            if oldest_key:
                await self._cleanup_coordinate_group(oldest_key)
        
        self.coordinate_groups[coord_key] = (lat, lon)
        self.group_subscribers[coord_key].add((sid, device_id))
        
        if coord_key not in self.timezone_results:
            await self._calculate_timezone(coord_key, lat, lon)
            
        return coord_key
    
    async def unsubscribe_connection(self, sid, coord_key=None):
        if coord_key:
            self.group_subscribers[coord_key].discard((sid, None))
            if not self.group_subscribers[coord_key]:
                await self._cleanup_coordinate_group(coord_key)
        else:
            for key in list(self.group_subscribers.keys()):
                self.group_subscribers[key] = {
                    (s, d) for s, d in self.group_subscribers[key] if s != sid
                }
                if not self.group_subscribers[key]:
                    await self._cleanup_coordinate_group(key)
    
    async def _calculate_timezone(self, coord_key, lat, lon):
        async with self.calculation_lock:
            if coord_key in self.timezone_results:
                return
                
            try:
                def get_timezone_data():
                    tz_name = self.tf.timezone_at(lng=lon, lat=lat)
                    if tz_name:
                        tz = pytz.timezone(tz_name)
                        now_utc = datetime.datetime.now(datetime.timezone.utc)
                        now_local = now_utc.replace(tzinfo=pytz.utc).astimezone(tz)
                        
                        offset = now_local.utcoffset()
                        total_seconds = offset.total_seconds()
                        hours = int(total_seconds // 3600)
                        minutes = int((abs(total_seconds) % 3600) // 60)
                        sign = '+' if hours >= 0 else '-'
                        
                        timezone_str = f"UTC{sign}{abs(hours)}" if minutes == 0 else f"UTC{sign}{abs(hours)}:{abs(minutes):02d}"
                        
                        return {
                            'timezone_str': timezone_str,
                            'date_str': now_local.strftime("%d.%m.%Y"),
                            'time_str': now_local.strftime("%H:%M:%S"),
                            'tz_obj': tz
                        }
                    return None
                
                result = await asyncio.to_thread(get_timezone_data)
                if result:
                    self.timezone_results[coord_key] = result
                    self.last_calculation[coord_key] = time.time()
                    
            except Exception as e:
                print(f"[{datetime.datetime.now()}] Timezone calculation error for {coord_key}: {e}")
    
    async def _cleanup_coordinate_group(self, coord_key):
        self.coordinate_groups.pop(coord_key, None)
        self.group_subscribers.pop(coord_key, None)
        self.timezone_results.pop(coord_key, None)
        self.last_calculation.pop(coord_key, None)
    
    async def get_timezone_update(self, coord_key):
        if coord_key not in self.timezone_results:
            return None
            
        tz_data = self.timezone_results[coord_key]
        if not tz_data or 'tz_obj' not in tz_data:
            return tz_data
            
        try:
            def update_time():
                now_utc = datetime.datetime.now(datetime.timezone.utc)
                now_local = now_utc.replace(tzinfo=pytz.utc).astimezone(tz_data['tz_obj'])
                return {
                    'timezone_str': tz_data['timezone_str'],
                    'date_str': now_local.strftime("%d.%m.%Y"),
                    'time_str': now_local.strftime("%H:%M:%S")
                }
            
            return await asyncio.to_thread(update_time)
        except Exception as e:
            print(f"[{datetime.datetime.now()}] Timezone update error for {coord_key}: {e}")
            return tz_data
    
    async def broadcast_timezone_updates(self, sio):
        for coord_key, subscribers in self.group_subscribers.items():
            if not subscribers:
                continue
                
            tz_update = await self.get_timezone_update(coord_key)
            if not tz_update:
                continue
                
            for sid, device_id in list(subscribers):
                try:
                    await sio.emit("time_update", {
                        "device_id": device_id,
                        "location_date": tz_update.get('date_str', 'N/A'),
                        "location_time": tz_update.get('time_str', 'N/A'),
                        "location_timezone": tz_update.get('timezone_str', 'N/A'),
                        "shared_calculation": True
                    }, room=sid)
                except Exception as e:
                    await self.unsubscribe_connection(sid, coord_key)
    
    def get_stats(self):
        return {
            'coordinate_groups': len(self.coordinate_groups),
            'total_subscribers': sum(len(subs) for subs in self.group_subscribers.values()),
            'max_groups': self.max_groups,
            'memory_efficiency': f"{(1 - len(self.coordinate_groups) / max(1, sum(len(subs) for subs in self.group_subscribers.values()))) * 100:.1f}%"
        }
