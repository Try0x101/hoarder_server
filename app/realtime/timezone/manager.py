import asyncio
import datetime
from typing import Dict, Optional
from collections import defaultdict
from .calculator import TimezoneCalculator

class SharedTimezoneManager:
    def __init__(self):
        self.group_subscribers = defaultdict(set)
        self.calculator = TimezoneCalculator()
        
    async def subscribe_connection(self, sid: str, device_id: str, lat, lon) -> Optional[str]:
        coord_key = self.calculator._normalize_coordinate_key(lat, lon)
        if not coord_key:
            return None
            
        self.calculator.cleanup_old_calculations()
        
        self.calculator.coordinate_groups[coord_key] = (float(lat), float(lon))
        self.group_subscribers[coord_key].add((sid, device_id))
        
        if coord_key not in self.calculator.timezone_results:
            await self.calculator.calculate_timezone(coord_key, float(lat), float(lon))
            
        return coord_key
    
    async def unsubscribe_connection(self, sid: str, coord_key: Optional[str] = None):
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
    
    async def _cleanup_coordinate_group(self, coord_key: str):
        self.calculator.coordinate_groups.pop(coord_key, None)
        self.group_subscribers.pop(coord_key, None)
        self.calculator.timezone_results.pop(coord_key, None)
        self.calculator.last_calculation.pop(coord_key, None)

    async def broadcast_timezone_updates(self, sio):
        for coord_key, subscribers in self.group_subscribers.items():
            if not subscribers:
                continue
                
            tz_update = await self.calculator.get_timezone_update(coord_key)
            if not tz_update:
                continue
                
            valid_subscribers = []
            for sid, device_id in list(subscribers):
                try:
                    await sio.emit("time_update", {
                        "device_id": device_id,
                        "location_date": tz_update.get('date_str', 'N/A'),
                        "location_time": tz_update.get('time_str', 'N/A'),
                        "location_timezone": tz_update.get('timezone_str', 'N/A'),
                        "shared_calculation": True
                    }, room=sid)
                    valid_subscribers.append((sid, device_id))
                except Exception:
                    continue
            
            if len(valid_subscribers) != len(subscribers):
                self.group_subscribers[coord_key] = set(valid_subscribers)
                if not valid_subscribers:
                    await self._cleanup_coordinate_group(coord_key)

    def get_stats(self) -> Dict:
        total_subscribers = sum(len(subs) for subs in self.group_subscribers.values())
        efficiency = 0.0
        if total_subscribers > 0:
            efficiency = (1 - len(self.calculator.coordinate_groups) / total_subscribers) * 100
        
        return {
            'coordinate_groups': len(self.calculator.coordinate_groups),
            'total_subscribers': total_subscribers,
            'max_groups': self.calculator.max_groups,
            'memory_efficiency': f"{efficiency:.1f}%",
            'cached_timezones': len(self.calculator.timezone_results)
        }
