import time
import datetime
import psutil
import asyncio
from typing import Dict

CONNECTION_TIMEOUT = 120
CONNECTION_RATE_LIMIT = 8
MAX_CONNECTIONS = 100
MEMORY_THRESHOLD_MB = 200
AGGRESSIVE_THRESHOLD_MB = 230

class ConnectionManager:
    def __init__(self):
        self.connections: Dict[str, Dict] = {}
        self.device_connections: Dict[str, set] = {}
        self.ip_counts: Dict[str, int] = {}
        self.last_cleanup = 0
        
    def get_memory_usage_mb(self) -> float:
        return psutil.Process().memory_info().rss / 1024 / 1024
    
    def _get_client_ip(self, environ: dict) -> str:
        forwarded = environ.get('HTTP_X_FORWARDED_FOR', '').split(',')[0].strip()
        real_ip = environ.get('HTTP_X_REAL_IP', '')
        return forwarded or real_ip or environ.get('REMOTE_ADDR', 'unknown')

    async def can_accept_connection(self, sid: str, environ: dict) -> bool:
        if len(self.connections) >= MAX_CONNECTIONS: return False
        
        client_ip = self._get_client_ip(environ)
        if self.ip_counts.get(client_ip, 0) >= CONNECTION_RATE_LIMIT: return False
        
        memory_mb = self.get_memory_usage_mb()
        if memory_mb > AGGRESSIVE_THRESHOLD_MB:
            await self._cleanup(None, aggressive=True)
            if self.get_memory_usage_mb() > AGGRESSIVE_THRESHOLD_MB: return False
        elif memory_mb > MEMORY_THRESHOLD_MB:
            return False

        self.connections[sid] = {'connected_at': time.time(), 'last_ping': time.time(), 'client_ip': client_ip}
        self.ip_counts[client_ip] = self.ip_counts.get(client_ip, 0) + 1
        return True

    async def remove_connection(self, sid: str):
        conn_data = self.connections.pop(sid, None)
        if not conn_data: return

        if (client_ip := conn_data.get('client_ip')) and client_ip in self.ip_counts:
            self.ip_counts[client_ip] -= 1
            if self.ip_counts[client_ip] == 0: del self.ip_counts[client_ip]

        if (device_id := conn_data.get('device_id')) and device_id in self.device_connections:
            self.device_connections[device_id].discard(sid)
            if not self.device_connections[device_id]: del self.device_connections[device_id]

    async def register_device(self, sid: str, device_id: str):
        if sid not in self.connections or not isinstance(device_id, str): return
        
        device_id = device_id.strip()[:100]
        if not device_id: return
        
        if (old_device_id := self.connections[sid].get('device_id')) and old_device_id in self.device_connections:
            self.device_connections[old_device_id].discard(sid)
            
        self.connections[sid]['device_id'] = device_id
        self.device_connections.setdefault(device_id, set()).add(sid)
    
    def update_ping(self, sid: str):
        if sid in self.connections: self.connections[sid]['last_ping'] = time.time()

    async def periodic_cleanup(self, sio):
        if time.time() - self.last_cleanup > 30:
            await self._cleanup(sio)
            self.last_cleanup = time.time()

    async def _cleanup(self, sio, aggressive: bool = False):
        timeout = 60 if aggressive else CONNECTION_TIMEOUT
        stale_sids = [
            sid for sid, conn in self.connections.items()
            if time.time() - conn.get('last_ping', 0) > timeout
        ]
        for sid in stale_sids:
            if sio: await sio.disconnect(sid, ignore_queue=True)
            await self.remove_connection(sid)
        if stale_sids:
            print(f"[{datetime.datetime.now()}] Cleaned {len(stale_sids)} stale connections (aggressive={aggressive})")

    def get_stats(self) -> Dict:
        return {
            'total_connections': len(self.connections), 'max_connections': MAX_CONNECTIONS,
            'device_connections': len(self.device_connections), 'unique_ips': len(self.ip_counts),
            'memory_usage_mb': f"{self.get_memory_usage_mb():.1f}",
            'memory_threshold_mb': MEMORY_THRESHOLD_MB
        }
