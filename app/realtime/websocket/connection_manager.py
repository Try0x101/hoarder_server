import time
import datetime
import psutil
import asyncio
from typing import Dict, Optional

CONNECTION_TIMEOUT = 120
CONNECTION_RATE_LIMIT = 8
MAX_CONNECTIONS = 100
MEMORY_CLEANUP_THRESHOLD = 200
AGGRESSIVE_CLEANUP_THRESHOLD = 230

class ConnectionManager:
    def __init__(self):
        self.connections: Dict[str, Dict] = {}
        self.device_connections: Dict[str, set] = {}
        self.connection_counts: Dict[str, int] = {}
        self.last_cleanup = 0
        self.cleanup_lock = asyncio.Lock()
        
    def get_memory_usage_mb(self) -> float:
        try:
            return psutil.Process().memory_info().rss / 1024 / 1024
        except Exception:
            return 0.0
    
    def _get_client_ip(self, environ: dict) -> str:
        try:
            forwarded = environ.get('HTTP_X_FORWARDED_FOR', '').split(',')[0].strip()
            if forwarded:
                return forwarded[:45]
            real_ip = environ.get('HTTP_X_REAL_IP', '').strip()
            if real_ip:
                return real_ip[:45]
            remote_addr = environ.get('REMOTE_ADDR', 'unknown').strip()
            return remote_addr[:45]
        except Exception:
            return 'unknown'
    
    async def can_accept_connection(self, client_ip: str) -> bool:
        memory_mb = self.get_memory_usage_mb()
        
        if memory_mb > AGGRESSIVE_CLEANUP_THRESHOLD:
            await self._aggressive_cleanup()
            
        if len(self.connections) >= MAX_CONNECTIONS:
            return False
            
        if memory_mb > MEMORY_CLEANUP_THRESHOLD:
            return False
            
        ip_connections = self.connection_counts.get(client_ip, 0)
        if ip_connections >= CONNECTION_RATE_LIMIT:
            return False
            
        return True
    
    def add_connection(self, sid: str, environ: dict):
        try:
            client_ip = self._get_client_ip(environ)
            
            self.connections[sid] = {
                'connected_at': time.time(),
                'last_ping': time.time(),
                'client_ip': client_ip,
                'device_id': None,
                'coord_key': None,
                'memory_allocated': 1.0
            }
            
            self.connection_counts[client_ip] = self.connection_counts.get(client_ip, 0) + 1
        except Exception as e:
            print(f"[{datetime.datetime.now()}] Error adding connection {sid}: {e}")

    async def remove_connection(self, sid: str):
        async with self.cleanup_lock:
            try:
                if sid not in self.connections:
                    return
                    
                connection = self.connections[sid]
                client_ip = connection.get('client_ip')
                device_id = connection.get('device_id')
                
                if device_id and device_id in self.device_connections:
                    self.device_connections[device_id].discard(sid)
                    if not self.device_connections[device_id]:
                        del self.device_connections[device_id]
                        
                if client_ip and client_ip in self.connection_counts:
                    self.connection_counts[client_ip] = max(0, self.connection_counts[client_ip] - 1)
                    if self.connection_counts[client_ip] == 0:
                        del self.connection_counts[client_ip]
                        
                del self.connections[sid]
            except Exception as e:
                print(f"[{datetime.datetime.now()}] Error removing connection {sid}: {e}")

    async def register_device(self, sid: str, device_id: str) -> bool:
        try:
            if sid not in self.connections:
                return False
            
            if not device_id or not isinstance(device_id, str):
                return False
            
            device_id = str(device_id).strip()[:100]
            if not device_id:
                return False
                
            old_device_id = self.connections[sid].get('device_id')
            if old_device_id and old_device_id in self.device_connections:
                self.device_connections[old_device_id].discard(sid)
                
            self.connections[sid]['device_id'] = device_id
            
            if device_id not in self.device_connections:
                self.device_connections[device_id] = set()
            self.device_connections[device_id].add(sid)
            
            return True
        except Exception as e:
            print(f"[{datetime.datetime.now()}] Error registering device {device_id} for {sid}: {e}")
            return False
    
    def update_ping(self, sid: str):
        try:
            if sid in self.connections:
                self.connections[sid]['last_ping'] = time.time()
        except Exception:
            pass

    async def _aggressive_cleanup(self):
        async with self.cleanup_lock:
            current_time = time.time()
            stale_sids = []
            
            for sid, connection in list(self.connections.items()):
                connection_age = current_time - connection.get('last_ping', 0)
                if connection_age > 60:
                    stale_sids.append(sid)
            
            for sid in stale_sids:
                await self.remove_connection(sid)
            
            if stale_sids:
                print(f"[{datetime.datetime.now()}] Aggressive cleanup: removed {len(stale_sids)} connections")

    async def periodic_cleanup(self, sio):
        async with self.cleanup_lock:
            try:
                current_time = time.time()
                if current_time - self.last_cleanup < 30:
                    return
                    
                stale_sids = []
                memory_mb = self.get_memory_usage_mb()
                timeout = CONNECTION_TIMEOUT
                
                if memory_mb > MEMORY_CLEANUP_THRESHOLD:
                    timeout = 60
                
                for sid, connection in list(self.connections.items()):
                    try:
                        connection_age = current_time - connection.get('last_ping', 0)
                        
                        if connection_age > timeout:
                            stale_sids.append(sid)
                    except Exception:
                        stale_sids.append(sid)
                        
                for sid in stale_sids:
                    try:
                        await sio.disconnect(sid)
                        await self.remove_connection(sid)
                    except Exception:
                        pass
                        
                if len(stale_sids) > 0:
                    print(f"[{datetime.datetime.now()}] Cleaned {len(stale_sids)} stale connections")
                    
                self.last_cleanup = current_time
            except Exception as e:
                print(f"[{datetime.datetime.now()}] Error in cleanup: {e}")
        
    def get_stats(self) -> Dict:
        try:
            memory_mb = self.get_memory_usage_mb()
            
            return {
                'total_connections': len(self.connections),
                'max_connections': MAX_CONNECTIONS,
                'device_connections': len(self.device_connections),
                'connection_rate_limits': len(self.connection_counts),
                'memory_usage_mb': f"{memory_mb:.1f}",
                'memory_threshold_mb': MEMORY_CLEANUP_THRESHOLD,
                'aggressive_threshold_mb': AGGRESSIVE_CLEANUP_THRESHOLD,
                'timeout_seconds': CONNECTION_TIMEOUT,
                'rate_limit_per_ip': CONNECTION_RATE_LIMIT
            }
        except Exception:
            return {
                'error': 'stats_collection_failed',
                'total_connections': len(self.connections),
                'max_connections': MAX_CONNECTIONS
            }
