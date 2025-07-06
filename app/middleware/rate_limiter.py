import time
import asyncio
from typing import Dict, Optional
from fastapi import Request
from fastapi.responses import JSONResponse
from collections import defaultdict

class SimpleRateLimiter:
    def __init__(self):
        self.trackers: Dict[str, Dict] = {}
        self.last_cleanup = time.time()
        self.limits = {
            'telemetry': {'rpm': 300, 'rph': 7200},
            'batch': {'rpm': 10, 'rph': 100},
            'api': {'rpm': 60, 'rph': 600},
            'default': {'rpm': 30, 'rph': 300}
        }
    
    def _get_client_id(self, request: Request) -> str:
        forwarded = request.headers.get('x-forwarded-for', '').split(',')[0].strip()
        real_ip = request.headers.get('x-real-ip', '')
        client_ip = forwarded or real_ip or request.client.host or 'unknown'
        return client_ip
    
    def _get_endpoint_category(self, path: str, method: str) -> str:
        if '/api/telemetry' in path and method == 'POST':
            return 'telemetry'
        elif '/api/batch' in path and method == 'POST':
            return 'batch'
        elif path.startswith('/api/') and method == 'GET':
            return 'api'
        return 'default'
    
    def _cleanup_old_trackers(self, current_time: float):
        if current_time - self.last_cleanup < 60:
            return
        
        cutoff = current_time - 3600
        to_remove = []
        
        for client_id, tracker in self.trackers.items():
            tracker['requests'] = [t for t in tracker['requests'] if t > cutoff]
            if not tracker['requests']:
                to_remove.append(client_id)
        
        for client_id in to_remove:
            del self.trackers[client_id]
        
        self.last_cleanup = current_time
    
    async def check_rate_limit(self, request: Request) -> Optional[JSONResponse]:
        current_time = time.time()
        self._cleanup_old_trackers(current_time)
        
        client_id = self._get_client_id(request)
        category = self._get_endpoint_category(str(request.url.path), request.method)
        limits = self.limits[category]
        
        if client_id not in self.trackers:
            self.trackers[client_id] = {'requests': []}
        
        tracker = self.trackers[client_id]
        tracker['requests'] = [t for t in tracker['requests'] if t > current_time - 3600]
        
        minute_requests = len([t for t in tracker['requests'] if t > current_time - 60])
        hour_requests = len(tracker['requests'])
        
        if minute_requests >= limits['rpm']:
            return JSONResponse(
                status_code=429,
                content={
                    "error": "Rate limit exceeded",
                    "message": f"Too many requests per minute ({minute_requests}/{limits['rpm']})",
                    "retry_after": 60
                },
                headers={"Retry-After": "60"}
            )
        
        if hour_requests >= limits['rph']:
            return JSONResponse(
                status_code=429,
                content={
                    "error": "Rate limit exceeded", 
                    "message": f"Too many requests per hour ({hour_requests}/{limits['rph']})",
                    "retry_after": 3600
                },
                headers={"Retry-After": "3600"}
            )
        
        tracker['requests'].append(current_time)
        return None
    
    def get_stats(self):
        return {
            'total_clients': len(self.trackers),
            'active_clients': len([t for t in self.trackers.values() if t['requests']]),
            'limits': self.limits
        }

rate_limiter = SimpleRateLimiter()

async def rate_limit_middleware(request: Request, call_next):
    rate_limit_response = await rate_limiter.check_rate_limit(request)
    if rate_limit_response:
        return rate_limit_response
    
    response = await call_next(request)
    
    stats = rate_limiter.get_stats()
    response.headers["X-RateLimit-Clients"] = str(stats["total_clients"])
    
    return response
