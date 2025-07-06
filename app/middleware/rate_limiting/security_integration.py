import time
import gc
from typing import Dict, Optional
from fastapi import Request
from fastapi.responses import JSONResponse
from ..security import SuspiciousActivityDetector, ClientIdentifier
from .rate_limit_core import RateLimitCore

class SecurityIntegratedRateLimiter:
    def __init__(self):
        self.rate_limit_core = RateLimitCore()
        self.security_detector = SuspiciousActivityDetector()
        self.client_identifier = ClientIdentifier()
        
    async def check_rate_limit(self, request: Request) -> Optional[JSONResponse]:
        current_time = time.time()
        
        if current_time - self.rate_limit_core.last_cleanup > 300:
            await self._cleanup_trackers(current_time)
            self.rate_limit_core.last_cleanup = current_time
        
        identifier = self.client_identifier.get_client_identifier(request)
        client_ip = identifier.split('_')[0]
        
        if self.security_detector.is_ip_blocked(client_ip):
            return JSONResponse(
                status_code=429,
                content={"error": "IP blocked due to suspicious activity"},
                headers={"Retry-After": "3600"}
            )
        
        rate_limit_response = await self.rate_limit_core.check_rate_limit_for_request(request, identifier)
        
        if rate_limit_response:
            path = str(request.url.path)
            self.security_detector.detect_suspicious_activity(identifier, path, current_time)
        
        return rate_limit_response
    
    async def _cleanup_trackers(self, current_time: float):
        to_remove = []
        
        for identifier, tracker in self.rate_limit_core.trackers.items():
            if (not tracker.minute_requests and 
                not tracker.hour_requests and 
                current_time - tracker.last_reset > 3600):
                to_remove.append(identifier)
        
        for identifier in to_remove:
            del self.rate_limit_core.trackers[identifier]
        
        self.security_detector.cleanup_old_patterns(current_time)
        
        if len(self.rate_limit_core.trackers) > 2500:
            await self.rate_limit_core._emergency_cleanup()
        
        gc.collect()
    
    def get_stats(self) -> Dict:
        core_stats = self.rate_limit_core.get_core_stats()
        security_stats = self.security_detector.get_security_stats()
        
        return {
            **core_stats,
            **security_stats
        }
