from typing import Optional
from fastapi import Request
from fastapi.responses import JSONResponse
from .security_integration import SecurityIntegratedRateLimiter

class AdvancedRateLimiter:
    def __init__(self):
        self.security_limiter = SecurityIntegratedRateLimiter()
        
    async def check_rate_limit(self, request: Request) -> Optional[JSONResponse]:
        return await self.security_limiter.check_rate_limit(request)
    
    def get_stats(self):
        return self.security_limiter.get_stats()
