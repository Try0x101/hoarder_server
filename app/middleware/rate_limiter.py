from fastapi import Request
from .rate_limiting.limiter import AdvancedRateLimiter

rate_limiter = AdvancedRateLimiter()

async def rate_limit_middleware(request: Request, call_next):
    rate_limit_response = await rate_limiter.check_rate_limit(request)
    if rate_limit_response:
        return rate_limit_response
    
    response = await call_next(request)
    
    stats = rate_limiter.get_stats()
    response.headers["X-RateLimit-Trackers"] = str(stats["total_trackers"])
    response.headers["X-RateLimit-Active"] = str(stats["active_trackers"])
    
    return response
