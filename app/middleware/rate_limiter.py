import time
from typing import Dict, List
from fastapi import Request
from fastapi.responses import JSONResponse
from starlette.middleware.base import BaseHTTPMiddleware
import asyncio

class RateLimitMiddleware(BaseHTTPMiddleware):
    def __init__(
        self,
        app,
        default_limit: int = 2,
        default_window: int = 1,
        large_query_limit: int = 256,
        large_query_rate_limit: int = 2,
        large_query_window: int = 1,
    ):
        super().__init__(app)
        self.request_counts: Dict[str, List[float]] = {}
        self.default_limit = default_limit
        self.default_window = default_window
        self.large_query_limit = large_query_limit
        self.large_query_rate_limit = large_query_rate_limit
        self.large_query_window = large_query_window
        self._lock = asyncio.Lock()

    async def dispatch(self, request: Request, call_next):
        client_ip = request.client.host if request.client else "unknown"
        
        is_large_query = False
        if request.url.path.startswith("/data/history"):
            try:
                limit = int(request.query_params.get("limit", "0"))
                if limit > self.large_query_limit:
                    is_large_query = True
            except (ValueError, TypeError):
                pass

        if is_large_query:
            limit, window = self.large_query_rate_limit, self.large_query_window
            key = f"{client_ip}:large_query"
            error_detail = f"Rate limit exceeded for large history query. For queries with limit > {self.large_query_limit}, maximum {limit} requests per {window} second(s) are allowed."
        else:
            limit, window = self.default_limit, self.default_window
            key = f"{client_ip}:default"
            error_detail = f"Rate limit exceeded. Maximum {limit} requests per {window} second(s) are allowed for this endpoint."

        async with self._lock:
            current_time = time.time()
            
            request_timestamps = self.request_counts.get(key, [])
            valid_timestamps = [t for t in request_timestamps if current_time - t < window]
            
            if len(valid_timestamps) >= limit:
                return JSONResponse(
                    status_code=429,
                    content={"error": "Rate limit exceeded", "detail": error_detail}
                )
            
            valid_timestamps.append(current_time)
            self.request_counts[key] = valid_timestamps

        return await call_next(request)
