import time
import asyncio
import hashlib
from typing import Dict, Optional, Tuple
from fastapi import Request, HTTPException
from fastapi.responses import JSONResponse
import weakref
import gc

class RateLimitRule:
    def __init__(self, requests_per_minute: int, requests_per_hour: int, burst_limit: int = None):
        self.requests_per_minute = requests_per_minute
        self.requests_per_hour = requests_per_hour
        self.burst_limit = burst_limit or requests_per_minute * 3

class RequestTracker:
    def __init__(self, rule: RateLimitRule):
        self.rule = rule
        self.minute_requests = []
        self.hour_requests = []
        self.burst_count = 0
        self.last_reset = time.time()
        self.blocked_until = 0
        
    def is_allowed(self, current_time: float) -> Tuple[bool, str, Dict]:
        if current_time < self.blocked_until:
            remaining = int(self.blocked_until - current_time)
            return False, f"Blocked for {remaining}s", {"retry_after": remaining}
        
        self._cleanup_old_requests(current_time)
        
        minute_count = len(self.minute_requests)
        hour_count = len(self.hour_requests)
        
        if minute_count >= self.rule.requests_per_minute:
            self.blocked_until = current_time + 10
            return False, "Minute limit exceeded", {"retry_after": 10}
            
        if hour_count >= self.rule.requests_per_hour:
            self.blocked_until = current_time + 60
            return False, "Hour limit exceeded", {"retry_after": 60}
            
        if self.burst_count >= self.rule.burst_limit:
            self.blocked_until = current_time + 5
            return False, "Burst limit exceeded", {"retry_after": 5}
        
        self.minute_requests.append(current_time)
        self.hour_requests.append(current_time)
        self.burst_count += 1
        
        if current_time - self.last_reset > 5:
            self.burst_count = max(0, self.burst_count - 10)
            self.last_reset = current_time
        
        stats = {
            "minute_remaining": self.rule.requests_per_minute - minute_count - 1,
            "hour_remaining": self.rule.requests_per_hour - hour_count - 1,
            "burst_remaining": self.rule.burst_limit - self.burst_count
        }
        
        return True, "OK", stats
    
    def _cleanup_old_requests(self, current_time: float):
        minute_cutoff = current_time - 60
        hour_cutoff = current_time - 3600
        
        self.minute_requests = [t for t in self.minute_requests if t > minute_cutoff]
        self.hour_requests = [t for t in self.hour_requests if t > hour_cutoff]

class AdvancedRateLimiter:
    def __init__(self):
        self.trackers: Dict[str, RequestTracker] = {}
        self.rules = {
            'telemetry': RateLimitRule(300, 7200, 600),
            'batch': RateLimitRule(10, 100, 20),
            'api_read': RateLimitRule(300, 3000, 500),
            'default': RateLimitRule(60, 600, 100)
        }
        self.blocked_ips = set()
        self.suspicious_patterns = {}
        self.last_cleanup = time.time()
        
    def _get_client_identifier(self, request: Request) -> str:
        forwarded = request.headers.get('x-forwarded-for', '').split(',')[0].strip()
        real_ip = request.headers.get('x-real-ip', '')
        client_ip = forwarded or real_ip or request.client.host or 'unknown'
        
        user_agent = request.headers.get('user-agent', '')[:50]
        identifier_parts = [client_ip, hashlib.md5(user_agent.encode()).hexdigest()[:8]]
        
        return '_'.join(identifier_parts)
    
    def _get_endpoint_category(self, path: str, method: str) -> str:
        if '/api/telemetry' in path and method == 'POST':
            return 'telemetry'
        elif '/api/batch' in path and method == 'POST':
            return 'batch'
        elif path.startswith('/api/') and method == 'GET':
            return 'api_read'
        return 'default'
    
    def _detect_suspicious_activity(self, identifier: str, path: str, current_time: float):
        pattern_key = f"{identifier}:{path}"
        
        if pattern_key not in self.suspicious_patterns:
            self.suspicious_patterns[pattern_key] = []
        
        self.suspicious_patterns[pattern_key].append(current_time)
        
        recent_requests = [t for t in self.suspicious_patterns[pattern_key] if current_time - t < 10]
        self.suspicious_patterns[pattern_key] = recent_requests
        
        if len(recent_requests) > 100:
            self.blocked_ips.add(identifier.split('_')[0])
            print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] SECURITY: Blocked suspicious IP {identifier.split('_')[0]}")
    
    async def check_rate_limit(self, request: Request) -> Optional[JSONResponse]:
        current_time = time.time()
        
        if current_time - self.last_cleanup > 300:
            await self._cleanup_trackers(current_time)
            self.last_cleanup = current_time
        
        identifier = self._get_client_identifier(request)
        client_ip = identifier.split('_')[0]
        
        if client_ip in self.blocked_ips:
            return JSONResponse(
                status_code=429,
                content={"error": "IP blocked due to suspicious activity"},
                headers={"Retry-After": "3600"}
            )
        
        path = str(request.url.path)
        method = request.method
        category = self._get_endpoint_category(path, method)
        
        content_length = int(request.headers.get('content-length', 0))
        if content_length > 100 * 1024 * 1024:
            return JSONResponse(
                status_code=413,
                content={"error": "Request too large"},
                headers={"Content-Type": "application/json"}
            )
        
        if identifier not in self.trackers:
            if len(self.trackers) > 5000:
                await self._emergency_cleanup()
            self.trackers[identifier] = RequestTracker(self.rules[category])
        
        tracker = self.trackers[identifier]
        allowed, message, stats = tracker.is_allowed(current_time)
        
        if not allowed:
            self._detect_suspicious_activity(identifier, path, current_time)
            
            headers = {"Content-Type": "application/json"}
            if "retry_after" in stats:
                headers["Retry-After"] = str(stats["retry_after"])
            
            return JSONResponse(
                status_code=429,
                content={
                    "error": "Rate limit exceeded",
                    "message": message,
                    "stats": stats,
                    "category": category
                },
                headers=headers
            )
        
        return None
    
    async def _cleanup_trackers(self, current_time: float):
        to_remove = []
        
        for identifier, tracker in self.trackers.items():
            if (not tracker.minute_requests and 
                not tracker.hour_requests and 
                current_time - tracker.last_reset > 3600):
                to_remove.append(identifier)
        
        for identifier in to_remove:
            del self.trackers[identifier]
        
        old_patterns = []
        for pattern_key, timestamps in self.suspicious_patterns.items():
            recent = [t for t in timestamps if current_time - t < 3600]
            if recent:
                self.suspicious_patterns[pattern_key] = recent
            else:
                old_patterns.append(pattern_key)
        
        for pattern_key in old_patterns:
            del self.suspicious_patterns[pattern_key]
        
        if len(self.trackers) > 2500:
            await self._emergency_cleanup()
        
        gc.collect()
    
    async def _emergency_cleanup(self):
        oldest_trackers = sorted(
            self.trackers.items(),
            key=lambda x: x[1].last_reset
        )[:len(self.trackers)//2]
        
        for identifier, _ in oldest_trackers:
            del self.trackers[identifier]
        
        print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] Emergency cleanup: removed {len(oldest_trackers)} trackers")
    
    def get_stats(self) -> Dict:
        current_time = time.time()
        
        active_trackers = 0
        blocked_requests = 0
        
        for tracker in self.trackers.values():
            if tracker.minute_requests or tracker.hour_requests:
                active_trackers += 1
            if current_time < tracker.blocked_until:
                blocked_requests += 1
        
        return {
            "total_trackers": len(self.trackers),
            "active_trackers": active_trackers,
            "blocked_requests": blocked_requests,
            "blocked_ips": len(self.blocked_ips),
            "suspicious_patterns": len(self.suspicious_patterns),
            "rules": {name: {
                "requests_per_minute": rule.requests_per_minute,
                "requests_per_hour": rule.requests_per_hour,
                "burst_limit": rule.burst_limit
            } for name, rule in self.rules.items()}
        }

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
