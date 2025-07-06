import time
import asyncio
from typing import Dict, Optional
from fastapi import Request
from fastapi.responses import JSONResponse
from .rule_engine import RuleEngine
from .tracker import RequestTracker

class RateLimitCore:
    def __init__(self):
        self.trackers: Dict[str, RequestTracker] = {}
        self.rule_engine = RuleEngine()
        self.last_cleanup = time.time()
        
    async def check_rate_limit_for_request(self, request: Request, identifier: str) -> Optional[JSONResponse]:
        current_time = time.time()
        
        path = str(request.url.path)
        method = request.method
        category = self.rule_engine.get_endpoint_category(path, method)
        
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
            rule = self.rule_engine.get_rule(category)
            self.trackers[identifier] = RequestTracker(rule)
        
        tracker = self.trackers[identifier]
        allowed, message, stats = tracker.is_allowed(current_time)
        
        if not allowed:
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
    
    async def _emergency_cleanup(self):
        oldest_trackers = sorted(
            self.trackers.items(),
            key=lambda x: x[1].last_reset
        )[:len(self.trackers)//2]
        
        for identifier, _ in oldest_trackers:
            del self.trackers[identifier]
        
        print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] Emergency cleanup: removed {len(oldest_trackers)} trackers")
    
    def get_core_stats(self) -> Dict:
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
            "rules": self.rule_engine.get_rules_info()
        }
