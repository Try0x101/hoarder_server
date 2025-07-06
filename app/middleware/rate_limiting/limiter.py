import time
import asyncio
import gc
from typing import Dict, Optional
from fastapi import Request
from fastapi.responses import JSONResponse
from ..security import SuspiciousActivityDetector, ClientIdentifier
from .rule_engine import RuleEngine
from .tracker import RequestTracker

CONNECTION_TIMEOUT = 180
CONNECTION_RATE_LIMIT = 10
MAX_CONNECTIONS = 500

class AdvancedRateLimiter:
    def __init__(self):
        self.trackers: Dict[str, RequestTracker] = {}
        self.connection_counts: Dict[str, int] = {}
        self.rule_engine = RuleEngine()
        self.security_detector = SuspiciousActivityDetector()
        self.client_identifier = ClientIdentifier()
        self.last_cleanup = time.time()
        
    async def check_rate_limit(self, request: Request) -> Optional[JSONResponse]:
        current_time = time.time()
        
        if current_time - self.last_cleanup > 300:
            await self._cleanup_trackers(current_time)
            self.last_cleanup = current_time
        
        identifier = self.client_identifier.get_client_identifier(request)
        client_ip = identifier.split('_')[0]
        
        if self.security_detector.is_ip_blocked(client_ip):
            return JSONResponse(
                status_code=429,
                content={"error": "IP blocked due to suspicious activity"},
                headers={"Retry-After": "3600"}
            )
        
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
            self.security_detector.detect_suspicious_activity(identifier, path, current_time)
            
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
        
        self.security_detector.cleanup_old_patterns(current_time)
        
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
        
        security_stats = self.security_detector.get_security_stats()
        
        return {
            "total_trackers": len(self.trackers),
            "active_trackers": active_trackers,
            "blocked_requests": blocked_requests,
            "rules": self.rule_engine.get_rules_info(),
            **security_stats
        }
