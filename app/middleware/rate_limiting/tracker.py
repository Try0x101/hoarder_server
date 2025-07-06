import time
from typing import Tuple, Dict
from .rule_engine import RateLimitRule

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
