import time
from typing import Dict

class SuspiciousActivityDetector:
    def __init__(self):
        self.blocked_ips = set()
        self.suspicious_patterns = {}
        
    def detect_suspicious_activity(self, identifier: str, path: str, current_time: float):
        pattern_key = f"{identifier}:{path}"
        
        if pattern_key not in self.suspicious_patterns:
            self.suspicious_patterns[pattern_key] = []
        
        self.suspicious_patterns[pattern_key].append(current_time)
        
        recent_requests = [t for t in self.suspicious_patterns[pattern_key] if current_time - t < 10]
        self.suspicious_patterns[pattern_key] = recent_requests
        
        if len(recent_requests) > 100:
            client_ip = identifier.split('_')[0]
            self.blocked_ips.add(client_ip)
            print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] SECURITY: Blocked suspicious IP {client_ip}")
    
    def is_ip_blocked(self, client_ip: str) -> bool:
        return client_ip in self.blocked_ips
    
    def cleanup_old_patterns(self, current_time: float):
        old_patterns = []
        for pattern_key, timestamps in self.suspicious_patterns.items():
            recent = [t for t in timestamps if current_time - t < 3600]
            if recent:
                self.suspicious_patterns[pattern_key] = recent
            else:
                old_patterns.append(pattern_key)
        
        for pattern_key in old_patterns:
            del self.suspicious_patterns[pattern_key]
    
    def get_security_stats(self) -> Dict:
        return {
            "blocked_ips": len(self.blocked_ips),
            "suspicious_patterns": len(self.suspicious_patterns)
        }
