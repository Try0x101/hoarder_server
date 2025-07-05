import time
import datetime
import psutil
from typing import Dict, List

RESPONSE_TIME_WINDOW = 50
MEMORY_THRESHOLD_MB = 250

class SystemMonitor:
    def __init__(self):
        self.start_time = time.time()
        self.request_count = 0
        self.error_count = 0
        self.response_times = []
        self.endpoint_stats = {}
        self.health_status = "healthy"
        self.alerts = []
        
    def record_request(self, endpoint: str, response_time: float, status_code: int):
        self.request_count += 1
        
        if len(self.response_times) >= RESPONSE_TIME_WINDOW:
            self.response_times.pop(0)
        self.response_times.append(response_time)
        
        if endpoint not in self.endpoint_stats:
            self.endpoint_stats[endpoint] = {
                'count': 0, 'avg_time': 0, 'errors': 0, 'last_accessed': time.time()
            }
        
        stats = self.endpoint_stats[endpoint]
        stats['count'] += 1
        stats['avg_time'] = (stats['avg_time'] * (stats['count'] - 1) + response_time) / stats['count']
        stats['last_accessed'] = time.time()
        
        if status_code >= 400:
            self.error_count += 1
            stats['errors'] += 1
            
        if status_code >= 500:
            self.add_alert(f"Server error on {endpoint}: {status_code}")
    
    def add_alert(self, message: str):
        alert = {
            'timestamp': datetime.datetime.now().isoformat(),
            'message': message
        }
        self.alerts.append(alert)
        
        if len(self.alerts) > 100:
            self.alerts = self.alerts[-50:]
    
    def get_system_health(self):
        memory_mb = psutil.Process().memory_info().rss / 1024 / 1024
        cpu_percent = psutil.Process().cpu_percent()
        
        uptime_seconds = int(time.time() - self.start_time)
        avg_response_time = sum(self.response_times) / len(self.response_times) if self.response_times else 0
        error_rate = (self.error_count / max(self.request_count, 1)) * 100
        
        health_score = 100
        issues = []
        
        if memory_mb > MEMORY_THRESHOLD_MB:
            health_score -= 20
            issues.append(f"High memory usage: {memory_mb:.1f}MB")
            
        if cpu_percent > 80:
            health_score -= 15
            issues.append(f"High CPU usage: {cpu_percent:.1f}%")
            
        if avg_response_time > 2.0:
            health_score -= 15
            issues.append(f"Slow response times: {avg_response_time:.2f}s")
            
        if error_rate > 5:
            health_score -= 25
            issues.append(f"High error rate: {error_rate:.1f}%")
        
        if health_score >= 90:
            self.health_status = "excellent"
        elif health_score >= 70:
            self.health_status = "good"
        elif health_score >= 50:
            self.health_status = "degraded"
        else:
            self.health_status = "critical"
        
        return {
            'health_status': self.health_status,
            'health_score': health_score,
            'uptime_seconds': uptime_seconds,
            'memory_usage_mb': f"{memory_mb:.1f}",
            'cpu_usage_percent': f"{cpu_percent:.1f}",
            'total_requests': self.request_count,
            'error_count': self.error_count,
            'error_rate_percent': f"{error_rate:.2f}",
            'avg_response_time_ms': f"{avg_response_time * 1000:.2f}",
            'current_issues': issues,
            'recent_alerts': self.alerts[-10:] if self.alerts else []
        }

    def get_endpoint_stats(self, limit: int = 20) -> Dict:
        return dict(list(self.endpoint_stats.items())[:limit])

    def cleanup_old_response_times(self):
        if len(self.response_times) > RESPONSE_TIME_WINDOW * 2:
            self.response_times[:] = self.response_times[-RESPONSE_TIME_WINDOW:]
