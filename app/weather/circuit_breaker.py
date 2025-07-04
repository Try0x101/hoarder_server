import asyncio
import datetime
import httpx
from enum import Enum
from typing import Dict, Any

class CircuitState(Enum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"

class EnhancedCircuitBreaker:
    def __init__(self, service_name, failure_threshold=5, recovery_timeout=30, success_threshold=3, health_window=300):
        self.service_name = service_name
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.success_threshold = success_threshold
        self.health_window = health_window
        
        self.failure_count = 0
        self.success_count = 0
        self.last_failure_time = None
        self.last_success_time = None
        self.state = CircuitState.CLOSED
        
        self.recent_attempts = []
        self.error_types = {}
        
    def _cleanup_recent_attempts(self):
        current_time = datetime.datetime.now().timestamp()
        cutoff = current_time - self.health_window
        self.recent_attempts = [attempt for attempt in self.recent_attempts if attempt['timestamp'] > cutoff]
    
    def _record_attempt(self, success, error_type=None):
        self._cleanup_recent_attempts()
        self.recent_attempts.append({
            'timestamp': datetime.datetime.now().timestamp(),
            'success': success,
            'error_type': error_type
        })
        
        if error_type and not success:
            self.error_types[error_type] = self.error_types.get(error_type, 0) + 1
    
    def _get_health_metrics(self):
        self._cleanup_recent_attempts()
        if not self.recent_attempts:
            return {'success_rate': 1.0, 'total_attempts': 0}
            
        successful = sum(1 for attempt in self.recent_attempts if attempt['success'])
        total = len(self.recent_attempts)
        
        return {
            'success_rate': successful / total,
            'total_attempts': total,
            'recent_failures': total - successful
        }

    async def call(self, func, *args, **kwargs):
        if self.state == CircuitState.OPEN:
            if self._should_attempt_reset():
                print(f"[{datetime.datetime.now()}] {self.service_name} circuit breaker transitioning to HALF_OPEN")
                self.state = CircuitState.HALF_OPEN
                self.success_count = 0
            else:
                metrics = self._get_health_metrics()
                raise Exception(f"{self.service_name} circuit breaker is OPEN - last failure: {self.last_failure_time}, health: {metrics}")

        try:
            result = await func(*args, **kwargs)
            self._on_success()
            return result
        except asyncio.TimeoutError as e:
            self._on_failure("timeout")
            raise e
        except httpx.ConnectTimeout as e:
            self._on_failure("connect_timeout")
            raise e
        except httpx.ReadTimeout as e:
            self._on_failure("read_timeout")
            raise e
        except httpx.HTTPStatusError as e:
            if e.response.status_code >= 500:
                self._on_failure(f"server_error_{e.response.status_code}")
            elif e.response.status_code == 429:
                self._on_failure("rate_limited")
            else:
                self._on_failure(f"client_error_{e.response.status_code}")
            raise e
        except Exception as e:
            self._on_failure("unknown_error")
            raise e

    def _should_attempt_reset(self):
        if not self.last_failure_time:
            return True
            
        time_since_failure = datetime.datetime.now().timestamp() - self.last_failure_time
        
        adaptive_timeout = self.recovery_timeout
        if self.failure_count > 10:
            adaptive_timeout = min(300, self.recovery_timeout * 2)
        elif self.failure_count > 5:
            adaptive_timeout = int(self.recovery_timeout * 1.5)
            
        return time_since_failure >= adaptive_timeout

    def _on_success(self):
        self._record_attempt(True)
        self.last_success_time = datetime.datetime.now().timestamp()
        
        if self.state == CircuitState.HALF_OPEN:
            self.success_count += 1
            if self.success_count >= self.success_threshold:
                print(f"[{datetime.datetime.now()}] {self.service_name} circuit breaker RECOVERED (required {self.success_threshold} successes)")
                self.state = CircuitState.CLOSED
                self.failure_count = 0
                self.error_types.clear()
        elif self.state == CircuitState.CLOSED:
            if self.failure_count > 0:
                self.failure_count = max(0, self.failure_count - 1)

    def _on_failure(self, error_type):
        self._record_attempt(False, error_type)
        self.failure_count += 1
        self.last_failure_time = datetime.datetime.now().timestamp()
        
        metrics = self._get_health_metrics()
        
        should_open = False
        if self.failure_count >= self.failure_threshold:
            should_open = True
        elif metrics['success_rate'] < 0.2 and metrics['total_attempts'] >= 5:
            should_open = True
            
        if should_open and self.state != CircuitState.OPEN:
            print(f"[{datetime.datetime.now()}] {self.service_name} circuit breaker OPENED after {self.failure_count} failures")
            print(f"[{datetime.datetime.now()}] Health metrics: {metrics}")
            print(f"[{datetime.datetime.now()}] Error types: {self.error_types}")
            self.state = CircuitState.OPEN

    def get_status(self):
        metrics = self._get_health_metrics()
        return {
            'service': self.service_name,
            'state': self.state.value,
            'failure_count': self.failure_count,
            'success_count': self.success_count,
            'last_failure_time': self.last_failure_time,
            'last_success_time': self.last_success_time,
            'health_metrics': metrics,
            'error_types': self.error_types.copy(),
            'thresholds': {
                'failure_threshold': self.failure_threshold,
                'recovery_timeout': self.recovery_timeout,
                'success_threshold': self.success_threshold
            }
        }

weather_circuit = EnhancedCircuitBreaker(
    service_name="OpenMeteo",
    failure_threshold=5,
    recovery_timeout=30,
    success_threshold=3,
    health_window=300
)

wttr_circuit = EnhancedCircuitBreaker(
    service_name="WTTR",
    failure_threshold=3,
    recovery_timeout=20,
    success_threshold=2,
    health_window=180
)

async def get_weather_circuit_status():
    return {
        "circuit_breakers": {
            "open_meteo": weather_circuit.get_status(),
            "wttr": wttr_circuit.get_status()
        }
    }
