import time
import asyncio
from enum import Enum

class BreakerState(Enum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"

class SimpleCircuitBreaker:
    def __init__(self, name, failure_threshold=3, timeout=30):
        self.name = name
        self.failure_threshold = failure_threshold
        self.timeout = timeout
        self.failure_count = 0
        self.last_failure_time = 0
        self.state = BreakerState.CLOSED
    
    async def call(self, func, *args, **kwargs):
        if self.state == BreakerState.OPEN:
            if time.time() - self.last_failure_time > self.timeout:
                self.state = BreakerState.HALF_OPEN
            else:
                raise Exception(f"{self.name} circuit breaker is OPEN")
        
        try:
            result = await func(*args, **kwargs)
            self._on_success()
            return result
        except Exception as e:
            self._on_failure()
            raise e
    
    def _on_success(self):
        self.failure_count = 0
        if self.state == BreakerState.HALF_OPEN:
            self.state = BreakerState.CLOSED
    
    def _on_failure(self):
        self.failure_count += 1
        self.last_failure_time = time.time()
        if self.failure_count >= self.failure_threshold:
            self.state = BreakerState.OPEN
    
    def get_status(self):
        return {
            'name': self.name,
            'state': self.state.value,
            'failures': self.failure_count
        }

weather_breaker = SimpleCircuitBreaker("OpenMeteo", failure_threshold=3, timeout=30)
wttr_breaker = SimpleCircuitBreaker("WTTR", failure_threshold=2, timeout=20)

async def get_breaker_status():
    return {
        "circuit_breakers": {
            "open_meteo": weather_breaker.get_status(),
            "wttr": wttr_breaker.get_status()
        }
    }
