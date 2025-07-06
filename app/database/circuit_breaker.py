import time
from .config import CIRCUIT_BREAKER_TIMEOUT

class DatabaseCircuitBreaker:
    def __init__(self):
        self.failure_count = 0
        self.last_failure_time = 0
        self.state = "CLOSED"
    
    def can_execute(self):
        if self.state == "CLOSED":
            return True
        elif self.state == "OPEN":
            if time.time() - self.last_failure_time > CIRCUIT_BREAKER_TIMEOUT:
                self.state = "HALF_OPEN"
                return True
            return False
        elif self.state == "HALF_OPEN":
            return True
        return False
    
    def record_success(self):
        self.failure_count = 0
        self.state = "CLOSED"
    
    def record_failure(self):
        self.failure_count += 1
        self.last_failure_time = time.time()
        if self.failure_count >= 2:
            self.state = "OPEN"

db_circuit_breaker = DatabaseCircuitBreaker()