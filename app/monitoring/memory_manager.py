import asyncio

MEMORY_THRESHOLD_MB = 250

class GlobalMemoryManager:
    def __init__(self):
        self.websocket_memory_mb = 0
        self.batch_memory_mb = 0
        self.max_total_memory_mb = MEMORY_THRESHOLD_MB
        self.max_websocket_memory_mb = 100
        self.max_batch_memory_mb = 150
        self.memory_lock = asyncio.Lock()
        
    async def request_websocket_memory(self, estimated_mb):
        async with self.memory_lock:
            if self.websocket_memory_mb + estimated_mb > self.max_websocket_memory_mb:
                return False
            self.websocket_memory_mb += estimated_mb
            return True
            
    async def release_websocket_memory(self, estimated_mb):
        async with self.memory_lock:
            self.websocket_memory_mb = max(0, self.websocket_memory_mb - estimated_mb)
            
    async def request_batch_memory(self, estimated_mb):
        async with self.memory_lock:
            total_projected = self.websocket_memory_mb + self.batch_memory_mb + estimated_mb
            if total_projected > self.max_total_memory_mb:
                return False
            if self.batch_memory_mb + estimated_mb > self.max_batch_memory_mb:
                return False
            self.batch_memory_mb += estimated_mb
            return True
            
    async def release_batch_memory(self, estimated_mb):
        async with self.memory_lock:
            self.batch_memory_mb = max(0, self.batch_memory_mb - estimated_mb)
            
    def get_memory_stats(self):
        return {
            'websocket_memory_mb': self.websocket_memory_mb,
            'batch_memory_mb': self.batch_memory_mb,
            'total_used_mb': self.websocket_memory_mb + self.batch_memory_mb,
            'total_available_mb': self.max_total_memory_mb,
            'memory_pressure': (self.websocket_memory_mb + self.batch_memory_mb) / self.max_total_memory_mb
        }

    def is_memory_critical(self) -> bool:
        pressure = self.get_memory_stats()['memory_pressure']
        return pressure > 0.9

    def get_available_websocket_memory(self) -> float:
        return self.max_websocket_memory_mb - self.websocket_memory_mb

    def get_available_batch_memory(self) -> float:
        return self.max_batch_memory_mb - self.batch_memory_mb
