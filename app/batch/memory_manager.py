import asyncio
import datetime
import psutil
import gc

MAX_CONCURRENT_BATCHES = 2
ESTIMATED_ITEM_SIZE_BYTES = 2048
MEMORY_SAFETY_MARGIN_MB = 50

class BatchMemoryManager:
    def __init__(self):
        self.active_batches = {}
        self.total_batch_memory_mb = 0
        self.max_batch_memory_mb = 120
        self.batch_lock = asyncio.Lock()
        
    async def estimate_batch_memory(self, batch_size):
        estimated_bytes = batch_size * ESTIMATED_ITEM_SIZE_BYTES
        estimated_mb = estimated_bytes / (1024 * 1024)
        return estimated_mb
        
    async def request_batch_memory(self, batch_id, estimated_mb):
        async with self.batch_lock:
            current_memory = psutil.Process().memory_info().rss / 1024 / 1024
            available_memory = max(0, 400 - current_memory - MEMORY_SAFETY_MARGIN_MB)
            
            if self.total_batch_memory_mb + estimated_mb > self.max_batch_memory_mb:
                return False, f"Batch memory limit exceeded ({self.total_batch_memory_mb:.1f} + {estimated_mb:.1f} > {self.max_batch_memory_mb})"
                
            if estimated_mb > available_memory:
                return False, f"Insufficient system memory ({estimated_mb:.1f}MB needed, {available_memory:.1f}MB available)"
                
            if len(self.active_batches) >= MAX_CONCURRENT_BATCHES:
                return False, f"Too many concurrent batches ({len(self.active_batches)}/{MAX_CONCURRENT_BATCHES})"
                
            self.active_batches[batch_id] = {
                'estimated_mb': estimated_mb,
                'start_time': datetime.datetime.now(datetime.timezone.utc),
                'processed_items': 0
            }
            self.total_batch_memory_mb += estimated_mb
            return True, "Memory allocated"
            
    async def release_batch_memory(self, batch_id):
        async with self.batch_lock:
            if batch_id in self.active_batches:
                estimated_mb = self.active_batches[batch_id]['estimated_mb']
                self.total_batch_memory_mb = max(0, self.total_batch_memory_mb - estimated_mb)
                del self.active_batches[batch_id]
                gc.collect()
                
    async def update_batch_progress(self, batch_id, processed_items):
        if batch_id in self.active_batches:
            self.active_batches[batch_id]['processed_items'] = processed_items
            
    def get_memory_stats(self):
        return {
            'active_batches': len(self.active_batches),
            'total_batch_memory_mb': self.total_batch_memory_mb,
            'max_batch_memory_mb': self.max_batch_memory_mb,
            'memory_pressure': self.total_batch_memory_mb / self.max_batch_memory_mb,
            'batch_details': self.active_batches.copy()
        }

    def get_system_memory_pressure(self):
        process = psutil.Process()
        memory_mb = process.memory_info().rss / 1024 / 1024
        
        if memory_mb > 350:
            return "CRITICAL"
        elif memory_mb > 280:
            return "HIGH"
        elif memory_mb > 220:
            return "MEDIUM"
        return "LOW"

    def get_adaptive_chunk_size(self):
        pressure = self.get_system_memory_pressure()
        if pressure == "CRITICAL":
            return 5
        elif pressure == "HIGH":
            return 10
        elif pressure == "MEDIUM":
            return 12
        return 15

    async def aggressive_memory_cleanup(self):
        gc.collect()
        await asyncio.sleep(0.01)
        
        for obj in gc.get_objects():
            if isinstance(obj, dict) and len(obj) > 100:
                if 'batch_temp_' in str(obj):
                    obj.clear()
