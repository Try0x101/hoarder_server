from .simple_processor import task_processor, TaskPriority, timeout_manager

class SimpleTaskManager:
    def __init__(self):
        self.processor = task_processor
        
    @property
    def worker_count(self):
        return self.processor.worker_count
        
    async def init_workers(self):
        await self.processor.init_workers()
    
    async def enqueue_task(self, coro, priority: TaskPriority, task_id: str) -> bool:
        timeout = 25.0 if priority == TaskPriority.CRITICAL else 15.0
        return await self.processor.enqueue_task(coro, priority, timeout)
    
    def get_queue_pressure(self):
        return self.processor.get_queue_pressure()
    
    def get_stats(self):
        return self.processor.get_stats()
    
    async def shutdown(self):
        await self.processor.shutdown()

simple_task_manager = SimpleTaskManager()
