from .workers.worker_manager import WorkerManager
from .scheduler import TaskPriority

class PriorityQueueManager:
    def __init__(self):
        self.worker_manager = WorkerManager(worker_count=3)
        
    @property
    def worker_count(self):
        return self.worker_manager.worker_count
        
    async def init_workers(self):
        await self.worker_manager.init_workers()
    
    async def enqueue_task(self, coro, priority: TaskPriority, task_id: str) -> bool:
        return await self.worker_manager.enqueue_task(coro, priority, task_id)
    
    def get_queue_pressure(self):
        return self.worker_manager.get_queue_pressure()
    
    def get_stats(self):
        return self.worker_manager.get_stats()
    
    async def shutdown(self):
        await self.worker_manager.shutdown()
