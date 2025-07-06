import asyncio
import time
from ..scheduler import TaskPriority, PriorityTask
from .worker_pool import WorkerPool

class WorkerManager:
    def __init__(self, worker_count: int = 3):
        self.worker_pool = WorkerPool(worker_count)
        
    @property
    def worker_count(self):
        return self.worker_pool.worker_count
        
    async def init_workers(self):
        await self.worker_pool.init_workers()
    
    async def enqueue_task(self, coro, priority: TaskPriority, task_id: str) -> bool:
        if len(task_id) > 50:
            task_id = f"{task_id[:40]}_{int(time.time())}"
        
        task = PriorityTask(coro, priority, task_id)
        return await self.worker_pool.queue_manager.enqueue_task(task, priority)
    
    def get_queue_pressure(self):
        return self.worker_pool.queue_manager.get_queue_pressure()
    
    def get_stats(self):
        return self.worker_pool.get_stats()
    
    async def shutdown(self):
        await self.worker_pool.shutdown()
