import asyncio
import gc
from ..scheduler import FairTaskScheduler
from ..queues.queue_manager import QueueManager
from .task_executor import TaskExecutor

class WorkerPool:
    def __init__(self, worker_count: int = 3):
        self.worker_count = worker_count
        self.task_workers = []
        self.queue_manager = QueueManager()
        self.fair_scheduler = FairTaskScheduler()
        self.task_executor = TaskExecutor(self.queue_manager, self.fair_scheduler)
        self.last_cleanup = 0
        
    async def init_workers(self):
        if self.task_workers:
            return
        
        for i in range(self.worker_count):
            worker = asyncio.create_task(self._task_worker(f"worker-{i}"))
            self.task_workers.append(worker)
        
        cleanup_task = asyncio.create_task(self._periodic_cleanup())
        self.task_workers.append(cleanup_task)
    
    async def _task_worker(self, worker_id: str):
        consecutive_empty_cycles = 0
        
        while True:
            try:
                from .task_dispatcher import get_next_task
                task = await get_next_task(self.queue_manager, self.fair_scheduler)
                
                if not task:
                    consecutive_empty_cycles += 1
                    sleep_time = min(0.1, 0.01 * consecutive_empty_cycles)
                    await asyncio.sleep(sleep_time)
                    continue
                
                consecutive_empty_cycles = 0
                
                import time
                if time.time() - task.created_at > 45:
                    self.queue_manager.stats['dropped'] += 1
                    continue
                
                await self.task_executor.execute_task(task, task.priority)
                    
            except Exception as e:
                print(f"Worker {worker_id} error: {e}")
                await asyncio.sleep(0.5)
    
    async def _periodic_cleanup(self):
        while True:
            try:
                await asyncio.sleep(20)
                gc.collect()
            except Exception as e:
                print(f"Cleanup task error: {e}")
                await asyncio.sleep(60)
    
    async def shutdown(self):
        for worker in self.task_workers:
            worker.cancel()
        
        await asyncio.gather(*self.task_workers, return_exceptions=True)
        print(f"Priority queue workers shutdown complete")
    
    def get_stats(self):
        stats = self.queue_manager.get_stats()
        stats['active_workers'] = len(self.task_workers)
        stats['fair_scheduling'] = self.fair_scheduler.get_scheduling_stats()
        return stats
