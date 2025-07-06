import asyncio
import time
import gc
from ..scheduler import TaskPriority, FairTaskScheduler
from ..queues.queue_manager import QueueManager
from .task_executor import TaskExecutor

class WorkerManager:
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
    
    async def enqueue_task(self, coro, priority: TaskPriority, task_id: str) -> bool:
        if len(task_id) > 50:
            task_id = f"{task_id[:40]}_{int(time.time())}"
        
        from ..scheduler import PriorityTask
        task = PriorityTask(coro, priority, task_id)
        return await self.queue_manager.enqueue_task(task, priority)
    
    async def _task_worker(self, worker_id: str):
        consecutive_empty_cycles = 0
        
        while True:
            try:
                task = await self._get_next_task()
                
                if not task:
                    consecutive_empty_cycles += 1
                    sleep_time = min(0.1, 0.01 * consecutive_empty_cycles)
                    await asyncio.sleep(sleep_time)
                    continue
                
                consecutive_empty_cycles = 0
                
                if time.time() - task.created_at > 45:
                    self.queue_manager.stats['dropped'] += 1
                    continue
                
                await self.task_executor.execute_task(task, task.priority)
                    
            except Exception as e:
                print(f"Worker {worker_id} error: {e}")
                await asyncio.sleep(0.5)
    
    async def _get_next_task(self):
        selected_priority = self.fair_scheduler.get_next_priority()
        
        if selected_priority:
            queue = self.queue_manager.task_queues[selected_priority]
            try:
                return queue.get_nowait()
            except asyncio.QueueEmpty:
                pass
        
        for priority in TaskPriority:
            queue = self.queue_manager.task_queues[priority]
            try:
                task = queue.get_nowait()
                if priority != selected_priority:
                    self.queue_manager.record_starvation_prevention()
                return task
            except asyncio.QueueEmpty:
                continue
        
        return None
    
    async def _periodic_cleanup(self):
        while True:
            try:
                await asyncio.sleep(20)
                gc.collect()
            except Exception as e:
                print(f"Cleanup task error: {e}")
                await asyncio.sleep(60)
    
    def get_queue_pressure(self):
        return self.queue_manager.get_queue_pressure()
    
    def get_stats(self):
        stats = self.queue_manager.get_stats()
        stats['active_workers'] = len(self.task_workers)
        stats['fair_scheduling'] = self.fair_scheduler.get_scheduling_stats()
        return stats
    
    async def shutdown(self):
        for worker in self.task_workers:
            worker.cancel()
        
        await asyncio.gather(*self.task_workers, return_exceptions=True)
        print(f"Priority queue workers shutdown complete")
