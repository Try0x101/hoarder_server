import asyncio
import time
from enum import Enum
from typing import Optional

class TaskPriority(Enum):
    CRITICAL = 1
    NORMAL = 2
    HIGH = 2
    LOW = 3

class SimpleTaskProcessor:
    def __init__(self, worker_count: int = 3):
        self.worker_count = worker_count
        self.critical_queue = asyncio.Queue(maxsize=50)
        self.normal_queue = asyncio.Queue(maxsize=100)
        self.workers = []
        self.stats = {'completed': 0, 'failed': 0, 'timeout': 0, 'retried': 0, 'permanently_failed': 0, 'start_time': time.time()}
        
    async def init_workers(self):
        if self.workers:
            return
        for i in range(self.worker_count):
            worker = asyncio.create_task(self._worker(f"worker-{i}"))
            self.workers.append(worker)
    
    async def enqueue_task(self, coro, priority: TaskPriority = TaskPriority.NORMAL, timeout: float = 15.0) -> bool:
        task_data = {'coro': coro, 'timeout': timeout, 'created': time.time(), 'retries': 0, 'max_retries': 2, 'priority': priority}
        try:
            if priority == TaskPriority.CRITICAL:
                self.critical_queue.put_nowait(task_data)
            else:
                self.normal_queue.put_nowait(task_data)
            return True
        except asyncio.QueueFull:
            return False
    
    async def _worker(self, worker_id: str):
        while True:
            try:
                task_data = None
                try:
                    task_data = self.critical_queue.get_nowait()
                except asyncio.QueueEmpty:
                    try:
                        task_data = await asyncio.wait_for(self.normal_queue.get(), timeout=0.1)
                    except asyncio.TimeoutError:
                        continue
                if task_data:
                    await self._execute_task(task_data)
            except Exception as e:
                print(f"Worker {worker_id} error: {e}")
                await asyncio.sleep(0.5)
    
    async def _execute_task(self, task_data):
        try:
            await asyncio.wait_for(task_data['coro'], timeout=task_data['timeout'])
            self.stats['completed'] += 1
        except (asyncio.TimeoutError, Exception) as e:
            if isinstance(e, asyncio.TimeoutError):
                self.stats['timeout'] += 1
            else:
                self.stats['failed'] += 1
            if task_data.get('retries', 0) < task_data.get('max_retries', 1):
                task_data['retries'] += 1
                self.stats['retried'] += 1
                await asyncio.sleep(0.1 * task_data['retries'])
                await self.enqueue_task(task_data['coro'], task_data['priority'], task_data['timeout'])
            else:
                self.stats['permanently_failed'] += 1
    
    def get_queue_pressure(self):
        critical_size = self.critical_queue.qsize()
        normal_size = self.normal_queue.qsize()
        total_capacity = 150
        current_load = critical_size + normal_size
        return current_load / total_capacity
    
    def get_stats(self):
        return {
            'critical_queue_size': self.critical_queue.qsize(),
            'normal_queue_size': self.normal_queue.qsize(),
            'active_workers': len(self.workers),
            'queue_pressure': f"{self.get_queue_pressure():.2f}",
            'performance_stats': self.stats.copy()
        }
    
    async def shutdown(self):
        for worker in self.workers:
            worker.cancel()
        await asyncio.gather(*self.workers, return_exceptions=True)

class AdaptiveTimeoutManager:
    def get_degradation_mode(self, queue_pressure: float = 0.0):
        if queue_pressure > 0.8:
            return "CRITICAL"
        elif queue_pressure > 0.5:
            return "HIGH"
        elif queue_pressure > 0.3:
            return "MEDIUM"
        return "LOW"
    
    def get_timeout_stats(self, queue_pressure: float = 0.0):
        return {'degradation_mode': self.get_degradation_mode(queue_pressure), 'queue_pressure': f"{queue_pressure:.2f}"}

class PriorityQueueManager:
    def __init__(self):
        self.processor = SimpleTaskProcessor()
        self.task_queues = {TaskPriority.CRITICAL: self.processor.critical_queue, TaskPriority.NORMAL: self.processor.normal_queue}
        
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

task_processor = SimpleTaskProcessor(worker_count=3)
timeout_manager = AdaptiveTimeoutManager()
