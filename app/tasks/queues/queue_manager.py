import asyncio
import time
import weakref
from ..scheduler import TaskPriority
from .task_limits import TaskLimits

class QueueManager:
    def __init__(self):
        self.task_limits = TaskLimits()
        self.task_queues = {}
        self.active_tasks = weakref.WeakSet()
        self.stats = {
            'completed': 0,
            'failed': 0,
            'timeout': 0,
            'retry': 0,
            'dropped': 0,
            'starvation_prevented': 0,
            'start_time': time.time()
        }
        
        self._initialize_queues()
        
    def _initialize_queues(self):
        for priority in TaskPriority:
            limit = self.task_limits.get_limit(priority)
            self.task_queues[priority] = asyncio.Queue(maxsize=limit)
    
    async def enqueue_task(self, task, priority: TaskPriority) -> bool:
        queue = self.task_queues[priority]
        
        try:
            queue.put_nowait(task)
            return True
        except asyncio.QueueFull:
            self.stats['dropped'] += 1
            
            if priority == TaskPriority.CRITICAL:
                if await self._try_critical_fallback(task):
                    return True
            
            return False
    
    async def _try_critical_fallback(self, task):
        try:
            low_queue = self.task_queues[TaskPriority.LOW]
            try:
                low_queue.get_nowait()
                self.task_queues[TaskPriority.CRITICAL].put_nowait(task)
                return True
            except asyncio.QueueEmpty:
                pass
        except asyncio.QueueFull:
            pass
        return False
    
    def get_queue_pressure(self):
        total_tasks = sum(queue.qsize() for queue in self.task_queues.values())
        total_capacity = self.task_limits.get_total_capacity()
        return total_tasks / total_capacity
    
    def get_stats(self):
        queue_stats = {priority.name: queue.qsize() for priority, queue in self.task_queues.items()}
        
        return {
            'queue_pressure': f"{self.get_queue_pressure():.2f}",
            'queue_stats': queue_stats,
            'performance_stats': self.stats.copy()
        }
    
    def record_completion(self):
        self.stats['completed'] += 1
        
    def record_failure(self):
        self.stats['failed'] += 1
        
    def record_timeout(self):
        self.stats['timeout'] += 1
        
    def record_retry(self):
        self.stats['retry'] += 1
        
    def record_starvation_prevention(self):
        self.stats['starvation_prevented'] += 1
