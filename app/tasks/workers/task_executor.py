import asyncio
import time
import gc
from ..scheduler import TaskPriority, classify_task_type

class TaskExecutor:
    def __init__(self, queue_manager, fair_scheduler):
        self.queue_manager = queue_manager
        self.fair_scheduler = fair_scheduler
        
    async def execute_task(self, task, selected_priority: TaskPriority):
        self.fair_scheduler.record_processing(selected_priority)
        
        task_type = classify_task_type(task.task_id)
        timeout_duration = self._get_task_timeout(task_type, selected_priority)
        
        try:
            await asyncio.wait_for(task.coro, timeout=timeout_duration)
            self.queue_manager.record_completion()
            
        except asyncio.TimeoutError:
            self.queue_manager.record_timeout()
            
            if task.retries < task.max_retries and selected_priority in [TaskPriority.CRITICAL, TaskPriority.HIGH]:
                await self._retry_task(task, selected_priority)
        
        except Exception as e:
            self.queue_manager.record_failure()
            print(f"Task {task.task_id} failed: {e}")
        
        del task
        if self.queue_manager.stats['completed'] % 25 == 0:
            gc.collect()
    
    async def _retry_task(self, task, priority):
        task.retries += 1
        retry_id = f"{task.task_id}_r{task.retries}"
        
        from ..scheduler import PriorityTask
        new_task = PriorityTask(task.coro, priority, retry_id)
        new_task.retries = task.retries
        
        if await self.queue_manager.enqueue_task(new_task, priority):
            self.queue_manager.record_retry()
        else:
            self.queue_manager.stats['dropped'] += 1
    
    def _get_task_timeout(self, task_type: str, priority: TaskPriority):
        base_timeouts = {
            'database_write': 25,
            'weather_enrichment': 8,
            'state_update': 15,
            'batch_processing': 30,
            'default': 12
        }
        
        base_timeout = base_timeouts.get(task_type, base_timeouts['default'])
        
        priority_modifier = 1.0
        if priority == TaskPriority.CRITICAL:
            priority_modifier = 1.5
        elif priority == TaskPriority.HIGH:
            priority_modifier = 1.2
        elif priority == TaskPriority.LOW:
            priority_modifier = 0.8
            
        return min(base_timeout * priority_modifier, 45)
