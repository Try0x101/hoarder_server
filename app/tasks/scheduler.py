import time
import asyncio
import datetime
from enum import Enum
from typing import Optional

class TaskPriority(Enum):
    CRITICAL = 1
    HIGH = 2
    NORMAL = 3
    LOW = 4

class PriorityTask:
    def __init__(self, coro, priority: TaskPriority, task_id: str):
        self.coro = coro
        self.priority = priority
        self.task_id = task_id[:50]
        self.created_at = time.time()
        self.retries = 0
        self.max_retries = 1

class FairTaskScheduler:
    def __init__(self):
        self.weights = {
            TaskPriority.CRITICAL: 60,
            TaskPriority.HIGH: 25,
            TaskPriority.NORMAL: 10,
            TaskPriority.LOW: 5
        }
        self.processing_counters = {p: 0 for p in TaskPriority}
        self.total_processed = 0
        self.reset_interval = 100
        self.last_reset = time.time()
        
    def get_next_priority(self) -> Optional[TaskPriority]:
        current_time = time.time()
        if current_time - self.last_reset > 60 or self.total_processed >= self.reset_interval:
            self._reset_counters()
            self.last_reset = current_time
        
        for priority in TaskPriority:
            current_ratio = self.processing_counters[priority] / max(1, self.total_processed)
            target_ratio = self.weights[priority] / 100.0
            
            if current_ratio < target_ratio:
                return priority
        
        return TaskPriority.CRITICAL
    
    def record_processing(self, priority: TaskPriority):
        self.processing_counters[priority] += 1
        self.total_processed += 1
        
    def _reset_counters(self):
        self.processing_counters = {p: 0 for p in TaskPriority}
        self.total_processed = 0
        
    def get_scheduling_stats(self):
        if self.total_processed == 0:
            return {p.name: 0.0 for p in TaskPriority}
            
        return {
            p.name: {
                'processed': self.processing_counters[p],
                'current_ratio': self.processing_counters[p] / self.total_processed,
                'target_ratio': self.weights[p] / 100.0,
                'weight': self.weights[p]
            } for p in TaskPriority
        }

def classify_task_type(task_id: str) -> str:
    if 'storage_' in task_id:
        return 'database_write'
    elif 'state_' in task_id:
        return 'state_update'
    elif 'weather_' in task_id:
        return 'weather_enrichment'
    elif 'batch_' in task_id:
        return 'batch_processing'
    return 'default'
