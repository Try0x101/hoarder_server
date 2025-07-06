from ..scheduler import TaskPriority

MAX_CRITICAL_TASKS = 15
MAX_HIGH_TASKS = 12
MAX_NORMAL_TASKS = 8
MAX_LOW_TASKS = 5
TASK_AGE_LIMIT = 45
QUEUE_CLEANUP_INTERVAL = 20

class TaskLimits:
    def __init__(self):
        self.limits = {
            TaskPriority.CRITICAL: MAX_CRITICAL_TASKS,
            TaskPriority.HIGH: MAX_HIGH_TASKS,
            TaskPriority.NORMAL: MAX_NORMAL_TASKS,
            TaskPriority.LOW: MAX_LOW_TASKS
        }
        self.task_age_limit = TASK_AGE_LIMIT
        self.cleanup_interval = QUEUE_CLEANUP_INTERVAL
        
    def get_limit(self, priority: TaskPriority) -> int:
        return self.limits.get(priority, MAX_NORMAL_TASKS)
        
    def get_total_capacity(self) -> int:
        return sum(self.limits.values())
        
    def is_task_aged(self, task_age: float) -> bool:
        return task_age > self.task_age_limit
        
    def should_cleanup(self, last_cleanup: float, current_time: float) -> bool:
        return current_time - last_cleanup > self.cleanup_interval
