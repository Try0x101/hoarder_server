from .simple_processor import TaskPriority, timeout_manager
from .simple_manager import SimpleTaskManager

PriorityQueueManager = SimpleTaskManager
AdaptiveTimeoutManager = type('AdaptiveTimeoutManager', (), {
    'get_degradation_mode': lambda self, queue_pressure=0.0: timeout_manager.get_degradation_mode(queue_pressure),
    'get_timeout_stats': lambda self, queue_pressure=0.0: timeout_manager.get_timeout_stats(queue_pressure)
})

__all__ = [
    'TaskPriority', 
    'PriorityQueueManager', 
    'AdaptiveTimeoutManager',
    'timeout_manager'
]
