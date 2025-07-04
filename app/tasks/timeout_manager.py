import psutil
from .scheduler import TaskPriority

class AdaptiveTimeoutManager:
    def __init__(self):
        self.base_timeouts = {
            'database_write': 25,
            'weather_enrichment': 8,
            'state_update': 15,
            'batch_processing': 30,
            'default': 12
        }
        self.load_multipliers = {
            'low': 1.0,
            'medium': 1.3,
            'high': 1.6,
            'critical': 2.0
        }
        
    def get_system_load_level(self):
        memory_mb = psutil.Process().memory_info().rss / 1024 / 1024
        
        if memory_mb > 300:
            return 'critical'
        elif memory_mb > 250:
            return 'high'
        elif memory_mb > 200:
            return 'medium'
        return 'low'
    
    def get_timeout_for_task(self, task_type: str, priority: TaskPriority, queue_pressure: float = 0.0):
        base_timeout = self.base_timeouts.get(task_type, self.base_timeouts['default'])
        load_level = self.get_system_load_level()
        load_multiplier = self.load_multipliers[load_level]
        
        priority_modifier = 1.0
        if priority == TaskPriority.CRITICAL:
            priority_modifier = 1.5
        elif priority == TaskPriority.HIGH:
            priority_modifier = 1.2
        elif priority == TaskPriority.LOW:
            priority_modifier = 0.8
        
        queue_modifier = 1.0 + (queue_pressure * 0.5)
            
        final_timeout = base_timeout * load_multiplier * priority_modifier * queue_modifier
        return min(final_timeout, 45)
    
    def get_degradation_mode(self, queue_pressure: float = 0.0):
        load_level = self.get_system_load_level()
        
        if queue_pressure > 0.85 or load_level == 'critical':
            return "CRITICAL"
        elif queue_pressure > 0.7 or load_level == 'high':
            return "HIGH"
        elif queue_pressure > 0.5 or load_level == 'medium':
            return "MEDIUM"
        return "LOW"
    
    def should_reject_request(self, degradation_mode: str, task_priority: TaskPriority):
        if degradation_mode == "CRITICAL":
            return task_priority not in [TaskPriority.CRITICAL]
        elif degradation_mode == "HIGH":
            return task_priority == TaskPriority.LOW
        return False
    
    def get_timeout_stats(self, queue_pressure: float = 0.0):
        load_level = self.get_system_load_level()
        degradation_mode = self.get_degradation_mode(queue_pressure)
        
        return {
            'system_load': load_level,
            'degradation_mode': degradation_mode,
            'queue_pressure': f"{queue_pressure:.2f}",
            'base_timeouts': self.base_timeouts.copy(),
            'load_multipliers': self.load_multipliers.copy(),
            'memory_mb': f"{psutil.Process().memory_info().rss / 1024 / 1024:.1f}"
        }
