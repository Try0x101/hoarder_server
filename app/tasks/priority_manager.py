import asyncio
import datetime
import gc
import time
import weakref
from .scheduler import TaskPriority, PriorityTask, FairTaskScheduler, classify_task_type

MAX_CRITICAL_TASKS = 15
MAX_HIGH_TASKS = 12
MAX_NORMAL_TASKS = 8
MAX_LOW_TASKS = 5
TASK_AGE_LIMIT = 45
QUEUE_CLEANUP_INTERVAL = 20

class PriorityQueueManager:
    def __init__(self):
        self.task_queues = {
            TaskPriority.CRITICAL: asyncio.Queue(maxsize=MAX_CRITICAL_TASKS),
            TaskPriority.HIGH: asyncio.Queue(maxsize=MAX_HIGH_TASKS),
            TaskPriority.NORMAL: asyncio.Queue(maxsize=MAX_NORMAL_TASKS),
            TaskPriority.LOW: asyncio.Queue(maxsize=MAX_LOW_TASKS)
        }
        self.active_tasks = weakref.WeakSet()
        self.task_workers = []
        self.worker_count = 3
        self.stats = {
            'completed': 0,
            'failed': 0,
            'timeout': 0,
            'retry': 0,
            'dropped': 0,
            'starvation_prevented': 0,
            'start_time': time.time()
        }
        self.last_cleanup = 0
        self.fair_scheduler = FairTaskScheduler()
        
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
        
        queue = self.task_queues[priority]
        
        try:
            task = PriorityTask(coro, priority, task_id)
            queue.put_nowait(task)
            return True
        except asyncio.QueueFull:
            self.stats['dropped'] += 1
            
            if priority == TaskPriority.CRITICAL:
                try:
                    low_queue = self.task_queues[TaskPriority.LOW]
                    try:
                        low_queue.get_nowait()
                        queue.put_nowait(task)
                        return True
                    except asyncio.QueueEmpty:
                        pass
                except asyncio.QueueFull:
                    pass
            
            return False
    
    async def _task_worker(self, worker_id: str):
        consecutive_empty_cycles = 0
        
        while True:
            try:
                task = None
                selected_priority = self.fair_scheduler.get_next_priority()
                
                if selected_priority:
                    queue = self.task_queues[selected_priority]
                    try:
                        task = queue.get_nowait()
                        consecutive_empty_cycles = 0
                    except asyncio.QueueEmpty:
                        pass
                
                if not task:
                    for priority in TaskPriority:
                        queue = self.task_queues[priority]
                        try:
                            task = queue.get_nowait()
                            selected_priority = priority
                            if priority != self.fair_scheduler.get_next_priority():
                                self.stats['starvation_prevented'] += 1
                            break
                        except asyncio.QueueEmpty:
                            continue
                
                if not task:
                    consecutive_empty_cycles += 1
                    sleep_time = min(0.1, 0.01 * consecutive_empty_cycles)
                    await asyncio.sleep(sleep_time)
                    continue
                
                if time.time() - task.created_at > TASK_AGE_LIMIT:
                    self.stats['dropped'] += 1
                    continue
                
                self.fair_scheduler.record_processing(selected_priority)
                
                task_type = classify_task_type(task.task_id)
                timeout_duration = self._get_task_timeout(task_type, selected_priority)
                
                try:
                    await asyncio.wait_for(task.coro, timeout=timeout_duration)
                    self.stats['completed'] += 1
                    
                except asyncio.TimeoutError:
                    self.stats['timeout'] += 1
                    
                    if task.retries < task.max_retries and selected_priority in [TaskPriority.CRITICAL, TaskPriority.HIGH]:
                        task.retries += 1
                        retry_id = f"{task.task_id}_r{task.retries}"
                        
                        new_task = PriorityTask(task.coro, selected_priority, retry_id)
                        new_task.retries = task.retries
                        
                        try:
                            self.task_queues[selected_priority].put_nowait(new_task)
                            self.stats['retry'] += 1
                        except asyncio.QueueFull:
                            self.stats['dropped'] += 1
                
                except Exception as e:
                    self.stats['failed'] += 1
                    print(f"[{datetime.datetime.now(datetime.timezone.utc)}] Task {task.task_id} failed: {e}")
                
                del task
                if self.stats['completed'] % 25 == 0:
                    gc.collect()
                    
            except Exception as e:
                print(f"[{datetime.datetime.now(datetime.timezone.utc)}] Worker {worker_id} error: {e}")
                await asyncio.sleep(0.5)
    
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
    
    async def _periodic_cleanup(self):
        while True:
            try:
                current_time = time.time()
                if current_time - self.last_cleanup > QUEUE_CLEANUP_INTERVAL:
                    for priority, queue in self.task_queues.items():
                        cleaned = 0
                        temp_tasks = []
                        
                        while not queue.empty():
                            try:
                                task = queue.get_nowait()
                                if current_time - task.created_at <= TASK_AGE_LIMIT and task.retries < 5:
                                    temp_tasks.append(task)
                                else:
                                    cleaned += 1
                            except asyncio.QueueEmpty:
                                break
                        
                        for task in temp_tasks:
                            try:
                                queue.put_nowait(task)
                            except asyncio.QueueFull:
                                break
                        
                        if cleaned > 0:
                            print(f"[{datetime.datetime.now(datetime.timezone.utc)}] Cleaned {cleaned} aged tasks from {priority.name} queue")
                    
                    self.last_cleanup = current_time
                    gc.collect()
                
                await asyncio.sleep(QUEUE_CLEANUP_INTERVAL)
                
            except Exception as e:
                print(f"[{datetime.datetime.now(datetime.timezone.utc)}] Cleanup task error: {e}")
                await asyncio.sleep(30)
    
    def get_queue_pressure(self):
        total_tasks = sum(queue.qsize() for queue in self.task_queues.values())
        total_capacity = sum([MAX_CRITICAL_TASKS, MAX_HIGH_TASKS, MAX_NORMAL_TASKS, MAX_LOW_TASKS])
        return total_tasks / total_capacity
    
    def get_stats(self):
        queue_stats = {priority.name: queue.qsize() for priority, queue in self.task_queues.items()}
        scheduling_stats = self.fair_scheduler.get_scheduling_stats()
        
        return {
            'queue_pressure': f"{self.get_queue_pressure():.2f}",
            'active_workers': len(self.task_workers),
            'queue_stats': queue_stats,
            'performance_stats': self.stats.copy(),
            'fair_scheduling': scheduling_stats
        }
    
    async def shutdown(self):
        for worker in self.task_workers:
            worker.cancel()
        
        await asyncio.gather(*self.task_workers, return_exceptions=True)
        print(f"[{datetime.datetime.now(datetime.timezone.utc)}] Priority queue workers shutdown complete")
