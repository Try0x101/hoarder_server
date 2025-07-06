import asyncio
from ..scheduler import TaskPriority

async def get_next_task(queue_manager, fair_scheduler):
    selected_priority = fair_scheduler.get_next_priority()
    
    if selected_priority:
        queue = queue_manager.task_queues[selected_priority]
        try:
            return queue.get_nowait()
        except asyncio.QueueEmpty:
            pass
    
    for priority in TaskPriority:
        queue = queue_manager.task_queues[priority]
        try:
            task = queue.get_nowait()
            if priority != selected_priority:
                queue_manager.record_starvation_prevention()
            return task
        except asyncio.QueueEmpty:
            continue
    
    return None
