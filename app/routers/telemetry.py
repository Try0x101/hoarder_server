import datetime
import psutil
from fastapi import APIRouter, Request
from app.responses import PrettyJSONResponse
from app.api.telemetry.handler import handle_telemetry_request
from app.tasks import PriorityQueueManager

router = APIRouter()

priority_queue_manager = PriorityQueueManager()

@router.post("/api/telemetry", response_class=PrettyJSONResponse)
async def receive_telemetry(request: Request):
    return await handle_telemetry_request(request)

@router.get("/api/system/stats")
async def system_stats():
    queue_pressure = priority_queue_manager.get_queue_pressure()
    
    return {
        "task_management": {
            **priority_queue_manager.get_stats(),
        },
        "memory_usage_mb": f"{psutil.Process().memory_info().rss / 1024 / 1024:.1f}",
        "uptime_seconds": int(datetime.datetime.now().timestamp() - priority_queue_manager.stats.get('start_time', datetime.datetime.now().timestamp()))
    }

@router.on_event("startup")
async def startup_telemetry():
    await priority_queue_manager.init_workers()
    print(f"[{datetime.datetime.now(datetime.timezone.utc)}] Telemetry with fair scheduling: {priority_queue_manager.worker_count} workers, weighted queues")

@router.on_event("shutdown")
async def shutdown_telemetry():
    await priority_queue_manager.shutdown()
