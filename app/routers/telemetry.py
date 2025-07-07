from fastapi import APIRouter, Request
from app.responses import PrettyJSONResponse
from app.api.telemetry.handler import handle_telemetry_request

router = APIRouter()

@router.post("/api/telemetry", response_class=PrettyJSONResponse)
async def receive_telemetry(request: Request):
    return await handle_telemetry_request(request)

@router.on_event("startup")
async def startup_telemetry():
    from app.tasks import PriorityQueueManager
    priority_queue_manager = PriorityQueueManager()
    await priority_queue_manager.init_workers()

@router.on_event("shutdown")
async def shutdown_telemetry():
    from app.tasks import PriorityQueueManager
    priority_queue_manager = PriorityQueueManager()
    await priority_queue_manager.shutdown()
