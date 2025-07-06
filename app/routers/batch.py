from fastapi import APIRouter, Request
from app.responses import PrettyJSONResponse
from app.api.batch.handlers import handle_batch_upload, handle_delta_batch

router = APIRouter()

@router.post("/api/batch", response_class=PrettyJSONResponse)
async def receive_batch_data(request: Request):
    return await handle_batch_upload(request)

@router.post("/api/batch-delta", response_class=PrettyJSONResponse)
async def receive_batch_delta_data(request: Request):
    return await handle_delta_batch(request)

@router.get("/api/batch/stats")
async def batch_stats():
    from app.batch import stream_processor, delta_processor
    
    return {
        "stream_processor_stats": stream_processor.get_processor_stats(),
        "delta_processor_stats": delta_processor.get_delta_stats(),
        "memory_thresholds": {
            "critical_threshold": 0.9,
            "delta_threshold": 0.85,
            "max_items": 5000,
            "max_size_mb": 50
        }
    }
