import datetime
from fastapi import APIRouter, Request, HTTPException
from fastapi.responses import StreamingResponse
from app.validation import decode_raw_data
from app.batch import BatchMemoryManager, StreamProcessor, DeltaProcessor
from app.responses import PrettyJSONResponse

router = APIRouter()

batch_memory_manager = BatchMemoryManager()
stream_processor = StreamProcessor(batch_memory_manager)
delta_processor = DeltaProcessor(batch_memory_manager)

@router.post("/api/batch", response_class=PrettyJSONResponse)
async def receive_batch_data(request: Request):
    system_pressure = stream_processor.memory_manager.get_system_memory_pressure()
    if system_pressure == "CRITICAL":
        raise HTTPException(status_code=503, detail=f"Server memory critical: {system_pressure}")

    memory_stats = stream_processor.memory_manager.get_memory_stats()
    if memory_stats['memory_pressure'] > 0.9:
        raise HTTPException(status_code=503, detail="Batch processing memory exhausted")

    raw_body = await request.body()
    content_length = len(raw_body)

    if content_length > 50 * 1024 * 1024:
        raise HTTPException(status_code=413, detail="Request too large, max 50MB")

    try:
        batch_data = await decode_raw_data(raw_body)
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid data format")

    if not isinstance(batch_data, list):
        raise HTTPException(status_code=400, detail="Expected JSON array")

    batch_id = f"batch_{datetime.datetime.now(datetime.timezone.utc).strftime('%Y%m%d%H%M%S')}_{hash(str(request.client.host)) % 10000:04d}"
    source_ip = request.client.host if request.client else None
    user_agent = request.headers.get('user-agent')

    return StreamingResponse(
        stream_processor.process_batch_stream(batch_data, source_ip, user_agent, batch_id),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "Connection": "keep-alive"}
    )

@router.post("/api/batch-delta", response_class=PrettyJSONResponse)
async def receive_batch_delta_data(request: Request):
    system_pressure = delta_processor.memory_manager.get_system_memory_pressure()
    if system_pressure == "CRITICAL":
        raise HTTPException(status_code=503, detail=f"Server memory critical: {system_pressure}")

    memory_stats = delta_processor.memory_manager.get_memory_stats()
    if memory_stats['memory_pressure'] > 0.85:
        raise HTTPException(status_code=503, detail="Delta batch processing memory exhausted")

    try:
        delta_batch = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid JSON")

    is_valid, message = delta_processor.validate_delta_batch(delta_batch)
    if not is_valid:
        raise HTTPException(status_code=400, detail=message)

    sorted_deltas = delta_processor.sort_deltas_chronologically(delta_batch)
    source_ip = request.client.host if request.client else None
    user_agent = request.headers.get('user-agent')

    return StreamingResponse(
        delta_processor.process_delta_stream(sorted_deltas, source_ip, user_agent),
        media_type="text/event-stream"
    )

@router.get("/api/batch/stats")
async def batch_stats():
    return {
        "stream_processor_stats": stream_processor.get_processor_stats(),
        "delta_processor_stats": delta_processor.get_delta_stats()
    }
