import datetime
from fastapi import Request, HTTPException
from fastapi.responses import StreamingResponse
from app.validation import decode_raw_data
from app.batch import BatchMemoryManager, StreamProcessor, DeltaProcessor
from app.responses import PrettyJSONResponse
from .validation import validate_batch_structure

batch_memory_manager = BatchMemoryManager()
stream_processor = StreamProcessor(batch_memory_manager)
delta_processor = DeltaProcessor(batch_memory_manager)

def extract_request_metadata(request: Request):
    return {
        'source_ip': request.client.host if request.client else 'unknown',
        'user_agent': request.headers.get('user-agent', 'unknown'),
        'content_length': request.headers.get('content-length'),
        'content_type': request.headers.get('content-type'),
        'x_forwarded_for': request.headers.get('x-forwarded-for'),
        'x_real_ip': request.headers.get('x-real-ip')
    }

async def handle_batch_upload(request: Request):
    system_pressure = stream_processor.memory_manager.get_system_memory_pressure()
    if system_pressure == "CRITICAL":
        raise HTTPException(status_code=503, detail=f"Server memory critical: {system_pressure}")

    memory_stats = stream_processor.memory_manager.get_memory_stats()
    if memory_stats['memory_pressure'] > 0.9:
        raise HTTPException(status_code=503, detail="Batch processing memory exhausted")

    raw_body = await request.body()
    content_length = len(raw_body)

    if content_length == 0:
        raise HTTPException(status_code=400, detail="Empty request body")

    if content_length > 50 * 1024 * 1024:
        raise HTTPException(status_code=413, detail="Request too large, max 50MB")

    try:
        batch_data = await decode_raw_data(raw_body)
        if "error" in batch_data:
            raise HTTPException(status_code=400, detail=f"Decode error: {batch_data['error']}")
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Invalid data format: {str(e)}")

    validation_result = validate_batch_structure(batch_data, "batch")

    batch_id = f"batch_{datetime.datetime.now(datetime.timezone.utc).strftime('%Y%m%d%H%M%S')}_{hash(str(request.client.host)) % 10000:04d}"
    request_metadata = extract_request_metadata(request)

    return StreamingResponse(
        stream_processor.process_batch_stream(batch_data, request_metadata['source_ip'], request_metadata['user_agent'], batch_id),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache", 
            "Connection": "keep-alive",
            "X-Batch-Items": str(validation_result['total_items']),
            "X-Batch-Valid": str(validation_result['valid_items']),
            "X-Batch-Errors": str(validation_result['error_count'])
        }
    )

async def handle_delta_batch(request: Request):
    system_pressure = delta_processor.memory_manager.get_system_memory_pressure()
    if system_pressure == "CRITICAL":
        raise HTTPException(status_code=503, detail=f"Server memory critical: {system_pressure}")

    memory_stats = delta_processor.memory_manager.get_memory_stats()
    if memory_stats['memory_pressure'] > 0.85:
        raise HTTPException(status_code=503, detail="Delta batch processing memory exhausted")

    try:
        raw_body = await request.body()
        if len(raw_body) == 0:
            raise HTTPException(status_code=400, detail="Empty request body")
        
        delta_batch = await decode_raw_data(raw_body)
        if "error" in delta_batch:
            raise HTTPException(status_code=400, detail=f"Decode error: {delta_batch['error']}")
            
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Invalid JSON: {str(e)}")

    validation_result = validate_batch_structure(delta_batch, "delta batch")

    is_valid, message = delta_processor.validate_delta_batch(delta_batch)
    if not is_valid:
        raise HTTPException(status_code=400, detail=f"Delta validation failed: {message}")

    sorted_deltas = delta_processor.sort_deltas_chronologically(delta_batch)
    request_metadata = extract_request_metadata(request)

    return StreamingResponse(
        delta_processor.process_delta_stream(sorted_deltas, request_metadata['source_ip'], request_metadata['user_agent']),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive", 
            "X-Delta-Items": str(validation_result['total_items']),
            "X-Delta-Valid": str(validation_result['valid_items'])
        }
    )
