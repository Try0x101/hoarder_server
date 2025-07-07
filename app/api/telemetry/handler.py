import datetime
import psutil
from fastapi import Request, HTTPException
from app.responses import PrettyJSONResponse
from app.processing.validation.decoders import decode_raw_data
from app.processing.validation.binary_decoder import decode_maximum_compression
from app.processing.validation.validators import validate_device_data
from app.tasks import PriorityQueueManager, TaskPriority, AdaptiveTimeoutManager
from .client_info import extract_client_info
from .timestamp_parser import parse_device_timestamp
from .processing import critical_data_storage, weather_enrichment_and_state_update

priority_queue_manager = PriorityQueueManager()
timeout_manager = AdaptiveTimeoutManager()

async def handle_telemetry_request(request: Request):
    await priority_queue_manager.init_workers()
    
    queue_pressure = priority_queue_manager.get_queue_pressure()
    degradation_mode = timeout_manager.get_degradation_mode(queue_pressure)
    
    if degradation_mode == "CRITICAL":
        critical_queue = priority_queue_manager.task_queues[TaskPriority.CRITICAL]
        if critical_queue.full():
            raise HTTPException(status_code=503, detail="Server critically overloaded")
    
    raw = await request.body()
    
    if len(raw) > 5 * 1024 * 1024:
        raise HTTPException(status_code=413, detail="Payload too large (max 5MB)")
    
    if len(raw) == 0:
        raise HTTPException(status_code=400, detail="Empty payload")
    
    compression_type = request.headers.get("x-compression-type")

    try:
        if compression_type == "maximum":
            data = await decode_maximum_compression(raw)
        else:
            data = await decode_raw_data(raw)
        
        if "error" in data:
            raise HTTPException(status_code=400, detail=f"Decode error: {data.get('error', 'Unknown')}")
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Invalid data format: {str(e)}")

    validation_result = validate_device_data(data)
    if not validation_result['is_valid']:
        raise HTTPException(status_code=400, detail=f"Validation failed: {validation_result['errors']}")

    client_info = extract_client_info(request)
    data.update(client_info)

    data_timestamp = parse_device_timestamp(data)
    device_id = data.get("device_id") or data.get("id") or "auto-generated"

    critical_task_id = f"storage_{device_id}_{int(datetime.datetime.now().timestamp() * 1000)}"
    critical_enqueued = await priority_queue_manager.enqueue_task(
        critical_data_storage(data.copy(), data_timestamp),
        TaskPriority.CRITICAL,
        critical_task_id
    )

    state_priority = TaskPriority.NORMAL
    if degradation_mode in ["CRITICAL", "HIGH"]:
        state_priority = TaskPriority.LOW
    elif degradation_mode == "MEDIUM":
        state_priority = TaskPriority.HIGH

    state_task_id = f"state_{device_id}_{int(datetime.datetime.now().timestamp() * 1000)}"
    state_enqueued = await priority_queue_manager.enqueue_task(
        weather_enrichment_and_state_update(data.copy()),
        state_priority,
        state_task_id
    )

    return {
        "status": "received",
        "timestamp": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "device_id": device_id,
        "validation_warnings": validation_result.get('warnings', []),
        "source_ip": data.get("source_ip"),
        "user_agent_detected": bool(data.get("user_agent")),
        "data_size_bytes": len(raw),
        "content_size_bytes": data.get("content_size_bytes"),
        "has_coordinates": bool(data.get("lat") and data.get("lon")),
        "data_timestamp": data_timestamp.isoformat(),
        "processing": {
            "degradation_mode": degradation_mode,
            "critical_task_enqueued": critical_enqueued,
            "state_task_enqueued": state_enqueued
        }
    }
