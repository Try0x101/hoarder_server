import json
import datetime
import asyncio
import psutil
from fastapi import APIRouter, Request, HTTPException
from app.responses import PrettyJSONResponse
from app.validation import decode_raw_data, decode_maximum_compression, validate_device_data
from app.weather import enrich_with_weather_data
from app.db import save_timestamped_data, upsert_latest_state
from app.tasks import PriorityQueueManager, TaskPriority, AdaptiveTimeoutManager

router = APIRouter()

priority_queue_manager = PriorityQueueManager()
timeout_manager = AdaptiveTimeoutManager()

async def critical_data_storage(data: dict, data_timestamp: datetime.datetime):
    try:
        await save_timestamped_data(data, data_timestamp, is_offline=False)
    except Exception as e:
        print(f"[{datetime.datetime.now(datetime.timezone.utc)}] CRITICAL: Data storage failed: {e}")
        raise

async def weather_enrichment_and_state_update(data: dict):
    try:
        if data.get("lat") and data.get("lon"):
            try:
                enriched_payload = await asyncio.wait_for(enrich_with_weather_data(data), timeout=6)
                await upsert_latest_state(enriched_payload)
            except asyncio.TimeoutError:
                await upsert_latest_state(data)
            except Exception:
                await upsert_latest_state(data)
        else:
            await upsert_latest_state(data)
    except Exception as e:
        print(f"[{datetime.datetime.now(datetime.timezone.utc)}] ERROR in state update: {e}")

def safe_header_value(header_value, default=None):
    if header_value is None:
        return default
    try:
        return str(header_value).strip()
    except (ValueError, TypeError):
        return default

def safe_int_header(header_value, default=None):
    if header_value is None:
        return default
    try:
        return int(header_value)
    except (ValueError, TypeError):
        return default

def extract_client_info(request: Request):
    return {
        'source_ip': request.client.host if request.client else None,
        'user_agent': safe_header_value(request.headers.get('user-agent')),
        'content_size_bytes': safe_int_header(request.headers.get('content-length')),
        'x_forwarded_for': safe_header_value(request.headers.get('x-forwarded-for')),
        'x_real_ip': safe_header_value(request.headers.get('x-real-ip')),
        'content_type': safe_header_value(request.headers.get('content-type')),
        'content_encoding': safe_header_value(request.headers.get('content-encoding')),
        'server_received_at': datetime.datetime.now(datetime.timezone.utc).isoformat()
    }

def parse_device_timestamp(data: dict) -> datetime.datetime:
    data_timestamp = datetime.datetime.now(datetime.timezone.utc)
    
    timestamp_fields = ['timestamp', 'ts', 'time', 'datetime']
    
    for field in timestamp_fields:
        if field in data and data[field]:
            try:
                ts_value = data[field]
                
                if isinstance(ts_value, (int, float)):
                    if ts_value > 1000000000000:
                        ts_value = ts_value / 1000
                    data_timestamp = datetime.datetime.fromtimestamp(ts_value, tz=datetime.timezone.utc)
                    break
                
                if isinstance(ts_value, str):
                    ts_value = ts_value.strip()
                    
                    if ts_value.replace('.', '').isdigit():
                        unix_ts = float(ts_value)
                        if unix_ts > 1000000000000:
                            unix_ts = unix_ts / 1000
                        data_timestamp = datetime.datetime.fromtimestamp(unix_ts, tz=datetime.timezone.utc)
                        break
                    
                    dt = datetime.datetime.fromisoformat(ts_value.replace('Z', '+00:00'))
                    if dt.tzinfo is None:
                        dt = dt.replace(tzinfo=datetime.timezone.utc)
                    data_timestamp = dt
                    break
                    
            except (ValueError, TypeError, OSError):
                continue
    
    return data_timestamp

@router.post("/api/telemetry", response_class=PrettyJSONResponse)
async def receive_telemetry(request: Request):
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
            "state_task_enqueued": state_enqueued,
            **priority_queue_manager.get_stats(),
            **timeout_manager.get_timeout_stats(queue_pressure)
        }
    }

@router.get("/api/system/stats")
async def system_stats():
    queue_pressure = priority_queue_manager.get_queue_pressure()
    
    return {
        "task_management": {
            **priority_queue_manager.get_stats(),
            **timeout_manager.get_timeout_stats(queue_pressure)
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
