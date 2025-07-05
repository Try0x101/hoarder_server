import json
import datetime
import asyncio
import psutil
from fastapi import APIRouter, Request, HTTPException
from app.responses import PrettyJSONResponse
from app.validation import decode_raw_data, decode_maximum_compression
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

def safe_int_header(header_value):
    if header_value is None:
        return None
    try:
        return int(header_value)
    except (ValueError, TypeError):
        return None

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
        raise HTTPException(status_code=413, detail="Payload too large")
    
    compression_type = request.headers.get("x-compression-type")

    try:
        if compression_type == "maximum":
            data = await decode_maximum_compression(raw)
        else:
            data = await decode_raw_data(raw)
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid data format")

    data['source_ip'] = request.client.host if request.client else None
    data['user_agent'] = request.headers.get('user-agent')
    data['content_size_bytes'] = safe_int_header(request.headers.get('content-length'))
    data['x_forwarded_for'] = request.headers.get('x-forwarded-for')
    data['x_real_ip'] = request.headers.get('x-real-ip')
    data['server_received_at'] = datetime.datetime.now(datetime.timezone.utc).isoformat()

    data_timestamp = datetime.datetime.now(datetime.timezone.utc)
    if 'timestamp' in data:
        try:
            dt = datetime.datetime.fromisoformat(data['timestamp'].replace('Z','+00:00'))
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=datetime.timezone.utc)
            data_timestamp = dt
        except:
            pass

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
