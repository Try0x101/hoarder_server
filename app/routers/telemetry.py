import json
import datetime
import uuid
from fastapi import APIRouter, Request, BackgroundTasks
from fastapi.responses import JSONResponse
from app.utils import decode_raw_data, enrich_with_weather_data
from app.db import save_data, save_timestamped_data

class PrettyJSONResponse(JSONResponse):
    def render(self, content):
        return json.dumps(
            content,
            ensure_ascii=False,
            allow_nan=False,
            indent=2,
            separators=(",", ": ")
        ).encode("utf-8")

router = APIRouter()

@router.post("/api/telemetry", response_class=PrettyJSONResponse)
async def receive_telemetry(request: Request, background_tasks: BackgroundTasks):
    raw = await request.body()
    data = await decode_raw_data(raw)
    
    # Add server metadata
    data['source_ip'] = request.client.host if request.client else None
    data['server_received_at'] = datetime.datetime.now().isoformat()
    
    # Determine if this is online data (no client timestamp)
    is_online = 'timestamp' not in data and 'batch_timestamp' not in data
    
    # Enrich with weather data
    data = await enrich_with_weather_data(data)
    
    # Save to both old format and new timestamped format
    background_tasks.add_task(save_data, data)
    
    # For timestamped storage
    data_timestamp = datetime.datetime.now()  # Default to server time
    if 'timestamp' in data:
        try:
            data_timestamp = datetime.datetime.fromisoformat(data['timestamp'].replace('Z', '+00:00'))
        except:
            pass
    elif 'batch_timestamp' in data:
        try:
            data_timestamp = datetime.datetime.fromisoformat(data['batch_timestamp'].replace('Z', '+00:00'))
        except:
            pass
    
    background_tasks.add_task(
        save_timestamped_data, 
        data, 
        data_timestamp, 
        is_offline=not is_online
    )
    
    return {
        "status": "received",
        "timestamp": datetime.datetime.now().isoformat(),
        "device_id": data.get("device_id") or data.get("id"),
        "source_ip": data.get("source_ip"),
        "data_size_bytes": len(raw),
        "has_coordinates": bool(data.get("lat") and data.get("lon")),
        "weather_enriched": bool(any(k.startswith('weather_') for k in data.keys())),
        "is_online": is_online,
        "data_timestamp": data_timestamp.isoformat()
    }