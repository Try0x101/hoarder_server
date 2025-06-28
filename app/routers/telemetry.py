import json,datetime,copy
from fastapi import APIRouter,Request,BackgroundTasks
from app.responses import PrettyJSONResponse
from app.utils import decode_raw_data,enrich_with_weather_data
from app.db import save_data,save_timestamped_data

router=APIRouter()

@router.post("/api/telemetry",response_class=PrettyJSONResponse)
async def receive_telemetry(request:Request,background_tasks:BackgroundTasks):
    raw=await request.body()
    data=await decode_raw_data(raw)
    data['source_ip']=request.client.host if request.client else None
    data['server_received_at']=datetime.datetime.now().isoformat()
    
    payload_for_history = copy.deepcopy(data)
    
    data_timestamp=datetime.datetime.now(datetime.timezone.utc)
    if 'timestamp' in data:
        try:data_timestamp=datetime.datetime.fromisoformat(data['timestamp'].replace('Z','+00:00'))
        except:pass
    
    background_tasks.add_task(save_timestamped_data, payload_for_history, data_timestamp, is_offline=False)
    
    enriched_payload = await enrich_with_weather_data(data)
    background_tasks.add_task(save_data, enriched_payload)

    return {
        "status":"received",
        "timestamp":datetime.datetime.now().isoformat(),
        "device_id":data.get("device_id") or data.get("id"),
        "source_ip":data.get("source_ip"),
        "data_size_bytes":len(raw),
        "has_coordinates":bool(data.get("lat") and data.get("lon")),
        "weather_enriched":bool(any(k.startswith('weather_') for k in enriched_payload.keys())),
        "is_online":True,
        "data_timestamp":data_timestamp.isoformat()
    }