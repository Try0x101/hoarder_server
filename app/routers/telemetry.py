# app/routers/telemetry.py
import json,datetime
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
 is_online='timestamp' not in data and 'batch_timestamp' not in data
 data=await enrich_with_weather_data(data)
 background_tasks.add_task(save_data,data)
 data_timestamp=datetime.datetime.now()
 if 'timestamp' in data:
  try:data_timestamp=datetime.datetime.fromisoformat(data['timestamp'].replace('Z','+00:00'))
  except:pass
 elif 'batch_timestamp' in data:
  try:data_timestamp=datetime.datetime.fromisoformat(data['batch_timestamp'].replace('Z','+00:00'))
  except:pass
 background_tasks.add_task(save_timestamped_data,data,data_timestamp,is_offline=not is_online)
 return{"status":"received","timestamp":datetime.datetime.now().isoformat(),"device_id":data.get("device_id") or data.get("id"),"source_ip":data.get("source_ip"),"data_size_bytes":len(raw),"has_coordinates":bool(data.get("lat") and data.get("lon")),"weather_enriched":bool(any(k.startswith('weather_') for k in data.keys())),"is_online":is_online,"data_timestamp":data_timestamp.isoformat()}