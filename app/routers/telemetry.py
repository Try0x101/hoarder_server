import json,datetime
from fastapi import APIRouter,Request,BackgroundTasks
from fastapi.responses import JSONResponse
from app.utils import decode_raw_data,enrich_with_weather_data
from app.db import save_data
class PrettyJSONResponse(JSONResponse):
 def render(self,content):return json.dumps(content,ensure_ascii=False,allow_nan=False,indent=2,separators=(",",": ")).encode("utf-8")
router=APIRouter()
@router.post("/api/telemetry",response_class=PrettyJSONResponse)
async def receive_telemetry(request:Request,background_tasks:BackgroundTasks):
 raw=await request.body()
 data=await decode_raw_data(raw)
 data['source_ip']=request.client.host if request.client else None
 data=await enrich_with_weather_data(data)
 background_tasks.add_task(save_data,data)
 return{"status":"received","timestamp":datetime.datetime.now().isoformat(),"device_id":data.get("device_id")or data.get("id"),"source_ip":data.get("source_ip"),"data_size_bytes":len(raw),"has_coordinates":bool(data.get("lat")and data.get("lon")),"weather_enriched":bool(any(k.startswith('weather_')for k in data.keys()))}