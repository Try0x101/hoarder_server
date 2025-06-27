import json,datetime
from fastapi import APIRouter,Request,BackgroundTasks,HTTPException
from fastapi.responses import JSONResponse
from app.utils import enrich_with_weather_data
from app.db import save_data
from app.responses import PrettyJSONResponse

router=APIRouter()

@router.post("/api/batch",response_class=PrettyJSONResponse)
async def receive_batch_data(request:Request,background_tasks:BackgroundTasks):
 try:
  batch_data=await request.json()
  if not isinstance(batch_data,list):raise HTTPException(status_code=400,detail="Expected a JSON array of telemetry data objects")
  processed_count=0
  weather_enriched_count=0
  batch_id=f"batch_{datetime.datetime.now().strftime('%Y%m%d%H%M%S')}_{hash(str(request.client.host))}"
  for item in batch_data:
   if not isinstance(item,dict):continue
   item['source_ip']=request.client.host if request.client else None
   item['batch_id']=batch_id
   if 'lat' in item and 'lon' in item and item['lat'] and item['lon']:
    item=await enrich_with_weather_data(item)
    if any(k.startswith('weather_') for k in item.keys()):weather_enriched_count+=1
   background_tasks.add_task(save_data,item)
   processed_count+=1
  return{"status":"received","timestamp":datetime.datetime.now().isoformat(),"batch_id":batch_id,"items_processed":processed_count,"total_items":len(batch_data),"weather_enriched":weather_enriched_count,"source_ip":request.client.host}
 except json.JSONDecodeError:raise HTTPException(status_code=400,detail="Invalid JSON format")
 except Exception as e:
  print(f"[{datetime.datetime.now()}] ERROR in batch endpoint: {str(e)}")
  import traceback
  print(f"[{datetime.datetime.now()}] {traceback.format_exc()}")
  return JSONResponse(status_code=500,content={"error":"Internal server error"})
