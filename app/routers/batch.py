# app/routers/batch.py
import json,datetime,asyncpg
from fastapi import APIRouter,Request,BackgroundTasks,HTTPException
from fastapi.responses import JSONResponse
from app.utils import enrich_with_weather_data
from app.db import DB_CONFIG
from app.responses import PrettyJSONResponse

router=APIRouter()

async def save_data(data:dict):
 device_id=data.get("device_id") or data.get("id")
 if not device_id:
  print(f"[{datetime.datetime.now()}] Warning: No device_id found in data: {data}")
  return
 conn=await asyncpg.connect(**DB_CONFIG)
 try:
  await conn.execute("INSERT INTO device_data(device_id,payload)VALUES($1,$2)",device_id,json.dumps(data))
  timestamp=None
  if 'timestamp' in data and data['timestamp'] is not None:
   try:
    if isinstance(data['timestamp'],str):timestamp=datetime.datetime.fromisoformat(data['timestamp'])
    elif isinstance(data['timestamp'],(int,float)):timestamp=datetime.datetime.fromtimestamp(data['timestamp'])
   except(ValueError,TypeError):pass
  if timestamp is None:timestamp=datetime.datetime.now(datetime.timezone.utc)
  is_offline='batch_id' in data and data['batch_id'] is not None
  data_type=data.get('data_type','telemetry')
  batch_id=data.get('batch_id')
  await conn.execute("INSERT INTO timestamped_data(device_id,payload,data_timestamp,data_type,is_offline,batch_id)VALUES($1,$2,$3,$4,$5,$6)",device_id,json.dumps(data),timestamp,data_type,is_offline,batch_id)
  existing_latest_state_row=await conn.fetchrow("SELECT payload FROM latest_device_states WHERE device_id=$1",device_id)
  existing_payload_dict={}
  if existing_latest_state_row and existing_latest_state_row['payload']:
   try:existing_payload_dict=json.loads(existing_latest_state_row['payload'])
   except json.JSONDecodeError:existing_payload_dict={}
  def deep_merge(source,destination):
   for key,value in source.items():
    if isinstance(value,dict) and key in destination and isinstance(destination[key],dict):destination[key]=deep_merge(value,destination[key])
    elif value is not None:destination[key]=value
   return destination
  import copy
  current_state_for_merge=copy.deepcopy(existing_payload_dict)
  merged_data=deep_merge(data,current_state_for_merge)
  await conn.execute("INSERT INTO latest_device_states(device_id,payload,received_at)VALUES($1,$2,now())ON CONFLICT(device_id)DO UPDATE SET payload=EXCLUDED.payload,received_at=EXCLUDED.received_at",device_id,json.dumps(merged_data))
 finally:await conn.close()

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