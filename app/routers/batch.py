import json,datetime,asyncpg,copy
from fastapi import APIRouter,Request,BackgroundTasks,HTTPException
from fastapi.responses import JSONResponse
from app.utils import enrich_with_weather_data, deep_merge
from app.db import save_data, DB_CONFIG
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

@router.post("/api/batch-delta", response_class=PrettyJSONResponse)
async def receive_batch_delta_data(request: Request, background_tasks: BackgroundTasks):
    try:
        delta_batch = await request.json()
        if not isinstance(delta_batch, list):
            raise HTTPException(status_code=400, detail="Expected a JSON array of telemetry delta objects")

        sorted_deltas = sorted(delta_batch, key=lambda x: (x.get('id', ''), x.get('ts', 0)))
        
        background_tasks.add_task(process_delta_batch, sorted_deltas, request.client.host if request.client else None)

        return {
            "status": "accepted",
            "timestamp": datetime.datetime.now().isoformat(),
            "items_queued": len(delta_batch)
        }
    except json.JSONDecodeError:
        raise HTTPException(status_code=400, detail="Invalid JSON format")
    except Exception as e:
        print(f"[{datetime.datetime.now()}] ERROR in batch-delta endpoint: {str(e)}")
        return JSONResponse(status_code=500, content={"error": "Internal server error"})

async def process_delta_batch(deltas: list, source_ip: str):
    conn = None
    try:
        conn = await asyncpg.connect(**DB_CONFIG)
        device_states = {}
        batch_id = f"batch_delta_{datetime.datetime.now().strftime('%Y%m%d%H%M%S')}"
        batch_receive_time = datetime.datetime.now()

        for delta in deltas:
            device_id = delta.get('id')
            if not device_id:
                continue

            if device_id not in device_states:
                latest_state_row = await conn.fetchrow("SELECT payload FROM latest_device_states WHERE device_id = $1", device_id)
                if latest_state_row and latest_state_row['payload']:
                    device_states[device_id] = json.loads(latest_state_row['payload'])
                else:
                    device_states[device_id] = {}
            
            current_state = device_states[device_id]
            reconstructed_payload = deep_merge(delta, copy.deepcopy(current_state))
            
            if 'ts' in delta:
                ts_seconds = delta['ts']
                current_minute = int(batch_receive_time.timestamp()) // 60
                full_timestamp = (current_minute * 60) + ts_seconds
                reconstructed_payload['timestamp'] = full_timestamp
            
            reconstructed_payload['source_ip'] = source_ip
            reconstructed_payload['batch_id'] = batch_id
            
            if 'lat' in reconstructed_payload and 'lon' in reconstructed_payload:
                enriched_payload = await enrich_with_weather_data(reconstructed_payload)
            else:
                enriched_payload = reconstructed_payload

            await save_data(enriched_payload)
            device_states[device_id] = enriched_payload

    except Exception as e:
        print(f"[{datetime.datetime.now()}] ERROR processing delta batch: {str(e)}")
    finally:
        if conn:
            await conn.close()