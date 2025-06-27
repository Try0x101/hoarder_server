import asyncpg,json,datetime,copy,asyncio
from typing import Optional,Dict,Any,List
from app.utils import transform_device_data,deep_merge

DB_CONFIG={"user":"admin","password":"admin","database":"database","host":"localhost"}
pool=None
_init_lock=asyncio.Lock()
_initialized=False

async def init_db():
 global pool,_initialized
 async with _init_lock:
  if _initialized:return
  pool=await asyncpg.create_pool(**DB_CONFIG)
  async with pool.acquire() as conn:
   try:
    tables_exist=await conn.fetchval("SELECT COUNT(*) FROM information_schema.tables WHERE table_name IN ('device_data','latest_device_states','timestamped_data') AND table_schema='public'")
    if tables_exist<3:
     print(f"[{datetime.datetime.now()}] Creating database tables...")
     await conn.execute("DROP TABLE IF EXISTS device_data CASCADE;DROP TABLE IF EXISTS latest_device_states CASCADE;DROP TABLE IF EXISTS timestamped_data CASCADE;CREATE TABLE IF NOT EXISTS device_data(id SERIAL PRIMARY KEY,device_id TEXT NOT NULL,payload JSONB NOT NULL,received_at TIMESTAMPTZ DEFAULT now());CREATE TABLE IF NOT EXISTS latest_device_states(device_id TEXT PRIMARY KEY,payload JSONB NOT NULL,received_at TIMESTAMPTZ DEFAULT now());CREATE TABLE IF NOT EXISTS timestamped_data(id SERIAL PRIMARY KEY,device_id TEXT NOT NULL,payload JSONB NOT NULL,data_timestamp TIMESTAMPTZ NOT NULL,received_at TIMESTAMPTZ DEFAULT now(),data_type TEXT DEFAULT 'delta',is_offline BOOLEAN DEFAULT false,batch_id TEXT NULL);CREATE INDEX IF NOT EXISTS idx_timestamped_data_device_time ON timestamped_data(device_id,data_timestamp DESC);CREATE INDEX IF NOT EXISTS idx_timestamped_data_timestamp ON timestamped_data(data_timestamp DESC)")
     print(f"[{datetime.datetime.now()}] Database tables created successfully.")
    else:print(f"[{datetime.datetime.now()}] Database tables already exist, skipping creation.")
    _initialized=True
   except Exception as e:
    print(f"[{datetime.datetime.now()}] Error during database initialization: {e}")
    raise

async def save_data(data:dict):
 device_id=data.get("device_id") or data.get("id")
 if not device_id:return
 async with pool.acquire() as conn:
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
  existing_payload_dict={}
  existing_latest_state_row=await conn.fetchrow("SELECT payload FROM latest_device_states WHERE device_id=$1",device_id)
  if existing_latest_state_row and existing_latest_state_row['payload']:
   try:existing_payload_dict=json.loads(existing_latest_state_row['payload'])
   except json.JSONDecodeError:pass
  merged_data=deep_merge(data,copy.deepcopy(existing_payload_dict))
  await conn.execute("INSERT INTO latest_device_states(device_id,payload,received_at)VALUES($1,$2,now())ON CONFLICT(device_id)DO UPDATE SET payload=EXCLUDED.payload,received_at=EXCLUDED.received_at",device_id,json.dumps(merged_data))

async def save_timestamped_data(data:dict,data_timestamp:Optional[datetime.datetime]=None,is_offline:bool=False,batch_id:Optional[str]=None):
 device_id=data.get("device_id") or data.get("id")
 if not device_id:return
 if data_timestamp is None:data_timestamp=datetime.datetime.now()
 data_type=data.get("data_type","delta")
 async with pool.acquire() as conn:
  await conn.execute("INSERT INTO timestamped_data(device_id,payload,data_timestamp,data_type,is_offline,batch_id)VALUES($1,$2,$3,$4,$5,$6)",device_id,json.dumps(data),data_timestamp,data_type,is_offline,batch_id)

async def get_latest_data():
 async with pool.acquire() as conn:
  rows=await conn.fetch("SELECT device_id,payload,received_at FROM latest_device_states ORDER BY received_at DESC")
  return[{"device_id":r["device_id"],"payload":transform_device_data(json.loads(r["payload"])),"time":r["received_at"].isoformat()}for r in rows]

def calculate_delta_changes(current_payload:Dict[str,Any],previous_payload:Dict[str,Any])->Dict[str,Any]:
 delta_changes={}
 context_fields=['id','device_id','data_timestamp','received_at','is_offline','batch_id']
 for key,value in current_payload.items():
  if key in context_fields or key not in previous_payload or previous_payload[key]!=value:delta_changes[key]=value
 return delta_changes

async def get_timestamped_history(device_id:Optional[str]=None,days:int=30):
 async with pool.acquire() as conn:
  time_threshold=datetime.datetime.now()-datetime.timedelta(days=days)
  if device_id:rows=await conn.fetch("SELECT device_id,payload,data_timestamp,received_at,data_type,is_offline,batch_id FROM timestamped_data WHERE device_id=$1 AND data_timestamp>=$2 ORDER BY data_timestamp DESC",device_id,time_threshold)
  else:rows=await conn.fetch("SELECT device_id,payload,data_timestamp,received_at,data_type,is_offline,batch_id FROM timestamped_data WHERE data_timestamp>=$1 ORDER BY device_id,data_timestamp DESC",time_threshold)
  devices_data={}
  for r in rows:
   device_id_key=r["device_id"]
   if device_id_key not in devices_data:devices_data[device_id_key]=[]
   try:
    payload=json.loads(r["payload"])
    entry={"device_id":r["device_id"],"payload":payload,"data_timestamp":r["data_timestamp"].isoformat(),"received_at":r["received_at"].isoformat(),"data_type":r["data_type"],"is_offline":r["is_offline"],"batch_id":r["batch_id"]}
    devices_data[device_id_key].append(entry)
   except Exception as e:
    print(f"Error processing row: {e}")
    continue
  result=[]
  for device_id_key,entries in devices_data.items():
   previous_payload={}
   for entry in reversed(entries):
    current_payload=entry["payload"]
    delta_payload=calculate_delta_changes(current_payload,previous_payload)
    metadata_only_fields={'source_ip','server_received_at','batch_timestamp','batch_entry_index'}
    meaningful_changes=[k for k in delta_payload.keys() if k not in metadata_only_fields and k not in['id','device_id']]
    if meaningful_changes or not previous_payload:
     result.append({"device_id":entry["device_id"],"delta_payload":delta_payload,"data_timestamp":entry["data_timestamp"],"received_at":entry["received_at"],"data_type":entry["data_type"],"is_offline":entry["is_offline"],"batch_id":entry["batch_id"],"changed_fields":list(delta_payload.keys()),"meaningful_changes":meaningful_changes})
    previous_payload=current_payload.copy()
  result.sort(key=lambda x:x["data_timestamp"],reverse=True)
  return result

async def get_data_gaps(device_id:str,days:int=30):
 async with pool.acquire() as conn:
  time_threshold=datetime.datetime.now()-datetime.timedelta(days=days)
  rows=await conn.fetch("SELECT data_timestamp FROM timestamped_data WHERE device_id=$1 AND data_timestamp>=$2 ORDER BY data_timestamp DESC",device_id,time_threshold)
  gaps=[]
  if len(rows)>1:
   for i in range(len(rows)-1):
    current_time=rows[i]['data_timestamp']
    next_time=rows[i+1]['data_timestamp']
    gap_seconds=(current_time-next_time).total_seconds()
    if gap_seconds>300:gaps.append({"gap_start":next_time.isoformat(),"gap_end":current_time.isoformat(),"gap_duration_seconds":gap_seconds,"gap_duration_minutes":round(gap_seconds/60,2),"gap_duration_hours":round(gap_seconds/3600,2)})
  return gaps
