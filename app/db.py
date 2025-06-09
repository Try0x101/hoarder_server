import asyncpg,json,datetime,copy,asyncio
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
  async with pool.acquire()as conn:
   try:
    tables_exist=await conn.fetchval("SELECT COUNT(*)FROM information_schema.tables WHERE table_name IN('device_data','latest_device_states')AND table_schema='public'")
    if tables_exist<2:
     await conn.execute("DROP TABLE IF EXISTS device_data CASCADE")
     await conn.execute("DROP TABLE IF EXISTS latest_device_states CASCADE")
     await conn.execute("CREATE TABLE IF NOT EXISTS device_data(id SERIAL PRIMARY KEY,device_id TEXT NOT NULL,payload JSONB NOT NULL,received_at TIMESTAMPTZ DEFAULT now())")
     await conn.execute("CREATE TABLE IF NOT EXISTS latest_device_states(device_id TEXT PRIMARY KEY,payload JSONB NOT NULL,received_at TIMESTAMPTZ DEFAULT now())")
    _initialized=True
   except Exception as e:raise
async def save_data(data:dict):
 device_id=data.get("device_id")or data.get("id")
 if not device_id:return
 async with pool.acquire()as conn:
  await conn.execute("INSERT INTO device_data(device_id,payload)VALUES($1,$2)",device_id,json.dumps(data))
  existing_latest_state_row=await conn.fetchrow("SELECT payload FROM latest_device_states WHERE device_id=$1",device_id)
  existing_payload_dict={}
  if existing_latest_state_row and existing_latest_state_row['payload']:
   try:existing_payload_dict=json.loads(existing_latest_state_row['payload'])
   except:existing_payload_dict={}
  current_state_for_merge=copy.deepcopy(existing_payload_dict)
  merged_data=deep_merge(data,current_state_for_merge)
  await conn.execute("INSERT INTO latest_device_states(device_id,payload,received_at)VALUES($1,$2,now())ON CONFLICT(device_id)DO UPDATE SET payload=EXCLUDED.payload,received_at=EXCLUDED.received_at",device_id,json.dumps(merged_data))
async def get_latest_data():
 async with pool.acquire()as conn:
  rows=await conn.fetch("SELECT device_id,payload,received_at FROM latest_device_states ORDER BY received_at DESC")
  return[{"device_id":r["device_id"],"payload":transform_device_data(json.loads(r["payload"])),"time":r["received_at"].isoformat()}for r in rows]
