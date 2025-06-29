import asyncpg,json,datetime,copy,asyncio,random
from typing import Optional,Dict,Any,List
from app.utils import transform_device_data,deep_merge
from app.cache import invalidate_cache, CACHE_KEY_LATEST_DATA

DB_CONFIG={"user":"admin","password":"admin","database":"database","host":"localhost"}
pool=None
_init_lock=asyncio.Lock()
_initialized=False

async def create_partition_for_date(conn, target_date):
    partition_start = target_date.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    partition_end = (partition_start + datetime.timedelta(days=32)).replace(day=1)
    partition_name = f"timestamped_data_y{partition_start.strftime('%Y')}m{partition_start.strftime('%m')}"

    if not await conn.fetchval("SELECT EXISTS(SELECT 1 FROM pg_tables WHERE tablename = $1)", partition_name):
        try:
            await conn.execute(f"""
                CREATE TABLE {partition_name} PARTITION OF timestamped_data
                FOR VALUES FROM ('{partition_start.isoformat()}') TO ('{partition_end.isoformat()}');
            """)
            await conn.execute(f'CREATE INDEX ON {partition_name} (device_id, data_timestamp DESC);')
            await conn.execute(f'CREATE INDEX ON {partition_name} (data_timestamp DESC);')
            print(f"[{datetime.datetime.now()}] Created partition {partition_name}")
        except asyncpg.exceptions.DuplicateTableError:
            print(f"[{datetime.datetime.now()}] Partition {partition_name} already exists (race condition).")

async def init_db():
    global pool, _initialized
    async with _init_lock:
        if _initialized:
            return
        pool = await asyncpg.create_pool(**DB_CONFIG)
        async with pool.acquire() as conn:
            try:
                is_partitioned = await conn.fetchval("""
                    SELECT EXISTS (
                        SELECT 1 FROM pg_class WHERE relname = 'timestamped_data' AND relkind = 'p'
                    );
                """)

                if not is_partitioned:
                    print(f"[{datetime.datetime.now()}] 'timestamped_data' is not partitioned. Recreating table...")
                    await conn.execute("DROP TABLE IF EXISTS timestamped_data CASCADE;")
                    await conn.execute("""
                        CREATE TABLE timestamped_data (
                            device_id TEXT NOT NULL,
                            payload JSONB NOT NULL,
                            data_timestamp TIMESTAMPTZ NOT NULL,
                            received_at TIMESTAMPTZ DEFAULT now(),
                            data_type TEXT DEFAULT 'delta',
                            is_offline BOOLEAN DEFAULT false,
                            batch_id TEXT NULL
                        ) PARTITION BY RANGE (data_timestamp);
                    """)
                    print(f"[{datetime.datetime.now()}] Partitioned table 'timestamped_data' created.")
                
                now = datetime.datetime.now(datetime.timezone.utc)
                await create_partition_for_date(conn, now)
                await create_partition_for_date(conn, now + datetime.timedelta(days=32))

                await conn.execute("""
                    CREATE TABLE IF NOT EXISTS device_data(id SERIAL PRIMARY KEY,device_id TEXT NOT NULL,payload JSONB NOT NULL,received_at TIMESTAMPTZ DEFAULT now());
                    CREATE TABLE IF NOT EXISTS latest_device_states(device_id TEXT PRIMARY KEY,payload JSONB NOT NULL,received_at TIMESTAMPTZ DEFAULT now());
                """)
                print(f"[{datetime.datetime.now()}] Auxiliary tables checked/created.")
                _initialized = True
            except Exception as e:
                print(f"[{datetime.datetime.now()}] Error during database initialization: {e}")
                raise

async def upsert_latest_state(data: dict):
    device_id = data.get("device_id") or data.get("id")
    if not device_id:
        print(f"[{datetime.datetime.now()}] WARNING: No device_id found in data for latest state update")
        return

    try:
        async with pool.acquire() as conn:
            existing_payload_dict = {}
            existing_latest_state_row = await conn.fetchrow("SELECT payload FROM latest_device_states WHERE device_id=$1", device_id)
            if existing_latest_state_row and existing_latest_state_row['payload']:
                try:
                    existing_payload_dict = json.loads(existing_latest_state_row['payload'])
                except (json.JSONDecodeError, TypeError):
                    print(f"[{datetime.datetime.now()}] ERROR: Could not parse existing payload for device {device_id}")
                    pass

            merged_data = deep_merge(data, copy.deepcopy(existing_payload_dict))
            await conn.execute(
                "INSERT INTO latest_device_states(device_id, payload, received_at) VALUES($1, $2, now()) ON CONFLICT(device_id) DO UPDATE SET payload=EXCLUDED.payload, received_at=EXCLUDED.received_at",
                device_id, json.dumps(merged_data)
            )
            print(f"[{datetime.datetime.now()}] Successfully upserted latest state for device {device_id}.")
            await invalidate_cache(CACHE_KEY_LATEST_DATA)
            # CACHE_INVALIDATION_MARKER
    except Exception as e:
        print(f"[{datetime.datetime.now()}] CRITICAL ERROR in upsert_latest_state: {str(e)}")

async def save_data(data:dict):
 try:
  device_id=data.get("device_id") or data.get("id")
  if not device_id:
   print(f"[{datetime.datetime.now()}] WARNING: No device_id found in data")
   return

  async with pool.acquire() as conn:
   try:
    max_id = await conn.fetchval("SELECT COALESCE(MAX(id), 0) FROM device_data")
    new_id = max_id + random.randint(1, 100)

    await conn.execute(
      "INSERT INTO device_data(id, device_id, payload) VALUES($1, $2, $3)",
      new_id, device_id, json.dumps(data)
    )
    print(f"[{datetime.datetime.now()}] Successfully saved historical data for device {device_id}")
   except Exception as e:
    print(f"[{datetime.datetime.now()}] ERROR in save_data (historical insert): {str(e)}")
  
  await upsert_latest_state(data)

 except Exception as e:
  print(f"[{datetime.datetime.now()}] CRITICAL ERROR in save_data: {str(e)}")

async def save_timestamped_data(data:dict, data_timestamp:Optional[datetime.datetime]=None, is_offline:bool=False, batch_id:Optional[str]=None):
 try:
  device_id=data.get("device_id") or data.get("id")
  if not device_id:
   print(f"[{datetime.datetime.now()}] WARNING: No device_id found in data")
   return

  if data_timestamp is None:
   data_timestamp=datetime.datetime.now(datetime.timezone.utc)

  data_type=data.get("data_type","delta")

  async with pool.acquire() as conn:
   try:
    await conn.execute(
      "INSERT INTO timestamped_data(device_id, payload, data_timestamp, data_type, is_offline, batch_id) VALUES($1, $2, $3, $4, $5, $6)",
      device_id, json.dumps(data), data_timestamp, data_type, is_offline, batch_id
    )
    print(f"[{datetime.datetime.now()}] Successfully saved timestamped data for device {device_id}")
   except asyncpg.exceptions.UndefinedTableError:
        await create_partition_for_date(conn, data_timestamp)
        await conn.execute(
            "INSERT INTO timestamped_data(device_id, payload, data_timestamp, data_type, is_offline, batch_id) VALUES($1, $2, $3, $4, $5, $6)",
            device_id, json.dumps(data), data_timestamp, data_type, is_offline, batch_id
        )
   except Exception as e:
    print(f"[{datetime.datetime.now()}] ERROR in save_timestamped_data: {str(e)}")
 except Exception as e:
  print(f"[{datetime.datetime.now()}] CRITICAL ERROR in save_timestamped_data: {str(e)}")

async def get_latest_data():
 async with pool.acquire() as conn:
  rows=await conn.fetch("SELECT device_id, payload, received_at FROM latest_device_states ORDER BY received_at DESC")
  result = []
  for r in rows:
   try:
    payload_dict = json.loads(r["payload"])
    if 'received_at' not in payload_dict and r["received_at"] is not None:
     payload_dict['received_at'] = r["received_at"]
    result.append({
     "device_id": r["device_id"],
     "payload": transform_device_data(payload_dict)
    })
   except Exception as e:
    print(f"[{datetime.datetime.now()}] ERROR processing device {r['device_id']}: {str(e)}")
  return result

def calculate_delta_changes(current_payload:Dict[str,Any],previous_payload:Dict[str,Any])->Dict[str,Any]:
 delta_changes={}
 context_fields=['id','device_id','data_timestamp','received_at','is_offline','batch_id']
 for key,value in current_payload.items():
  if key in context_fields or key not in previous_payload or previous_payload[key]!=value:delta_changes[key]=value
 return delta_changes

async def get_timestamped_history(device_id: str, days: int = 30, limit: int = 256, last_timestamp: Optional[str] = None):
    async with pool.acquire() as conn:
        time_threshold = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(days=days)
        
        count_query = "SELECT COUNT(*) FROM timestamped_data WHERE device_id=$1 AND data_timestamp >= $2"
        total_records = await conn.fetchval(count_query, device_id, time_threshold)

        query_parts = [
            "SELECT device_id, payload, data_timestamp, data_type, is_offline",
            "FROM timestamped_data",
            "WHERE device_id = $1 AND data_timestamp >= $2"
        ]
        params = [device_id, time_threshold]

        if last_timestamp:
            last_dt = datetime.datetime.fromisoformat(last_timestamp)
            query_parts.append("AND data_timestamp < $3")
            params.append(last_dt)

        query_parts.append("ORDER BY data_timestamp DESC LIMIT ${}".format(len(params) + 1))
        params.append(limit)

        query = " ".join(query_parts)
        rows = await conn.fetch(query, *params)

        devices_data = {}
        for r in rows:
            device_id_key = r["device_id"]
            if device_id_key not in devices_data:
                devices_data[device_id_key] = []
            try:
                payload = json.loads(r["payload"])
                entry = {
                    "payload": payload,
                    "data_timestamp": r["data_timestamp"],
                    "data_type": r["data_type"],
                    "is_offline": r["is_offline"]
                }
                devices_data[device_id_key].append(entry)
            except Exception as e:
                print(f"Error processing row: {e}")
                continue

        result = []
        for device_id_key, entries in devices_data.items():
            previous_payload = {}
            for entry in sorted(entries, key=lambda x: x["data_timestamp"]):
                current_payload = entry["payload"]
                delta_payload = calculate_delta_changes(current_payload, previous_payload)
                metadata_only_fields = {'source_ip', 'server_received_at', 'batch_timestamp', 'batch_entry_index', 'batch_id'}
                meaningful_changes = [k for k in delta_payload.keys() if k not in metadata_only_fields and k not in ['id', 'device_id']]

                if meaningful_changes or not previous_payload:
                    delta_payload.pop('id', None)
                    delta_payload.pop('timestamp', None)
                    delta_payload.pop('server_received_at', None)
                    delta_payload.pop('batch_id', None)

                    if delta_payload:
                        result.append({
                            "delta_payload": delta_payload,
                            "data_type": entry["data_type"],
                            "is_offline": entry["is_offline"]
                        })
                previous_payload = copy.deepcopy(current_payload)
        
        next_cursor = None
        if rows and len(rows) == limit:
            last_row = rows[-1]
            next_cursor = last_row['data_timestamp'].isoformat()

        result.reverse() # Sort from newest to oldest
        return result, total_records, next_cursor

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

async def get_data_for_latest():
    async with pool.acquire() as conn:
        rows = await conn.fetch("SELECT device_id, payload, received_at FROM latest_device_states ORDER BY received_at DESC")
        result = []

        for row in rows:
            try:
                device_id = row["device_id"]
                received_at = row["received_at"]
                payload = json.loads(row["payload"])

                last_refresh_time = received_at.astimezone(datetime.timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC") if received_at else None

                transformed = transform_device_data(payload)

                transformed["last_refresh_time"] = last_refresh_time
                if "barometric_data" in transformed:
                    transformed.pop("barometric_data")

                result.append({
                    "device_id": device_id,
                    "payload": transformed
                })
            except Exception as e:
                print(f"[{datetime.datetime.now()}] Error processing device {row['device_id']}: {str(e)}")
                import traceback
                print(traceback.format_exc())

        return result
