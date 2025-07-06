import json
import datetime
import asyncpg
from typing import Optional, Dict, Any

from .connection import get_pool
from .partitions.manager import ensure_partition_exists
from .helpers import sanitize_payload
from app.cache import invalidate_cache, CACHE_KEY_LATEST_DATA

def safe_json_serialize(data: Any) -> str:
    try:
        return json.dumps(sanitize_payload(data), ensure_ascii=False, separators=(',', ':'))
    except (TypeError, ValueError) as e:
        print(f"[{datetime.datetime.now(datetime.timezone.utc)}] JSON serialization error: {e}")
        
        fallback_data = {}
        if isinstance(data, dict):
            for key, value in data.items():
                try:
                    json.dumps(value)
                    fallback_data[str(key)] = value
                except (TypeError, ValueError):
                    fallback_data[str(key)] = str(value) if value is not None else None
        else:
            fallback_data = {"raw_data": str(data) if data is not None else None}
        
        return json.dumps(fallback_data, ensure_ascii=False, separators=(',', ':'))

def extract_device_id(data: Dict[str, Any]) -> Optional[str]:
    id_fields = ['device_id', 'id', 'deviceId', 'device', 'dev_id']
    
    for field in id_fields:
        if field in data and data[field]:
            device_id = str(data[field]).strip()
            if device_id and device_id.lower() not in ['null', 'none', 'undefined', '']:
                return device_id[:100]
    
    return None

async def upsert_latest_state(data: dict):
    device_id = extract_device_id(data)
    if not device_id:
        print(f"[{datetime.datetime.now(datetime.timezone.utc)}] WARNING: No valid device_id found for state upsert")
        return

    try:
        pool = await get_pool()
        async with pool.acquire() as conn:
            payload_json = safe_json_serialize(data)
            
            await conn.execute(
                """
                INSERT INTO latest_device_states(device_id, payload, received_at)
                VALUES($1, $2, now())
                ON CONFLICT(device_id) DO UPDATE
                SET payload = jsonb_recursive_merge(latest_device_states.payload, EXCLUDED.payload),
                    received_at = EXCLUDED.received_at;
                """,
                device_id, payload_json
            )
            
            await invalidate_cache(CACHE_KEY_LATEST_DATA)
            await invalidate_cache(f"latest_data_raw_{device_id}")
            
    except asyncpg.exceptions.DataError as e:
        print(f"[{datetime.datetime.now(datetime.timezone.utc)}] DATA ERROR in upsert_latest_state: {str(e)}")
        
        try:
            minimal_data = {
                'device_id': device_id,
                'error': 'data_format_error',
                'received_at': datetime.datetime.now(datetime.timezone.utc).isoformat(),
                'raw_data_size': len(str(data))
            }
            payload_json = json.dumps(minimal_data)
            
            async with pool.acquire() as conn:
                await conn.execute(
                    """
                    INSERT INTO latest_device_states(device_id, payload, received_at)
                    VALUES($1, $2, now())
                    ON CONFLICT(device_id) DO UPDATE
                    SET payload = EXCLUDED.payload, received_at = EXCLUDED.received_at;
                    """,
                    device_id, payload_json
                )
        except Exception as fallback_error:
            print(f"[{datetime.datetime.now(datetime.timezone.utc)}] FALLBACK ERROR: {str(fallback_error)}")
            
    except Exception as e:
        print(f"[{datetime.datetime.now(datetime.timezone.utc)}] CRITICAL ERROR in upsert_latest_state: {str(e)}")

async def save_timestamped_data(data: dict, data_timestamp: Optional[datetime.datetime] = None, is_offline: bool = False, batch_id: Optional[str] = None):
    device_id = extract_device_id(data)
    if not device_id:
        print(f"[{datetime.datetime.now(datetime.timezone.utc)}] WARNING: No valid device_id found for timestamped data")
        return

    ts = data_timestamp or datetime.datetime.now(datetime.timezone.utc)
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=datetime.timezone.utc)
    
    payload_json = safe_json_serialize(data)
    data_type = str(data.get("data_type", "delta"))[:50]
    
    safe_batch_id = None
    if batch_id:
        safe_batch_id = str(batch_id)[:100]

    try:
        pool = await get_pool()
        async with pool.acquire() as conn:
            await conn.execute(
                "INSERT INTO timestamped_data(device_id, payload, data_timestamp, data_type, is_offline, batch_id) VALUES($1, $2, $3, $4, $5, $6)",
                device_id, payload_json, ts, data_type, is_offline, safe_batch_id
            )
            
    except asyncpg.exceptions.UndefinedTableError:
        await ensure_partition_exists(ts)
        async with (await get_pool()).acquire() as conn:
            await conn.execute(
                "INSERT INTO timestamped_data(device_id, payload, data_timestamp, data_type, is_offline, batch_id) VALUES($1, $2, $3, $4, $5, $6)",
                device_id, payload_json, ts, data_type, is_offline, safe_batch_id
            )
            
    except asyncpg.exceptions.DataError as e:
        print(f"[{datetime.datetime.now(datetime.timezone.utc)}] DATA ERROR in save_timestamped_data: {str(e)}")
        
        try:
            minimal_data = {
                'device_id': device_id,
                'error': 'data_format_error',
                'original_timestamp': ts.isoformat(),
                'raw_data_size': len(str(data)),
                'batch_id': safe_batch_id
            }
            fallback_json = json.dumps(minimal_data)
            
            async with pool.acquire() as conn:
                await conn.execute(
                    "INSERT INTO timestamped_data(device_id, payload, data_timestamp, data_type, is_offline, batch_id) VALUES($1, $2, $3, $4, $5, $6)",
                    device_id, fallback_json, ts, "error_fallback", is_offline, safe_batch_id
                )
        except Exception as fallback_error:
            print(f"[{datetime.datetime.now(datetime.timezone.utc)}] FALLBACK ERROR: {str(fallback_error)}")
            
    except Exception as e:
        print(f"[{datetime.datetime.now(datetime.timezone.utc)}] ERROR in save_timestamped_data: {str(e)}")
