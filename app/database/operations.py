import json
import datetime
import asyncpg
from typing import Optional, Dict, Any

from .connection import get_pool
from .partitions.manager import ensure_partition_exists
from .helpers import sanitize_payload
from app.cache import invalidate_device_cache

def safe_json_serialize(data: Any) -> str:
    try:
        sanitized = sanitize_payload(data)
        return json.dumps(sanitized, ensure_ascii=False, separators=(',', ':'))
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

def prepare_state_merge_data(new_data: dict) -> dict:
    merge_data = {}
    
    for key, value in new_data.items():
        if key in ['received_at', 'server_received_at']:
            continue
            
        if value is None:
            continue
            
        if isinstance(value, str) and value.strip().lower() in ['null', 'none', 'undefined', '']:
            continue
            
        if isinstance(value, dict):
            cleaned_dict = {k: v for k, v in value.items() if v is not None}
            if cleaned_dict:
                merge_data[key] = cleaned_dict
        elif isinstance(value, list):
            cleaned_list = [item for item in value if item is not None]
            if cleaned_list:
                merge_data[key] = cleaned_list
        else:
            merge_data[key] = value
    
    return merge_data

async def upsert_latest_state(data: dict):
    device_id = extract_device_id(data)
    if not device_id:
        print(f"[{datetime.datetime.now(datetime.timezone.utc)}] WARNING: No valid device_id found for state upsert")
        return

    try:
        pool = await get_pool()
        async with pool.acquire() as conn:
            merge_data = prepare_state_merge_data(data)
            if not merge_data:
                print(f"[{datetime.datetime.now(datetime.timezone.utc)}] WARNING: No valid data to merge for device {device_id}")
                return
                
            payload_json = safe_json_serialize(merge_data)
            
            async with conn.transaction():
                existing_row = await conn.fetchrow(
                    "SELECT payload FROM latest_device_states WHERE device_id = $1 FOR UPDATE",
                    device_id, timeout=10
                )
                
                if existing_row:
                    try:
                        existing_data = json.loads(existing_row['payload'])
                        merged_data = deep_merge_safe(existing_data, merge_data)
                        final_payload = safe_json_serialize(merged_data)
                    except Exception as e:
                        print(f"[{datetime.datetime.now(datetime.timezone.utc)}] Merge error for {device_id}: {e}")
                        final_payload = payload_json
                else:
                    final_payload = payload_json
                
                await conn.execute(
                    """
                    INSERT INTO latest_device_states(device_id, payload, received_at)
                    VALUES($1, $2, now())
                    ON CONFLICT(device_id) DO UPDATE SET
                    payload = EXCLUDED.payload,
                    received_at = EXCLUDED.received_at
                    """,
                    device_id, final_payload, timeout=10
                )
            
            await invalidate_device_cache(device_id)
            
    except asyncpg.exceptions.DataError as e:
        print(f"[{datetime.datetime.now(datetime.timezone.utc)}] DATA ERROR in upsert_latest_state: {str(e)}")
        
        try:
            minimal_data = {
                'device_id': device_id,
                'error': 'data_format_error',
                'received_at': datetime.datetime.now(datetime.timezone.utc).isoformat(),
                'raw_data_size': len(str(data))
            }
            payload_json = json.dumps(minimal_data, separators=(',', ':'))
            
            async with pool.acquire() as conn:
                await conn.execute(
                    """
                    INSERT INTO latest_device_states(device_id, payload, received_at)
                    VALUES($1, $2, now())
                    ON CONFLICT(device_id) DO UPDATE SET
                    payload = EXCLUDED.payload, received_at = EXCLUDED.received_at
                    """,
                    device_id, payload_json, timeout=10
                )
        except Exception as fallback_error:
            print(f"[{datetime.datetime.now(datetime.timezone.utc)}] FALLBACK ERROR: {str(fallback_error)}")
            
    except Exception as e:
        print(f"[{datetime.datetime.now(datetime.timezone.utc)}] CRITICAL ERROR in upsert_latest_state: {str(e)}")

def deep_merge_safe(existing: dict, new_data: dict) -> dict:
    try:
        result = existing.copy()
        
        for key, new_value in new_data.items():
            if key not in result:
                result[key] = new_value
                continue
                
            existing_value = result[key]
            
            if isinstance(new_value, dict) and isinstance(existing_value, dict):
                result[key] = deep_merge_safe(existing_value, new_value)
            elif isinstance(new_value, list) and isinstance(existing_value, list):
                if len(new_value) > len(existing_value):
                    result[key] = new_value
                else:
                    combined = existing_value.copy()
                    for i, item in enumerate(new_value):
                        if i < len(combined):
                            combined[i] = item
                        else:
                            combined.append(item)
                    result[key] = combined
            else:
                result[key] = new_value
        
        return result
    except Exception as e:
        print(f"[{datetime.datetime.now(datetime.timezone.utc)}] Deep merge error: {e}")
        return {**existing, **new_data}

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
                device_id, payload_json, ts, data_type, is_offline, safe_batch_id, timeout=15
            )
            
    except asyncpg.exceptions.UndefinedTableError:
        await ensure_partition_exists(ts)
        async with (await get_pool()).acquire() as conn:
            await conn.execute(
                "INSERT INTO timestamped_data(device_id, payload, data_timestamp, data_type, is_offline, batch_id) VALUES($1, $2, $3, $4, $5, $6)",
                device_id, payload_json, ts, data_type, is_offline, safe_batch_id, timeout=15
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
            fallback_json = json.dumps(minimal_data, separators=(',', ':'))
            
            async with pool.acquire() as conn:
                await conn.execute(
                    "INSERT INTO timestamped_data(device_id, payload, data_timestamp, data_type, is_offline, batch_id) VALUES($1, $2, $3, $4, $5, $6)",
                    device_id, fallback_json, ts, "error_fallback", is_offline, safe_batch_id, timeout=15
                )
        except Exception as fallback_error:
            print(f"[{datetime.datetime.now(datetime.timezone.utc)}] FALLBACK ERROR: {str(fallback_error)}")
            
    except Exception as e:
        print(f"[{datetime.datetime.now(datetime.timezone.utc)}] ERROR in save_timestamped_data: {str(e)}")
