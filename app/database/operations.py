import json
import datetime
import asyncpg
from typing import Optional, Dict, Any, List
from .connection import get_pool
from .partitions.manager import ensure_partition_exists
from app.cache import invalidate_cache, CACHE_KEY_LATEST_DATA

def _sanitize_payload(data: Any) -> Any:
    if isinstance(data, dict):
        return {k: _sanitize_payload(v) for k, v in data.items()}
    if isinstance(data, list):
        return [_sanitize_payload(v) for v in data]
    if isinstance(data, str):
        stripped_data = data.strip()
        while stripped_data.startswith('"') and stripped_data.endswith('"') and len(stripped_data) > 1:
            stripped_data = stripped_data[1:-1].strip()
        
        if stripped_data == "":
            return ""
        
        try:
            val = float(stripped_data)
            if val == int(val):
                return int(val)
            return val
        except (ValueError, TypeError):
            return stripped_data
    
    if isinstance(data, (int, float)):
        return data
    
    return data

async def upsert_latest_state(data: dict):
    device_id = data.get("device_id") or data.get("id")
    if not device_id:
        print(f"[{datetime.datetime.now(datetime.timezone.utc)}] WARNING: No device_id found for latest state update")
        return

    try:
        pool = await get_pool()
        async with pool.acquire() as conn:
            sanitized_payload = _sanitize_payload(data)
            payload_json = json.dumps(sanitized_payload)

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
            
            print(f"[{datetime.datetime.now(datetime.timezone.utc)}] Successfully upserted latest state for device {device_id}.")
            await invalidate_cache(CACHE_KEY_LATEST_DATA)
            await invalidate_cache(f"latest_data_raw_{device_id}")
    except Exception as e:
        print(f"[{datetime.datetime.now(datetime.timezone.utc)}] CRITICAL ERROR in upsert_latest_state: {str(e)}")

async def save_timestamped_data(data:dict, data_timestamp:Optional[datetime.datetime]=None, is_offline:bool=False, batch_id:Optional[str]=None):
    try:
        device_id = data.get("device_id") or data.get("id")
        if not device_id:
            print(f"[{datetime.datetime.now(datetime.timezone.utc)}] WARNING: No device_id found in data")
            return

        if data_timestamp is None:
            data_timestamp = datetime.datetime.now(datetime.timezone.utc)

        data_type = data.get("data_type", "delta")
        sanitized_payload = _sanitize_payload(data)

        pool = await get_pool()
        async with pool.acquire() as conn:
            try:
                await conn.execute(
                    "INSERT INTO timestamped_data(device_id, payload, data_timestamp, data_type, is_offline, batch_id) VALUES($1, $2, $3, $4, $5, $6)",
                    device_id, json.dumps(sanitized_payload), data_timestamp, data_type, is_offline, batch_id
                )
                print(f"[{datetime.datetime.now(datetime.timezone.utc)}] Successfully saved timestamped data for device {device_id}")
            except asyncpg.exceptions.UndefinedTableError:
                await ensure_partition_exists(data_timestamp)
                await conn.execute(
                    "INSERT INTO timestamped_data(device_id, payload, data_timestamp, data_type, is_offline, batch_id) VALUES($1, $2, $3, $4, $5, $6)",
                    device_id, json.dumps(sanitized_payload), data_timestamp, data_type, is_offline, batch_id
                )
            except Exception as e:
                print(f"[{datetime.datetime.now(datetime.timezone.utc)}] ERROR in save_timestamped_data: {str(e)}")
    except Exception as e:
        print(f"[{datetime.datetime.now(datetime.timezone.utc)}] CRITICAL ERROR in save_timestamped_data: {str(e)}")

async def get_latest_data():
    pool = await get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch("SELECT device_id, payload, received_at FROM latest_device_states ORDER BY received_at DESC")
        result = []
        from app.utils import transform_device_data
        for r in rows:
            try:
                payload_dict = json.loads(r["payload"])
                if 'received_at' not in payload_dict and r["received_at"] is not None:
                    payload_dict['received_at'] = r["received_at"]

                transformed_data = await transform_device_data(payload_dict)
                result.append({
                    "device_id": r["device_id"],
                    "payload": transformed_data
                })
            except Exception as e:
                print(f"[{datetime.datetime.now(datetime.timezone.utc)}] ERROR processing device {r['device_id']}: {str(e)}")
        return result

async def get_raw_latest_payload_for_device(device_id: str) -> Optional[Dict[str, Any]]:
    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow("SELECT payload, received_at FROM latest_device_states WHERE device_id = $1", device_id)
        if not row:
            return None
        try:
            payload = json.loads(row["payload"])
            if 'received_at' not in payload and row['received_at']:
                 payload['received_at'] = row['received_at'].isoformat()
            return payload
        except (json.JSONDecodeError, TypeError) as e:
            print(f"[{datetime.datetime.now(datetime.timezone.utc)}] Error parsing raw payload for device {device_id}: {str(e)}")
            return None

async def get_raw_latest_data_for_all_devices() -> List[Dict[str, Any]]:
    pool = await get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch("SELECT device_id, payload, received_at FROM latest_device_states ORDER BY received_at DESC")
        result = []
        for row in rows:
            try:
                payload = json.loads(row["payload"])
                if 'received_at' not in payload and row['received_at']:
                    payload['received_at'] = row['received_at'].isoformat()
                result.append({
                    "device_id": row["device_id"],
                    "payload": payload
                })
            except (json.JSONDecodeError, TypeError) as e:
                print(f"[{datetime.datetime.now(datetime.timezone.utc)}] Error parsing raw payload for device {row['device_id']}: {str(e)}")
        return result
