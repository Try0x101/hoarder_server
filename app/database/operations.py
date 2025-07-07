import json
import datetime
import asyncpg
from typing import Optional, Dict, Any

from .connection import get_pool
from .partitions.manager import ensure_partition_exists
from .helpers import safe_json_serialize, extract_device_id, sanitize_payload, deep_merge
from app.cache import invalidate_device_cache

async def upsert_latest_state(data: dict):
    device_id = extract_device_id(data)
    if not device_id: return

    sanitized_data = sanitize_payload(data)
    merge_data = {k: v for k, v in sanitized_data.items() if k not in ['received_at', 'server_received_at']}
    if not merge_data: return

    try:
        pool = await get_pool()
        async with pool.acquire() as conn:
            async with conn.transaction():
                existing_row = await conn.fetchrow(
                    "SELECT payload FROM latest_device_states WHERE device_id = $1 FOR UPDATE",
                    device_id, timeout=10
                )
                existing_data = json.loads(existing_row['payload']) if (existing_row and existing_row['payload']) else {}
                merged_data = deep_merge(merge_data, existing_data)
                final_payload = safe_json_serialize(merged_data)
                await conn.execute(
                    """
                    INSERT INTO latest_device_states(device_id, payload, received_at) VALUES($1, $2, now())
                    ON CONFLICT(device_id) DO UPDATE SET
                    payload = EXCLUDED.payload, received_at = EXCLUDED.received_at
                    """,
                    device_id, final_payload, timeout=10
                )
        await invalidate_device_cache(device_id)
    except Exception as e:
        print(f"[{datetime.datetime.now(datetime.timezone.utc)}] CRITICAL ERROR in upsert for {device_id}: {e}")

async def save_timestamped_data(
    data: dict, data_timestamp: Optional[datetime.datetime] = None, 
    is_offline: bool = False, batch_id: Optional[str] = None
):
    device_id = extract_device_id(data)
    if not device_id: return

    ts = data_timestamp or datetime.datetime.now(datetime.timezone.utc)
    if ts.tzinfo is None: ts = ts.replace(tzinfo=datetime.timezone.utc)
    
    payload_json = safe_json_serialize(data)
    data_type = str(data.get("data_type", "delta"))[:50]
    safe_batch_id = str(batch_id)[:100] if batch_id else None

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
    except Exception as e:
        print(f"[{datetime.datetime.now(datetime.timezone.utc)}] ERROR in save_timestamped_data for {device_id}: {e}")
