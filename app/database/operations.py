import json
import datetime
import asyncpg
from typing import Optional

from .connection import get_pool
from .partitions.manager import ensure_partition_exists
from .helpers import sanitize_payload
from app.cache import invalidate_cache, CACHE_KEY_LATEST_DATA

async def upsert_latest_state(data: dict):
    device_id = data.get("device_id") or data.get("id")
    if not device_id:
        return

    try:
        pool = await get_pool()
        async with pool.acquire() as conn:
            payload_json = json.dumps(sanitize_payload(data))
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
    except Exception as e:
        print(f"[{datetime.datetime.now(datetime.timezone.utc)}] CRITICAL ERROR in upsert_latest_state: {str(e)}")

async def save_timestamped_data(data:dict, data_timestamp:Optional[datetime.datetime]=None, is_offline:bool=False, batch_id:Optional[str]=None):
    device_id = data.get("device_id") or data.get("id")
    if not device_id:
        return

    ts = data_timestamp or datetime.datetime.now(datetime.timezone.utc)
    payload_json = json.dumps(sanitize_payload(data))
    data_type = data.get("data_type", "delta")

    try:
        pool = await get_pool()
        async with pool.acquire() as conn:
            await conn.execute(
                "INSERT INTO timestamped_data(device_id, payload, data_timestamp, data_type, is_offline, batch_id) VALUES($1, $2, $3, $4, $5, $6)",
                device_id, payload_json, ts, data_type, is_offline, batch_id
            )
    except asyncpg.exceptions.UndefinedTableError:
        await ensure_partition_exists(ts)
        async with (await get_pool()).acquire() as conn:
            await conn.execute(
                "INSERT INTO timestamped_data(device_id, payload, data_timestamp, data_type, is_offline, batch_id) VALUES($1, $2, $3, $4, $5, $6)",
                device_id, payload_json, ts, data_type, is_offline, batch_id
            )
    except Exception as e:
        print(f"[{datetime.datetime.now(datetime.timezone.utc)}] ERROR in save_timestamped_data: {str(e)}")
