import json
import datetime
from typing import Optional, Dict, Any, List

from .connection import get_pool

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
        except (json.JSONDecodeError, TypeError):
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
                result.append({"device_id": row["device_id"], "payload": payload})
            except (json.JSONDecodeError, TypeError):
                continue
        return result
