import json
import datetime
import copy
from typing import Optional, Dict, Any

from .connection import get_pool
from .helpers import calculate_delta_changes

async def get_timestamped_history(device_id: str, days: int = 30, limit: int = 256, last_timestamp: Optional[str] = None):
    pool = await get_pool()
    async with pool.acquire() as conn:
        time_thresh = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(days=days)
        query = "SELECT payload, data_timestamp, data_type, is_offline FROM timestamped_data WHERE device_id = $1 AND data_timestamp >= $2"
        params = [device_id, time_thresh]

        if last_timestamp:
            query += " AND data_timestamp < $3"
            params.append(datetime.datetime.fromisoformat(last_timestamp))

        query += " ORDER BY data_timestamp DESC LIMIT ${}".format(len(params) + 1)
        params.append(limit)
        rows = await conn.fetch(query, *params)

        if not rows:
            return [], None

        entries = [dict(r) for r in rows]
        result = []
        previous_payload = {}
        for entry in sorted(entries, key=lambda x: x["data_timestamp"]):
            current_payload = json.loads(entry["payload"])
            delta = calculate_delta_changes(current_payload, previous_payload)
            
            meta_keys = {'source_ip', 'server_received_at', 'batch_id', 'id', 'timestamp'}
            meaningful_changes = any(k not in meta_keys for k in delta)

            if meaningful_changes or not previous_payload or entry.get("is_offline"):
                for key in meta_keys: delta.pop(key, None)
                if delta:
                    result.append({
                        "delta_payload": delta,
                        "data_type": entry["data_type"],
                        "is_offline": entry["is_offline"],
                        "data_timestamp": entry["data_timestamp"].isoformat()
                    })
            previous_payload = copy.deepcopy(current_payload)

        next_cursor = rows[-1]['data_timestamp'].isoformat() if len(rows) == limit else None
        return list(reversed(result)), next_cursor

async def get_data_gaps(device_id: str, days: int = 30):
    pool = await get_pool()
    async with pool.acquire() as conn:
        time_thresh = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(days=days)
        rows = await conn.fetch("SELECT data_timestamp FROM timestamped_data WHERE device_id=$1 AND data_timestamp>=$2 ORDER BY data_timestamp DESC", device_id, time_thresh)
        gaps = []
        for i in range(len(rows) - 1):
            gap = (rows[i]['data_timestamp'] - rows[i+1]['data_timestamp']).total_seconds()
            if gap > 300:
                gaps.append({"start": rows[i+1]['data_timestamp'].isoformat(), "end": rows[i]['data_timestamp'].isoformat(), "duration_minutes": round(gap / 60, 2)})
        return gaps

async def get_top_devices_by_records(limit: int = 5):
    pool = await get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch("SELECT device_id, COUNT(*) as c FROM timestamped_data GROUP BY device_id ORDER BY c DESC LIMIT $1", limit)
        return [dict(r) for r in rows]

async def get_total_records_summary():
    pool = await get_pool()
    async with pool.acquire() as conn:
        summary = {}
        total = 0
        for table in ['timestamped_data', 'latest_device_states', 'device_data']:
            try:
                count = await conn.fetchval("SELECT reltuples::bigint FROM pg_class WHERE relname = $1", table) or 0
                summary[table], total = int(count), total + int(count)
            except Exception:
                summary[table] = 'error'
        summary['total'] = total
        return summary
