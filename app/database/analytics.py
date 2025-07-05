import json
import datetime
import copy
from typing import Optional, Dict, Any, List, Tuple
from .connection import get_pool

def calculate_delta_changes(current_payload: Dict[str, Any], previous_payload: Dict[str, Any]) -> Dict[str, Any]:
    delta_changes = {}
    context_fields = ['id', 'device_id', 'data_timestamp', 'received_at', 'is_offline', 'batch_id']
    for key, value in current_payload.items():
        if key in context_fields or key not in previous_payload or previous_payload[key] != value:
            delta_changes[key] = value
    return delta_changes

async def get_timestamped_history(device_id: str, days: int = 30, limit: int = 256, last_timestamp: Optional[str] = None):
    pool = await get_pool()
    async with pool.acquire() as conn:
        time_threshold = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(days=days)

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

        if not rows:
            return [], None

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

                if meaningful_changes or not previous_payload or entry.get("is_offline"):
                    content_size_bytes = delta_payload.pop('content_size_bytes', None)
                    delta_payload.pop('id', None)
                    delta_payload.pop('timestamp', None)
                    delta_payload.pop('server_received_at', None)
                    delta_payload.pop('batch_id', None)

                    if delta_payload:
                        response_entry = {
                            "delta_payload": delta_payload,
                            "data_type": entry["data_type"],
                            "is_offline": entry["is_offline"],
                            "data_timestamp": entry["data_timestamp"].isoformat() if entry["data_timestamp"] else None
                        }

                        if content_size_bytes is not None:
                            response_entry["content_size_bytes"] = f"{content_size_bytes} Bytes"

                        result.append(response_entry)

                previous_payload = copy.deepcopy(current_payload)

        next_cursor = None
        if len(rows) == limit:
            last_row = rows[-1]
            next_cursor = last_row['data_timestamp'].isoformat()

        result.reverse()
        return result, next_cursor

async def get_data_gaps(device_id: str, days: int = 30):
    pool = await get_pool()
    async with pool.acquire() as conn:
        time_threshold = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(days=days)
        rows = await conn.fetch("SELECT data_timestamp FROM timestamped_data WHERE device_id=$1 AND data_timestamp>=$2 ORDER BY data_timestamp DESC", device_id, time_threshold)
        gaps = []
        if len(rows) > 1:
            for i in range(len(rows) - 1):
                current_time = rows[i]['data_timestamp']
                next_time = rows[i + 1]['data_timestamp']
                gap_seconds = (current_time - next_time).total_seconds()
                if gap_seconds > 300:
                    gaps.append({
                        "gap_start": next_time.isoformat(),
                        "gap_end": current_time.isoformat(),
                        "gap_duration_seconds": gap_seconds,
                        "gap_duration_minutes": round(gap_seconds / 60, 2),
                        "gap_duration_hours": round(gap_seconds / 3600, 2)
                    })
        return gaps

async def get_top_devices_by_records(limit: int = 5):
    pool = await get_pool()
    async with pool.acquire() as conn:
        query = """
            SELECT device_id, COUNT(*) as record_count
            FROM timestamped_data
            GROUP BY device_id
            ORDER BY record_count DESC
            LIMIT $1;
        """
        rows = await conn.fetch(query, limit)
        return [dict(row) for row in rows]

async def get_total_records_summary():
    pool = await get_pool()
    async with pool.acquire() as conn:
        tables = ['timestamped_data', 'latest_device_states', 'device_data']
        summary = {}
        total = 0
        for table in tables:
            try:
                count = await conn.fetchval(
                    "SELECT reltuples::bigint FROM pg_class WHERE relname = $1",
                    table
                )
                count = count or 0
                summary[table] = int(count)
                total += count
            except Exception as e:
                print(f"Could not get count for table {table}: {e}")
                summary[table] = 'error'
        summary['total'] = int(total)
        return summary
