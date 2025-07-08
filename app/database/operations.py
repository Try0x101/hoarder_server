import json
import datetime
from typing import List, Dict, Any, Optional, Tuple
from .connection import safe_db_operation

async def get_latest_device_data() -> List[Dict[str, Any]]:
    async def query_operation(conn):
        rows = await conn.fetch(
            "SELECT device_id, payload, received_at FROM latest_device_states ORDER BY received_at DESC"
        )
        result = []
        for row in rows:
            try:
                payload = json.loads(row['payload']) if isinstance(row['payload'], str) else row['payload']
                result.append({
                    "device_id": row['device_id'],
                    "payload": payload,
                    "received_at": row['received_at'].isoformat() if row['received_at'] else None
                })
            except (json.JSONDecodeError, TypeError):
                continue
        return result
    
    return await safe_db_operation(query_operation)

async def get_device_latest_data(device_id: str) -> Optional[Dict[str, Any]]:
    async def query_operation(conn):
        row = await conn.fetchrow(
            "SELECT payload, received_at FROM latest_device_states WHERE device_id = $1",
            device_id
        )
        if not row:
            return None
        try:
            payload = json.loads(row['payload']) if isinstance(row['payload'], str) else row['payload']
            return {
                "device_id": device_id,
                "payload": payload,
                "received_at": row['received_at'].isoformat() if row['received_at'] else None
            }
        except (json.JSONDecodeError, TypeError):
            return None
    
    return await safe_db_operation(query_operation)

async def get_device_history(device_id: str, days: int = 30, limit: int = 100, cursor: Optional[str] = None) -> Tuple[List[Dict], Optional[str]]:
    async def query_operation(conn):
        cutoff_date = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(days=days)
        query = "SELECT payload, data_timestamp FROM timestamped_data WHERE device_id = $1 AND data_timestamp >= $2"
        params = [device_id, cutoff_date]
        
        if cursor:
            query += " AND data_timestamp < $3"
            params.append(datetime.datetime.fromisoformat(cursor))
        
        query += " ORDER BY data_timestamp DESC LIMIT $" + str(len(params) + 1)
        params.append(limit)
        
        rows = await conn.fetch(query, *params)
        
        result = []
        for row in rows:
            try:
                payload = json.loads(row['payload']) if isinstance(row['payload'], str) else row['payload']
                result.append({
                    "payload": payload,
                    "timestamp": row['data_timestamp'].isoformat()
                })
            except (json.JSONDecodeError, TypeError):
                continue
        
        next_cursor = rows[-1]['data_timestamp'].isoformat() if len(rows) == limit else None
        return result, next_cursor
    
    return await safe_db_operation(query_operation)

async def get_active_devices(days: int = 30) -> List[Dict[str, Any]]:
    async def query_operation(conn):
        cutoff_date = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(days=days)
        rows = await conn.fetch(
            "SELECT device_id, received_at FROM latest_device_states WHERE received_at >= $1 ORDER BY received_at DESC",
            cutoff_date
        )
        return [{"device_id": row['device_id'], "last_active": row['received_at'].isoformat()} for row in rows]
    
    return await safe_db_operation(query_operation)
