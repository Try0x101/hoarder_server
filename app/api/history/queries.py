import datetime
from app import db

async def get_active_devices(days: int):
    async with (await db.get_database_pool()).acquire() as conn:
        cutoff_date = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(days=days)
        return await conn.fetch(
            "SELECT device_id, received_at as last_active FROM latest_device_states WHERE received_at >= $1 ORDER BY last_active DESC",
            cutoff_date, timeout=15
        )

async def get_device_statistics(device_id: str, cutoff_date: datetime.datetime):
    async with (await db.get_database_pool()).acquire() as conn:
        stats_query = """
        SELECT COUNT(*) as record_count, MIN(data_timestamp) as first_record,
               MAX(data_timestamp) as last_record,
               EXTRACT(EPOCH FROM (MAX(data_timestamp) - MIN(data_timestamp)))/3600 as hours_span
        FROM timestamped_data 
        WHERE device_id = $1 AND data_timestamp >= $2
        """
        return await conn.fetchrow(stats_query, device_id, cutoff_date, timeout=15)
