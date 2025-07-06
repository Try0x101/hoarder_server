import datetime
from app import db

async def get_active_devices(days: int):
    async with (await db.get_database_pool()).acquire() as conn:
        cutoff_date = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(days=days)
        return await conn.fetch(
            "SELECT device_id, received_at as last_active FROM latest_device_states WHERE received_at >= $1 ORDER BY last_active DESC",
            cutoff_date, timeout=15
        )

async def get_device_gaps_data(device_id: str, cutoff_date: datetime.datetime):
    async with (await db.get_database_pool()).acquire() as conn:
        gaps_query = """
        WITH time_series AS (
            SELECT device_id, data_timestamp,
                   LEAD(data_timestamp) OVER (PARTITION BY device_id ORDER BY data_timestamp) as next_timestamp
            FROM timestamped_data 
            WHERE device_id = $1 AND data_timestamp >= $2
        )
        SELECT device_id, data_timestamp as gap_start, next_timestamp as gap_end,
               EXTRACT(EPOCH FROM (next_timestamp - data_timestamp))/60 as gap_minutes
        FROM time_series 
        WHERE EXTRACT(EPOCH FROM (next_timestamp - data_timestamp))/60 > 15 
        ORDER BY gap_minutes DESC LIMIT 100
        """
        return await conn.fetch(gaps_query, device_id, cutoff_date, timeout=15)

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

async def get_device_activity_stats(device_id: str, cutoff_date: datetime.datetime):
    async with (await db.get_database_pool()).acquire() as conn:
        activity_query = """
        WITH hourly_stats AS (
            SELECT device_id, DATE_TRUNC('hour', data_timestamp) as hour, COUNT(*) as records_per_hour
            FROM timestamped_data WHERE device_id = $1 AND data_timestamp >= $2
            GROUP BY device_id, hour ORDER BY hour
        )
        SELECT AVG(records_per_hour) as avg_records_per_hour, MAX(records_per_hour) as max_records_per_hour,
               MIN(records_per_hour) as min_records_per_hour, COUNT(DISTINCT hour) as active_hours,
               SUM(records_per_hour) as total_records
        FROM hourly_stats
        """
        return await conn.fetchrow(activity_query, device_id, cutoff_date, timeout=15)

async def get_device_position_stats(device_id: str, cutoff_date: datetime.datetime):
    async with (await db.get_database_pool()).acquire() as conn:
        position_query = """
        SELECT COUNT(*) as position_count, AVG(CAST(payload->>'lat' AS FLOAT)) as avg_lat,
               AVG(CAST(payload->>'lon' AS FLOAT)) as avg_lon, MIN(CAST(payload->>'lat' AS FLOAT)) as min_lat,
               MAX(CAST(payload->>'lat' AS FLOAT)) as max_lat, MIN(CAST(payload->>'lon' AS FLOAT)) as min_lon,
               MAX(CAST(payload->>'lon' AS FLOAT)) as max_lon
        FROM timestamped_data 
        WHERE device_id = $1 AND data_timestamp >= $2 
        AND payload->>'lat' IS NOT NULL AND payload->>'lon' IS NOT NULL
        AND CAST(payload->>'lat' AS FLOAT) BETWEEN -90 AND 90
        AND CAST(payload->>'lon' AS FLOAT) BETWEEN -180 AND 180
        """
        return await conn.fetchrow(position_query, device_id, cutoff_date, timeout=15)
