from app.database import (
    init_db,
    get_pool,
    create_partition_for_date,
    ensure_partition_exists,
    close_pool,
    get_pool_stats,
    DB_CONFIG,
    upsert_latest_state,
    save_timestamped_data,
    get_latest_data,
    get_raw_latest_payload_for_device,
    get_raw_latest_data_for_all_devices,
    calculate_delta_changes,
    get_timestamped_history,
    get_data_gaps,
    get_top_devices_by_records,
    get_total_records_summary
)

pool = None

async def get_database_pool():
    global pool
    if pool is None:
        pool = await get_pool()
    return pool

async def save_data(data: dict):
    try:
        device_id = data.get("device_id") or data.get("id")
        if not device_id:
            print(f"[{datetime.datetime.now(datetime.timezone.utc)}] WARNING: No device_id found in data")
            return

        await upsert_latest_state(data)

    except Exception as e:
        print(f"[{datetime.datetime.now(datetime.timezone.utc)}] CRITICAL ERROR in save_data: {str(e)}")

async def get_total_records_count():
    pool_instance = await get_database_pool()
    async with pool_instance.acquire() as conn:
        try:
            count = await conn.fetchval("SELECT reltuples::bigint FROM pg_class WHERE relname = 'timestamped_data'")
            return int(count) if count else 0
        except Exception as e:
            print(f"Error getting record count: {e}")
            return 0

async def get_database_size():
    pool_instance = await get_database_pool()
    async with pool_instance.acquire() as conn:
        try:
            size_bytes = await conn.fetchval("SELECT pg_database_size(current_database())")
            size_mb = round(size_bytes / (1024 * 1024), 2)
            return f"{size_mb} MB"
        except Exception as e:
            print(f"Error getting database size: {e}")
            return "Unknown"

async def cleanup_old_data():
    import datetime
    pool_instance = await get_database_pool()
    async with pool_instance.acquire() as conn:
        try:
            cutoff_date = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(days=365)

            deleted_count = await conn.execute(
                "DELETE FROM timestamped_data WHERE data_timestamp < $1",
                cutoff_date
            )

            print(f"[{datetime.datetime.now(datetime.timezone.utc)}] Cleaned up old data: {deleted_count} records")
            return deleted_count
        except Exception as e:
            print(f"[{datetime.datetime.now(datetime.timezone.utc)}] Error in cleanup_old_data: {e}")
            return 0

__all__ = [
    'init_db',
    'get_pool',
    'get_pool_stats',
    'save_data',
    'save_timestamped_data',
    'upsert_latest_state',
    'get_latest_data',
    'get_raw_latest_payload_for_device',
    'get_raw_latest_data_for_all_devices',
    'get_timestamped_history',
    'get_data_gaps',
    'calculate_delta_changes',
    'get_total_records_count',
    'get_database_size',
    'cleanup_old_data',
    'get_top_devices_by_records',
    'get_total_records_summary',
    'pool'
]
