import datetime
from app.database import (
    init_db, get_pool, get_simple_pool_stats, save_timestamped_data,
    upsert_latest_state, get_raw_latest_payload_for_device,
    get_raw_latest_data_for_all_devices, get_timestamped_history,
    get_data_gaps, calculate_delta_changes, get_top_devices_by_records,
    get_total_records_summary
)

pool = None

async def get_database_pool():
    global pool
    if pool is None:
        pool = await get_pool()
    return pool

async def get_database_size():
    pool_instance = await get_database_pool()
    async with pool_instance.acquire() as conn:
        try:
            size_bytes = await conn.fetchval("SELECT pg_database_size(current_database())")
            return f"{round(size_bytes / (1024 * 1024), 2)} MB"
        except Exception:
            return "Unknown"

__all__ = [
    'init_db', 'get_pool', 'get_simple_pool_stats', 'save_timestamped_data',
    'upsert_latest_state', 'get_raw_latest_payload_for_device',
    'get_raw_latest_data_for_all_devices', 'get_timestamped_history',
    'get_data_gaps', 'calculate_delta_changes', 'get_top_devices_by_records',
    'get_total_records_summary', 'pool', 'get_database_size'
]
