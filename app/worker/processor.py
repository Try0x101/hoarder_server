import asyncio
import orjson
import datetime
from app.db import get_database_pool
from app.api.telemetry.processing import critical_data_storage, weather_enrichment_and_state_update
from app.api.telemetry.timestamp_parser import parse_device_timestamp

async def process_telemetry_payload(payload: dict):
    data_timestamp = parse_device_timestamp(payload)
    # The copy() is important to avoid side effects
    await critical_data_storage(payload.copy(), data_timestamp)
    await weather_enrichment_and_state_update(payload.copy())

async def process_job(job_id: int, payload: dict, endpoint: str):
    try:
        if endpoint == 'telemetry':
            await process_telemetry_payload(payload)
        elif endpoint == 'batch':
            # Placeholder for batch processing logic
            print(f"[{datetime.datetime.now()}] WARN: Batch processing not yet implemented for job {job_id}.")
            pass
        elif endpoint == 'batch-delta':
            # Placeholder for delta batch processing logic
            print(f"[{datetime.datetime.now()}] WARN: Delta batch processing not yet implemented for job {job_id}.")
            pass
        else:
            print(f"[{datetime.datetime.now()}] ERROR: Unknown endpoint type '{endpoint}' for job {job_id}.")
            return False
        return True
    except Exception as e:
        print(f"[{datetime.datetime.now()}] CRITICAL: Failed to process job {job_id}. Error: {e}")
        # In a real scenario, you might move this to a dead-letter queue
        return False

async def worker_loop():
    print(f"[{datetime.datetime.now()}] Worker loop started.")
    pool = await get_database_pool()
    
    while True:
        try:
            async with pool.acquire() as conn:
                # Fetch a batch of jobs, locking the rows to prevent other workers from taking them
                jobs = await conn.fetch("""
                    SELECT id, payload, ingest_endpoint
                    FROM ingested_data
                    WHERE processed = FALSE
                    ORDER BY received_at ASC
                    LIMIT 10
                    FOR UPDATE SKIP LOCKED
                """)
                
                if not jobs:
                    await asyncio.sleep(2)  # Sleep if no jobs
                    continue

                for job in jobs:
                    payload_data = orjson.loads(job['payload'])
                    success = await process_job(job['id'], payload_data, job['ingest_endpoint'])
                    
                    if success:
                        await conn.execute("UPDATE ingested_data SET processed = TRUE WHERE id = $1", job['id'])

        except Exception as e:
            print(f"[{datetime.datetime.now()}] Worker main loop error: {e}")
            await asyncio.sleep(5) # Wait longer after a major error
