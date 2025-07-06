import datetime
import asyncio
from app.db import save_timestamped_data, upsert_latest_state
from app.weather import enrich_with_weather_data

async def critical_data_storage(data: dict, data_timestamp: datetime.datetime):
    try:
        await save_timestamped_data(data, data_timestamp, is_offline=False)
    except Exception as e:
        print(f"[{datetime.datetime.now(datetime.timezone.utc)}] CRITICAL: Data storage failed: {e}")
        raise

async def weather_enrichment_and_state_update(data: dict):
    try:
        if data.get("lat") and data.get("lon"):
            try:
                enriched_payload = await asyncio.wait_for(enrich_with_weather_data(data), timeout=6)
                await upsert_latest_state(enriched_payload)
            except asyncio.TimeoutError:
                await upsert_latest_state(data)
            except Exception:
                await upsert_latest_state(data)
        else:
            await upsert_latest_state(data)
    except Exception as e:
        print(f"[{datetime.datetime.now(datetime.timezone.utc)}] ERROR in state update: {e}")
