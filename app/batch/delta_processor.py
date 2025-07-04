import json
import datetime
import asyncio
import asyncpg
import gc
from typing import AsyncGenerator
from app.weather import enrich_with_weather_data
from app.db import DB_CONFIG
from .memory_manager import BatchMemoryManager

MAX_DEVICE_STATES_CACHE = 30
MEMORY_CHECK_INTERVAL = 5

class DeltaProcessor:
    def __init__(self, memory_manager: BatchMemoryManager):
        self.memory_manager = memory_manager
        
    async def process_delta_stream(self, deltas: list, source_ip: str, user_agent: str) -> AsyncGenerator[str, None]:
        batch_id = f"delta_{datetime.datetime.now(datetime.timezone.utc).strftime('%Y%m%d%H%M%S')}_{hash(str(source_ip)) % 10000:04d}"
        
        try:
            estimated_memory = await self.memory_manager.estimate_batch_memory(len(deltas))
            memory_granted, message = await self.memory_manager.request_batch_memory(batch_id, estimated_memory)
            
            if not memory_granted:
                yield f"data: {json.dumps({'error': f'Delta memory allocation failed: {message}'})}\n\n"
                return
                
            processed = 0
            conn = await asyncpg.connect(**DB_CONFIG, command_timeout=60)
            
            try:
                for i, delta in enumerate(deltas):
                    device_id = delta.get('id')
                    if not device_id:
                        continue

                    data_timestamp = self._convert_relative_timestamp(delta.get('ts'))
                    
                    record_exists = await conn.fetchval(
                        "SELECT EXISTS(SELECT 1 FROM timestamped_data WHERE device_id = $1 AND data_timestamp = $2)",
                        device_id, data_timestamp
                    )
                    if record_exists:
                        processed += 1
                        continue

                    row = await conn.fetchrow("SELECT payload FROM latest_device_states WHERE device_id = $1", device_id)
                    base_payload = json.loads(row['payload']) if row and row['payload'] else {}
                    
                    reconstructed = {**base_payload, **delta}
                    
                    if 'lat' in reconstructed and 'lon' in reconstructed:
                        try:
                            enriched = await asyncio.wait_for(enrich_with_weather_data(reconstructed), timeout=2)
                            reconstructed = enriched
                        except (asyncio.TimeoutError, Exception):
                            pass

                    reconstructed.update({
                        'batch_id': batch_id,
                        'source_ip': source_ip,
                        'user_agent': user_agent
                    })

                    await conn.execute(
                        "INSERT INTO timestamped_data(device_id, payload, data_timestamp, data_type, is_offline, batch_id) VALUES($1, $2, $3, $4, $5, $6)",
                        device_id, json.dumps(reconstructed), data_timestamp, 'delta', True, batch_id
                    )

                    await conn.execute(
                        """
                        INSERT INTO latest_device_states(device_id, payload, received_at) VALUES($1, $2, now())
                        ON CONFLICT(device_id) DO UPDATE SET
                        payload = jsonb_recursive_merge(latest_device_states.payload, EXCLUDED.payload),
                        received_at = EXCLUDED.received_at
                        """,
                        device_id, json.dumps(reconstructed)
                    )

                    processed += 1
                    
                    if processed % MEMORY_CHECK_INTERVAL == 0:
                        pressure = self.memory_manager.get_system_memory_pressure()
                        yield f"data: {json.dumps({'processed': processed, 'pressure': pressure})}\n\n"
                        await self.memory_manager.update_batch_progress(batch_id, processed)
                        if pressure in ["HIGH", "CRITICAL"]:
                            await self.memory_manager.aggressive_memory_cleanup()

                yield f"data: {json.dumps({'completed': True, 'processed': processed})}\n\n"
                
            finally:
                await conn.close()
                
        except Exception as e:
            yield f"data: {json.dumps({'error': str(e)})}\n\n"
        finally:
            await self.memory_manager.release_batch_memory(batch_id)
            await self.memory_manager.aggressive_memory_cleanup()

    def _convert_relative_timestamp(self, ts_value):
        data_timestamp = datetime.datetime.now(datetime.timezone.utc)
        if ts_value:
            try:
                ts_seconds = int(ts_value)
                quarter_start = datetime.datetime(datetime.datetime.now().year, 1, 1, tzinfo=datetime.timezone.utc)
                data_timestamp = quarter_start + datetime.timedelta(seconds=ts_seconds)
            except (ValueError, TypeError):
                pass
        return data_timestamp

    def sort_deltas_chronologically(self, deltas: list) -> list:
        return sorted(deltas, key=lambda x: (x.get('id', ''), x.get('ts', 0)))

    def validate_delta_batch(self, deltas: list) -> tuple[bool, str]:
        if not isinstance(deltas, list):
            return False, "Expected JSON array"
        if len(deltas) == 0:
            return False, "Empty delta batch"
        missing_id_count = sum(1 for delta in deltas if not delta.get('id'))
        if missing_id_count > len(deltas) * 0.1:
            return False, f"Too many deltas missing device ID: {missing_id_count}/{len(deltas)}"
        return True, "Valid delta batch"

    def get_delta_stats(self):
        return {
            'max_device_cache': MAX_DEVICE_STATES_CACHE,
            'memory_check_interval': MEMORY_CHECK_INTERVAL,
            'memory_stats': self.memory_manager.get_memory_stats(),
            'system_pressure': self.memory_manager.get_system_memory_pressure()
        }
