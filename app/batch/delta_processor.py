import json
import datetime
import asyncio
import asyncpg
import time
from typing import AsyncGenerator, Dict, Any
from collections import defaultdict
from app.weather import enrich_with_weather_data
from app.database import DB_CONFIG
from app.database.helpers import deep_merge, safe_json_serialize, extract_device_id
from .memory_manager import BatchMemoryManager

class DeltaProcessor:
    def __init__(self, memory_manager: BatchMemoryManager):
        self.memory_manager = memory_manager
        self.device_locks = defaultdict(asyncio.Lock)
        self.device_states_cache = {}

    async def process_delta_stream(self, deltas: list, source_ip: str, user_agent: str) -> AsyncGenerator[str, None]:
        batch_id = f"delta_{datetime.datetime.now(datetime.timezone.utc).strftime('%Y%m%d%H%M%S')}_{hash(source_ip) % 10000:04d}"
        memory_granted, msg = await self.memory_manager.request_batch_memory(batch_id, len(deltas))
        if not memory_granted:
            yield f"data: {json.dumps({'error': f'Memory allocation failed: {msg}'})}\n\n"
            return
        
        try:
            processed = 0
            conn = await asyncpg.connect(**DB_CONFIG, command_timeout=60)
            try:
                for i, delta in enumerate(deltas):
                    if await self._process_single_delta(conn, delta, source_ip, user_agent, batch_id):
                        processed += 1
                    if (i + 1) % 5 == 0:
                        yield f"data: {json.dumps({'processed': i + 1})}\n\n"
                yield f"data: {json.dumps({'completed': True, 'processed': processed})}\n\n"
            finally:
                await conn.close()
        except Exception as e:
            yield f"data: {json.dumps({'error': str(e)})}\n\n"
        finally:
            await self.memory_manager.release_batch_memory(batch_id)
            self.device_states_cache.clear()

    async def _process_single_delta(self, conn, delta: Dict[str, Any], source_ip: str, user_agent: str, batch_id: str) -> bool:
        device_id = extract_device_id(delta)
        if not device_id: return False

        async with self.device_locks[device_id]:
            try:
                base_payload = await self._get_device_base_state(conn, device_id)
                
                # Use the robust deep_merge, same as the full-payload endpoint
                reconstructed = deep_merge(delta, base_payload)
                reconstructed.update({'batch_id': batch_id, 'source_ip': source_ip, 'user_agent': user_agent, 'data_type': 'delta'})
                
                if reconstructed.get('lat') and reconstructed.get('lon'):
                    reconstructed = await enrich_with_weather_data(reconstructed)

                final_payload = safe_json_serialize(reconstructed)
                data_timestamp = self._convert_relative_timestamp(delta.get('ts'))
                
                async with conn.transaction():
                    await conn.execute(
                        "INSERT INTO timestamped_data(device_id, payload, data_timestamp, data_type, is_offline, batch_id) VALUES($1, $2, $3, 'delta', true, $4) ON CONFLICT (device_id, data_timestamp) DO NOTHING",
                        device_id, final_payload, data_timestamp, batch_id
                    )
                    # Use a full replacement, same as the full-payload endpoint
                    await conn.execute(
                        "INSERT INTO latest_device_states(device_id, payload, received_at) VALUES($1, $2, now()) ON CONFLICT(device_id) DO UPDATE SET payload = EXCLUDED.payload, received_at = EXCLUDED.received_at",
                        device_id, final_payload
                    )
                
                self.device_states_cache[device_id] = {'state': reconstructed, 'timestamp': time.time()}
                return True
            except Exception as e:
                print(f"[{datetime.datetime.now(datetime.timezone.utc)}] Single delta error for {device_id}: {e}")
                return False

    async def _get_device_base_state(self, conn, device_id: str) -> Dict[str, Any]:
        cache_entry = self.device_states_cache.get(device_id)
        if cache_entry and time.time() - cache_entry.get('timestamp', 0) < 300:
            return cache_entry.get('state', {}).copy()

        row = await conn.fetchrow("SELECT payload FROM latest_device_states WHERE device_id = $1", device_id, timeout=10)
        state = json.loads(row['payload']) if row and row['payload'] else {}
        self.device_states_cache[device_id] = {'state': state, 'timestamp': time.time()}
        
        if len(self.device_states_cache) > 20: # MAX_DEVICE_STATES_CACHE
            oldest_key = min(self.device_states_cache, key=lambda k: self.device_states_cache[k]['timestamp'])
            del self.device_states_cache[oldest_key]
        return state.copy()

    def _convert_relative_timestamp(self, ts_value) -> datetime.datetime:
        now = datetime.datetime.now(datetime.timezone.utc)
        if not ts_value: return now
        try:
            return datetime.datetime.fromtimestamp(float(ts_value), tz=datetime.timezone.utc)
        except (ValueError, TypeError, OSError):
            return now

    def sort_deltas_chronologically(self, deltas: list) -> list:
        return sorted(deltas, key=lambda x: (x.get('id', ''), x.get('ts', 0)))

    def validate_delta_batch(self, deltas: list) -> tuple[bool, str]:
        if not isinstance(deltas, list) or not deltas: return False, "Expected non-empty JSON array"
        if sum(1 for d in deltas if not d.get('id')) > len(deltas) * 0.1:
            return False, "Too many deltas missing device ID"
        return True, "Valid delta batch"
