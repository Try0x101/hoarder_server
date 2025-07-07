import json
import datetime
import asyncio
import asyncpg
import gc
from typing import AsyncGenerator, Optional, Dict, Any
from collections import defaultdict
from app.weather import enrich_with_weather_data
from app.database import DB_CONFIG
from .memory_manager import BatchMemoryManager

MAX_DEVICE_STATES_CACHE = 20
MEMORY_CHECK_INTERVAL = 5

class DeltaProcessor:
    def __init__(self, memory_manager: BatchMemoryManager):
        self.memory_manager = memory_manager
        self.device_locks = defaultdict(asyncio.Lock)
        self.device_states_cache = {}
        self.cache_lock = asyncio.Lock()
        
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
                    try:
                        result = await self._process_single_delta_safe(conn, delta, source_ip, user_agent, batch_id)
                        if result:
                            processed += 1
                        
                        if processed % MEMORY_CHECK_INTERVAL == 0:
                            pressure = self.memory_manager.get_system_memory_pressure()
                            yield f"data: {json.dumps({'processed': processed, 'pressure': pressure})}\n\n"
                            await self.memory_manager.update_batch_progress(batch_id, processed)
                            
                            if pressure in ["HIGH", "CRITICAL"]:
                                await self._cleanup_device_cache()
                                await self.memory_manager.aggressive_memory_cleanup()
                                
                    except Exception as e:
                        print(f"[{datetime.datetime.now(datetime.timezone.utc)}] Delta processing error: {e}")
                        continue

                yield f"data: {json.dumps({'completed': True, 'processed': processed})}\n\n"
                
            finally:
                await conn.close()
                await self._cleanup_device_cache()
                
        except Exception as e:
            yield f"data: {json.dumps({'error': str(e)})}\n\n"
        finally:
            await self.memory_manager.release_batch_memory(batch_id)

    async def _process_single_delta_safe(self, conn, delta: Dict[str, Any], source_ip: str, user_agent: str, batch_id: str) -> bool:
        device_id = delta.get('id')
        if not device_id or not isinstance(device_id, str):
            return False

        device_id = str(device_id).strip()[:100]
        if not device_id:
            return False

        async with self.device_locks[device_id]:
            try:
                data_timestamp = self._convert_relative_timestamp(delta.get('ts'))
                
                record_exists = await conn.fetchval(
                    "SELECT EXISTS(SELECT 1 FROM timestamped_data WHERE device_id = $1 AND data_timestamp = $2)",
                    device_id, data_timestamp, timeout=10
                )
                if record_exists:
                    return True

                base_payload = await self._get_device_base_state_cached(conn, device_id)
                if base_payload is None:
                    print(f"[{datetime.datetime.now(datetime.timezone.utc)}] Warning: No base state for device {device_id}")
                    base_payload = {}
                
                reconstructed = await self._reconstruct_payload_safe(delta, base_payload, source_ip, user_agent, batch_id)
                
                if reconstructed.get('lat') and reconstructed.get('lon'):
                    try:
                        enriched = await asyncio.wait_for(enrich_with_weather_data(reconstructed), timeout=3)
                        reconstructed = enriched
                    except (asyncio.TimeoutError, Exception):
                        pass

                async with conn.transaction():
                    await self._save_delta_data(conn, device_id, reconstructed, data_timestamp, batch_id)
                    await self._update_device_state_atomic(conn, device_id, reconstructed)
                
                await self._update_device_cache(device_id, reconstructed)
                return True
                
            except Exception as e:
                print(f"[{datetime.datetime.now(datetime.timezone.utc)}] Single delta error for {device_id}: {e}")
                return False

    async def _get_device_base_state_cached(self, conn, device_id: str) -> Dict[str, Any]:
        async with self.cache_lock:
            if device_id in self.device_states_cache:
                cache_entry = self.device_states_cache[device_id]
                if time.time() - cache_entry['timestamp'] < 300:
                    return cache_entry['state'].copy()

        try:
            row = await conn.fetchrow(
                "SELECT payload FROM latest_device_states WHERE device_id = $1", 
                device_id, timeout=10
            )
            if row and row['payload']:
                state = json.loads(row['payload'])
                await self._update_device_cache(device_id, state)
                return state.copy()
        except Exception as e:
            print(f"[{datetime.datetime.now(datetime.timezone.utc)}] Error getting base state for {device_id}: {e}")
        
        return {}

    async def _update_device_cache(self, device_id: str, state: Dict[str, Any]):
        async with self.cache_lock:
            import time
            self.device_states_cache[device_id] = {
                'state': state.copy(),
                'timestamp': time.time()
            }
            
            if len(self.device_states_cache) > MAX_DEVICE_STATES_CACHE:
                oldest_key = min(
                    self.device_states_cache.keys(),
                    key=lambda k: self.device_states_cache[k]['timestamp']
                )
                del self.device_states_cache[oldest_key]

    async def _cleanup_device_cache(self):
        async with self.cache_lock:
            import time
            current_time = time.time()
            expired_keys = [
                k for k, v in self.device_states_cache.items()
                if current_time - v['timestamp'] > 600
            ]
            for key in expired_keys:
                del self.device_states_cache[key]

    async def _reconstruct_payload_safe(self, delta: Dict[str, Any], base_payload: Dict[str, Any], source_ip: str, user_agent: str, batch_id: str) -> Dict[str, Any]:
        try:
            reconstructed = {}
            reconstructed.update(base_payload)
            reconstructed.update(delta)
            reconstructed.update({
                'batch_id': batch_id,
                'source_ip': source_ip,
                'user_agent': user_agent,
                'data_type': 'delta'
            })
            return reconstructed
        except Exception as e:
            print(f"[{datetime.datetime.now(datetime.timezone.utc)}] Payload reconstruction error: {e}")
            return {
                'id': delta.get('id', 'unknown'),
                'batch_id': batch_id,
                'source_ip': source_ip,
                'user_agent': user_agent,
                'data_type': 'delta',
                'reconstruction_error': str(e)
            }

    async def _save_delta_data(self, conn, device_id: str, payload: Dict[str, Any], data_timestamp: datetime.datetime, batch_id: str):
        await conn.execute(
            "INSERT INTO timestamped_data(device_id, payload, data_timestamp, data_type, is_offline, batch_id) VALUES($1, $2, $3, $4, $5, $6)",
            device_id, json.dumps(payload, separators=(',', ':')), data_timestamp, 'delta', True, batch_id,
            timeout=15
        )

    async def _update_device_state_atomic(self, conn, device_id: str, payload: Dict[str, Any]):
        await conn.execute(
            """
            INSERT INTO latest_device_states(device_id, payload, received_at) VALUES($1, $2, now())
            ON CONFLICT(device_id) DO UPDATE SET
            payload = jsonb_recursive_merge(latest_device_states.payload, EXCLUDED.payload),
            received_at = EXCLUDED.received_at
            """,
            device_id, json.dumps(payload, separators=(',', ':')), timeout=15
        )

    def _convert_relative_timestamp(self, ts_value) -> datetime.datetime:
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
        try:
            return sorted(deltas, key=lambda x: (x.get('id', ''), x.get('ts', 0)))
        except Exception:
            return deltas

    def validate_delta_batch(self, deltas: list) -> tuple[bool, str]:
        try:
            if not isinstance(deltas, list):
                return False, "Expected JSON array"
            if len(deltas) == 0:
                return False, "Empty delta batch"
            
            missing_id_count = sum(1 for delta in deltas if not delta.get('id'))
            if missing_id_count > len(deltas) * 0.1:
                return False, f"Too many deltas missing device ID: {missing_id_count}/{len(deltas)}"
            return True, "Valid delta batch"
        except Exception as e:
            return False, f"Validation error: {e}"

    def get_delta_stats(self) -> Dict[str, Any]:
        try:
            return {
                'max_device_cache': MAX_DEVICE_STATES_CACHE,
                'cached_devices': len(self.device_states_cache),
                'memory_check_interval': MEMORY_CHECK_INTERVAL,
                'memory_stats': self.memory_manager.get_memory_stats(),
                'system_pressure': self.memory_manager.get_system_memory_pressure()
            }
        except Exception:
            return {'error': 'stats_collection_failed'}
