import json
import datetime
import asyncio
import psutil
from typing import AsyncGenerator, Optional
from app.weather import enrich_with_weather_data
from app.db import save_timestamped_data, upsert_latest_state
from .memory_manager import BatchMemoryManager

MEMORY_CHECK_INTERVAL = 5

class StreamProcessor:
    def __init__(self, memory_manager: BatchMemoryManager):
        self.memory_manager = memory_manager
        
    async def process_batch_stream(self, batch_data: list, source_ip: str, user_agent: str, batch_id: str) -> AsyncGenerator[str, None]:
        try:
            estimated_memory = await self.memory_manager.estimate_batch_memory(len(batch_data))
            memory_granted, message = await self.memory_manager.request_batch_memory(batch_id, estimated_memory)
            if not memory_granted:
                yield f"data: {json.dumps({'error': f'Memory allocation failed: {message}'})}\n\n"
                return
                
            processed, errors, offline_sessions = 0, 0, 0
            current_bts, pressure = None, self.memory_manager.get_system_memory_pressure()
            chunk_size = self.memory_manager.get_adaptive_chunk_size()
            
            yield f"data: {json.dumps({'started': True, 'total_items': len(batch_data), 'estimated_memory_mb': f'{estimated_memory:.1f}', 'chunk_size': chunk_size})}\n\n"
            
            for i in range(0, len(batch_data), chunk_size):
                chunk = batch_data[i:i + chunk_size]
                
                if processed % (chunk_size * 3) == 0:
                    memory_mb = psutil.Process().memory_info().rss / 1024 / 1024
                    pressure = self.memory_manager.get_system_memory_pressure()
                    if pressure == "CRITICAL":
                        await self.memory_manager.aggressive_memory_cleanup()
                    yield f"data: {json.dumps({'processed': processed, 'memory_mb': f'{memory_mb:.1f}', 'pressure': pressure})}\n\n"
                
                chunk_errors = 0
                for item in chunk:
                    if not isinstance(item, dict):
                        chunk_errors += 1
                        continue
                    try:
                        item_copy = {'source_ip': source_ip, 'user_agent': user_agent, 'batch_id': batch_id, **item}
                        
                        if 'bts' in item_copy:
                            current_bts = self._parse_base_timestamp(item_copy['bts'])
                            if current_bts:
                                offline_sessions += 1
                                yield f"data: {json.dumps({'new_offline_session': offline_sessions, 'bts': current_bts.isoformat()})}\n\n"
                        
                        actual_timestamp = self._calculate_actual_timestamp(item_copy, current_bts)
                        await self._process_batch_item_optimized(item_copy, actual_timestamp)
                        processed += 1
                    except Exception:
                        chunk_errors += 1
                        errors += 1
                
                del chunk
                
                if errors > len(batch_data) * 0.15:
                    yield f"data: {json.dumps({'error': f'Too many processing errors: {errors}/{len(batch_data)}'})}\n\n"
                    return
                
                current_pressure = self.memory_manager.get_system_memory_pressure()
                if current_pressure != pressure:
                    pressure = current_pressure
                    chunk_size = self.memory_manager.get_adaptive_chunk_size()
                    yield f"data: {json.dumps({'chunk_size_adjusted': chunk_size, 'pressure': current_pressure})}\n\n"

                if processed % MEMORY_CHECK_INTERVAL == 0:
                    await self.memory_manager.update_batch_progress(batch_id, processed)
            
            final_memory = psutil.Process().memory_info().rss / 1024 / 1024
            yield f"data: {json.dumps({'completed': True, 'processed': processed, 'errors': errors, 'offline_sessions': offline_sessions, 'final_memory_mb': f'{final_memory:.1f}'})}\n\n"
        except Exception as e:
            yield f"data: {json.dumps({'error': f'Batch processing failed: {str(e)}'})}\n\n"
        finally:
            await self.memory_manager.release_batch_memory(batch_id)
            await self.memory_manager.aggressive_memory_cleanup()

    def _parse_base_timestamp(self, bts_value) -> Optional[datetime.datetime]:
        if not bts_value: return None
        try:
            unix_timestamp = float(bts_value)
            if unix_timestamp > 1000000000000: unix_timestamp /= 1000
            return datetime.datetime.fromtimestamp(unix_timestamp, tz=datetime.timezone.utc)
        except (ValueError, TypeError, OSError): return None

    def _calculate_actual_timestamp(self, item: dict, current_bts: Optional[datetime.datetime]) -> datetime.datetime:
        if "bts" in item:
            return self._parse_base_timestamp(item["bts"]) or datetime.datetime.now(datetime.timezone.utc)
        if current_bts and "tso" in item:
            try:
                return current_bts + datetime.timedelta(seconds=float(item["tso"]))
            except (ValueError, TypeError): pass
        if "timestamp" in item:
            try:
                ts_val = item["timestamp"]
                if isinstance(ts_val, (int, float)):
                    if ts_val > 1000000000000: ts_val /= 1000
                    return datetime.datetime.fromtimestamp(ts_val, tz=datetime.timezone.utc)
                if isinstance(ts_val, str):
                    return datetime.datetime.fromisoformat(ts_val.strip().replace("Z", "+00:00"))
            except (ValueError, TypeError, OSError): pass
        return datetime.datetime.now(datetime.timezone.utc)

    async def _process_batch_item_optimized(self, item: dict, data_timestamp: datetime.datetime):
        await save_timestamped_data(item, data_timestamp, is_offline=True, batch_id=item.get('batch_id'))
        if item.get('lat') and item.get('lon'):
            try:
                enriched = await asyncio.wait_for(enrich_with_weather_data(item), timeout=3)
                await upsert_latest_state(enriched)
            except (asyncio.TimeoutError, Exception):
                await upsert_latest_state(item)
        else:
            await upsert_latest_state(item)

    def get_processor_stats(self):
        return {
            'memory_stats': self.memory_manager.get_memory_stats(),
            'system_pressure': self.memory_manager.get_system_memory_pressure(),
            'adaptive_chunk_size': self.memory_manager.get_adaptive_chunk_size()
        }
