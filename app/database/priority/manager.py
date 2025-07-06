import asyncio
from fastapi import HTTPException
from ..queue.monitor import queue_monitor
from ..config import CONNECTION_QUEUE_TIMEOUT

class PriorityConnectionManager:
    def __init__(self):
        self.critical_pool_size = 3
        self.critical_connections_used = 0
        self.critical_lock = asyncio.Lock()
        
    async def acquire_critical_connection(self, pool, timeout=CONNECTION_QUEUE_TIMEOUT):
        async with self.critical_lock:
            if self.critical_connections_used >= self.critical_pool_size:
                stats = queue_monitor.get_stats()
                if stats['queue_pressure'] > 0.8:
                    raise HTTPException(503, "Critical database pool exhausted")
            
            try:
                queue_monitor.request_started()
                conn = await asyncio.wait_for(pool.acquire(), timeout=timeout)
                self.critical_connections_used += 1
                return conn, True
            except asyncio.TimeoutError:
                queue_monitor.request_timeout()
                raise HTTPException(503, "Database connection timeout (critical)")
            except Exception as e:
                queue_monitor.request_completed()
                raise HTTPException(503, f"Database connection failed: {str(e)}")
    
    async def acquire_general_connection(self, pool, timeout=CONNECTION_QUEUE_TIMEOUT):
        try:
            queue_monitor.request_started()
            
            stats = queue_monitor.get_stats()
            if stats['pending_requests'] > 8:
                queue_monitor.queue_full()
                raise HTTPException(503, "Database overloaded, try again")
            
            adjusted_timeout = max(1, timeout - (stats['queue_pressure'] * 2))
            conn = await asyncio.wait_for(pool.acquire(), timeout=adjusted_timeout)
            return conn, False
        except asyncio.TimeoutError:
            queue_monitor.request_timeout()
            raise HTTPException(503, "Database connection timeout")
        except Exception as e:
            queue_monitor.request_completed()
            raise HTTPException(503, f"Database connection failed: {str(e)}")
    
    async def release_connection(self, pool, conn, is_critical=False):
        try:
            await pool.release(conn)
            if is_critical:
                async with self.critical_lock:
                    self.critical_connections_used = max(0, self.critical_connections_used - 1)
            queue_monitor.request_completed()
        except Exception as e:
            import datetime
            print(f"[{datetime.datetime.now(datetime.timezone.utc)}] Connection release error: {e}")

priority_manager = PriorityConnectionManager()