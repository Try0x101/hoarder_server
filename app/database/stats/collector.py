from ..config import POOL_MIN_SIZE, POOL_MAX_SIZE, POOL_MAX_QUERIES, QUERY_TIMEOUT
from ..queue.monitor import queue_monitor
from ..circuit_breaker import db_circuit_breaker
from ..priority.manager import priority_manager
from ..health.checker import get_health_status

async def get_pool_stats(pool):
    if not pool:
        return {"status": "not_initialized", "healthy": False}
    
    queue_stats = queue_monitor.get_stats()
    health_status = get_health_status()
    
    return {
        "size": pool.get_size(),
        "min_size": POOL_MIN_SIZE,
        "max_size": POOL_MAX_SIZE,
        "idle_connections": pool.get_idle_size(),
        "max_queries": POOL_MAX_QUERIES,
        "healthy": health_status['healthy'],
        "connection_failures": health_status['connection_failures'],
        "circuit_breaker_state": db_circuit_breaker.state,
        "partitions_cached": len(getattr(get_pool_stats, '_partition_cache', set())),
        "query_timeout": QUERY_TIMEOUT,
        "queue_stats": queue_stats,
        "critical_pool_used": priority_manager.critical_connections_used,
        "critical_pool_size": priority_manager.critical_pool_size
    }