import time
import datetime

async def cleanup_stale_connections(connection_manager, sio):
    try:
        current_time = time.time()
        if current_time - connection_manager.last_cleanup < 30:
            return
            
        stale_sids = []
        memory_mb = connection_manager.get_memory_usage_mb()
        
        for sid, connection in connection_manager.connections.items():
            try:
                connection_age = current_time - connection.get('last_ping', 0)
                
                if connection_age > 180:
                    stale_sids.append(sid)
                elif memory_mb > 250 and connection_age > 90:
                    stale_sids.append(sid)
            except Exception:
                stale_sids.append(sid)
                
        for sid in stale_sids:
            try:
                await sio.disconnect(sid)
                await connection_manager.remove_connection(sid)
            except Exception:
                pass
                
        if len(stale_sids) > 0:
            print(f"[{datetime.datetime.now()}] Cleaned {len(stale_sids)} stale connections")
            
        connection_manager.last_cleanup = current_time
    except Exception as e:
        print(f"[{datetime.datetime.now()}] Error in cleanup: {e}")
