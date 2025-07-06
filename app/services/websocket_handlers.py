import datetime
from app.realtime.websocket.connection_manager import ConnectionManager
from app.realtime.timezone.manager import SharedTimezoneManager
from app.db import get_raw_latest_payload_for_device
from app.transforms import safe_float

def setup_websocket_events(sio, connection_manager: ConnectionManager, shared_timezone_manager: SharedTimezoneManager):
    
    @sio.event
    async def connect(sid, environ):
        client_ip = connection_manager._get_client_ip(environ)
        
        if not await connection_manager.can_accept_connection(client_ip):
            await sio.disconnect(sid)
            return False
        
        connection_manager.add_connection(sid, environ)
        print(f"[{datetime.datetime.now()}] WebSocket connected: {sid} from {client_ip} ({len(connection_manager.connections)} total)")

    @sio.event
    async def disconnect(sid):
        await connection_manager.remove_connection(sid)
        print(f"[{datetime.datetime.now()}] WebSocket disconnected: {sid}")

    @sio.event
    async def ping(sid):
        connection_manager.update_ping(sid)

    @sio.on("register_device")
    async def register_device(sid, device_id):
        if not await connection_manager.register_device(sid, device_id):
            return
            
        print(f"[{datetime.datetime.now()}] Device {device_id} registered for updates on {sid}")
        
        try:
            payload = await get_raw_latest_payload_for_device(device_id)
            if payload:
                lat, lon = safe_float(payload.get('lat')), safe_float(payload.get('lon'))
                if lat is not None and lon is not None:
                    coord_key = await shared_timezone_manager.subscribe_connection(sid, device_id, lat, lon)
                    if coord_key and sid in connection_manager.connections:
                        connection_manager.connections[sid]['coord_key'] = coord_key
        except Exception as e:
            print(f"[{datetime.datetime.now()}] Error registering device timezone: {e}")
