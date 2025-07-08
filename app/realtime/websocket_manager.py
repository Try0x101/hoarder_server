import datetime
import socketio
from typing import Dict, Set
from app.database.operations import get_device_latest_data
from app.utils.transformer import transform_device_data

class ConnectionManager:
    def __init__(self):
        self.connections: Dict[str, Dict] = {}
        self.device_subscribers: Dict[str, Set[str]] = {}

    async def add_connection(self, sid: str, environ: dict):
        self.connections[sid] = {
            "connected_at": datetime.datetime.now(datetime.timezone.utc),
            "client_ip": environ.get("REMOTE_ADDR", "unknown")
        }

    async def remove_connection(self, sid: str):
        if sid in self.connections:
            del self.connections[sid]
        
        for device_id, sids in self.device_subscribers.items():
            sids.discard(sid)

    async def subscribe_device(self, sid: str, device_id: str):
        if device_id not in self.device_subscribers:
            self.device_subscribers[device_id] = set()
        self.device_subscribers[device_id].add(sid)

connection_manager = ConnectionManager()

def setup_websocket_events(sio: socketio.AsyncServer):
    
    @sio.event
    async def connect(sid, environ):
        await connection_manager.add_connection(sid, environ)
        print(f"WebSocket connected: {sid}")

    @sio.event
    async def disconnect(sid):
        await connection_manager.remove_connection(sid)
        print(f"WebSocket disconnected: {sid}")

    @sio.on("register_device")
    async def register_device(sid, device_id):
        await connection_manager.subscribe_device(sid, device_id)
        
        try:
            device_data = await get_device_latest_data(device_id)
            if device_data:
                transformed = await transform_device_data(device_data)
                await sio.emit("device_update", transformed, room=sid)
        except Exception as e:
            print(f"Error sending device data: {e}")

    @sio.event
    async def ping(sid):
        await sio.emit("pong", room=sid)
