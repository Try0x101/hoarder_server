import datetime
from fastapi import Request
from app.responses import PrettyJSONResponse
from app.realtime.websocket.connection_manager import ConnectionManager
from app.db import get_database_size, get_total_records_summary, get_top_devices_by_records

def setup_api_endpoints(app, connection_manager: ConnectionManager):
    
    @app.get("/", response_class=PrettyJSONResponse)
    async def root_endpoints(request: Request):
        base_url = f"{request.url.scheme}://{request.url.netloc}"
        
        try:
            db_size = await get_database_size()
            records_summary = await get_total_records_summary()
            top_devices = await get_top_devices_by_records(limit=5)
        except Exception:
            db_size, records_summary, top_devices = "unavailable", {"error": "db_unavailable"}, []

        connection_stats = connection_manager.get_stats()

        return {
            "server": "Hoarder Server - Advanced IoT Telemetry Platform",
            "version": "3.3.0",
            "status": "healthy",
            "timestamp_utc": datetime.datetime.now(datetime.timezone.utc).isoformat(),
            "statistics": {
                "database_size": db_size,
                "total_records": records_summary,
                "top_devices_by_records": top_devices,
                "websocket_stats": connection_stats
            },
            "endpoints": {
                "self": f"{base_url}/",
                "latest_data_all_devices": f"{base_url}/data/latest",
                "device_history": f"{base_url}/data/history?device_id={{device_id}}",
                "websocket": f"ws://{request.url.netloc}/socket.io/"
            }
        }
