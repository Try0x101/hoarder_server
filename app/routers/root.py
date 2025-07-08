import datetime
from fastapi import APIRouter, Request
from responses import PrettyJSONResponse
from database.connection import get_simple_pool_stats

router = APIRouter()

@router.get("/", response_class=PrettyJSONResponse)
async def root_endpoints(request: Request):
    base_url = f"{request.url.scheme}://{request.url.netloc}"
    
    try:
        pool_stats = await get_simple_pool_stats()
    except Exception:
        pool_stats = {"error": "db_unavailable"}

    return {
        "server": "Hoarder API Server - IoT Telemetry API Platform",
        "version": "2.0.0",
        "status": "healthy",
        "timestamp_utc": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "statistics": {
            "database_stats": pool_stats
        },
        "endpoints": {
            "self": f"{base_url}/",
            "latest_data_all_devices": f"{base_url}/data/latest",
            "device_data": f"{base_url}/data/latest/{{device_id}}"
        }
    }
