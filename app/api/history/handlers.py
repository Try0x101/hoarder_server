import datetime
from fastapi import Request, Query, HTTPException
from fastapi.responses import JSONResponse
from typing import Optional
from .queries import get_active_devices, get_device_gaps_data, get_device_statistics, get_device_activity_stats, get_device_position_stats
from ..shared.url_helpers import build_base_url, safe_int_param, create_device_links, create_pagination_links
from app.db import get_timestamped_history

async def handle_device_list(request: Request, limit: int, days: int):
    base_url = build_base_url(request)
    devices = await get_active_devices(days)
    device_links = [{
        "device_id": d["device_id"],
        "last_active": d["last_active"].isoformat(),
        "links": create_device_links(base_url, d["device_id"], limit, days)
    } for d in devices]

    return {
        "links": {
            "self": f"{base_url}{request.url.path}?{request.url.query}",
            "home": f"{base_url}/"
        },
        "server_time": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "period": f"{days}days",
        "total_devices": len(devices),
        "devices": device_links
    }

async def handle_device_history(request: Request, device_id: str, limit: int, days: int, cursor: Optional[str]):
    base_url = build_base_url(request)
    results, next_cursor = await get_timestamped_history(
        device_id=device_id, days=days, limit=limit, last_timestamp=cursor
    )

    if not results:
        return JSONResponse(content={
            "data": [],
            "info": f"No data found for device '{device_id}' in the last {days} days",
            "links": {"up": f"{base_url}/data/history?days={days}"}
        })

    response = {
        "links": create_pagination_links(base_url, device_id, limit, days, next_cursor),
        "device_id": device_id,
        "period": f"{days}days",
        "records_shown": len(results),
        "pagination": {"limit": limit, "next_cursor": next_cursor},
        "data": results
    }

    return response
