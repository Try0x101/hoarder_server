import datetime
from fastapi import Request
from fastapi.responses import JSONResponse
from typing import Optional
from app.database.operations import get_device_history, get_active_devices

def build_base_url(request: Request) -> str:
    return f"{request.url.scheme}://{request.url.netloc}"

async def handle_device_list(request: Request, days: int):
    base_url = build_base_url(request)
    try:
        devices = await get_active_devices(days)
        device_links = [{
            "device_id": d["device_id"],
            "last_active": d["last_active"],
            "links": {
                "latest": f"{base_url}/data/latest/{d['device_id']}",
                "history": f"{base_url}/data/history?device_id={d['device_id']}"
            }
        } for d in devices]

        return {
            "links": {"self": f"{base_url}/data/history", "home": f"{base_url}/"},
            "server_time": datetime.datetime.now(datetime.timezone.utc).isoformat(),
            "period": f"{days}days",
            "total_devices": len(devices),
            "devices": device_links
        }
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})

async def handle_device_history(request: Request, device_id: str, limit: int, days: int, cursor: Optional[str]):
    base_url = build_base_url(request)
    try:
        results, next_cursor = await get_device_history(device_id, days, limit, cursor)
        
        if not results:
            return JSONResponse(content={
                "data": [],
                "info": f"No data found for device '{device_id}' in the last {days} days",
                "links": {"up": f"{base_url}/data/history?days={days}"}
            })

        links = {
            "self": f"{base_url}/data/history?device_id={device_id}&limit={limit}&days={days}",
            "up": f"{base_url}/data/history?days={days}",
            "latest": f"{base_url}/data/latest/{device_id}"
        }
        
        if next_cursor:
            links["next_page"] = f"{base_url}/data/history?device_id={device_id}&limit={limit}&days={days}&cursor={next_cursor}"

        return {
            "links": links,
            "device_id": device_id,
            "period": f"{days}days",
            "records_shown": len(results),
            "pagination": {"limit": limit, "next_cursor": next_cursor},
            "data": results
        }
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})
