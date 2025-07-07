import datetime
from fastapi import Request, HTTPException, Query
from fastapi.responses import JSONResponse
from typing import Optional
from app.responses import PrettyJSONResponse
from ..shared.url_helpers import build_base_url, create_device_links, safe_int_param, create_pagination_links
from .cache_manager import get_cached_latest_data, get_cached_device_data
from .transformers import process_and_link_data, safe_transform_device_data
from app.db import get_raw_latest_data_for_all_devices, get_timestamped_history
from app.utils import transform_device_data

async def handle_latest_data(request: Request):
    base_url = build_base_url(request)
    try:
        raw_data = await get_cached_latest_data()
        processed_data = await process_and_link_data(raw_data, base_url)
        return {
            "meta": {
                "server_time": "2025-07-06T18:45:00Z",
                "total_devices": len(processed_data),
                "links": {
                    "self": f"{base_url}/data/latest", 
                    "home": f"{base_url}/", 
                    "history": f"{base_url}/data/history"
                }
            },
            "devices": processed_data
        }
    except Exception as e:
        print(f"Latest data endpoint error: {e}")
        return {"error": "data_retrieval_failed", "message": "Unable to retrieve latest device data", "links": {"home": f"{base_url}/"}}

async def handle_device_data(device_id: str, request: Request):
    if not device_id or not device_id.strip():
        raise HTTPException(status_code=400, detail="Invalid device ID")
    device_id = device_id.strip()[:100]
    base_url = build_base_url(request)
    try:
        raw_payload = await get_cached_device_data(device_id)
        if raw_payload is None:
            return await handle_device_not_found(device_id, base_url)
        
        transformed_payload = await transform_device_data(raw_payload)
        
        response_payload = {
            "links": create_device_links(base_url, device_id),
            **transformed_payload
        }
        return response_payload
        
    except Exception as e:
        print(f"Device data endpoint error for {device_id}: {e}")
        raise HTTPException(status_code=500, detail="Data retrieval failed")

async def handle_device_not_found(device_id: str, base_url: str):
    try:
        all_devices_raw = await get_raw_latest_data_for_all_devices()
        devices_with_time = []
        for device in all_devices_raw[:20]:
            if not isinstance(device, dict): continue
            dev_id = device.get("device_id")
            if not dev_id: continue
            try:
                transformed = await transform_device_data(device.get("payload", {}))
                last_active_str = transformed.get("timestamps", {}).get("last_refresh_time_utc", "unknown")
            except Exception:
                last_active_str = "transformation_failed"
            devices_with_time.append({"device_id": dev_id, "last_active": last_active_str, "link": f"{base_url}/data/latest/{dev_id}"})
        devices_with_time.sort(key=lambda x: x.get('last_active', '1970-01-01 00:00:00 UTC'), reverse=True)
        response_content = {"detail": f"Device with id '{device_id}' not found.", "available_devices": devices_with_time, "links": {"up": f"{base_url}/data/latest"}}
        return PrettyJSONResponse(status_code=404, content=response_content)
    except Exception as e:
        print(f"Error generating device list: {e}")
        raise HTTPException(status_code=404, detail=f"Device '{device_id}' not found")

async def handle_device_list(request: Request, limit: int, days: int):
    from ..history.queries import get_active_devices
    base_url = build_base_url(request)
    devices = await get_active_devices(days)
    device_links = [{
        "device_id": d["device_id"],
        "last_active": d["last_active"].isoformat(),
        "links": create_device_links(base_url, d["device_id"], limit, days)
    } for d in devices]
    return {
        "links": {"self": f"{base_url}{request.url.path}?{request.url.query}", "home": f"{base_url}/"},
        "server_time": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "period": f"{days}days",
        "total_devices": len(devices),
        "devices": device_links
    }

async def handle_device_history(request: Request, device_id: str, limit: int, days: int, cursor: Optional[str]):
    base_url = build_base_url(request)
    results, next_cursor = await get_timestamped_history(device_id=device_id, days=days, limit=limit, last_timestamp=cursor)
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
