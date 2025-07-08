import datetime
from fastapi import Request, HTTPException
from fastapi.responses import JSONResponse
from typing import Optional
from app.database.operations import get_latest_device_data, get_device_latest_data
from app.services.cache import get_cached_data, set_cached_data
from app.utils.transformer import transform_device_data

def build_base_url(request: Request) -> str:
    return f"{request.url.scheme}://{request.url.netloc}"

def create_device_links(base_url: str, device_id: str) -> dict:
    return {
        "latest": f"{base_url}/data/latest/{device_id}",
        "history": f"{base_url}/data/history?device_id={device_id}"
    }

async def handle_latest_data(request: Request):
    base_url = build_base_url(request)
    try:
        cached_data = await get_cached_data("latest_data_all")
        if cached_data:
            raw_data = cached_data
        else:
            raw_data = await get_latest_device_data()
            await set_cached_data("latest_data_all", raw_data, ttl=5)
        
        processed_data = []
        for device_entry in raw_data:
            try:
                transformed = await transform_device_data(device_entry)
                device_id = transformed.get("identity", {}).get("device_id", "unknown")
                
                linked_device_data = {
                    "links": create_device_links(base_url, device_id),
                    **transformed
                }
                processed_data.append(linked_device_data)
            except Exception as e:
                print(f"Transform error for device: {e}")
                continue
        
        return {
            "meta": {
                "server_time": datetime.datetime.now(datetime.timezone.utc).isoformat(),
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
        return JSONResponse(
            status_code=500,
            content={"error": "data_retrieval_failed", "message": str(e)}
        )

async def handle_device_data(device_id: str, request: Request):
    if not device_id or not device_id.strip():
        raise HTTPException(status_code=400, detail="Invalid device ID")
    
    device_id = device_id.strip()[:100]
    base_url = build_base_url(request)
    
    try:
        cache_key = f"device_data_{device_id}"
        cached_data = await get_cached_data(cache_key)
        
        if cached_data:
            device_data = cached_data
        else:
            device_data = await get_device_latest_data(device_id)
            if device_data:
                await set_cached_data(cache_key, device_data, ttl=5)
        
        if not device_data:
            raise HTTPException(status_code=404, detail=f"Device '{device_id}' not found")
        
        transformed = await transform_device_data(device_data)
        
        response_payload = {
            "links": create_device_links(base_url, device_id),
            **transformed
        }
        
        return response_payload
        
    except HTTPException:
        raise
    except Exception as e:
        print(f"Device data endpoint error for {device_id}: {e}")
        raise HTTPException(status_code=500, detail="Data retrieval failed")
