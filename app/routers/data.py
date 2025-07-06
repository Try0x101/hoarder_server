from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import JSONResponse
from app.responses import PrettyJSONResponse
from app.db import get_raw_latest_data_for_all_devices, get_raw_latest_payload_for_device
from app.cache import get_cached_data, set_cached_data, CACHE_KEY_LATEST_DATA
from app.utils import transform_device_data

router = APIRouter()

def build_base_url(request: Request) -> str:
    return f"{request.url.scheme}://{request.url.netloc}"

def create_device_links(base_url: str, device_id: str) -> dict:
    return {
        "self": f"{base_url}/data/latest/{device_id}",
        "history": f"{base_url}/data/history?device_id={device_id}",
        "gaps": f"{base_url}/data/gaps?device_id={device_id}",
        "summary": f"{base_url}/data/summary?device_id={device_id}"
    }

async def safe_transform_device_data(device_entry: dict) -> dict:
    try:
        transformed_payload = await transform_device_data(device_entry["payload"])
        return {
            "device_id": device_entry.get("device_id"),
            "payload": transformed_payload,
            "transform_status": "success"
        }
    except Exception as e:
        print(f"Transform error for device {device_entry.get('device_id', 'unknown')}: {e}")
        return {
            "device_id": device_entry.get("device_id"),
            "payload": {"error": "transformation_failed", "raw_data_available": True},
            "transform_status": "failed"
        }

async def process_and_link_data(raw_data: list, base_url: str) -> list:
    response_data = []
    for device_entry in raw_data:
        if not isinstance(device_entry, dict) or "device_id" not in device_entry:
            continue
            
        device_id = device_entry.get("device_id")
        if not device_id:
            continue
            
        transformed_entry = await safe_transform_device_data(device_entry)
        
        linked_device_data = {
            "links": create_device_links(base_url, device_id),
            **transformed_entry
        }
        response_data.append(linked_device_data)
    
    return response_data

@router.get("/data/latest", response_class=PrettyJSONResponse)
async def latest_data(request: Request):
    base_url = build_base_url(request)

    try:
        cached_raw_data = await get_cached_data(CACHE_KEY_LATEST_DATA)
        
        if cached_raw_data and isinstance(cached_raw_data, list):
            processed_data = await process_and_link_data(cached_raw_data, base_url)
        else:
            raw_data = await get_raw_latest_data_for_all_devices()
            if not isinstance(raw_data, list):
                raw_data = []
            
            await set_cached_data(CACHE_KEY_LATEST_DATA, raw_data, ttl=5)
            processed_data = await process_and_link_data(raw_data, base_url)

        return {
            "links": {
                "self": f"{base_url}/data/latest",
                "home": f"{base_url}/",
                "history": f"{base_url}/data/history"
            },
            "server_time": "2025-07-06T18:45:00Z",
            "total_devices": len(processed_data),
            "devices": processed_data
        }
    except Exception as e:
        print(f"Latest data endpoint error: {e}")
        return {
            "error": "data_retrieval_failed",
            "message": "Unable to retrieve latest device data",
            "links": {"home": f"{base_url}/"}
        }

@router.get("/data/latest/{device_id}", response_class=PrettyJSONResponse)
async def latest_data_for_device(device_id: str, request: Request):
    if not device_id or not device_id.strip():
        raise HTTPException(status_code=400, detail="Invalid device ID")
        
    device_id = device_id.strip()[:100]
    base_url = build_base_url(request)
    cache_key = f"latest_data_raw_{device_id}"

    try:
        cached_raw_payload = await get_cached_data(cache_key)
        
        if cached_raw_payload and isinstance(cached_raw_payload, dict):
            raw_payload = cached_raw_payload
        else:
            raw_payload = await get_raw_latest_payload_for_device(device_id)
            if raw_payload is None:
                return await handle_device_not_found(device_id, base_url)
            
            await set_cached_data(cache_key, raw_payload, ttl=5)

        try:
            transformed_payload = await transform_device_data(raw_payload)
        except Exception as e:
            print(f"Transform error for device {device_id}: {e}")
            transformed_payload = {
                "error": "transformation_failed",
                "device_id": device_id,
                "raw_data_size": len(str(raw_payload)) if raw_payload else 0
            }

        return {
            "links": {
                "parent": f"{base_url}/data/latest",
                **create_device_links(base_url, device_id)
            },
            "device_id": device_id,
            "payload": transformed_payload
        }
    except Exception as e:
        print(f"Device data endpoint error for {device_id}: {e}")
        raise HTTPException(status_code=500, detail="Data retrieval failed")

async def handle_device_not_found(device_id: str, base_url: str):
    try:
        all_devices_raw = await get_raw_latest_data_for_all_devices()
        devices_with_time = []
        
        for device in all_devices_raw[:20]:
            if not isinstance(device, dict):
                continue
                
            dev_id = device.get("device_id")
            if not dev_id:
                continue
                
            try:
                transformed = await transform_device_data(device.get("payload", {}))
                last_active_str = transformed.get("last_refresh_time_utc_reference", "unknown")
            except Exception:
                last_active_str = "transformation_failed"
                
            devices_with_time.append({
                "device_id": dev_id,
                "last_active": last_active_str,
                "link": f"{base_url}/data/latest/{dev_id}"
            })
        
        devices_with_time.sort(
            key=lambda x: x.get('last_active', '1970-01-01 00:00:00 UTC'),
            reverse=True
        )
        
        response_content = {
            "detail": f"Device with id '{device_id}' not found.",
            "available_devices": devices_with_time,
            "links": {"up": f"{base_url}/data/latest"}
        }
        return PrettyJSONResponse(status_code=404, content=response_content)
    except Exception as e:
        print(f"Error generating device list: {e}")
        raise HTTPException(status_code=404, detail=f"Device '{device_id}' not found")
