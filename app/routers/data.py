from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import JSONResponse
from app.responses import PrettyJSONResponse
from app.db import get_raw_latest_data_for_all_devices, get_raw_latest_payload_for_device
from app.cache import get_cached_data, set_cached_data, CACHE_KEY_LATEST_DATA
from app.utils import transform_device_data

router=APIRouter()

@router.get("/data/latest",response_class=PrettyJSONResponse)
async def latest_data(request: Request):
    base_url = f"{request.url.scheme}://{request.url.netloc}"

    async def process_and_link_data(raw_data):
        response_data = []
        for device_entry in raw_data:
            device_id = device_entry.get("device_id")
            transformed_payload = await transform_device_data(device_entry["payload"])

            linked_device_data = {
                "links": {
                    "self": f"{base_url}/data/latest/{device_id}",
                    "history": f"{base_url}/data/history?device_id={device_id}"
                },
                "device_id": device_id,
                "payload": transformed_payload
            }
            response_data.append(linked_device_data)
        return response_data

    cached_raw_data = await get_cached_data(CACHE_KEY_LATEST_DATA)

    if cached_raw_data:
        processed_data = await process_and_link_data(cached_raw_data)
    else:
        raw_data = await get_raw_latest_data_for_all_devices()
        await set_cached_data(CACHE_KEY_LATEST_DATA, raw_data, ttl=5)
        processed_data = await process_and_link_data(raw_data)

    return {
        "links": {
            "self": f"{base_url}/data/latest",
            "home": f"{base_url}/",
            "history": f"{base_url}/data/history"
        },
        "devices": processed_data
    }

@router.get("/data/latest/{device_id}", response_class=PrettyJSONResponse)
async def latest_data_for_device(device_id: str, request: Request):
    base_url = f"{request.url.scheme}://{request.url.netloc}"
    cache_key = f"latest_data_raw_{device_id}"

    cached_raw_payload = await get_cached_data(cache_key)

    if cached_raw_payload:
        raw_payload = cached_raw_payload
    else:
        raw_payload = await get_raw_latest_payload_for_device(device_id)
        if raw_payload is None:
            all_devices_raw = await get_raw_latest_data_for_all_devices()
            devices_with_time = []
            for device in all_devices_raw:
                transformed = await transform_device_data(device.get("payload", {}))
                last_active_str = transformed.get("last_refresh_time_utc_reference")
                devices_with_time.append({
                    "device_id": device["device_id"],
                    "last_active": last_active_str,
                    "link": f"{base_url}/data/latest/{device['device_id']}"
                })
            sorted_devices = sorted(
                devices_with_time,
                key=lambda x: x.get('last_active') or '1970-01-01 00:00:00 UTC',
                reverse=True
            )
            response_content = {
                "detail": f"Device with id '{device_id}' not found.",
                "available_devices": sorted_devices,
                "links": {"up": f"{base_url}/data/latest"}
            }
            return PrettyJSONResponse(status_code=404, content=response_content)

        await set_cached_data(cache_key, raw_payload, ttl=5)

    transformed_payload = await transform_device_data(raw_payload)

    return {
        "links": {
            "self": f"{base_url}/data/latest/{device_id}",
            "parent": f"{base_url}/data/latest",
            "history": f"{base_url}/data/history?device_id={device_id}",
            "gaps": f"{base_url}/data/gaps?device_id={device_id}",
            "summary": f"{base_url}/data/summary?device_id={device_id}"
        },
        "device_id": device_id,
        "payload": transformed_payload
    }
