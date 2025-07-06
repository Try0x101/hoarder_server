from app.utils import transform_device_data

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
    from ..shared.url_helpers import create_device_links
    
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
