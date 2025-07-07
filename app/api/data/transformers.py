from app.utils import transform_device_data

async def safe_transform_device_data(device_entry: dict) -> dict:
    try:
        device_id = device_entry.get("device_id")
        transformed_payload = await transform_device_data(device_entry["payload"])
        if not transformed_payload.get("identity", {}).get("device_id"):
             transformed_payload.setdefault("identity", {})["device_id"] = device_id
        
        return transformed_payload

    except Exception as e:
        print(f"Transform error for device {device_entry.get('device_id', 'unknown')}: {e}")
        return {
            "identity": {"device_id": device_entry.get("device_id")},
            "error": "transformation_failed"
        }

async def process_and_link_data(raw_data: list, base_url: str) -> list:
    from ..shared.url_helpers import create_device_links
    
    response_data = []
    for device_entry in raw_data:
        if not isinstance(device_entry, dict) or "device_id" not in device_entry:
            continue
            
        transformed_entry = await safe_transform_device_data(device_entry)
        
        device_id = transformed_entry.get("identity", {}).get("device_id")
        if not device_id:
            continue
            
        linked_device_data = {
            "links": create_device_links(base_url, device_id),
            **transformed_entry
        }
        response_data.append(linked_device_data)
    
    return response_data
