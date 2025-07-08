import datetime
from typing import Dict, Any

def safe_float(value):
    if value is None:
        return None
    try:
        return float(value)
    except (ValueError, TypeError):
        return None

def safe_int(value):
    if value is None:
        return None
    try:
        return int(float(value))
    except (ValueError, TypeError):
        return None

def safe_string(value):
    if value is None or value == "":
        return None
    return str(value)

async def transform_device_data(raw_data: Dict[str, Any]) -> Dict[str, Any]:
    try:
        payload = raw_data.get("payload", {})
        device_id = raw_data.get("device_id", "unknown")
        
        def format_value(key: str, unit: str):
            val = safe_int(payload.get(key))
            return f"{val}{unit}" if val is not None else None

        transformed = {
            "identity": {
                "device_id": device_id,
                "device_name": safe_string(payload.get('n'))
            },
            "network": {
                "cellular": {
                    "operator": safe_string(payload.get('op')),
                    "signal_strength": format_value('rssi', ' dBm'),
                    "type": safe_string(payload.get('nt'))
                },
                "source_ip": safe_string(payload.get('source_ip'))
            },
            "location": {
                "coordinates": {
                    "latitude": safe_string(safe_float(payload.get('lat'))),
                    "longitude": safe_string(safe_float(payload.get('lon'))),
                    "accuracy": format_value('acc', ' m'),
                    "altitude": format_value('alt', ' m'),
                    "speed": format_value('spd', ' km/h')
                }
            },
            "power": {
                "battery": {
                    "percent": format_value('perc', '%'),
                    "capacity": format_value('cap', ' mAh')
                }
            },
            "timestamps": {
                "last_refresh_time_utc": raw_data.get("received_at")
            }
        }

        def cleanup_empty(d):
            if not isinstance(d, dict):
                return d
            cleaned = {}
            for k, v in d.items():
                if isinstance(v, dict):
                    v = cleanup_empty(v)
                if v is not None and v != {} and v != '':
                    cleaned[k] = v
            return cleaned

        return cleanup_empty(transformed)

    except Exception as e:
        print(f"Transform error: {e}")
        return {"identity": {"device_id": raw_data.get("device_id", "unknown")}, "error": "transformation_failed"}
