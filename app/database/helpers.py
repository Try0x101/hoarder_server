import re
import ipaddress
import json
import datetime
from typing import Any, Optional, Dict

def is_valid_ip_address(value: str) -> bool:
    if not isinstance(value, str): return False
    try:
        ipaddress.ip_address(value.strip())
        return True
    except ValueError:
        return False

def deep_merge(source: dict, destination: dict) -> dict:
    result = destination.copy()
    for key, value in source.items():
        if isinstance(value, dict) and key in result and isinstance(result.get(key), dict):
            result[key] = deep_merge(value, result[key])
        else:
            result[key] = value
    return result

def sanitize_payload(data: any) -> any:
    if isinstance(data, dict): return {k: sanitize_payload(v) for k, v in data.items()}
    if isinstance(data, list): return [sanitize_payload(v) for v in data]
    if isinstance(data, float) and (data != data or abs(data) == float('inf')): return None
    if isinstance(data, str):
        stripped, lower = data.strip(), data.strip().lower()
        if lower in ['null', 'none', 'undefined', 'n/a']: return None
        return stripped
    return data

def safe_json_serialize(data: Any) -> str:
    try:
        return json.dumps(sanitize_payload(data), ensure_ascii=False, separators=(',', ':'))
    except (TypeError, ValueError):
        return json.dumps({"serialization_error": True, "raw_preview": str(data)[:100]})

def extract_device_id(data: Dict[str, Any]) -> Optional[str]:
    for field in ['device_id', 'id', 'deviceId', 'device', 'dev_id']:
        if device_id := str(data.get(field, '')).strip():
            if device_id.lower() not in ['null', 'none', 'undefined', '']:
                return device_id[:100]
    return None

def calculate_delta_changes(current: dict, previous: dict) -> dict:
    context = {'id', 'device_id', 'data_timestamp', 'received_at', 'is_offline', 'batch_id'}
    return {k: v for k, v in current.items() if k in context or k not in previous or previous[k] != v}

def safe_extract_ip_from_headers(headers: dict) -> str:
    for header in ['x-forwarded-for', 'x-real-ip', 'x-client-ip', 'cf-connecting-ip']:
        if ip_str := headers.get(header, '').strip().split(',')[0].strip():
            if is_valid_ip_address(ip_str): return ip_str[:45]
    return 'unknown'

def validate_device_identifiers(data: dict) -> dict:
    issues = []
    if not (device_id := extract_device_id(data)):
        issues.append('Invalid or missing device ID')
    elif len(device_id) > 100:
        issues.append('Device ID too long (>100 chars)')
    if (ip := data.get('source_ip')) and not is_valid_ip_address(str(ip)):
        issues.append('Invalid source IP format')
    return {'valid': not issues, 'issues': issues}
