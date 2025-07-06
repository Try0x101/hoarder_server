import re

def normalize_numeric_string(value_str):
    cleaned = re.sub(r'[^\d\-\+\.]', '', value_str)
    
    if not cleaned or cleaned in ['-', '+', '.']:
        return value_str
    
    dot_count = cleaned.count('.')
    if dot_count > 1:
        parts = cleaned.split('.')
        cleaned = '.'.join(parts[:2])
    
    try:
        val = float(cleaned)
        return int(val) if val == int(val) and abs(val) < 2**31 else val
    except (ValueError, OverflowError):
        return value_str

def is_ip_address(value_str):
    parts = value_str.split('.')
    if len(parts) == 4:
        try:
            return all(0 <= int(part) <= 255 for part in parts)
        except ValueError:
            return False
    return False

def sanitize_payload(data: any) -> any:
    if isinstance(data, dict):
        return {k: sanitize_payload(v) for k, v in data.items()}
    if isinstance(data, list):
        return [sanitize_payload(v) for v in data]
    if isinstance(data, str):
        stripped = data.strip()
        
        if not stripped:
            return ""
        
        if stripped.lower() in ['null', 'none', 'undefined', 'n/a']:
            return None
            
        if stripped.lower() in ['true', 'false']:
            return stripped.lower() == 'true'
        
        quote_chars = ['"', "'", '`']
        for quote in quote_chars:
            if stripped.startswith(quote) and stripped.endswith(quote) and len(stripped) > 1:
                stripped = stripped[1:-1].strip()
                break
        
        if is_ip_address(stripped):
            return stripped
        
        if re.match(r'^[\-\+]?\d*\.?\d+([eE][\-\+]?\d+)?$', stripped):
            try:
                val = float(stripped)
                if abs(val) < 2**31 and val == int(val):
                    return int(val)
                return val
            except (ValueError, OverflowError):
                pass
        
        if re.match(r'^[\-\+]?\d+[\.\,]\d+', stripped):
            return normalize_numeric_string(stripped.replace(',', '.'))
        
        return stripped
    
    if isinstance(data, (int, float)):
        if isinstance(data, float):
            if data != data:
                return None
            if abs(data) == float('inf'):
                return None
        return data
    
    if data is None or (hasattr(data, '__len__') and len(data) == 0):
        return None
    
    return str(data)

def calculate_delta_changes(current: dict, previous: dict) -> dict:
    delta = {}
    context_keys = {'id', 'device_id', 'data_timestamp', 'received_at', 'is_offline', 'batch_id'}
    for key, value in current.items():
        if key in context_keys or key not in previous or previous[key] != value:
            delta[key] = value
    return delta
