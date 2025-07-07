import re
import ipaddress

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

def is_valid_ip_address(value_str):
    if not isinstance(value_str, str):
        return False
    
    value_str = value_str.strip()
    if not value_str:
        return False
    
    try:
        ipaddress.ip_address(value_str)
        return True
    except ValueError:
        pass
    
    if '.' in value_str:
        parts = value_str.split('.')
        if len(parts) == 4:
            try:
                return all(0 <= int(part) <= 255 for part in parts if part.isdigit())
            except (ValueError, TypeError):
                return False
    
    if ':' in value_str:
        if value_str.count(':') >= 2 and value_str.count(':') <= 7:
            hex_pattern = re.match(r'^[0-9a-fA-F:]+$', value_str)
            if hex_pattern:
                return True
    
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
        
        if is_valid_ip_address(stripped):
            return stripped
        
        if re.match(r'^[\-\+]?\d*\.?\d+([eE][\-\+]?\d+)?$', stripped):
            if '.' in stripped and len(stripped.split('.')) == 4:
                return stripped
            
            try:
                val = float(stripped)
                if abs(val) < 2**31 and val == int(val):
                    return int(val)
                return val
            except (ValueError, OverflowError):
                pass
        
        if re.match(r'^[\-\+]?\d+[\.\,]\d+', stripped):
            if not is_valid_ip_address(stripped.replace(',', '.')):
                return normalize_numeric_string(stripped.replace(',', '.'))
        
        return stripped
    
    if isinstance(data, (int, float)):
        if isinstance(data, float):
            if data != data:
                return None
            if abs(data) == float('inf'):
                return None
        return data
    
    if isinstance(data, bool):
        return data
    
    if data is None:
        return None
    
    if hasattr(data, '__len__') and len(data) == 0:
        return data
    
    return data

def calculate_delta_changes(current: dict, previous: dict) -> dict:
    delta = {}
    context_keys = {'id', 'device_id', 'data_timestamp', 'received_at', 'is_offline', 'batch_id'}
    for key, value in current.items():
        if key in context_keys or key not in previous or previous[key] != value:
            delta[key] = value
    return delta

def safe_extract_ip_from_headers(headers: dict) -> str:
    ip_headers = [
        'x-forwarded-for',
        'x-real-ip', 
        'x-client-ip',
        'cf-connecting-ip',
        'true-client-ip'
    ]
    
    for header in ip_headers:
        if header in headers:
            ip_value = headers[header]
            if isinstance(ip_value, str):
                ip_value = ip_value.strip()
                if ',' in ip_value:
                    ip_value = ip_value.split(',')[0].strip()
                
                if is_valid_ip_address(ip_value):
                    return ip_value[:45]
    
    return 'unknown'

def validate_device_identifiers(data: dict) -> dict:
    validation_result = {'valid': True, 'issues': []}
    
    device_id = data.get('id') or data.get('device_id')
    if device_id:
        device_id_str = str(device_id).strip()
        if not device_id_str or device_id_str.lower() in ['null', 'none', 'undefined']:
            validation_result['issues'].append('Invalid device ID format')
        elif len(device_id_str) > 100:
            validation_result['issues'].append('Device ID too long (>100 chars)')
    
    source_ip = data.get('source_ip')
    if source_ip and not is_valid_ip_address(str(source_ip)):
        validation_result['issues'].append('Invalid source IP format')
    
    if validation_result['issues']:
        validation_result['valid'] = False
    
    return validation_result
