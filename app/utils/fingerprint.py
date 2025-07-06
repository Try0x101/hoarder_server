import hashlib
import re
from app.transforms import safe_int

def normalize_ip_address(ip_value):
    if not ip_value:
        return "unknown_ip"
    
    ip_str = str(ip_value).strip()
    
    if ip_str.lower() in ['null', 'none', 'undefined', 'localhost', '127.0.0.1', '::1']:
        return "local_ip"
    
    ip_clean = re.sub(r'[^\d\.\:]', '', ip_str)
    if not ip_clean:
        return "invalid_ip"
    
    parts = ip_clean.split('.')
    if len(parts) == 4 and all(p.isdigit() and 0 <= int(p) <= 255 for p in parts):
        return ip_clean
    
    if ':' in ip_clean and len(ip_clean) > 7:
        return ip_clean[:45]
    
    return f"parsed_{hashlib.md5(ip_str.encode()).hexdigest()[:8]}"

def normalize_user_agent(ua_value):
    if not ua_value:
        return None
    
    ua_str = str(ua_value).strip()
    if len(ua_str) < 5 or ua_str.lower() in ['null', 'none', 'undefined']:
        return None
    
    ua_clean = re.sub(r'[^\w\s\-\.\(\)\/]', '', ua_str)
    if len(ua_clean) < 5:
        return None
    
    return ua_clean[:100]

def extract_device_characteristics(payload_data):
    characteristics = {}
    
    numeric_fields = {
        'cap': ('battery_capacity', 1, 50000),
        'mcc': ('mobile_country_code', 100, 999),
        'mnc': ('mobile_network_code', 0, 999),
        'ci': ('cell_id', 1, 268435455),
        'tac': ('tracking_area_code', 1, 65535)
    }
    
    for field, (desc, min_val, max_val) in numeric_fields.items():
        if field in payload_data:
            val = safe_int(payload_data[field])
            if val is not None and min_val <= val <= max_val:
                characteristics[desc] = str(val)
    
    string_fields = {
        'nt': 'network_type',
        'op': 'operator', 
        'n': 'device_name',
        'model': 'device_model',
        'brand': 'device_brand'
    }
    
    for field, desc in string_fields.items():
        if field in payload_data:
            val = payload_data[field]
            if val and str(val).strip() and str(val).lower() not in ['null', 'none', 'undefined', 'unknown']:
                clean_val = re.sub(r'[^\w\-]', '', str(val))[:20]
                if clean_val:
                    characteristics[desc] = clean_val
    
    return characteristics

def create_device_fingerprint(source_ip, user_agent, payload_data):
    fingerprint_components = []
    
    normalized_ip = normalize_ip_address(source_ip)
    fingerprint_components.append(f"ip_{normalized_ip}")
    
    normalized_ua = normalize_user_agent(user_agent)
    if normalized_ua:
        ua_hash = hashlib.md5(normalized_ua.encode()).hexdigest()[:8]
        fingerprint_components.append(f"ua_{ua_hash}")
    
    characteristics = extract_device_characteristics(payload_data)
    if characteristics:
        char_string = "_".join(f"{k}_{v}" for k, v in sorted(characteristics.items()))
        char_hash = hashlib.md5(char_string.encode()).hexdigest()[:8]
        fingerprint_components.append(f"dev_{char_hash}")
    
    base_fingerprint = "_".join(fingerprint_components)
    
    if len(base_fingerprint) > 50:
        final_hash = hashlib.md5(base_fingerprint.encode()).hexdigest()[:16]
        return f"auto_{final_hash}"
    
    return f"auto_{base_fingerprint}"

def safe_device_id(device_id_value, source_ip=None, user_agent=None, payload_data=None):
    if device_id_value:
        clean_id = str(device_id_value).strip()
        if clean_id and clean_id.lower() not in ['null', 'none', 'undefined', '', '0']:
            clean_id = re.sub(r'[^\w\-\.]', '_', clean_id)[:50]
            if clean_id and not clean_id.replace('_', '').replace('.', '').replace('-', ''):
                pass
            else:
                return clean_id
    
    if source_ip and payload_data:
        return create_device_fingerprint(source_ip, user_agent, payload_data)
    
    fallback_components = [
        str(source_ip or 'unknown')[:15],
        str(user_agent or 'unknown')[:10] if user_agent else 'no_ua',
        str(hash(str(device_id_value)) % 10000).zfill(4)
    ]
    
    return f"fallback_{'_'.join(fallback_components)}"[:50]

def validate_device_id_consistency(device_id, source_ip, payload_data):
    if not device_id or not device_id.startswith('auto_'):
        return True, "manual_id"
    
    expected_fingerprint = create_device_fingerprint(source_ip, None, payload_data)
    
    if device_id == expected_fingerprint:
        return True, "consistent_fingerprint"
    
    base_ip = normalize_ip_address(source_ip)
    if base_ip in device_id:
        return True, "ip_based_match"
    
    return False, "inconsistent_fingerprint"
