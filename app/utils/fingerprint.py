import hashlib
from app.transforms import safe_int

def create_device_fingerprint(source_ip, user_agent, payload_data):
    fingerprint_parts = [source_ip or "unknown_ip"]
    if user_agent:
        ua_hash = hashlib.md5(user_agent.encode()).hexdigest()[:8]
        fingerprint_parts.append(f"ua_{ua_hash}")

    chars = []
    if payload_data.get('cap'):
        chars.append(f"cap{safe_int(payload_data.get('cap'))}")
    if payload_data.get('nt'):
        chars.append(f"nt{str(payload_data.get('nt'))[:3]}")
    if payload_data.get('op'):
        chars.append(f"op{str(payload_data.get('op'))[:3]}")
    if payload_data.get('mcc'):
        chars.append(f"mcc{safe_int(payload_data.get('mcc'))}")

    if chars:
        char_hash = hashlib.md5("_".join(chars).encode()).hexdigest()[:6]
        fingerprint_parts.append(char_hash)

    fingerprint = "_".join(fingerprint_parts)
    if len(fingerprint) > 50:
        fingerprint = hashlib.md5(fingerprint.encode()).hexdigest()[:16]
    return f"auto_{fingerprint}"

def safe_device_id(device_id_value, source_ip=None, user_agent=None, payload_data=None):
    if device_id_value and str(device_id_value).strip():
        return str(device_id_value)
    if source_ip and payload_data:
        return create_device_fingerprint(source_ip, user_agent, payload_data)
    return f"unknown_{hash(str(device_id_value) + str(source_ip))%10000:04d}"
