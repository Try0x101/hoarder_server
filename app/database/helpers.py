def sanitize_payload(data: any) -> any:
    if isinstance(data, dict):
        return {k: sanitize_payload(v) for k, v in data.items()}
    if isinstance(data, list):
        return [sanitize_payload(v) for v in data]
    if isinstance(data, str):
        stripped = data.strip()
        if stripped.startswith('"') and stripped.endswith('"') and len(stripped) > 1:
            stripped = stripped[1:-1].strip()
        if stripped == "":
            return ""
        try:
            val = float(stripped)
            return int(val) if val == int(val) else val
        except (ValueError, TypeError):
            return stripped
    return data

def calculate_delta_changes(current: dict, previous: dict) -> dict:
    delta = {}
    context_keys = {'id', 'device_id', 'data_timestamp', 'received_at', 'is_offline', 'batch_id'}
    for key, value in current.items():
        if key in context_keys or key not in previous or previous[key] != value:
            delta[key] = value
    return delta
