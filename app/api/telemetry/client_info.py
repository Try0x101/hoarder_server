import datetime
from fastapi import Request

def safe_header_value(header_value, default=None):
    if header_value is None:
        return default
    try:
        return str(header_value).strip()
    except (ValueError, TypeError):
        return default

def safe_int_header(header_value, default=None):
    if header_value is None:
        return default
    try:
        return int(header_value)
    except (ValueError, TypeError):
        return default

def extract_client_info(request: Request):
    return {
        'source_ip': request.client.host if request.client else None,
        'user_agent': safe_header_value(request.headers.get('user-agent')),
        'content_size_bytes': safe_int_header(request.headers.get('content-length')),
        'x_forwarded_for': safe_header_value(request.headers.get('x-forwarded-for')),
        'x_real_ip': safe_header_value(request.headers.get('x-real-ip')),
        'content_type': safe_header_value(request.headers.get('content-type')),
        'content_encoding': safe_header_value(request.headers.get('content-encoding')),
        'server_received_at': datetime.datetime.now(datetime.timezone.utc).isoformat()
    }
