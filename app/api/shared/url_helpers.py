from fastapi import Request

def build_base_url(request: Request) -> str:
    return f"{request.url.scheme}://{request.url.netloc}"

def safe_int_param(value: str, default: int, min_val: int, max_val: int) -> int:
    try:
        result = int(value) if value else default
        return max(min_val, min(result, max_val))
    except (ValueError, TypeError):
        return default

def create_device_links(base_url: str, device_id: str, limit: int = 256, days: int = 30) -> dict:
    return {
        "latest": f"{base_url}/data/latest/{device_id}",
        "history": f"{base_url}/data/history?device_id={device_id}&limit={limit}&days={days}",
        "gaps": f"{base_url}/data/gaps?device_id={device_id}",
        "summary": f"{base_url}/data/summary?device_id={device_id}"
    }

def create_pagination_links(base_url: str, device_id: str, limit: int, days: int, next_cursor: str = None) -> dict:
    links = {
        "self": f"{base_url}/data/history?device_id={device_id}&limit={limit}&days={days}",
        "up": f"{base_url}/data/history?days={days}",
        **create_device_links(base_url, device_id, limit, days)
    }
    
    if next_cursor:
        links["next_page"] = f"{base_url}/data/history?device_id={device_id}&limit={limit}&days={days}&cursor={next_cursor}"
    
    return links
