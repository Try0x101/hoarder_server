import datetime
from fastapi import APIRouter, Query, Request
from typing import Optional
from app.api.history.handlers import handle_device_list, handle_device_history
from app.api.history.queries import get_device_statistics
from app.api.shared.url_helpers import build_base_url, safe_int_param, create_device_links

router = APIRouter()

@router.get("/data/history")
async def get_history(
    request: Request,
    device_id: str = Query(None, description="Device ID to get history for"),
    limit: int = Query(256, description="Maximum records per page"),
    days: int = Query(30, description="Days of history to retrieve"),
    cursor: Optional[str] = Query(None, description="Timestamp cursor for pagination")
):
    limit = safe_int_param(str(limit), 256, 1, 1024)
    days = safe_int_param(str(days), 30, 1, 365)

    if not device_id:
        return await handle_device_list(request, limit, days)
    else:
        return await handle_device_history(request, device_id, limit, days, cursor)
