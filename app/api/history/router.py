from fastapi import APIRouter, Request, Query
from typing import Optional
from .handlers import handle_device_history, handle_device_list

router = APIRouter()

@router.get("/history")
async def get_history(
    request: Request,
    device_id: str = Query(None),
    limit: int = Query(100, ge=1, le=1000),
    days: int = Query(30, ge=1, le=365),
    cursor: Optional[str] = Query(None)
):
    if device_id:
        return await handle_device_history(request, device_id, limit, days, cursor)
    else:
        return await handle_device_list(request, days)
