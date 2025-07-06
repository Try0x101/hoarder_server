from fastapi import APIRouter, Request
from app.responses import PrettyJSONResponse
from app.api.data.handlers import handle_latest_data, handle_device_data

router = APIRouter()

@router.get("/data/latest", response_class=PrettyJSONResponse)
async def latest_data(request: Request):
    return await handle_latest_data(request)

@router.get("/data/latest/{device_id}", response_class=PrettyJSONResponse)
async def latest_data_for_device(device_id: str, request: Request):
    return await handle_device_data(device_id, request)
