from fastapi import APIRouter, Request, HTTPException
from fastapi.responses import JSONResponse
from typing import Optional
from .handlers import handle_latest_data, handle_device_data

router = APIRouter()

@router.get("/latest")
async def latest_data(request: Request):
    return await handle_latest_data(request)

@router.get("/latest/{device_id}")
async def device_data(device_id: str, request: Request):
    return await handle_device_data(device_id, request)
