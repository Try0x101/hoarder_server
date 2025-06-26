# app/routers/data.py
from fastapi import APIRouter
from app.responses import PrettyJSONResponse
from app.db import get_latest_data

router=APIRouter()

@router.get("/data/latest",response_class=PrettyJSONResponse)
async def latest_data():return await get_latest_data()