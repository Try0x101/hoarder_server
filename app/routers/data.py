from fastapi import APIRouter
from app.db import get_latest_data

router = APIRouter()

@router.get("/data/latest")
async def latest_data():
    data = await get_latest_data()
    return data
