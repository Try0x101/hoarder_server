from fastapi import APIRouter
from app.db import get_latest_data
router=APIRouter()
@router.get("/data/latest")
async def latest_data():return await get_latest_data()
