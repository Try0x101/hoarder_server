from fastapi import APIRouter
from app.db import get_latest_data
<<<<<<< HEAD
router=APIRouter()
@router.get("/data/latest")
async def latest_data():return await get_latest_data()
=======

router = APIRouter()

@router.get("/data/latest")
async def latest_data():
    data = await get_latest_data()
    return data
>>>>>>> c0f02561b62130cdb8e6492ea11157bf7aa103f9
