from fastapi import APIRouter
from app.responses import PrettyJSONResponse
from app.db import get_data_for_latest

router=APIRouter()

@router.get("/data/latest",response_class=PrettyJSONResponse)
async def latest_data():
    return await get_data_for_latest()
