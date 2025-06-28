from fastapi import APIRouter
from app.responses import PrettyJSONResponse
from app.db import get_data_for_latest
from app.cache import get_cached_data, set_cached_data, CACHE_KEY_LATEST_DATA

router=APIRouter()

@router.get("/data/latest",response_class=PrettyJSONResponse)
async def latest_data():
    cached_data = await get_cached_data(CACHE_KEY_LATEST_DATA)
    if cached_data:
        return cached_data
    
    data = await get_data_for_latest()
    await set_cached_data(CACHE_KEY_LATEST_DATA, data, ttl=5) # Cache for 5 seconds
    return data
