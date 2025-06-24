import json
from fastapi import APIRouter
from fastapi.responses import JSONResponse
from app.db import get_latest_data
class PrettyJSONResponse(JSONResponse):
 def render(self,content):return json.dumps(content,ensure_ascii=False,allow_nan=False,indent=2,separators=(",",": ")).encode("utf-8")
router=APIRouter()
@router.get("/data/latest",response_class=PrettyJSONResponse)
async def latest_data():return await get_latest_data()