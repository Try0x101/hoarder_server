<<<<<<< HEAD
from fastapi import APIRouter,Request
from fastapi.templating import Jinja2Templates
from fastapi.responses import HTMLResponse
from app.db import get_latest_data
router=APIRouter()
templates=Jinja2Templates(directory="app/templates")
@router.get("/",response_class=HTMLResponse)
async def dashboard(request:Request):
 data=await get_latest_data()
 return templates.TemplateResponse("dashboard.html",{"request":request,"data":data})
=======
from fastapi import APIRouter, Request
from fastapi.templating import Jinja2Templates
from fastapi.responses import HTMLResponse
from app.db import get_latest_data

router = APIRouter()
templates = Jinja2Templates(directory="app/templates")

@router.get("/", response_class=HTMLResponse)
async def dashboard(request: Request):
    data = await get_latest_data()
    return templates.TemplateResponse("dashboard.html", {"request": request, "data": data})
>>>>>>> c0f02561b62130cdb8e6492ea11157bf7aa103f9
