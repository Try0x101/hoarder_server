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

@router.get("/export",response_class=HTMLResponse)
async def export_page(request:Request):
 return templates.TemplateResponse("export.html",{"request":request})

@router.get("/import",response_class=HTMLResponse)
async def import_page(request:Request):
 return templates.TemplateResponse("import.html",{"request":request})
