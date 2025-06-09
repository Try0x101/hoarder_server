from fastapi import APIRouter, Request, Response
from fastapi.responses import HTMLResponse, JSONResponse
from starlette.background import BackgroundTasks
from app.utils import decode_raw_data, enrich_with_location_data # Добавляем enrich_with_location_data
from app.db import save_data, get_latest_data
from fastapi.templating import Jinja2Templates

router = APIRouter()
templates = Jinja2Templates(directory="app/templates")

@router.post("/data")
async def receive_data(request: Request, background_tasks: BackgroundTasks):
    raw = await request.body()
    data = await decode_raw_data(raw)
    
    # Добавляем IP-адрес источника к данным
    if request.client:
        data['source_ip'] = request.client.host
    else:
        data['source_ip'] = None # В случае, если IP-адрес не может быть определен

    # Вычисляем и добавляем данные о местоположении на основе GPS
    await enrich_with_location_data(data) # Добавляем вызов новой функции

    background_tasks.add_task(save_data, data)
    return {"status": "received"}

@router.get("/data/latest")
async def latest_data():
    data = await get_latest_data()
    return JSONResponse(content=data)

@router.get("/dashboard", response_class=HTMLResponse)
async def dashboard(request: Request):
    data = await get_latest_data()
    return templates.TemplateResponse("dashboard.html", {"request": request, "data": data})