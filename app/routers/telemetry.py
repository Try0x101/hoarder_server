# app/routers/telemetry.py
from fastapi import APIRouter, Request, BackgroundTasks
from app.utils import decode_raw_data, enrich_with_weather_data
from app.db import save_data

router = APIRouter()

@router.post("/api/telemetry")
async def receive_telemetry(request: Request, background_tasks: BackgroundTasks):
    raw = await request.body()
    data = await decode_raw_data(raw)
    
    # Добавляем IP-адрес источника к данным
    if request.client:
        data['source_ip'] = request.client.host
    else:
        data['source_ip'] = None
    
    # Обогащаем данные погодной информацией
    data = await enrich_with_weather_data(data)
    
    background_tasks.add_task(save_data, data)
    return {"status": "received"}