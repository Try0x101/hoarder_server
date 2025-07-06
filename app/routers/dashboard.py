from fastapi import APIRouter, Request
from fastapi.templating import Jinja2Templates
from fastapi.responses import HTMLResponse
from app.database.retrieval import get_raw_latest_data_for_all_devices
from app.utils.transformer import transform_device_data

router = APIRouter()
templates = Jinja2Templates(directory="app/templates")

@router.get("/", response_class=HTMLResponse)
async def dashboard(request: Request):
    raw_data = await get_raw_latest_data_for_all_devices()
    transformed_data = []
    for item in raw_data:
        transformed_payload = await transform_device_data(item["payload"])
        transformed_data.append({
            "device_id": item["device_id"],
            "payload": transformed_payload,
            "time": transformed_payload.get("last_refresh_time_utc_reference")
        })
    return templates.TemplateResponse("dashboard.html", {"request": request, "data": transformed_data})

@router.get("/export", response_class=HTMLResponse)
async def export_page(request: Request):
    return templates.TemplateResponse("export.html", {"request": request})

@router.get("/import", response_class=HTMLResponse)
async def import_page(request: Request):
    return templates.TemplateResponse("import.html", {"request": request})
