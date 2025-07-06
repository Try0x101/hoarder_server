import os
import uuid
from fastapi import APIRouter, HTTPException, BackgroundTasks, File, UploadFile, Form, Request
from fastapi.responses import FileResponse
from .constants import EXPORT_DIR, IMPORT_DIR
from .status import update_status
from .service import perform_export, perform_import

router = APIRouter(prefix="/export_import", tags=["Export/Import"])

@router.post("/export")
async def export_database(background_tasks: BackgroundTasks, device_id: str = Form(None)):
    export_id = str(uuid.uuid4())
    export_file = os.path.join(EXPORT_DIR, f"export_{export_id}.json")
    status_file = os.path.join(EXPORT_DIR, f"status_{export_id}.json")
    update_status(status_file, {"status": "started"})
    background_tasks.add_task(perform_export, export_file, status_file, device_id)
    return {"export_id": export_id, "status_endpoint": f"/export_import/status/{export_id}"}

@router.post("/import")
async def import_database(background_tasks: BackgroundTasks, file: UploadFile = File(...), merge: bool = Form(True)):
    import_id = str(uuid.uuid4())
    import_file = os.path.join(IMPORT_DIR, f"import_{import_id}.json")
    status_file = os.path.join(IMPORT_DIR, f"status_{import_id}.json")
    with open(import_file, "wb") as f:
        f.write(await file.read())
    update_status(status_file, {"status": "started"})
    background_tasks.add_task(perform_import, import_file, status_file, merge)
    return {"import_id": import_id, "status_endpoint": f"/export_import/status/{import_id}"}

@router.get("/status/{task_id}")
async def get_task_status(task_id: str):
    status_file_export = os.path.join(EXPORT_DIR, f"status_{task_id}.json")
    status_file_import = os.path.join(IMPORT_DIR, f"status_{task_id}.json")
    status_file = status_file_export if os.path.exists(status_file_export) else status_file_import
    if not os.path.exists(status_file):
        raise HTTPException(status_code=404, detail="Task ID not found")
    with open(status_file, "r") as f:
        status = f.read()
    return status

@router.get("/download/{export_id}")
async def download_export(export_id: str):
    export_file = os.path.join(EXPORT_DIR, f"export_{export_id}.json")
    if not os.path.exists(export_file):
        raise HTTPException(status_code=404, detail="Export file not found")
    return FileResponse(export_file, filename=f"hoarder_export_{export_id}.json")
