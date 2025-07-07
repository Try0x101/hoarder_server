from fastapi import APIRouter, Request
from app.responses import PrettyJSONResponse
from app.api.batch.handlers import handle_batch_upload, handle_delta_batch

router = APIRouter()

@router.post("/api/batch", response_class=PrettyJSONResponse)
async def receive_batch_data(request: Request):
    return await handle_batch_upload(request)

@router.post("/api/batch-delta", response_class=PrettyJSONResponse)
async def receive_batch_delta_data(request: Request):
    return await handle_delta_batch(request)
