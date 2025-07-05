import os
import traceback
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles
from app.responses import PrettyJSONResponse
from app.routers import data, dashboard, history, telemetry, batch, export_import

async def generic_exception_handler(request: Request, exc: Exception):
    print("="*80)
    print(f"Unhandled exception for request: {request.method} {request.url}")
    traceback.print_exc()
    print("="*80)
    
    return JSONResponse(
        status_code=500,
        content={"error": "internal_error", "detail": "An unexpected server error occurred."},
    )

def create_app() -> FastAPI:
    app = FastAPI(
        default_response_class=PrettyJSONResponse,
        debug=False,
        title="Hoarder Server",
        version="3.3.0",
        description="Advanced IoT Telemetry Platform"
    )
    
    app.add_exception_handler(Exception, generic_exception_handler)
    
    _setup_static_files(app)
    _setup_routers(app)
    
    return app

def _setup_static_files(app: FastAPI):
    static_dir = "app/static"
    if os.path.exists(static_dir):
        app.mount("/static", StaticFiles(directory=static_dir), name="static")

def _setup_routers(app: FastAPI):
    app.include_router(data.router)
    app.include_router(dashboard.router, prefix="/dashboard")
    app.include_router(telemetry.router)
    app.include_router(history.router)
    app.include_router(batch.router)
    app.include_router(export_import.router)
