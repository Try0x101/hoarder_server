import os
import traceback
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles
from responses import PrettyJSONResponse

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
        title="Hoarder API Server",
        version="2.0.0",
        description="IoT Telemetry API Platform"
    )
    
    app.add_exception_handler(Exception, generic_exception_handler)
    
    static_dir = "app/static"
    if os.path.exists(static_dir):
        app.mount("/static", StaticFiles(directory=static_dir), name="static")
    
    return app
