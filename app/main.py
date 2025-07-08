from fastapi import FastAPI
import socketio
from contextlib import asynccontextmanager
from app.database.connection import init_db, close_db
from app.services.cache import init_redis
from app.api.data.router import router as data_router
from app.api.history.router import router as history_router

@asynccontextmanager
async def lifespan(app: FastAPI):
    await init_db()
    await init_redis()
    yield
    await close_db()

app = FastAPI(
    title="Hoarder API Server",
    version="1.0.0",
    lifespan=lifespan
)

sio = socketio.AsyncServer(async_mode="asgi", cors_allowed_origins="*")
socket_app = socketio.ASGIApp(sio, other_asgi_app=app)

app.include_router(data_router, prefix="/data")
app.include_router(history_router, prefix="/data")

@app.get("/")
async def root():
    return {
        "service": "Hoarder API Server",
        "version": "1.0.0",
        "status": "operational"
    }

@app.get("/health")
async def health():
    return {"status": "healthy"}
