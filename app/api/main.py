from fastapi import APIRouter
from app.api.routes.health import health_router
from app.api.routes.sse import sse_router

api_router = APIRouter()
api_router.include_router(health_router)
api_router.include_router(sse_router)
