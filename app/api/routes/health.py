from fastapi import APIRouter

from app.schemas.apiSchema import CheckHealthResponse

health_router = APIRouter(prefix="/health", tags=["Check Health"])

@health_router.get("/", description="Check Health", response_model=CheckHealthResponse)
async def health_check():
    return CheckHealthResponse(status="ok")