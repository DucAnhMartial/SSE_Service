from app.schemas.apiSchema import CheckHealthResponse
from fastapi import FastAPI

from app.core.config import settings

app = FastAPI(
    title=settings.API_TITLE,
    version=settings.API_VERSION,
    openapi_url=settings.API_OPENAPI_URL,
    docs_url=settings.API_DOCS_URL,
    redoc_url=None,
)


@app.get("/health",description="Health check endpoint",response_model=CheckHealthResponse)
async def health_check():
    return CheckHealthResponse(status="ok")