from fastapi.middleware.cors import CORSMiddleware
from fastapi import FastAPI

from app.core.config import settings
from app.api.main import api_router
from app.core import logging_config

app = FastAPI(
    title=settings.API_TITLE,
    version=settings.API_VERSION,
    openapi_url=settings.API_OPENAPI_URL,
    docs_url=settings.API_DOCS_URL,
    redoc_url=None,
)

# Setup CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.cors_allow_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"]
)

app.include_router(api_router, prefix=settings.API_V1_STR)