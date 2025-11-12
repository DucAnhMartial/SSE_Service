import os
from typing import Annotated, Iterable, List

from pydantic import AnyUrl, BeforeValidator, Field, PositiveInt, computed_field
from pydantic_settings import BaseSettings, SettingsConfigDict


def _parse_str_list(value: Iterable[str] | str | None) -> List[str]:
    
    if value is None:
        return []

    if isinstance(value, str):
        if not value:
            return []
        if value.startswith("[") and value.endswith("]"):
            value = value.strip("[]")
        tokens = value.split(",")
    else:
        tokens = list(value)
    return [token.strip() for token in tokens if token and token.strip()]


def _parse_cors_allow_origins(value: Iterable[str] | str | None) -> List[str]:
    origins = _parse_str_list(value)
    return [origin.rstrip("/") for origin in origins]


def _parse_allowed_hosts(value: Iterable[str] | str | None) -> List[str]:
    hosts = _parse_str_list(value)
    return [host.lower() for host in hosts]


class Settings(BaseSettings):

    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    API_TITLE: str = Field(default="ETS SSE Service")
    API_VERSION: str = Field(default="1.0.0")
    API_DOCS_URL: str = Field(default="/api/v1/docs")
    API_V1_STR: str = Field(default="/api/v1")
    API_OPENAPI_URL: str = Field(default="/api/v1/openapi.json")

    REDIS_URL: str = Field(os.getenv("REDIS_URL", "redis://localhost:6379/0"))
    REDIS_CHANNEL_PREFIX: str = Field(default="ets_payment:success:")

    SSE_RETRY_MS: PositiveInt = Field(os.getenv("SSE_RETRY_MS", 3000))
    SSE_HEARTBEAT_SEC: PositiveInt = Field(os.getenv("SSE_HEARTBEAT_SEC", 25))

    BACKEND_CORS_ORIGINS: Annotated[
        list[AnyUrl] | str,
        BeforeValidator(_parse_cors_allow_origins),
    ] = Field(os.getenv("BCORS_ORIGINS", []))
    ALLOWED_HOSTS: Annotated[
        list[str] | str,
        BeforeValidator(_parse_allowed_hosts),
    ] = Field(default_factory=list, description="Allowed values for the Host header.")

    
    @computed_field  
    @property
    def cors_allow_origins(self) -> List[str]:
        """Return cors origins as list[str]."""
        return [str(origin) for origin in self.BACKEND_CORS_ORIGINS]

    @computed_field  
    @property
    def allowed_hosts(self) -> List[str]:
        """Return allowed hosts as lower-case list."""
        return [host.lower() for host in self.ALLOWED_HOSTS]  

    def redis_channel_for_order(self, order_id: str) -> str:
        """Compose Redis pub/sub channel name for a given order."""
        order_id = order_id.strip()
        if not order_id:
            msg = "order_id must be a non-empty string"
            raise ValueError(msg)
        return f"{self.REDIS_CHANNEL_PREFIX}{order_id}"

settings = Settings()
