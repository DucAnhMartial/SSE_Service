from typing import Optional
from pydantic import BaseModel, Field, ConfigDict

class CheckHealthResponse(BaseModel):
    model_config = ConfigDict(
        json_schema_extra={
            "example": {"status": "ok"},
            "examples": [{"status": "ok"}],
        }
    )
    status: str