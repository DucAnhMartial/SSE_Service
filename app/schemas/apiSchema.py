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

class SSEStreamIn(BaseModel):
    model_config = ConfigDict(
        json_schema_extra={
            "example": 
            {
                "event_source": "ets_payment",
                "event_data": {
                    "order_id": "12345",
                    "amount": 100.00,
                    "event_code":"ets_payment",
                },
            },
            "examples": [
                {
                    "event_source": "ets_payment",
                    "event_data": {
                        "order_id": "12345",
                        "amount": 100.00,
                        "event_code":"ets_payment",
                    },
                }
            ],
        }
    )

class SSEStreamOut(BaseModel):
    model_config = ConfigDict(
        json_schema_extra={
            "example": 
            {
                "event_source": "ets_payment",
                "event_data": {
                    "order_id": "12345",
                    "amount": 100.00,
                    "event_code":"ets_payment",
                    "order_status":"paid",
                },
            },
            "examples": [
                {
                    "event_source": "ets_payment",
                    "event_data": {
                        "order_id": "12345",
                        "amount": 100.00,
                        "event_code":"ets_payment",
                        "order_status":"paid",
                    },
                }
            ],
        }
    )
    event_source: str
    event_data: dict
    order_status: str