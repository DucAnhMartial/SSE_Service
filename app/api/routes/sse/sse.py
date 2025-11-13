from fastapi import APIRouter, HTTPException
from app.api.routes.sse.function import event_generator, make_sse_response
from app.core.config import settings

sse_router = APIRouter(prefix="/sse", tags=["SSE"])

@sse_router.get("/stream/{event_code}/{order_id}/")
async def sse_stream(event_code: str, order_id: str):
    try:
        channel = settings.redis_channel_for_event(event_code=event_code, order_id=order_id)
    except ValueError as e:
        raise HTTPException(status_code=422, detail=str(e))

    gen = event_generator(channel)
    return make_sse_response(gen)