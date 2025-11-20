from fastapi import APIRouter, HTTPException
from app.api.routes.sse.function import event_generator, make_sse_response, get_cache, parse_url
from app.core.config import settings

sse_router = APIRouter(prefix="/sse", tags=["SSE"])

@sse_router.get("/stream/{event_code}/{order_id}/{unique_id}")
async def sse_stream(event_code: str, order_id: str, unique_id: str):
    try:
        key = parse_url(f"/{event_code}/{order_id}/{unique_id}")
        if await get_cache(key) == False:
            raise HTTPException(status_code=403, detail="Connection refused")

        channel = settings.redis_channel_for_event(
            event_code=event_code, 
            order_id=order_id
        )
        gen = event_generator(channel,key)
        return make_sse_response(gen)
        
    except ValueError as e:
        raise HTTPException(status_code=422, detail=str(e))

    