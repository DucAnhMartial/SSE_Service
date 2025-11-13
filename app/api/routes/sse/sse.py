from time import monotonic
from fastapi import APIRouter, HTTPException
from fastapi.responses import StreamingResponse
import redis.asyncio as redis
import asyncio
import json
from app.core.config import settings

sse_router = APIRouter(prefix="/sse", tags=["SSE"])

@sse_router.get("/stream/{event_code}/{order_id}/")
async def sse_stream(event_code: str, order_id: str):
    redis_client = redis.from_url(settings.REDIS_URL)
    pubsub = redis_client.pubsub()

    try:
        channel = settings.redis_channel_for_event(event_code=event_code, order_id=order_id)
        await pubsub.subscribe(channel)
        print(f"Subscribed to channel: {channel}")
    except redis.RedisError as e:
        print(f"Error subscribing to Redis channel: {e}")
        raise HTTPException(status_code=500, detail=f"Error subscribing to Redis channel: {e}")

    async def event_generator():
        # Send config time reconnect to Event Source
        yield f"retry: {settings.SSE_RETRY_MS}\n\n"
        last_ping = monotonic()
        try:
            while True:
                # Lấy message từ Redis, có timeout để không bị block
                message = await pubsub.get_message(ignore_subscribe_messages=True, timeout=5.0)
                if message and message["type"] == "message":
                    try:
                        data = json.loads(message["data"])
                        print(data)
                    except Exception:
                        data = message["data"].decode() if isinstance(message["data"], bytes) else str(message["data"])
                    yield f"data: {json.dumps(data)}\n\n"

                # Gửi heartbeat giữ kết nối SSE (tránh timeout)
                now = monotonic()
                if now - last_ping > settings.SSE_HEARTBEAT_SEC:
                    yield f"keep-alive\n\n"
                    last_ping = now

                await asyncio.sleep(0.2)
        except asyncio.CancelledError:
            print(f"Client disconnected from channel: {channel}")
        finally:
            await pubsub.unsubscribe(channel)
            await pubsub.close()
            print(f"Unsubscribed and closed Redis pubsub for channel: {channel}")
        
    return StreamingResponse(event_generator(), media_type="text/event-stream", headers= {
        "X-Accel-Buffering": "no",
        "Cache-Control": "no-cache",
        "Connection": "keep-alive",
    })
