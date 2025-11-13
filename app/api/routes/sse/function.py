import asyncio
import json
from time import monotonic
from typing import AsyncGenerator

from redis.asyncio import Redis
from fastapi.responses import StreamingResponse

from app.core.config import settings

def sse_headers() -> dict[str, str]:
    return {
        "Cache-Control": "no-cache",
        "Connection": "keep-alive",
        "X-Accel-Buffering": "no",
    }

async def event_generator(channel: str) -> AsyncGenerator[str, None]:
    redis_client = Redis.from_url(settings.REDIS_URL)
    pubsub = redis_client.pubsub()
    await pubsub.subscribe(channel)
    print("subscribed to channel", channel)
    
    try:
       yield f"retry: {settings.SSE_RETRY_MS}\n\n"
       last_ping = monotonic()

       while True:
            msg = await pubsub.get_message(ignore_subscribe_messages=True, timeout=5.0)
            if msg and msg["type"] == "message":
                raw = msg["data"]
                try: 
                    data = json.loads(raw)
                    print("sending data to client", data)
                except Exception:
                    data = raw.decode() if isinstance(raw, bytes) else str(raw)
                yield f"data: {json.dumps(data)}\n\n"
                
            now = monotonic()
            if now - last_ping > settings.SSE_HEARTBEAT_SEC:
                # return for client 
                yield f"keep-alive\n\n"
                last_ping = now

            await asyncio.sleep(0.2)
    except asyncio.CancelledError:
        raise
    finally:
        try:
            await pubsub.unsubscribe(channel)
        finally:
            await pubsub.close()


def make_sse_response(generator: AsyncGenerator[str, None]) -> StreamingResponse:
    return StreamingResponse(generator, media_type="text/event-stream", headers=sse_headers())