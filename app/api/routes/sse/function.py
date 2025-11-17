import asyncio
import json
import logging
from time import monotonic
from typing import AsyncGenerator

from redis.asyncio import Redis
from fastapi.responses import StreamingResponse

from app.core.config import settings

logger = logging.getLogger(__name__)

def sse_headers() -> dict[str, str]:
    return {
        "Cache-Control": "no-cache",
        "Connection": "keep-alive",
        "X-Accel-Buffering": "no",
    }


async def event_generator(channel: str) -> AsyncGenerator[str, None]:
    redis_client = Redis.from_url(settings.REDIS_URL)
    pubsub = redis_client.pubsub()

    # Subscribe to channel
    await pubsub.subscribe(channel)
    logger.info("Subscribing to %s", channel)

    # await subscribe confirmation from Redis
    sub_msg = await pubsub.get_message(ignore_subscribe_messages=False, timeout=1.0)
    if sub_msg and sub_msg.get("type") == "subscribe":
        await redis_client.set(f"sse_subscribed:{channel}", "1", ex=300)
        logger.info("Subscription confirmed + ready flag set for %s", channel)
    else:
        logger.warning("No subscribe confirmation received for %s, still setting flag", channel)
        await redis_client.set(f"sse_subscribed:{channel}", "1", ex=60)

    try:
        # SSE retry
        yield f"retry: {settings.SSE_RETRY_MS}\n\n"

        last_ping = monotonic()
        while True:
            msg = await pubsub.get_message(ignore_subscribe_messages=True, timeout=1.0)

            if msg and msg.get("type") == "message":
                raw = msg["data"]
                try:
                    data = json.loads(raw)
                except Exception:
                    data = raw.decode() if isinstance(raw, bytes) else str(raw)
                yield f"data: {json.dumps(data)}\n\n"
                logger.info("Sent SSE event on %s: %s", channel, data)

            # Heartbeat
            now = monotonic()
            if now - last_ping > settings.SSE_HEARTBEAT_SEC:
                yield "keep-alive\n\n"
                last_ping = now

            await asyncio.sleep(0.05)

    except asyncio.CancelledError:
        logger.info("SSE cancelled for %s", channel)
        raise

    finally:
        await redis_client.delete(f"sse_subscribed:{channel}")
        await pubsub.unsubscribe(channel)
        await pubsub.close()
        await redis_client.close()
        logger.info("Cleaned up pubsub and Redis connection for %s", channel)


def make_sse_response(generator: AsyncGenerator[str, None]) -> StreamingResponse:
    return StreamingResponse(generator, media_type="text/event-stream", headers=sse_headers())
