import asyncio
import json
import logging
from time import monotonic
from typing import AsyncGenerator

from redis.asyncio import Redis
from fastapi.responses import StreamingResponse

from app.core.config import settings

logger = logging.getLogger(__name__)

redis = Redis.from_url(settings.REDIS_URL)

CHANNELS = {}  # { channel: {"clients": set[Queue], "task": Task} }
LOCK = asyncio.Lock()  # avoid race when multiple clients join same channel


def sse_headers() -> dict[str, str]:
    return {
        "Cache-Control": "no-cache",
        "Connection": "keep-alive",
        "X-Accel-Buffering": "no",
    }


async def redis_listener(channel: str):
    pubsub = redis.pubsub()
    await pubsub.subscribe(channel)
    logger.info(f"[SharedSub] Started Redis subscriber for {channel}")

    try:
        
        async for msg in pubsub.listen():
            if msg["type"] == "message":
                data = msg["data"]
                
                if channel in CHANNELS:  
                    await asyncio.gather(*(q.put(data) for q in CHANNELS[channel]["clients"]))
    except asyncio.CancelledError:
        logger.info(f"[SharedSub] Cancelled Redis listener for {channel}")
        raise
    finally:
        await pubsub.unsubscribe(channel)
        await pubsub.close()
        logger.info(f"[SharedSub] Cleaned Redis subscriber for {channel}")


async def event_generator(channel: str, key: str) -> AsyncGenerator[str, None]:
    
    # Create queue for each client
    queue = asyncio.Queue()

    async with LOCK:
        # create Channel with first client
        if channel not in CHANNELS:
            CHANNELS[channel] = {
                "clients": set(),
                "task": asyncio.create_task(redis_listener(channel))
            }
            # Mark channel as having active subscribers in Redis
            subscriber_key = settings.redis_subscriber_key(channel)
            await redis.set(subscriber_key, "1")
            logger.info("[SharedSub] Created new shared subscriber for %s (set %s)", channel, subscriber_key)

        CHANNELS[channel]["clients"].add(queue)
        logger.info("[Client] Added client to channel %s (%d clients)",
                    channel, len(CHANNELS[channel]["clients"]))

    try:
        # Send retry config
        yield f"retry: {settings.SSE_RETRY_MS}\n\n"

        last_ping = monotonic()

        while True:
            try:
                data = await asyncio.wait_for(queue.get(), timeout=0.2)
            except asyncio.TimeoutError:
                # Heartbeat
                now = monotonic()
                if now - last_ping > settings.SSE_HEARTBEAT_SEC:
                    yield "keep-alive\n\n"
                    last_ping = now
                continue

            # Parse JSON if possible
            try:
                parsed = json.loads(data)
            except Exception:
                parsed = data.decode() if isinstance(data, bytes) else str(data)

            yield f"data: {json.dumps(parsed)}\n\n"

    except asyncio.CancelledError:
        logger.info("[Client] SSE cancelled for %s", channel)
        raise
    finally:
        async with LOCK:
            CHANNELS[channel]["clients"].remove(queue)
            await delete_cache(key)
            logger.info("[Client] Removed from channel %s (%d left)",
                        channel, len(CHANNELS[channel]["clients"]))
            logger.info("[Client] Deleted cache for key: %s", key)
            # remove shared subscriber if no clients
            if not CHANNELS[channel]["clients"]:
                CHANNELS[channel]["task"].cancel()
                # Remove subscriber tracking key from Redis
                subscriber_key = settings.redis_subscriber_key(channel)
                await redis.delete(subscriber_key)
                del CHANNELS[channel]
                logger.info("[SharedSub] Removed shared subscriber for %s (deleted %s)", channel, subscriber_key)


def make_sse_response(generator: AsyncGenerator[str, None]) -> StreamingResponse:
    return StreamingResponse(
        generator,
        media_type="text/event-stream",
        headers=sse_headers()
    )

from urllib.parse import urlparse

def parse_url(url: str) -> tuple[str, str, str]:
    path = urlparse(url).path           # /event_code/order_id/unique_id
    parts = path.strip("/").split("/")  # ['event_code', 'order_id', 'unique_id']
    key = f"{parts[2]}:{parts[0]}:{parts[1]}"
    return key

async def get_cache(key: str) -> bool:
    logger.info(f"Checking cache for key: {key}")
    if await redis.exists(key):
        logger.info(f"Cache found for key: {key}")
        return True
    else:
        logger.info(f"Cache not found for key: {key}")
        return False


async def delete_cache(key: str) -> bool:   
    logger.info(f"Deleting cache for key: {key}")
    if await redis.delete(key):
        logger.info(f"Cache deleted for key: {key}")
        return True
    else:
        logger.info(f"Cache not found for key: {key}")
        return False