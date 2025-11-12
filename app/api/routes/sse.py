from fastapi import APIRouter
from starlette.responses import StreamingResponse
from app.schemas.apiSchema import SSEStreamIn

sse_router = APIRouter(prefix="/sse", tags=["SSE"])

@sse_router.post("/", description="SSE Stream")
async def sse_stream(sse_stream_in: SSEStreamIn):
    return StreamingResponse(sse_stream(sse_stream_in), media_type="text/event-stream")