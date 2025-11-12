## SSE Service (FastAPI + Redis)

- Health: `GET /health` → `{"status":"ok"}`
- SSE stream: `GET /sse?order_id=...` (Content-Type: `text/event-stream`)

### Setup with uv (recommended)

Prereqs:
- python 3.10+
- uv installed (`pipx install uv` or `curl -LsSf https://astral.sh/uv/install.sh | sh`)

Steps:
1) Create venv and sync deps
   - `uv venv .venv`
   - `uv sync`

2) (Optional) load env
   - `cp .env.example .env` and edit
   - or set `REDIS_URL` in environment

3) Start
   - `uv run uvicorn app.main:app --host 0.0.0.0 --port 8080 --workers 1 --http h11`

   Notes:
   - Use a single worker for SSE (sticky connections). If you need multiple replicas, put them behind a load balancer with sticky sessions.
   - Configure `CORS_ALLOW_ORIGINS` (comma-separated) to restrict domains that can call the API in browsers.
     - Example: `CORS_ALLOW_ORIGINS=http://localhost:3000,https://app.example.com`
     - If unset, only same-origin requests are allowed; set `*` to allow all (not recommended for production).
    - Configure `ALLOWED_HOSTS` (comma-separated, matching the HTTP Host header) to drop requests for unknown domains/IPs.
      - Example: `ALLOWED_HOSTS=api.example.com`
      - Leave empty to accept any host (for local/dev).

### Redis Channels

- Clients subscribe per-order: `order:{order_id}`

Publish from Django (example):

```python
# Using redis-py
import json
import redis

r = redis.Redis.from_url("redis://localhost:6379/0", encoding="utf-8", decode_responses=True)
order_id = "12345"
event = {"status": "paid", "orderId": order_id, "ts": 1731400000}
r.publish(f"order:{order_id}", json.dumps(event))
```

### Client (Browser)

```html
<script>
  const orderId = "12345";
  const es = new EventSource(`/sse?order_id=${encodeURIComponent(orderId)}`);

  es.addEventListener("update", (e) => {
    const data = JSON.parse(e.data);
    console.log("Update:", data);
  });

  es.addEventListener("heartbeat", () => {
    // optional: used to keep connections warm through proxies
  });

  es.onerror = (e) => {
    console.warn("SSE error:", e);
  };
</script>
```

### Notes

- The service validates that `order_id` is provided.
- Heartbeats are sent every `SSE_HEARTBEAT_SEC` seconds to keep idle connections alive.
- Initial `retry:` directive hints reconnection interval to clients.