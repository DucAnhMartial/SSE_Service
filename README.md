## SSE Service (FastAPI + Redis)

A lightweight FastAPI application that fans out Redis Pub/Sub events to Server-Sent Events (SSE) clients.  
Each SSE subscription targets a logical channel composed of `event_code` + `order_id`, allowing multiple business events to share the same infrastructure while keeping traffic isolated per order.

### Highlights

- API-first design with `/api/v1` prefix and automatic OpenAPI docs
- `/api/v1/sse/stream/{event_code}/{order_id}/` SSE endpoint (content type `text/event-stream`)
- Redis-backed pub/sub with channel naming helper (`ets_payment:success:{event_code}:{order_id}`)
- Shared Redis subscriber per channel + asyncio queue per client for backpressure safety
- Heartbeat + `retry:` directives for resilient EventSource clients
- Structured logging that writes both to stdout and rotating files

---

## How it Works

1. **Client subscribes** to `/api/v1/sse/stream/{event_code}/{order_id}/`.
2. The router validates inputs and builds the Redis channel name via `settings.redis_channel_for_event`.
3. `event_generator` registers the client queue, reusing a single Redis subscription per channel.
4. **Publishers** (any service with Redis access) `PUBLISH` JSON payloads to that channel.
5. The generator forwards each message as `data: ...` frames, injecting heartbeat comments every `SSE_HEARTBEAT_SEC`.
6. When the last client disconnects, the Redis subscriber task is cancelled and resources are cleaned up.

---

## Project Layout

```
app/
├── api/
│   ├── main.py            # Mount health + SSE routers
│   └── routes/
│       ├── health.py      # GET /api/v1/health/
│       └── sse/
│           ├── function.py# Redis listener + SSE generator helpers
│           └── sse.py     # API endpoint wiring
├── core/
│   ├── config.py          # Pydantic Settings (env vars, channel helper)
│   └── logging_config.py  # Console + rotating-file logging
├── schemas/               # Pydantic response examples
└── main.py                # FastAPI app factory + CORS middleware
```

Other notable files:

- `gunicorn.py` – production Gunicorn config (Unix socket, logging, preload)
- `pyproject.toml` – dependencies (`fastapi`, `redis`, `pydantic-settings`, etc.) and tooling (`ruff`, `mypy`)
- `logs/` – runtime log output (ignored by Git)

---

## API Cheat Sheet

| Method | Path | Description |
| --- | --- | --- |
| `GET` | `/api/v1/health/` | Returns `{"status": "ok"}` (used by probes) |
| `GET` | `/api/v1/sse/stream/{event_code}/{order_id}/` | Opens SSE stream bound to `ets_payment:success:{event_code}:{order_id}` |

Missing or empty `event_code` / `order_id` produces `422 Unprocessable Entity`.

---

## Configuration

| Variable | Default | Description |
| --- | --- | --- |
| `REDIS_URL` | `redis://localhost:6379/0` | Redis instance used for pub/sub |
| `REDIS_CHANNEL_PREFIX` | `ets_payment:success:` | Prefix used before `{event_code}:{order_id}` |
| `SSE_RETRY_MS` | `3000` | Value injected into `retry:` directive |
| `SSE_HEARTBEAT_SEC` | `25` | Interval for keep-alive comments when idle |
| `BACKEND_CORS_ORIGINS` | _empty_ | comma/JSON list, parsed by settings into `cors_allow_origins` |
| `API_TITLE` / `API_VERSION` | `ETS SSE Service` / `1.0.0` | FastAPI metadata |
| `API_DOCS_URL` / `API_OPENAPI_URL` | `/api/v1/docs` / `/api/v1/openapi.json` | Docs paths |

Add overrides in `.env`, environment variables, or your process manager.

---

## Quick Start (uv)

Prereqs:
- Python 3.10+
- [`uv`](https://github.com/astral-sh/uv)
- Redis (local or remote)

Steps:
1. **Install deps**
   - `uv venv .venv`
   - `uv sync`
2. **Configure env**
   - `cp .env.example .env` and update `REDIS_URL`
   - or export variables in your shell
3. **Run API**
   - `uv run uvicorn app.main:app --host 0.0.0.0 --port 8080 --workers 1 --http h11`

Notes:
- Keep a single Uvicorn worker; load-balance at the edge with sticky sessions if you run multiple replicas.
- Set `BACKEND_CORS_ORIGINS` (e.g., `http://localhost:3000,https://app.example.com`) so browsers can connect.

### Docker (optional)

```bash
docker build -t sse-service .
docker run --rm -p 8080:8080 \
  -e REDIS_URL=redis://host.docker.internal:6379/0 \
  sse-service
```

---

## Publishing Events

The channel for a given `(event_code, order_id)` is computed as:

```
{REDIS_CHANNEL_PREFIX}{event_code}:{order_id}
# default => ets_payment:success:{event_code}:{order_id}
```

### Subscriber Tracking

Before publishing, you can check if any SSE clients are subscribed by checking for the key `sse_subscribed:{channel}`. This key is automatically set when the first client connects and removed when the last client disconnects.

Publish example (Python/redis-py) with subscriber check:

```python
import json
import redis

r = redis.Redis.from_url("redis://localhost:6379/0", encoding="utf-8", decode_responses=True)
event_code = "payment"
order_id = "12345"
channel = f"ets_payment:success:{event_code}:{order_id}"
subscriber_key = f"sse_subscribed:{channel}"

# Check if any SSE subscribers are listening (optional optimization)
if r.exists(subscriber_key):
    payload = {
        "event_code": event_code,
        "order_id": order_id,
        "order_status": "paid",
        "amount": 100.00,
    }
    r.publish(channel, json.dumps(payload))
else:
    print(f"No SSE subscribers for {channel}, skipping publish")
```

Publish example without subscriber check:

```python
import json
import redis

r = redis.Redis.from_url("redis://localhost:6379/0", encoding="utf-8", decode_responses=True)
event_code = "payment"
order_id = "12345"
payload = {
    "event_code": event_code,
    "order_id": order_id,
    "order_status": "paid",
    "amount": 100.00,
}
channel = f"ets_payment:success:{event_code}:{order_id}"
r.publish(channel, json.dumps(payload))
```

Any language or stack that can `PUBLISH` JSON strings to Redis can drive the stream.

---

## Browser Client Example

```html
<script>
  const eventCode = "payment";
  const orderId = "12345";
  const es = new EventSource(`/api/v1/sse/stream/${eventCode}/${orderId}/`);

  es.addEventListener("message", (event) => {
    const payload = JSON.parse(event.data);
    console.log("Update:", payload);
  });

  es.addEventListener("heartbeat", () => {
    // optional hook for dashboards
  });

  es.onerror = (err) => {
    console.warn("SSE error:", err);
  };
</script>
```

### Client Tips

- Browsers automatically reconnect; the `retry:` directive (default 3s) hints the reconnect delay.
- If you deliver custom `event:` fields, listen via `es.addEventListener("update", handler)`.
- Some corporate proxies kill idle connections — heartbeats mitigate this, but keep an exponential backoff fallback.

---

## Development & Testing

```bash
uv run ruff check app
uv run mypy app
uv run pytest               # add tests under tests/
```

- Use `redis-cli` or a helper script to publish sample messages and watch SSE output.
- Inspect the raw SSE stream with `curl -N http://localhost:8080/api/v1/sse/stream/payment/demo/`.
- Logs are written to stdout and `app/logs/sse-service.log` via `logging_config`.

---

## Production Deployment: Gunicorn + systemd

> Example assumes the code lives in `/opt/sse_service`, the virtualenv is `/opt/sse_service/.venv`, and the service user is `sse`. Adjust paths and usernames to match your host.

### 1. Prepare the host

```bash
sudo chown -R www-data:www-data /opt/sse_service
cd /opt/sse_service
sudo -u www-data uv venv .venv
sudo -u www-data .venv/bin/uv sync
sudo cp .env.example /opt/sse_service/.env   # edit REDIS_URL, etc.
sudo chown www-data:www-data /opt/sse_service/.env
```

Ensure Redis is reachable and that `.env` contains all required values (`REDIS_URL`, `CORS_ALLOW_ORIGINS`, ...).

### 2. Gunicorn config

- The repo ships with `gunicorn.py` configured to:
  - Bind to `unix:/run/gunicorn/sse-service.sock`
  - Use `uvicorn.workers.UvicornWorker`
  - Preload the app and log into `logs/gunicorn/*.log`
- Switch to TCP by editing the config to `bind = "0.0.0.0:8080"` if desired.

Create a tmpfiles entry so the socket directory exists at boot:

`/etc/tmpfiles.d/sse-service.conf`
```
d /run/gunicorn 0755 www-data www-data -
```

Apply it:
```bash
sudo systemd-tmpfiles --create /etc/tmpfiles.d/sse-service.conf
```

### 3. systemd unit

`/etc/systemd/system/sse-service.service`

```
[Unit]
Description=SSE Service (Gunicorn)
After=network.target redis.service
Wants=redis.service

[Service]
User=www-data
Group=www-data
WorkingDirectory=/opt/sse_service
EnvironmentFile=/opt/sse_service/.env
ExecStart=/opt/sse_service/.venv/bin/gunicorn -c /opt/sse_service/gunicorn.py app.main:app
Restart=on-failure
RestartSec=5
KillSignal=SIGQUIT
TimeoutStopSec=30
PrivateTmp=true
LimitNOFILE=65536

[Install]
WantedBy=multi-user.target
```

Reload and start:

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now sse-service
sudo systemctl status sse-service
```

Tail logs:

```bash
sudo journalctl -u sse-service -n 200 -f
tail -f /opt/sse_service/logs/gunicorn/error.log
```

### 4. Reverse proxy (optional)

If you want to expose the Unix socket through Nginx/Traefik, point the upstream to `unix:/run/gunicorn/sse-service.sock`. Nginx snippet:

```
location / {
  proxy_set_header Host $host;
  proxy_set_header X-Real-IP $remote_addr;
  proxy_http_version 1.1;
  proxy_set_header Connection "";
  proxy_pass http://unix:/run/gunicorn/sse-service.sock;
  proxy_buffering off;
}
```

Disabling `proxy_buffering` ensures SSE frames flush immediately.

---

## Notes

- `order_id` is mandatory; validation happens in the FastAPI router.
- Heartbeats (`keep-alive`) fire every `SSE_HEARTBEAT_SEC` seconds to keep proxies from closing idle connections.
- `retry:` directive is emitted once per connection to encourage fast reconnects.

Happy streaming!