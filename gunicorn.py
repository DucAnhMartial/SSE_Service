import multiprocessing
from pathlib import Path

bind = "unix:/run/gunicorn/sse-service.sock"

worker_class = "uvicorn.workers.UvicornWorker"
workers = multiprocessing.cpu_count() * 2 + 1
threads = 1

timeout = 120
keepalive = 10

proc_name = "sse_service"

# Logging
LOG_ROOT = Path("/opt/sse_service/logs/gunicorn")
LOG_ROOT.mkdir(parents=True, exist_ok=True)
accesslog = str(LOG_ROOT / "access.log")
errorlog = str(LOG_ROOT / "error.log")
loglevel = "info"
capture_output = True

# Security/performance
umask = 0o007
preload_app = True