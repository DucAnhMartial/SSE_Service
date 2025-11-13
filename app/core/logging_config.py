import logging
from logging.config import dictConfig
from pathlib import Path
import os

LOG_DIR = Path(__file__).resolve().parent.parent / "logs"
LOG_FILE = os.path.join(LOG_DIR, "sse-service.log")
LOG_FORMAT = (
    " %(levelname)s %(asctime)s %(name)s %(filename)s:%(lineno)d "
    "%(process)d %(thread)d >> %(message)s"
)
LOG_LEVEL = logging.INFO
LOG_MAX_BYTES = 10 * 1024 * 1024  # 10MB
LOG_BACKUP_DAYS = 14

LOG_DIR.mkdir(parents=True, exist_ok=True)

dictConfig(
    {
        "version": 1,
        "disable_existing_loggers": False,
        "formatters": {
            "default": {
                "format": LOG_FORMAT,
                "datefmt": "%Y-%m-%d %H:%M:%S,%f",
            },
        },
        "handlers": {
            "console": {
                "class": "logging.StreamHandler",
                "formatter": "default",
                "level": LOG_LEVEL,
            },
            "file": {
                "class": "logging.handlers.TimedRotatingFileHandler",
                "formatter": "default",
                "filename": str(LOG_FILE),
                "when": "midnight",
                "backupCount": LOG_BACKUP_DAYS,
                "encoding": "utf-8",
                "level": LOG_LEVEL,
            },
        },
        "root": {
            "level": LOG_LEVEL,
            "handlers": ["console", "file"],
        },
    }
)