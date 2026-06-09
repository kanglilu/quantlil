from __future__ import annotations

import logging
import re
from logging.handlers import RotatingFileHandler
from pathlib import Path


class RedactingFormatter(logging.Formatter):
    _patterns = (
        re.compile(r"bot\d+:[A-Za-z0-9_-]+"),
        re.compile(r"Bearer\s+[A-Za-z0-9._-]+"),
    )

    def format(self, record: logging.LogRecord) -> str:
        message = super().format(record)
        for pattern in self._patterns:
            message = pattern.sub("<redacted>", message)
        return message


def configure_logging(level: str, log_file: str) -> None:
    log_level = getattr(logging, level, logging.INFO)
    formatter = RedactingFormatter(
        "%(asctime)s %(levelname)s [%(name)s] %(message)s"
    )

    root = logging.getLogger()
    root.setLevel(log_level)
    root.handlers.clear()

    console = logging.StreamHandler()
    console.setFormatter(formatter)
    root.addHandler(console)

    log_path = Path(log_file)
    log_path.parent.mkdir(parents=True, exist_ok=True)
    file_handler = RotatingFileHandler(
        log_path,
        maxBytes=5_000_000,
        backupCount=5,
        encoding="utf-8",
    )
    file_handler.setFormatter(formatter)
    root.addHandler(file_handler)

    for noisy_logger in ("httpx", "httpcore"):
        logging.getLogger(noisy_logger).setLevel(logging.WARNING)
