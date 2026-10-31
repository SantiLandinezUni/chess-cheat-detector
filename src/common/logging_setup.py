"""Logging to the console and to logs/<step>_<UTC timestamp>.log."""

import logging
import time
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path

LOG_FORMAT = "%(asctime)s %(levelname)s %(name)s: %(message)s"


def setup_logging(step: str, log_dir: str, level: int = logging.INFO) -> logging.Logger:
    """Configure the root logger for one pipeline step and return the step's logger."""
    Path(log_dir).mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    log_file = Path(log_dir) / f"{step}_{stamp}.log"

    formatter = logging.Formatter(LOG_FORMAT, datefmt="%Y-%m-%dT%H:%M:%SZ")
    formatter.converter = time.gmtime  # timestamps in UTC

    root = logging.getLogger()
    root.setLevel(level)
    root.handlers.clear()
    for handler in (logging.StreamHandler(), logging.FileHandler(log_file, encoding="utf-8")):
        handler.setFormatter(formatter)
        root.addHandler(handler)

    logger = logging.getLogger(step)
    logger.info("log file: %s", log_file)
    return logger


@contextmanager
def log_duration(logger: logging.Logger, what: str) -> Iterator[None]:
    """Log start, end and duration of a block."""
    logger.info("start: %s", what)
    t0 = time.perf_counter()
    try:
        yield
    finally:
        logger.info("end: %s (%.1f s)", what, time.perf_counter() - t0)
