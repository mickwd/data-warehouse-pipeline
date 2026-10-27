"""Logging estructurado simple (una línea por evento, clave=valor)."""

from __future__ import annotations

import logging
import sys


def configure_logging(level: str = "INFO") -> None:
    logging.basicConfig(
        level=level.upper(),
        format="%(asctime)s level=%(levelname)s logger=%(name)s %(message)s",
        stream=sys.stdout,
        force=True,
    )
    # Librerías de red muy verbosas en DEBUG
    for noisy in ("aiokafka", "httpx", "httpcore", "aio_pika", "aiormq"):
        logging.getLogger(noisy).setLevel(logging.WARNING)
