"""Extractor de APIs REST paginadas con reintentos y backoff exponencial."""

from __future__ import annotations

import logging
import random
import time
from collections.abc import Iterator
from typing import Any

import httpx
import pandas as pd

from batch.watermark import ExtractionWindow

logger = logging.getLogger(__name__)

RETRYABLE_STATUS = frozenset({408, 425, 429, 500, 502, 503, 504})


class RestApiExtractor:
    """Recorre una API con paginación por cursor (`next_cursor`) o por página.

    Contrato esperado de la respuesta:
        {"data": [...], "next_cursor": "abc" | null}
    Si la respuesta no trae `next_cursor` se usa `page` incremental hasta
    recibir una página vacía o incompleta.
    """

    def __init__(
        self,
        base_url: str,
        path: str,
        *,
        token: str | None = None,
        page_size: int = 200,
        timeout_seconds: float = 30.0,
        max_retries: int = 5,
        backoff_base_seconds: float = 0.5,
        client: httpx.Client | None = None,
    ) -> None:
        headers = {"Accept": "application/json"}
        if token:
            headers["Authorization"] = f"Bearer {token}"
        self._client = client or httpx.Client(base_url=base_url, headers=headers, timeout=timeout_seconds)
        self._path = path
        self._page_size = page_size
        self._max_retries = max_retries
        self._backoff_base = backoff_base_seconds

    def extract(self, window: ExtractionWindow) -> Iterator[pd.DataFrame]:
        params: dict[str, Any] = {
            "updated_since": window.lower.isoformat(),
            "updated_until": window.upper.isoformat(),
            "limit": self._page_size,
        }
        page = 1
        cursor: str | None = None
        while True:
            request_params = dict(params)
            if cursor:
                request_params["cursor"] = cursor
            else:
                request_params["page"] = page

            payload = self._get_with_retries(request_params)
            records = payload.get("data", [])
            if not records:
                break

            logger.info("api_page path=%s page=%s records=%s", self._path, page, len(records))
            yield pd.DataFrame.from_records(records)

            if "next_cursor" in payload:
                # Modo cursor: la API indica explícitamente cuándo no hay más páginas
                cursor = payload["next_cursor"]
                if cursor is None:
                    break
            elif len(records) < self._page_size:
                # Modo página: una página incompleta es la última
                break
            page += 1

    def _get_with_retries(self, params: dict[str, Any]) -> dict[str, Any]:
        attempt = 0
        while True:
            attempt += 1
            try:
                response = self._client.get(self._path, params=params)
                if response.status_code in RETRYABLE_STATUS and attempt <= self._max_retries:
                    self._sleep(attempt, response.headers.get("Retry-After"))
                    continue
                response.raise_for_status()
                return response.json()
            except (httpx.TransportError, httpx.TimeoutException) as exc:
                if attempt > self._max_retries:
                    raise
                logger.warning("api_retry path=%s attempt=%s error=%s", self._path, attempt, exc)
                self._sleep(attempt, None)

    def _sleep(self, attempt: int, retry_after: str | None) -> None:
        if retry_after and retry_after.isdigit():
            delay = float(retry_after)
        else:
            # Backoff exponencial con jitter para no sincronizar reintentos
            delay = self._backoff_base * (2 ** (attempt - 1)) + random.uniform(0, self._backoff_base)
        time.sleep(min(delay, 60.0))

    def close(self) -> None:
        self._client.close()
