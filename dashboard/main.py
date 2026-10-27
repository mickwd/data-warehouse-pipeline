"""Dashboard en vivo: WebSocket alimentado por Redis Pub/Sub + REST de históricos.

Uso:
    uvicorn dashboard.main:app --host 0.0.0.0 --port 8000
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Annotated, Literal

from fastapi import FastAPI, Query, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from redis.asyncio import Redis
from sqlalchemy.ext.asyncio import create_async_engine

from common.config import get_settings
from common.logs import configure_logging
from dashboard.broadcaster import Broadcaster
from streaming.storage import WindowMetricsRepository

logger = logging.getLogger("dashboard")
STATIC_DIR = Path(__file__).parent / "static"
WindowType = Literal["tumbling", "sliding"]


async def relay_redis_to_clients(redis: Redis, channel: str, broadcaster: Broadcaster) -> None:
    """Una sola suscripción a Redis por proceso, reenviada a todos los sockets."""
    while True:
        try:
            async with redis.pubsub() as pubsub:
                await pubsub.subscribe(channel)
                logger.info("redis_subscribed channel=%s", channel)
                async for message in pubsub.listen():
                    if message.get("type") != "message":
                        continue
                    data = message["data"]
                    payload = data.decode() if isinstance(data, bytes) else data
                    # Se envuelve sin reparsear: el payload ya es JSON válido
                    broadcaster.broadcast(f'{{"type":"window","data":{payload}}}')
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("redis_subscription_lost, reintentando en 2s")
            await asyncio.sleep(2)


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    settings = get_settings()
    configure_logging(settings.log_level)

    redis = Redis.from_url(settings.redis.url)
    engine = create_async_engine(settings.metrics_db.url, pool_pre_ping=True)
    repo = WindowMetricsRepository(engine)
    await repo.create_schema()
    broadcaster = Broadcaster()

    app.state.repo = repo
    app.state.broadcaster = broadcaster
    relay = asyncio.create_task(relay_redis_to_clients(redis, settings.redis.metrics_channel, broadcaster))
    try:
        yield
    finally:
        relay.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await relay
        await redis.aclose()
        await engine.dispose()


app = FastAPI(title="Métricas de ventas en tiempo real", version="1.0.0", lifespan=lifespan)
app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")


@app.get("/", include_in_schema=False)
async def index() -> FileResponse:
    return FileResponse(STATIC_DIR / "index.html")


@app.get("/health")
async def health(request: Request) -> dict[str, object]:
    return {"status": "ok", "ws_clients": request.app.state.broadcaster.client_count}


@app.get("/api/metrics/history")
async def metrics_history(
    request: Request,
    window_type: WindowType = "tumbling",
    group_key: str = "all",
    minutes: Annotated[int, Query(ge=1, le=60 * 24 * 7)] = 30,
    limit: Annotated[int, Query(ge=1, le=5000)] = 500,
) -> list[dict[str, object]]:
    since = datetime.now(UTC) - timedelta(minutes=minutes)
    repo: WindowMetricsRepository = request.app.state.repo
    return await repo.history(window_type, group_key=group_key, since=since, limit=limit)


@app.websocket("/ws/metrics")
async def metrics_socket(websocket: WebSocket) -> None:
    await websocket.accept()
    broadcaster: Broadcaster = websocket.app.state.broadcaster
    repo: WindowMetricsRepository = websocket.app.state.repo

    # Snapshot inicial para que el gráfico no arranque vacío
    since = datetime.now(UTC) - timedelta(minutes=15)
    snapshot = await repo.history("tumbling", since=since, limit=2000)
    snapshot += await repo.history("sliding", group_key="all", since=since, limit=500)
    await websocket.send_text(json.dumps({"type": "snapshot", "data": snapshot}))

    channel = broadcaster.register(websocket)
    pump = asyncio.create_task(broadcaster.pump(channel))
    try:
        # El cliente no manda nada, se lee solo para detectar la desconexión
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        pass
    finally:
        broadcaster.unregister(websocket)
        pump.cancel()
        with contextlib.suppress(asyncio.CancelledError, Exception):
            await pump
