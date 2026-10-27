"""Publicación de ventanas cerradas en Redis Pub/Sub."""

from __future__ import annotations

import json
from collections.abc import Sequence

from redis.asyncio import Redis

from streaming.windows import WindowResult


class RedisMetricsPublisher:
    def __init__(self, redis: Redis, channel: str) -> None:
        self._redis = redis
        self._channel = channel

    async def publish(self, results: Sequence[WindowResult]) -> None:
        if not results:
            return
        # Pipeline para mandar todas las ventanas en un solo round-trip
        async with self._redis.pipeline(transaction=False) as pipe:
            for result in results:
                pipe.publish(self._channel, json.dumps(result.to_dict()))
            await pipe.execute()
