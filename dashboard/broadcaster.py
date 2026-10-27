"""Fan-out de mensajes a los clientes WebSocket conectados.

Cada cliente tiene su propia cola acotada: un navegador lento no frena al
resto, y si se llena la cola se descartan sus mensajes más viejos.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Protocol

logger = logging.getLogger(__name__)


class TextSender(Protocol):
    async def send_text(self, data: str) -> None: ...


class ClientChannel:
    def __init__(self, socket: TextSender, max_queue: int) -> None:
        self.socket = socket
        self.queue: asyncio.Queue[str] = asyncio.Queue(maxsize=max_queue)
        self.dropped = 0

    def offer(self, message: str) -> None:
        if self.queue.full():
            # Se privilegia el dato más reciente: es un dashboard en vivo
            self.queue.get_nowait()
            self.dropped += 1
        self.queue.put_nowait(message)


class Broadcaster:
    def __init__(self, max_queue_per_client: int = 256) -> None:
        self._clients: dict[int, ClientChannel] = {}
        self._max_queue = max_queue_per_client

    @property
    def client_count(self) -> int:
        return len(self._clients)

    def register(self, socket: TextSender) -> ClientChannel:
        channel = ClientChannel(socket, self._max_queue)
        self._clients[id(socket)] = channel
        return channel

    def unregister(self, socket: TextSender) -> None:
        self._clients.pop(id(socket), None)

    def broadcast(self, message: str) -> None:
        for channel in list(self._clients.values()):
            channel.offer(message)

    async def pump(self, channel: ClientChannel) -> None:
        """Envía al socket lo que llega a su cola hasta que se desconecte."""
        while True:
            message = await channel.queue.get()
            await channel.socket.send_text(message)
