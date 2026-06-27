"""
BaseAdapter — Abstract base class for all platform chat adapters.
"""

import asyncio
import logging
from abc import ABC, abstractmethod
from typing import Callable, Awaitable, Optional


MessageCallback = Callable[[dict], Awaitable[None]]


class BaseAdapter(ABC):
    """
    All platform adapters inherit from this class.
    Implement `_connect()` to establish platform connection.
    Implement `_listen()` to poll or receive messages.
    Call `self._emit(message_dict)` to forward messages upstream.
    """

    def __init__(self, platform: str):
        self.platform = platform
        self.logger = logging.getLogger(f"streamchat.adapter.{platform}")
        self._callback: Optional[MessageCallback] = None
        self._running = False

    async def start(self, callback: MessageCallback):
        """Called by ChatManager to start this adapter."""
        self._callback = callback
        self._running = True
        await self._run()

    async def stop(self):
        """Called by ChatManager to stop this adapter."""
        self._running = False

    async def _run(self):
        """Main run loop: connect then listen."""
        await self._connect()
        await self._listen()

    @abstractmethod
    async def _connect(self):
        """Establish connection to platform."""
        ...

    @abstractmethod
    async def _listen(self):
        """Continuously receive messages and call self._emit()."""
        ...

    async def _emit(self, message: dict):
        """Forward a normalized message to the ChatManager."""
        if self._callback and self._running:
            message["platform"] = self.platform
            await self._callback(message)
