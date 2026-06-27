"""
ChatManager — Orchestrates all platform adapters and maintains the unified message queue.
"""

import asyncio
import hashlib
import json
import logging
import time
from collections import deque
from typing import Dict, Optional

logger = logging.getLogger("streamchat.manager")

DEDUP_WINDOW = 60       # seconds to remember messages for deduplication
DEDUP_MAX_SIZE = 2000   # max fingerprints to store
RECENT_MAX = 200        # recent messages kept in memory for new connections


class ChatManager:
    def __init__(self):
        self.adapters: Dict[str, "BaseAdapter"] = {}
        self.adapter_tasks: Dict[str, asyncio.Task] = {}
        self.message_queue: asyncio.Queue = asyncio.Queue(maxsize=500)
        self.recent_messages: deque = deque(maxlen=RECENT_MAX)
        self._dedup_cache: deque = deque(maxlen=DEDUP_MAX_SIZE)
        self._dedup_timestamps: Dict[str, float] = {}
        self._running = False
        self._stats: Dict[str, int] = {}

    async def start(self):
        self._running = True
        logger.info("ChatManager started")

    async def stop(self):
        self._running = False
        for platform in list(self.adapters.keys()):
            await self.remove_adapter(platform)
        logger.info("ChatManager stopped")

    async def add_adapter(self, platform: str, adapter):
        """Register and start a platform adapter."""
        if platform in self.adapters:
            await self.remove_adapter(platform)

        self.adapters[platform] = adapter
        self._stats[platform] = 0
        task = asyncio.create_task(
            self._run_adapter(platform, adapter),
            name=f"adapter-{platform}"
        )
        self.adapter_tasks[platform] = task
        logger.info(f"✅ Adapter started: {platform}")

    async def remove_adapter(self, platform: str):
        """Stop and remove a platform adapter."""
        if platform in self.adapter_tasks:
            task = self.adapter_tasks.pop(platform)
            task.cancel()
            try:
                await task
            except (asyncio.CancelledError, Exception):
                pass

        if platform in self.adapters:
            adapter = self.adapters.pop(platform)
            try:
                await adapter.stop()
            except Exception:
                pass

        logger.info(f"🛑 Adapter stopped: {platform}")

    async def _run_adapter(self, platform: str, adapter):
        """Supervisor loop: run adapter, handle crashes, auto-reconnect."""
        retry_delay = 5
        max_delay = 120

        while True:
            try:
                logger.info(f"[{platform}] Connecting...")
                await adapter.start(self._on_message)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"[{platform}] Adapter crashed: {e}")
                logger.info(f"[{platform}] Reconnecting in {retry_delay}s...")
                await asyncio.sleep(retry_delay)
                retry_delay = min(retry_delay * 2, max_delay)
            else:
                # Adapter exited cleanly — try to reconnect anyway
                logger.warning(f"[{platform}] Adapter exited. Reconnecting in {retry_delay}s...")
                await asyncio.sleep(retry_delay)

    async def _on_message(self, message: dict):
        """Callback invoked by adapters when a new message arrives."""
        # Normalize
        message = self._normalize(message)
        if not message:
            return

        # Deduplication
        fingerprint = self._fingerprint(message)
        if self._is_duplicate(fingerprint):
            return

        # Enqueue
        try:
            self.message_queue.put_nowait(message)
            self.recent_messages.append(message)
            platform = message.get("platform", "unknown")
            self._stats[platform] = self._stats.get(platform, 0) + 1
        except asyncio.QueueFull:
            logger.warning("Message queue full — dropping oldest")
            try:
                self.message_queue.get_nowait()
                self.message_queue.put_nowait(message)
            except Exception:
                pass

    def _normalize(self, message: dict) -> Optional[dict]:
        """Ensure all required fields are present and typed correctly."""
        required = ["username", "message", "platform"]
        for field in required:
            if not message.get(field):
                return None

        return {
            "id": message.get("id", self._gen_id(message)),
            "username": str(message["username"])[:100],
            "message": str(message["message"])[:500],
            "platform": str(message["platform"]).lower(),
            "timestamp": message.get("timestamp", time.time()),
            "avatar": message.get("avatar", ""),
            "badges": message.get("badges", []),
            "color": message.get("color", ""),
            "is_superchat": message.get("is_superchat", False),
            "superchat_amount": message.get("superchat_amount", ""),
            "is_sub": message.get("is_sub", False),
            "is_mod": message.get("is_mod", False),
        }

    def _fingerprint(self, message: dict) -> str:
        raw = f"{message['platform']}:{message['username']}:{message['message']}"
        return hashlib.md5(raw.encode()).hexdigest()

    def _is_duplicate(self, fingerprint: str) -> bool:
        now = time.time()
        # Clean old entries
        while self._dedup_timestamps and len(self._dedup_timestamps) > DEDUP_MAX_SIZE:
            oldest_key = next(iter(self._dedup_timestamps))
            del self._dedup_timestamps[oldest_key]

        if fingerprint in self._dedup_timestamps:
            if now - self._dedup_timestamps[fingerprint] < DEDUP_WINDOW:
                return True

        self._dedup_timestamps[fingerprint] = now
        return False

    def _gen_id(self, message: dict) -> str:
        raw = f"{message.get('platform')}:{message.get('username')}:{message.get('message')}:{time.time()}"
        return hashlib.md5(raw.encode()).hexdigest()[:12]

    def get_status(self) -> dict:
        return {
            "adapters": {
                platform: {
                    "running": not self.adapter_tasks[platform].done() if platform in self.adapter_tasks else False,
                    "messages_received": self._stats.get(platform, 0)
                }
                for platform in self.adapters
            },
            "queue_size": self.message_queue.qsize(),
            "total_messages": sum(self._stats.values()),
            "recent_count": len(self.recent_messages),
        }
