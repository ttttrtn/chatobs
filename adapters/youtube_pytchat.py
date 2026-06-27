"""
YouTube Live Chat Adapter — uses pytchat library.
Runs in an executor thread to avoid blocking the asyncio event loop.
Supports live polling, author badges, avatars, superchats.
"""

import asyncio
import logging
import time
from concurrent.futures import ThreadPoolExecutor
from typing import Optional

from adapters import BaseAdapter

logger = logging.getLogger("streamchat.adapter.youtube")

try:
    import pytchat
    PYTCHAT_AVAILABLE = True
except ImportError:
    PYTCHAT_AVAILABLE = False
    logger.warning("pytchat not installed. YouTube adapter will use fallback mode.")


class YouTubeAdapter(BaseAdapter):
    """
    Polls YouTube Live Chat using pytchat.
    pytchat is synchronous, so we run it in a thread pool.
    """

    def __init__(self, video_id: str, poll_interval: float = 1.0):
        super().__init__("youtube")
        self.video_id = video_id
        self.poll_interval = poll_interval
        self._chat = None
        self._executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="yt-pytchat")
        self._seen_ids: set = set()
        self._seen_ids_maxsize = 5000

    async def _connect(self):
        logger.info(f"[YouTube] Connecting to video: {self.video_id}")
        if not PYTCHAT_AVAILABLE:
            logger.error("[YouTube] pytchat not available. Cannot connect.")
            return
        if not self.video_id:
            logger.error("[YouTube] No video_id provided.")
            return
        # Initialize pytchat in executor (it does HTTP calls)
        loop = asyncio.get_event_loop()
        self._chat = await loop.run_in_executor(
            self._executor,
            self._create_chat
        )

    def _create_chat(self):
        """Run in thread: create pytchat instance."""
        try:
            chat = pytchat.create(video_id=self.video_id)
            logger.info(f"[YouTube] pytchat connected to {self.video_id}")
            return chat
        except Exception as e:
            logger.error(f"[YouTube] Failed to create pytchat: {e}")
            return None

    async def _listen(self):
        """Poll pytchat in thread, emit messages to event loop."""
        if not PYTCHAT_AVAILABLE or not self._chat:
            logger.warning("[YouTube] Not connected — waiting...")
            await asyncio.sleep(30)
            return

        loop = asyncio.get_event_loop()

        while self._running:
            try:
                messages = await loop.run_in_executor(
                    self._executor,
                    self._fetch_messages
                )
                for msg in messages:
                    await self._emit(msg)
            except Exception as e:
                logger.error(f"[YouTube] Listen error: {e}")
                raise  # Let supervisor handle reconnect

            # Check if stream ended
            if self._chat and not self._chat.is_alive():
                logger.info("[YouTube] Stream ended or pytchat died. Reconnecting...")
                raise RuntimeError("pytchat stream ended")

            await asyncio.sleep(self.poll_interval)

    def _fetch_messages(self) -> list:
        """Synchronous: fetch available messages from pytchat."""
        if not self._chat or not self._chat.is_alive():
            return []

        results = []
        try:
            for item in self._chat.get().sync_items():
                # Deduplicate by message ID
                if item.id in self._seen_ids:
                    continue

                self._seen_ids.add(item.id)
                if len(self._seen_ids) > self._seen_ids_maxsize:
                    # Prune oldest (sets aren't ordered, so we clear a chunk)
                    self._seen_ids = set(list(self._seen_ids)[-self._seen_ids_maxsize // 2:])

                # Determine badges
                badges = []
                if hasattr(item, 'author') and item.author:
                    author = item.author
                    if hasattr(author, 'isChatOwner') and author.isChatOwner:
                        badges.append("owner")
                    if hasattr(author, 'isChatModerator') and author.isChatModerator:
                        badges.append("moderator")
                    if hasattr(author, 'isChatSponsor') and author.isChatSponsor:
                        badges.append("member")

                # Check for superchat
                is_superchat = hasattr(item, 'amountString') and bool(item.amountString)
                amount_str = ""
                if is_superchat:
                    amount_str = getattr(item, 'amountString', "")

                results.append({
                    "id": item.id,
                    "username": item.author.name if hasattr(item, 'author') else "Unknown",
                    "message": item.message,
                    "platform": "youtube",
                    "timestamp": item.timestamp.timestamp() if hasattr(item, 'timestamp') else time.time(),
                    "avatar": item.author.imageUrl if (hasattr(item, 'author') and hasattr(item.author, 'imageUrl')) else "",
                    "badges": badges,
                    "is_superchat": is_superchat,
                    "superchat_amount": amount_str,
                    "is_mod": "moderator" in badges,
                })
        except Exception as e:
            logger.error(f"[YouTube] fetch error: {e}")

        return results

    async def stop(self):
        self._running = False
        if self._chat:
            try:
                self._chat.terminate()
            except Exception:
                pass
        self._executor.shutdown(wait=False)
        logger.info("[YouTube] Adapter stopped")
