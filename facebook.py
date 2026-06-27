"""
Facebook Live Chat Adapter — uses Facebook Graph API to poll live comments.
Requires a page access token and video ID.
Falls back to longer polling intervals if rate limited.
"""

import asyncio
import logging
import time
from typing import Optional, Set

import aiohttp

from adapters import BaseAdapter

logger = logging.getLogger("streamchat.adapter.facebook")

FB_GRAPH_BASE = "https://graph.facebook.com/v19.0"


class FacebookAdapter(BaseAdapter):
    """
    Polls Facebook Live video comments via Graph API.
    Access token must have: pages_read_engagement, pages_manage_posts scope.
    """

    def __init__(
        self,
        access_token: str,
        video_id: str,
        poll_interval: float = 5.0,
    ):
        super().__init__("facebook")
        self.access_token = access_token
        self.video_id = video_id
        self.poll_interval = poll_interval
        self._session: Optional[aiohttp.ClientSession] = None
        self._after_cursor: Optional[str] = None
        self._seen: Set[str] = set()
        self._seen_maxsize = 2000
        self._rate_limit_until: float = 0

    async def _connect(self):
        logger.info(f"[Facebook] Connecting to video: {self.video_id}")
        self._session = aiohttp.ClientSession()

        # Validate token
        try:
            url = f"{FB_GRAPH_BASE}/me"
            params = {"access_token": self.access_token, "fields": "id,name"}
            async with self._session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                data = await resp.json()
                if "error" in data:
                    logger.error(f"[Facebook] Token error: {data['error'].get('message')}")
                else:
                    logger.info(f"[Facebook] Authenticated as: {data.get('name', data.get('id'))}")
        except Exception as e:
            logger.error(f"[Facebook] Auth check failed: {e}")

    async def _listen(self):
        """Poll Facebook Graph API for live video comments."""
        if not self._session:
            return

        logger.info("[Facebook] Starting comment polling...")

        # Use live_comments edge for real-time comments
        url = f"{FB_GRAPH_BASE}/{self.video_id}/comments"

        while self._running:
            # Respect rate limits
            if time.time() < self._rate_limit_until:
                sleep_for = self._rate_limit_until - time.time()
                logger.info(f"[Facebook] Rate limited. Sleeping {sleep_for:.0f}s")
                await asyncio.sleep(sleep_for)
                continue

            try:
                params = {
                    "access_token": self.access_token,
                    "fields": "id,from{id,name,picture},message,created_time,message_tags",
                    "limit": 25,
                    "filter": "stream",
                    "order": "chronological",
                }
                if self._after_cursor:
                    params["after"] = self._after_cursor

                async with self._session.get(
                    url,
                    params=params,
                    timeout=aiohttp.ClientTimeout(total=10)
                ) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        comments = data.get("data", [])
                        paging = data.get("paging", {})

                        # Update cursor for next request
                        cursors = paging.get("cursors", {})
                        if cursors.get("after"):
                            self._after_cursor = cursors["after"]
                        elif paging.get("next"):
                            # Extract cursor from next URL
                            import re
                            cursor_match = re.search(r'after=([^&]+)', paging["next"])
                            if cursor_match:
                                self._after_cursor = cursor_match.group(1)

                        for comment in comments:
                            await self._process_comment(comment)

                    elif resp.status == 429:
                        logger.warning("[Facebook] Rate limited (429)")
                        self._rate_limit_until = time.time() + 60
                    elif resp.status == 400:
                        error_data = await resp.json()
                        err_msg = error_data.get("error", {}).get("message", "Unknown")
                        logger.error(f"[Facebook] API error 400: {err_msg}")
                        if "Live" in err_msg or "ended" in err_msg.lower():
                            logger.warning("[Facebook] Stream may have ended")
                            await asyncio.sleep(60)
                    else:
                        logger.warning(f"[Facebook] Poll returned {resp.status}")

            except asyncio.TimeoutError:
                logger.warning("[Facebook] Request timeout")
            except Exception as e:
                logger.error(f"[Facebook] Polling error: {e}")
                raise

            await asyncio.sleep(self.poll_interval)

    async def _process_comment(self, comment: dict):
        """Process a single Facebook comment."""
        comment_id = comment.get("id", "")
        if comment_id in self._seen:
            return
        self._seen.add(comment_id)
        if len(self._seen) > self._seen_maxsize:
            self._seen = set(list(self._seen)[-self._seen_maxsize // 2:])

        from_data = comment.get("from", {})
        username = from_data.get("name", "Unknown")

        message_text = comment.get("message", "")
        if not message_text:
            return

        # Get avatar
        avatar = ""
        picture_data = from_data.get("picture", {})
        if isinstance(picture_data, dict):
            avatar = picture_data.get("data", {}).get("url", "")

        # Parse created time
        created_str = comment.get("created_time", "")
        try:
            from datetime import datetime, timezone
            dt = datetime.fromisoformat(created_str.replace("Z", "+00:00"))
            timestamp = dt.timestamp()
        except Exception:
            timestamp = time.time()

        await self._emit({
            "id": comment_id,
            "username": username,
            "message": message_text,
            "platform": "facebook",
            "timestamp": timestamp,
            "avatar": avatar,
            "badges": [],
            "is_sub": False,
            "is_mod": False,
        })

    async def stop(self):
        self._running = False
        if self._session:
            await self._session.close()
        logger.info("[Facebook] Adapter stopped")
