"""
Facebook Live Chat Adapter — uses Facebook Graph API to poll live comments.
Uses httpx (pure-Python, no C extensions) for async HTTP.
"""

import asyncio
import logging
import re
import time
from datetime import datetime, timezone
from typing import Optional, Set

import httpx

from adapters import BaseAdapter

logger = logging.getLogger("streamchat.adapter.facebook")

FB_GRAPH_BASE = "https://graph.facebook.com/v19.0"


class FacebookAdapter(BaseAdapter):
    def __init__(self, access_token: str, video_id: str, poll_interval: float = 5.0):
        super().__init__("facebook")
        self.access_token = access_token
        self.video_id = video_id
        self.poll_interval = poll_interval
        self._client: Optional[httpx.AsyncClient] = None
        self._after_cursor: Optional[str] = None
        self._seen: Set[str] = set()
        self._seen_maxsize = 2000
        self._rate_limit_until: float = 0

    async def _connect(self):
        logger.info(f"[Facebook] Connecting to video: {self.video_id}")
        self._client = httpx.AsyncClient(timeout=10.0)
        try:
            resp = await self._client.get(
                f"{FB_GRAPH_BASE}/me",
                params={"access_token": self.access_token, "fields": "id,name"},
            )
            data = resp.json()
            if "error" in data:
                logger.error(f"[Facebook] Token error: {data['error'].get('message')}")
            else:
                logger.info(f"[Facebook] Authenticated as: {data.get('name', data.get('id'))}")
        except Exception as e:
            logger.error(f"[Facebook] Auth check failed: {e}")

    async def _listen(self):
        if not self._client:
            return
        logger.info("[Facebook] Starting comment polling...")
        url = f"{FB_GRAPH_BASE}/{self.video_id}/comments"

        while self._running:
            if time.time() < self._rate_limit_until:
                await asyncio.sleep(self._rate_limit_until - time.time())
                continue
            try:
                params = {
                    "access_token": self.access_token,
                    "fields": "id,from{id,name,picture},message,created_time",
                    "limit": 25,
                    "filter": "stream",
                    "order": "chronological",
                }
                if self._after_cursor:
                    params["after"] = self._after_cursor

                resp = await self._client.get(url, params=params)

                if resp.status_code == 200:
                    data = resp.json()
                    paging = data.get("paging", {})
                    cursors = paging.get("cursors", {})
                    if cursors.get("after"):
                        self._after_cursor = cursors["after"]
                    elif paging.get("next"):
                        m = re.search(r'after=([^&]+)', paging["next"])
                        if m:
                            self._after_cursor = m.group(1)
                    for comment in data.get("data", []):
                        await self._process_comment(comment)
                elif resp.status_code == 429:
                    logger.warning("[Facebook] Rate limited (429)")
                    self._rate_limit_until = time.time() + 60
                elif resp.status_code == 400:
                    err_msg = resp.json().get("error", {}).get("message", "Unknown")
                    logger.error(f"[Facebook] API error 400: {err_msg}")
                    if "ended" in err_msg.lower():
                        await asyncio.sleep(60)
                else:
                    logger.warning(f"[Facebook] Poll returned {resp.status_code}")
            except httpx.TimeoutException:
                logger.warning("[Facebook] Request timeout")
            except Exception as e:
                logger.error(f"[Facebook] Polling error: {e}")
                raise
            await asyncio.sleep(self.poll_interval)

    async def _process_comment(self, comment: dict):
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

        avatar = ""
        picture_data = from_data.get("picture", {})
        if isinstance(picture_data, dict):
            avatar = picture_data.get("data", {}).get("url", "")

        try:
            dt = datetime.fromisoformat(comment.get("created_time", "").replace("Z", "+00:00"))
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
        if self._client:
            await self._client.aclose()
        logger.info("[Facebook] Adapter stopped")
