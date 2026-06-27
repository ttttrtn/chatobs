"""
Rumble Chat Adapter — scrapes the Rumble live chat iframe.
Uses httpx (pure-Python, no C extensions) for async HTTP.
"""

import asyncio
import hashlib
import logging
import re
import time
from typing import Optional, Set
from urllib.parse import urlparse

import httpx

from adapters import BaseAdapter

logger = logging.getLogger("streamchat.adapter.rumble")

try:
    from bs4 import BeautifulSoup
    BS4_AVAILABLE = True
except ImportError:
    BS4_AVAILABLE = False
    logger.warning("[Rumble] BeautifulSoup not installed. Install beautifulsoup4.")

_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}


class RumbleAdapter(BaseAdapter):
    def __init__(self, stream_url: str, poll_interval: float = 4.0):
        super().__init__("rumble")
        self.stream_url = stream_url
        self.poll_interval = poll_interval
        self._client: Optional[httpx.AsyncClient] = None
        self._chat_url: Optional[str] = None
        self._seen: Set[str] = set()
        self._seen_maxsize = 2000

    async def _connect(self):
        logger.info(f"[Rumble] Connecting to: {self.stream_url}")
        if not BS4_AVAILABLE:
            logger.error("[Rumble] BeautifulSoup4 required. pip install beautifulsoup4")
            return
        self._client = httpx.AsyncClient(headers=_HEADERS, timeout=15.0, follow_redirects=True)
        await self._discover_chat_url()

    async def _discover_chat_url(self):
        try:
            resp = await self._client.get(self.stream_url)
            if resp.status_code != 200:
                logger.error(f"[Rumble] Stream page returned {resp.status_code}")
                return
            html = resp.text
            soup = BeautifulSoup(html, "html.parser")
            video_id = self._extract_video_id(html, soup)
            if video_id:
                self._chat_url = f"https://rumble.com/chat/popup/{video_id}"
                logger.info(f"[Rumble] Chat URL: {self._chat_url}")
            else:
                logger.warning("[Rumble] Could not find video ID")
                self._chat_url = self.stream_url + "/chat"
        except Exception as e:
            logger.error(f"[Rumble] Discovery error: {e}")

    def _extract_video_id(self, html: str, soup) -> Optional[str]:
        slug_match = re.search(r'rumble\.com/v([a-zA-Z0-9]+)-', self.stream_url)
        if slug_match:
            return slug_match.group(1)
        for script in soup.find_all("script", type="application/ld+json"):
            try:
                import json
                data = json.loads(script.string or "")
                if "video" in data.get("@type", "").lower():
                    m = re.search(r'/embed/([^/?]+)', data.get("embedUrl", ""))
                    if m:
                        return m.group(1)
            except Exception:
                pass
        embed_match = re.search(r'rumble\.com/embed/([a-zA-Z0-9]+)', html)
        if embed_match:
            return embed_match.group(1)
        video_el = soup.find(attrs={"data-video": True})
        if video_el:
            return video_el["data-video"]
        return None

    async def _listen(self):
        if not self._client or not BS4_AVAILABLE or not self._chat_url:
            logger.error("[Rumble] Cannot listen — not connected or no chat URL")
            await asyncio.sleep(30)
            return

        logger.info(f"[Rumble] Polling chat at: {self._chat_url}")
        while self._running:
            try:
                resp = await self._client.get(self._chat_url)
                if resp.status_code == 200:
                    for msg in self._parse_chat_html(resp.text):
                        await self._emit(msg)
                elif resp.status_code == 403:
                    logger.warning("[Rumble] 403 — chat may be private")
                    await asyncio.sleep(30)
                elif resp.status_code == 404:
                    logger.warning("[Rumble] 404 — stream may have ended")
                    await asyncio.sleep(60)
                else:
                    logger.warning(f"[Rumble] Poll returned {resp.status_code}")
            except httpx.TimeoutException:
                logger.warning("[Rumble] Request timeout")
            except Exception as e:
                logger.error(f"[Rumble] Polling error: {e}")
                raise
            await asyncio.sleep(self.poll_interval)

    def _parse_chat_html(self, html: str) -> list:
        results = []
        try:
            soup = BeautifulSoup(html, "html.parser")
            message_selectors = [
                ".chat-history--row", ".chat-message",
                "[data-message-id]", ".rumbles-vote-btn",
            ]
            messages_els = []
            for sel in message_selectors:
                found = soup.select(sel)
                if found:
                    messages_els = found
                    break

            for el in messages_els:
                try:
                    username_el = (
                        el.select_one(".chat-history--username") or
                        el.select_one(".username") or
                        el.select_one("[data-username]")
                    )
                    username = username_el.get_text(strip=True) if username_el else ""
                    message_el = (
                        el.select_one(".chat-history--message") or
                        el.select_one(".message") or
                        el.select_one("[data-message]")
                    )
                    message_text = message_el.get_text(strip=True) if message_el else ""
                    if not username or not message_text:
                        continue
                    fp = hashlib.md5(f"{username}:{message_text}".encode()).hexdigest()
                    if fp in self._seen:
                        continue
                    self._seen.add(fp)
                    if len(self._seen) > self._seen_maxsize:
                        self._seen = set(list(self._seen)[-self._seen_maxsize // 2:])
                    badges = []
                    for badge_el in el.select(".badge, .user-badge, [data-badge]"):
                        badge_text = badge_el.get("title", badge_el.get_text(strip=True))
                        if badge_text:
                            badges.append(badge_text.lower())
                    results.append({
                        "username": username,
                        "message": message_text,
                        "platform": "rumble",
                        "timestamp": time.time(),
                        "badges": badges,
                        "is_mod": any("mod" in b for b in badges),
                    })
                except Exception as e:
                    logger.debug(f"[Rumble] Error parsing element: {e}")
        except Exception as e:
            logger.error(f"[Rumble] HTML parse error: {e}")
        return results

    async def stop(self):
        self._running = False
        if self._client:
            await self._client.aclose()
        logger.info("[Rumble] Adapter stopped")
