"""
Rumble Chat Adapter — scrapes the Rumble live chat iframe.
Rumble doesn't have an official chat API, so we parse the embedded chat HTML.
Uses aiohttp for async HTTP and BeautifulSoup for parsing.
"""

import asyncio
import hashlib
import logging
import re
import time
from typing import Optional, Set
from urllib.parse import urlparse

import aiohttp

from adapters import BaseAdapter

logger = logging.getLogger("streamchat.adapter.rumble")

try:
    from bs4 import BeautifulSoup
    BS4_AVAILABLE = True
except ImportError:
    BS4_AVAILABLE = False
    logger.warning("[Rumble] BeautifulSoup not installed. Install beautifulsoup4.")


class RumbleAdapter(BaseAdapter):
    """
    Scrapes Rumble live chat.
    Rumble embeds chat at: https://rumble.com/chat/popup/<video_id>
    We extract the video ID from the stream URL and poll the chat endpoint.
    """

    def __init__(self, stream_url: str, poll_interval: float = 4.0):
        super().__init__("rumble")
        self.stream_url = stream_url
        self.poll_interval = poll_interval
        self._session: Optional[aiohttp.ClientSession] = None
        self._chat_url: Optional[str] = None
        self._video_id: Optional[str] = None
        self._seen: Set[str] = set()
        self._seen_maxsize = 2000

    async def _connect(self):
        logger.info(f"[Rumble] Connecting to: {self.stream_url}")
        if not BS4_AVAILABLE:
            logger.error("[Rumble] BeautifulSoup4 required. pip install beautifulsoup4")
            return

        self._session = aiohttp.ClientSession(
            headers={
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Accept-Language": "en-US,en;q=0.9",
            }
        )

        await self._discover_chat_url()

    async def _discover_chat_url(self):
        """Scrape the Rumble stream page to find the embedded chat URL."""
        try:
            async with self._session.get(
                self.stream_url,
                timeout=aiohttp.ClientTimeout(total=15)
            ) as resp:
                if resp.status != 200:
                    logger.error(f"[Rumble] Stream page returned {resp.status}")
                    return

                html = await resp.text()
                soup = BeautifulSoup(html, "html.parser")

                # Look for video ID in various places
                video_id = self._extract_video_id(html, soup)

                if video_id:
                    self._video_id = video_id
                    self._chat_url = f"https://rumble.com/chat/popup/{video_id}"
                    logger.info(f"[Rumble] Chat URL: {self._chat_url}")
                else:
                    # Try using the chat iframe directly from og:video tags or data attrs
                    og_video = soup.find("meta", property="og:video:secure_url")
                    if og_video:
                        logger.info(f"[Rumble] Found og video: {og_video.get('content')}")

                    logger.warning("[Rumble] Could not find video ID — will try direct chat scraping")
                    # Try chat popup from the stream URL
                    self._chat_url = self.stream_url + "/chat"

        except Exception as e:
            logger.error(f"[Rumble] Discovery error: {e}")

    def _extract_video_id(self, html: str, soup) -> Optional[str]:
        """Try multiple methods to extract the Rumble video ID."""
        # Method 1: URL slug pattern from current page
        slug_match = re.search(r'rumble\.com/v([a-zA-Z0-9]+)-', self.stream_url)
        if slug_match:
            return slug_match.group(1)

        # Method 2: JSON-LD or structured data
        for script in soup.find_all("script", type="application/ld+json"):
            try:
                import json
                data = json.loads(script.string or "")
                if "@type" in data and "video" in data.get("@type", "").lower():
                    embed_url = data.get("embedUrl", "")
                    vid_match = re.search(r'/embed/([^/?]+)', embed_url)
                    if vid_match:
                        return vid_match.group(1)
            except Exception:
                pass

        # Method 3: Embedded player URL in page source
        embed_match = re.search(r'rumble\.com/embed/([a-zA-Z0-9]+)', html)
        if embed_match:
            return embed_match.group(1)

        # Method 4: data-video attribute
        video_el = soup.find(attrs={"data-video": True})
        if video_el:
            return video_el["data-video"]

        return None

    async def _listen(self):
        """Poll Rumble chat page and parse new messages."""
        if not self._session or not BS4_AVAILABLE:
            logger.error("[Rumble] Cannot listen — not connected")
            await asyncio.sleep(30)
            return

        if not self._chat_url:
            logger.error("[Rumble] No chat URL found")
            await asyncio.sleep(30)
            return

        logger.info(f"[Rumble] Polling chat at: {self._chat_url}")

        while self._running:
            try:
                async with self._session.get(
                    self._chat_url,
                    timeout=aiohttp.ClientTimeout(total=10)
                ) as resp:
                    if resp.status == 200:
                        html = await resp.text()
                        messages = self._parse_chat_html(html)
                        for msg in messages:
                            await self._emit(msg)
                    elif resp.status == 403:
                        logger.warning("[Rumble] 403 Forbidden — chat may be private or geo-blocked")
                        await asyncio.sleep(30)
                    elif resp.status == 404:
                        logger.warning("[Rumble] 404 — stream may have ended")
                        await asyncio.sleep(60)
                    else:
                        logger.warning(f"[Rumble] Chat poll returned {resp.status}")

            except asyncio.TimeoutError:
                logger.warning("[Rumble] Request timeout")
            except Exception as e:
                logger.error(f"[Rumble] Polling error: {e}")
                raise

            await asyncio.sleep(self.poll_interval)

    def _parse_chat_html(self, html: str) -> list:
        """Parse Rumble chat HTML to extract messages."""
        results = []
        try:
            soup = BeautifulSoup(html, "html.parser")

            # Rumble chat messages are in elements with class "chat-history--row"
            # or similar — selectors may change with site updates
            message_selectors = [
                ".chat-history--row",
                ".chat-message",
                "[data-message-id]",
                ".rumbles-vote-btn",  # fallback for different layouts
            ]

            messages_els = []
            for sel in message_selectors:
                found = soup.select(sel)
                if found:
                    messages_els = found
                    break

            for el in messages_els:
                try:
                    # Extract username
                    username_el = (
                        el.select_one(".chat-history--username") or
                        el.select_one(".username") or
                        el.select_one("[data-username]")
                    )
                    username = username_el.get_text(strip=True) if username_el else ""

                    # Extract message
                    message_el = (
                        el.select_one(".chat-history--message") or
                        el.select_one(".message") or
                        el.select_one("[data-message]")
                    )
                    message_text = message_el.get_text(strip=True) if message_el else ""

                    if not username or not message_text:
                        continue

                    # Deduplication fingerprint
                    fp = hashlib.md5(f"{username}:{message_text}".encode()).hexdigest()
                    if fp in self._seen:
                        continue

                    self._seen.add(fp)
                    if len(self._seen) > self._seen_maxsize:
                        self._seen = set(list(self._seen)[-self._seen_maxsize // 2:])

                    # Badges
                    badges = []
                    badge_els = el.select(".badge, .user-badge, [data-badge]")
                    for badge_el in badge_els:
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
                    logger.debug(f"[Rumble] Error parsing message element: {e}")

        except Exception as e:
            logger.error(f"[Rumble] HTML parse error: {e}")

        return results

    async def stop(self):
        self._running = False
        if self._session:
            await self._session.close()
        logger.info("[Rumble] Adapter stopped")
