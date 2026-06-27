"""
Kick Chat Adapter — connects to Kick's Pusher-based WebSocket.
Kick uses Pusher protocol on soketi infrastructure.
Falls back to polling the Kick chatroom API if WebSocket fails.
"""

import asyncio
import json
import logging
import time
from typing import Optional

import aiohttp
import websockets
from websockets.exceptions import ConnectionClosed

from adapters import BaseAdapter

logger = logging.getLogger("streamchat.adapter.kick")

KICK_API_BASE = "https://kick.com/api/v2"
KICK_WS_URI = "wss://ws-us2.pusher.com/app/eb1d5f283081a78b932c?protocol=7&client=js&version=7.6.0&flash=false"


class KickAdapter(BaseAdapter):

    def __init__(self, channel: str, poll_interval: float = 3.0):
        super().__init__("kick")
        self.channel = channel.lower()
        self.poll_interval = poll_interval
        self._chatroom_id: Optional[int] = None
        self._ws = None
        self._session: Optional[aiohttp.ClientSession] = None
        self._last_message_id: Optional[int] = None

    async def _connect(self):
        logger.info(f"[Kick] Connecting to channel: {self.channel}")
        self._session = aiohttp.ClientSession(
            headers={"User-Agent": "StreamChatOverlay/1.0"}
        )
        await self._fetch_channel_info()

    async def _fetch_channel_info(self):
        """Fetch chatroom ID from Kick channel API."""
        try:
            url = f"{KICK_API_BASE}/channels/{self.channel}"
            async with self._session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    chatroom = data.get("chatroom", {})
                    self._chatroom_id = chatroom.get("id")
                    logger.info(f"[Kick] Chatroom ID: {self._chatroom_id}")
                else:
                    logger.warning(f"[Kick] Channel API returned {resp.status}")
        except Exception as e:
            logger.error(f"[Kick] Failed to fetch channel info: {e}")

    async def _listen(self):
        """Try WebSocket first, fall back to polling."""
        if self._chatroom_id:
            try:
                await self._listen_websocket()
            except Exception as e:
                logger.warning(f"[Kick] WebSocket failed ({e}), falling back to polling")
                await self._listen_polling()
        else:
            logger.warning("[Kick] No chatroom ID — polling mode")
            await self._listen_polling()

    async def _listen_websocket(self):
        """Listen via Pusher WebSocket (Kick's real-time system)."""
        logger.info("[Kick] Starting WebSocket listener...")
        async with websockets.connect(KICK_WS_URI, ping_interval=20) as ws:
            self._ws = ws

            # Subscribe to chatroom channel
            channel_name = f"chatrooms.{self._chatroom_id}.v2"
            subscribe_msg = json.dumps({
                "event": "pusher:subscribe",
                "data": {"auth": "", "channel": channel_name}
            })
            await ws.send(subscribe_msg)
            logger.info(f"[Kick] Subscribed to {channel_name}")

            async for raw in ws:
                if not self._running:
                    break
                try:
                    envelope = json.loads(raw)
                    event = envelope.get("event", "")

                    if event == "pusher:ping":
                        await ws.send(json.dumps({"event": "pusher:pong", "data": {}}))

                    elif event == "App\\Events\\ChatMessageEvent":
                        data_raw = envelope.get("data", "{}")
                        data = json.loads(data_raw) if isinstance(data_raw, str) else data_raw
                        await self._process_ws_message(data)

                    elif event == "App\\Events\\SubscriptionEvent":
                        data_raw = envelope.get("data", "{}")
                        data = json.loads(data_raw) if isinstance(data_raw, str) else data_raw
                        username = data.get("username", "")
                        if username:
                            await self._emit({
                                "username": username,
                                "message": "🎉 Just subscribed!",
                                "platform": "kick",
                                "timestamp": time.time(),
                                "badges": ["subscriber"],
                                "is_sub": True,
                            })
                except json.JSONDecodeError:
                    pass
                except Exception as e:
                    logger.error(f"[Kick] WS message error: {e}")

    async def _process_ws_message(self, data: dict):
        """Process a ChatMessageEvent from Kick WebSocket."""
        try:
            sender = data.get("sender", {})
            username = sender.get("username", "Unknown")
            content = data.get("content", "")
            msg_id = data.get("id", "")

            # Parse badges
            badges = []
            identity = sender.get("identity", {})
            for badge in identity.get("badges", []):
                badge_type = badge.get("type", "")
                if badge_type:
                    badges.append(badge_type.lower())

            # Parse color
            color = identity.get("color", "")

            # Avatar
            avatar = ""
            if sender.get("profile_pic"):
                avatar = sender["profile_pic"]

            is_sub = "subscriber" in badges
            is_mod = "moderator" in badges or "broadcaster" in badges

            await self._emit({
                "id": str(msg_id),
                "username": username,
                "message": content,
                "platform": "kick",
                "timestamp": time.time(),
                "avatar": avatar,
                "badges": badges,
                "color": color,
                "is_sub": is_sub,
                "is_mod": is_mod,
            })
        except Exception as e:
            logger.error(f"[Kick] Error processing message: {e}")

    async def _listen_polling(self):
        """Fallback: poll Kick chatroom API for new messages."""
        logger.info("[Kick] Using REST polling fallback...")
        if not self._chatroom_id:
            logger.error("[Kick] Cannot poll without chatroom ID")
            return

        while self._running:
            try:
                url = f"{KICK_API_BASE}/channels/{self.channel}/messages"
                params = {}
                if self._last_message_id:
                    params["after"] = self._last_message_id

                async with self._session.get(
                    url,
                    params=params,
                    timeout=aiohttp.ClientTimeout(total=10)
                ) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        messages = data.get("data", {}).get("messages", [])
                        for msg in reversed(messages):
                            self._last_message_id = msg.get("id")
                            await self._process_ws_message(msg)
                    elif resp.status == 429:
                        logger.warning("[Kick] Rate limited — sleeping 10s")
                        await asyncio.sleep(10)
            except Exception as e:
                logger.error(f"[Kick] Polling error: {e}")
                await asyncio.sleep(5)

            await asyncio.sleep(self.poll_interval)

    async def stop(self):
        self._running = False
        if self._ws:
            await self._ws.close()
        if self._session:
            await self._session.close()
        logger.info("[Kick] Adapter stopped")
