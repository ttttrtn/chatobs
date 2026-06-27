"""
Kick Chat Adapter — connects to Kick's Pusher-based WebSocket.
Uses httpx (pure-Python) for REST calls; websockets for real-time chat.
"""

import asyncio
import json
import logging
import time
from typing import Optional

import httpx
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
        self._client: Optional[httpx.AsyncClient] = None
        self._last_message_id: Optional[int] = None

    async def _connect(self):
        logger.info(f"[Kick] Connecting to channel: {self.channel}")
        self._client = httpx.AsyncClient(
            headers={"User-Agent": "StreamChatOverlay/1.0"},
            timeout=10.0,
            follow_redirects=True,
        )
        await self._fetch_channel_info()

    async def _fetch_channel_info(self):
        try:
            resp = await self._client.get(f"{KICK_API_BASE}/channels/{self.channel}")
            if resp.status_code == 200:
                data = resp.json()
                self._chatroom_id = data.get("chatroom", {}).get("id")
                logger.info(f"[Kick] Chatroom ID: {self._chatroom_id}")
            else:
                logger.warning(f"[Kick] Channel API returned {resp.status_code}")
        except Exception as e:
            logger.error(f"[Kick] Failed to fetch channel info: {e}")

    async def _listen(self):
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
        logger.info("[Kick] Starting WebSocket listener...")
        async with websockets.connect(KICK_WS_URI, ping_interval=20) as ws:
            self._ws = ws
            channel_name = f"chatrooms.{self._chatroom_id}.v2"
            await ws.send(json.dumps({
                "event": "pusher:subscribe",
                "data": {"auth": "", "channel": channel_name}
            }))
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
        try:
            sender = data.get("sender", {})
            username = sender.get("username", "Unknown")
            content = data.get("content", "")
            msg_id = data.get("id", "")
            identity = sender.get("identity", {})
            badges = [b.get("type", "").lower() for b in identity.get("badges", []) if b.get("type")]
            color = identity.get("color", "")
            avatar = sender.get("profile_pic", "")
            await self._emit({
                "id": str(msg_id),
                "username": username,
                "message": content,
                "platform": "kick",
                "timestamp": time.time(),
                "avatar": avatar,
                "badges": badges,
                "color": color,
                "is_sub": "subscriber" in badges,
                "is_mod": "moderator" in badges or "broadcaster" in badges,
            })
        except Exception as e:
            logger.error(f"[Kick] Error processing message: {e}")

    async def _listen_polling(self):
        logger.info("[Kick] Using REST polling fallback...")
        if not self._chatroom_id:
            logger.error("[Kick] Cannot poll without chatroom ID")
            return

        while self._running:
            try:
                params = {}
                if self._last_message_id:
                    params["after"] = self._last_message_id
                resp = await self._client.get(
                    f"{KICK_API_BASE}/channels/{self.channel}/messages",
                    params=params,
                )
                if resp.status_code == 200:
                    data = resp.json()
                    for msg in reversed(data.get("data", {}).get("messages", [])):
                        self._last_message_id = msg.get("id")
                        await self._process_ws_message(msg)
                elif resp.status_code == 429:
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
        if self._client:
            await self._client.aclose()
        logger.info("[Kick] Adapter stopped")
