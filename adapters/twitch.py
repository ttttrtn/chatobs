"""
Twitch Chat Adapter — connects via IRC over WebSocket (TLS).
Supports anonymous read (justinfan) or authenticated bot account.
Parses badges: subscriber, moderator, vip, broadcaster.
"""

import asyncio
import logging
import re
import time
from typing import Optional

import websockets
from websockets.exceptions import ConnectionClosed, WebSocketException

from adapters import BaseAdapter

logger = logging.getLogger("streamchat.adapter.twitch")

TWITCH_IRC_URI = "wss://irc-ws.chat.twitch.tv:443"

# Parse IRC badge tag into list of badge names
BADGE_RE = re.compile(r"(\w+)/(\d+)")

# Parse @key=value;... tag prefix
TAG_RE = re.compile(r"@([^ ]+)")

# Full IRC message
IRC_RE = re.compile(
    r"(?:@(?P<tags>[^ ]+) )?(?::(?P<prefix>[^ ]+) )?(?P<command>[A-Z]+)(?: (?P<params>.+))?"
)


def parse_tags(tag_str: str) -> dict:
    tags = {}
    for part in tag_str.split(";"):
        if "=" in part:
            k, _, v = part.partition("=")
            tags[k] = v
    return tags


def parse_badges(badge_str: str) -> list:
    badges = []
    for match in BADGE_RE.finditer(badge_str):
        badges.append(match.group(1))
    return badges


class TwitchAdapter(BaseAdapter):

    def __init__(self, channel: str, token: Optional[str] = None, nick: str = "justinfan12345"):
        super().__init__("twitch")
        self.channel = channel.lower().lstrip("#")
        # Anonymous read: use justinfan + no token
        self.token = token or ""
        self.nick = nick
        self._ws = None

    async def _connect(self):
        logger.info(f"[Twitch] Connecting to #{self.channel}...")
        self._ws = await websockets.connect(
            TWITCH_IRC_URI,
            ping_interval=30,
            ping_timeout=10,
        )
        # Authenticate
        await self._ws.send("CAP REQ :twitch.tv/tags twitch.tv/commands")
        if self.token:
            await self._ws.send(f"PASS oauth:{self.token}")
        else:
            await self._ws.send("PASS SCHMOOPIIE")  # anon pass
        await self._ws.send(f"NICK {self.nick}")
        await self._ws.send(f"JOIN #{self.channel}")
        logger.info(f"[Twitch] Joined #{self.channel}")

    async def _listen(self):
        if not self._ws:
            return

        try:
            async for raw in self._ws:
                if not self._running:
                    break
                await self._handle_raw(raw)
        except (ConnectionClosed, WebSocketException) as e:
            logger.warning(f"[Twitch] Connection lost: {e}")
            raise
        except Exception as e:
            logger.error(f"[Twitch] Error: {e}")
            raise

    async def _handle_raw(self, raw: str):
        """Parse IRC message and emit chat messages."""
        for line in raw.strip().split("\r\n"):
            line = line.strip()
            if not line:
                continue

            # Respond to PING
            if line.startswith("PING"):
                await self._ws.send("PONG :tmi.twitch.tv")
                continue

            m = IRC_RE.match(line)
            if not m:
                continue

            command = m.group("command")
            params = m.group("params") or ""
            tag_str = m.group("tags") or ""
            prefix = m.group("prefix") or ""

            tags = parse_tags(tag_str) if tag_str else {}

            if command == "PRIVMSG":
                # Extract channel and message
                parts = params.split(":", 1)
                if len(parts) < 2:
                    return
                message_text = parts[1]

                # Extract username from prefix (nick!user@host)
                username = tags.get("display-name") or prefix.split("!")[0]

                badges_raw = tags.get("badges", "")
                badges = parse_badges(badges_raw)

                # Color from tags
                color = tags.get("color", "")

                is_sub = "subscriber" in badges or tags.get("subscriber") == "1"
                is_mod = "moderator" in badges or tags.get("mod") == "1"

                await self._emit({
                    "id": tags.get("id", ""),
                    "username": username,
                    "message": message_text,
                    "platform": "twitch",
                    "timestamp": int(tags.get("tmi-sent-ts", time.time() * 1000)) / 1000,
                    "avatar": "",
                    "badges": badges,
                    "color": color,
                    "is_sub": is_sub,
                    "is_mod": is_mod,
                })

            elif command == "USERNOTICE":
                # Subscriptions, resubs, etc.
                msg_id = tags.get("msg-id", "")
                username = tags.get("display-name", tags.get("login", ""))
                system_msg = tags.get("system-msg", "").replace("\\s", " ")
                if username:
                    await self._emit({
                        "id": tags.get("id", ""),
                        "username": username,
                        "message": f"[{msg_id.upper()}] {system_msg}",
                        "platform": "twitch",
                        "timestamp": time.time(),
                        "avatar": "",
                        "badges": ["subscriber"],
                        "is_sub": True,
                        "is_mod": False,
                    })

    async def stop(self):
        self._running = False
        if self._ws:
            await self._ws.close()
        logger.info("[Twitch] Adapter stopped")
