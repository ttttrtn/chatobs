"""
StreamChat Overlay - Multi-platform live chat aggregator
Production-ready FastAPI + WebSocket server
"""

import asyncio
import json
import logging
import os
import time
import uuid
from collections import deque
from contextlib import asynccontextmanager
from typing import Optional

import uvicorn
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from dotenv import load_dotenv

from chat_manager import ChatManager
from adapters.youtube_pytchat import YouTubeAdapter
from adapters.twitch import TwitchAdapter
from adapters.kick import KickAdapter
from adapters.rumble import RumbleAdapter
from adapters.facebook import FacebookAdapter
from adapters.instagram import InstagramAdapter

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
logger = logging.getLogger("streamchat")

# Global chat manager instance
chat_manager: Optional[ChatManager] = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Startup and shutdown lifecycle manager."""
    global chat_manager
    logger.info("🚀 StreamChat Overlay starting up...")

    chat_manager = ChatManager()
    await chat_manager.start()

    # Auto-start adapters from env config
    await auto_start_adapters(chat_manager)

    yield

    logger.info("🛑 StreamChat Overlay shutting down...")
    await chat_manager.stop()


async def auto_start_adapters(manager: ChatManager):
    """Auto-start platform adapters based on environment variables."""
    # YouTube
    yt_video_id = os.getenv("YOUTUBE_VIDEO_ID")
    if yt_video_id:
        adapter = YouTubeAdapter(video_id=yt_video_id)
        await manager.add_adapter("youtube", adapter)

    # Twitch
    twitch_channel = os.getenv("TWITCH_CHANNEL")
    twitch_token = os.getenv("TWITCH_OAUTH_TOKEN")
    twitch_nick = os.getenv("TWITCH_BOT_NICK", "justinfan12345")
    if twitch_channel:
        adapter = TwitchAdapter(
            channel=twitch_channel,
            token=twitch_token,
            nick=twitch_nick
        )
        await manager.add_adapter("twitch", adapter)

    # Kick
    kick_channel = os.getenv("KICK_CHANNEL")
    if kick_channel:
        adapter = KickAdapter(channel=kick_channel)
        await manager.add_adapter("kick", adapter)

    # Rumble
    rumble_url = os.getenv("RUMBLE_STREAM_URL")
    if rumble_url:
        adapter = RumbleAdapter(stream_url=rumble_url)
        await manager.add_adapter("rumble", adapter)

    # Facebook
    fb_token = os.getenv("FACEBOOK_ACCESS_TOKEN")
    fb_video_id = os.getenv("FACEBOOK_VIDEO_ID")
    if fb_token and fb_video_id:
        adapter = FacebookAdapter(access_token=fb_token, video_id=fb_video_id)
        await manager.add_adapter("facebook", adapter)

    # ── Instagram Live ────────────────────────────────────────────────────────
    # Enable real scraping mode when IG_LIVE_URL + credentials are provided.
    # Falls back to mock mode when INSTAGRAM_MOCK_MODE=true or credentials
    # are missing.  Set INSTAGRAM_ENABLED=true to activate either mode.
    ig_enabled = os.getenv("INSTAGRAM_ENABLED", "false").lower() == "true"
    if ig_enabled:
        ig_live_url = os.getenv("IG_LIVE_URL", "")
        ig_username = os.getenv("IG_USERNAME", "")
        ig_password = os.getenv("IG_PASSWORD", "")
        ig_session_cookie = os.getenv("IG_SESSION_COOKIE", "")
        ig_mock_mode = os.getenv("INSTAGRAM_MOCK_MODE", "false").lower() == "true"

        adapter = InstagramAdapter(
            live_url=ig_live_url,
            username=ig_username,
            password=ig_password,
            session_cookie=ig_session_cookie,
            mock_mode=ig_mock_mode if ig_mock_mode else None,  # None = auto-detect
        )
        await manager.add_adapter("instagram", adapter)
        logger.info(
            f"[Instagram] Adapter registered — "
            f"mode={'mock' if ig_mock_mode or not (ig_live_url and (ig_session_cookie or (ig_username and ig_password))) else 'real'}"
        )


app = FastAPI(
    title="StreamChat Overlay",
    description="Multi-platform live chat aggregator for OBS overlays",
    version="1.0.0",
    lifespan=lifespan
)

app.mount("/static", StaticFiles(directory="static"), name="static")
templates = Jinja2Templates(directory="templates")


# ─── WebSocket Connection Manager ──────────────────────────────────────────────

class ConnectionManager:
    def __init__(self):
        self.active_connections: list[WebSocket] = []

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)
        logger.info(f"WS client connected. Total: {len(self.active_connections)}")

    def disconnect(self, websocket: WebSocket):
        if websocket in self.active_connections:
            self.active_connections.remove(websocket)
        logger.info(f"WS client disconnected. Total: {len(self.active_connections)}")

    async def broadcast(self, message: dict):
        if not self.active_connections:
            return
        data = json.dumps(message)
        dead = []
        for ws in self.active_connections:
            try:
                await ws.send_text(data)
            except Exception:
                dead.append(ws)
        for ws in dead:
            self.disconnect(ws)


ws_manager = ConnectionManager()


# ─── Routes ─────────────────────────────────────────────────────────────────────

@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    """Main overlay page — use as OBS Browser Source."""
    return templates.TemplateResponse("overlay.html", {"request": request})


@app.get("/dashboard", response_class=HTMLResponse)
async def dashboard(request: Request):
    """Admin dashboard to manage adapters."""
    return templates.TemplateResponse("dashboard.html", {"request": request})


@app.get("/health")
async def health():
    """Health check endpoint for Render."""
    return {"status": "ok", "timestamp": time.time()}


@app.get("/api/status")
async def status():
    """Return current adapter status and message counts."""
    if not chat_manager:
        raise HTTPException(status_code=503, detail="Chat manager not initialized")
    return chat_manager.get_status()


@app.get("/api/recent")
async def recent_messages(limit: int = 50):
    """Return recent messages for page reload."""
    if not chat_manager:
        return {"messages": []}
    return {"messages": list(chat_manager.recent_messages)[-limit:]}


@app.post("/api/adapter/{platform}/start")
async def start_adapter(platform: str, config: dict = None):
    """Dynamically start an adapter."""
    if not chat_manager:
        raise HTTPException(status_code=503, detail="Chat manager not initialized")
    try:
        adapter = _build_adapter(platform, config or {})
        await chat_manager.add_adapter(platform, adapter)
        return {"status": "started", "platform": platform}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@app.post("/api/adapter/{platform}/stop")
async def stop_adapter(platform: str):
    """Stop a running adapter."""
    if not chat_manager:
        raise HTTPException(status_code=503, detail="Chat manager not initialized")
    await chat_manager.remove_adapter(platform)
    return {"status": "stopped", "platform": platform}


@app.post("/api/youtube/update")
async def update_youtube(body: dict):
    """Update YouTube video ID on the fly."""
    video_id = body.get("video_id")
    if not video_id:
        raise HTTPException(status_code=400, detail="video_id required")
    await chat_manager.remove_adapter("youtube")
    adapter = YouTubeAdapter(video_id=video_id)
    await chat_manager.add_adapter("youtube", adapter)
    return {"status": "updated", "video_id": video_id}


@app.post("/api/instagram/update")
async def update_instagram(body: dict):
    """
    Update Instagram Live URL (and optionally credentials) on the fly.
    Restarts the adapter with the new configuration.

    Body params (all optional):
        live_url        Full URL to the Instagram Live page
        session_cookie  IG session cookie string or JSON
        mock_mode       true/false
    """
    if not chat_manager:
        raise HTTPException(status_code=503, detail="Chat manager not initialized")

    live_url = body.get("live_url", os.getenv("IG_LIVE_URL", ""))
    session_cookie = body.get("session_cookie", os.getenv("IG_SESSION_COOKIE", ""))
    mock_mode = body.get("mock_mode", None)

    await chat_manager.remove_adapter("instagram")
    adapter = InstagramAdapter(
        live_url=live_url,
        username=os.getenv("IG_USERNAME", ""),
        password=os.getenv("IG_PASSWORD", ""),
        session_cookie=session_cookie,
        mock_mode=mock_mode,
    )
    await chat_manager.add_adapter("instagram", adapter)
    return {"status": "updated", "live_url": live_url}


@app.get("/tts")
async def tts_hook(message: str, platform: str = "system"):
    """TTS hook endpoint — returns message for external TTS integrations."""
    return {"text": message, "platform": platform, "timestamp": time.time()}


@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    """Main WebSocket endpoint for the overlay client."""
    await ws_manager.connect(websocket)

    # Send recent messages on connect
    if chat_manager:
        for msg in list(chat_manager.recent_messages)[-30:]:
            try:
                await websocket.send_text(json.dumps(msg))
            except Exception:
                break

    try:
        while True:
            # Keep connection alive, receive pings
            data = await websocket.receive_text()
            if data == "ping":
                await websocket.send_text("pong")
    except WebSocketDisconnect:
        ws_manager.disconnect(websocket)


def _build_adapter(platform: str, config: dict):
    """Factory to build adapter instances from config."""
    if platform == "youtube":
        return YouTubeAdapter(video_id=config.get("video_id", ""))
    elif platform == "twitch":
        return TwitchAdapter(
            channel=config.get("channel", ""),
            token=config.get("token"),
            nick=config.get("nick", "justinfan12345")
        )
    elif platform == "kick":
        return KickAdapter(channel=config.get("channel", ""))
    elif platform == "rumble":
        return RumbleAdapter(stream_url=config.get("stream_url", ""))
    elif platform == "facebook":
        return FacebookAdapter(
            access_token=config.get("access_token", ""),
            video_id=config.get("video_id", "")
        )
    elif platform == "instagram":
        return InstagramAdapter(
            live_url=config.get("live_url", os.getenv("IG_LIVE_URL", "")),
            username=config.get("username", os.getenv("IG_USERNAME", "")),
            password=config.get("password", os.getenv("IG_PASSWORD", "")),
            session_cookie=config.get("session_cookie", os.getenv("IG_SESSION_COOKIE", "")),
            mock_mode=config.get("mock_mode", None),
        )
    else:
        raise ValueError(f"Unknown platform: {platform}")


# ─── Background task: broadcast messages from queue ──────────────────────────

async def message_broadcaster():
    """Background task: pull from chat_manager queue and broadcast to WS clients."""
    while True:
        if chat_manager:
            try:
                msg = await asyncio.wait_for(
                    chat_manager.message_queue.get(), timeout=1.0
                )
                await ws_manager.broadcast(msg)
            except asyncio.TimeoutError:
                pass
            except Exception as e:
                logger.error(f"Broadcaster error: {e}")
        else:
            await asyncio.sleep(0.1)


@app.on_event("startup")
async def start_broadcaster():
    asyncio.create_task(message_broadcaster())


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=port,
        reload=False,
        log_level="info"
    )
