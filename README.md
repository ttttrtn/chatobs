# ⚡ StreamChat Overlay

A production-ready multi-platform live chat aggregator for OBS, Prism Live, and Streamlabs. Merges YouTube, Twitch, Kick, Rumble, Facebook, and Instagram chat into a single transparent browser source.

---

## Features

- **6 platform adapters** — YouTube (pytchat), Twitch (IRC), Kick (WebSocket + REST), Rumble (scraping), Facebook (Graph API), Instagram (mock)
- **Transparent OBS overlay** — drop it in as a Browser Source, nothing else needed
- **Auto-reconnect** — every adapter has a supervised restart loop with exponential backoff
- **Deduplication** — rolling fingerprint cache prevents message spam
- **Admin dashboard** — live preview and per-adapter controls at `/dashboard`
- **Modular adapter system** — add a new platform by subclassing `BaseAdapter`

---

## Quick Start (Local)

### 1. Clone and install

```bash
git clone <your-repo>
cd streamchat
pip install -r requirements.txt
```

### 2. Configure

```bash
cp .env.example .env
# Edit .env and add your platform credentials
```

### 3. Run

```bash
python main.py
# or: uvicorn main:app --reload --port 8000
```

Open `http://localhost:8000/dashboard` to see the admin panel.

---

## OBS / Streamlabs / Prism Setup

1. Add a **Browser Source** in OBS
2. Set URL to: `http://localhost:8000/` (or your Render URL)
3. Set Width: **420**, Height: **900** (or your stream height)
4. Check **"Transparent background"** / **"Allow transparency"**
5. Uncheck "Shutdown source when not visible"

That's it — messages will scroll in from the bottom-left corner.

---

## Render.com Deployment

### Option A: render.yaml (recommended)

1. Push this repo to GitHub
2. In Render, click **New → Blueprint**
3. Connect your repo — Render will read `render.yaml` automatically
4. Set your environment variables in the Render dashboard
5. Deploy

### Option B: Manual

1. New → **Web Service**
2. Language: **Python**
3. Build command: `pip install -r requirements.txt`
4. Start command: `uvicorn main:app --host 0.0.0.0 --port $PORT`
5. Add env vars from `.env.example`

---

## Environment Variables

| Variable | Required | Description |
|---|---|---|
| `PORT` | Auto (Render) | Server port — Render sets this automatically |
| `YOUTUBE_VIDEO_ID` | Optional | YouTube live video ID (e.g. `dQw4w9WgXcQ`) |
| `TWITCH_CHANNEL` | Optional | Twitch channel name (no `#`) |
| `TWITCH_OAUTH_TOKEN` | Optional | Twitch OAuth for sub events; leave blank for anonymous |
| `TWITCH_BOT_NICK` | Optional | Bot nick (default: `justinfan12345` for anon) |
| `KICK_CHANNEL` | Optional | Kick channel name (lowercase) |
| `RUMBLE_STREAM_URL` | Optional | Full Rumble stream URL |
| `FACEBOOK_ACCESS_TOKEN` | Optional | FB Page Access Token |
| `FACEBOOK_VIDEO_ID` | Optional | Facebook Live video ID |
| `INSTAGRAM_ENABLED` | Optional | `"true"` to enable mock Instagram messages |

---

## API Endpoints

| Method | Path | Description |
|---|---|---|
| GET | `/` | Overlay (use as OBS browser source) |
| GET | `/dashboard` | Admin dashboard |
| GET | `/health` | Health check for Render |
| GET | `/api/status` | Adapter status + message counts |
| GET | `/api/recent` | Last N messages (for page reloads) |
| POST | `/api/youtube/update` | Change YouTube video ID at runtime |
| POST | `/api/adapter/{platform}/start` | Start an adapter with config JSON |
| POST | `/api/adapter/{platform}/stop` | Stop a running adapter |
| WS | `/ws` | WebSocket feed for the overlay |
| GET | `/tts` | TTS hook (for external TTS services) |

---

## Adding a New Platform

1. Create `/adapters/myplatform.py`
2. Subclass `BaseAdapter`
3. Implement `_connect()` and `_listen()`
4. Call `await self._emit({...})` with normalized message dict
5. Register in `main.py` → `auto_start_adapters()` and `_build_adapter()`

```python
from adapters import BaseAdapter

class MyPlatformAdapter(BaseAdapter):
    def __init__(self, channel: str):
        super().__init__("myplatform")
        self.channel = channel

    async def _connect(self):
        # Establish connection
        pass

    async def _listen(self):
        while self._running:
            # Receive messages and emit
            await self._emit({
                "username": "user",
                "message": "Hello!",
                "platform": "myplatform",
                "timestamp": time.time(),
                "badges": [],
            })
            await asyncio.sleep(1)
```

---

## Platform Notes

### YouTube
Uses `pytchat` which reverse-engineers the YouTube Live Chat API. Works well for public streams. The `video_id` must be for an **active live stream**.

### Twitch
Connects via Twitch IRC over WebSocket (standard TLS). Anonymous mode (`justinfan`) is read-only. Provide `TWITCH_OAUTH_TOKEN` for subscriber/mod event parsing.

### Kick
Uses Kick's Pusher-based WebSocket (`soketi`). Falls back to REST polling if WebSocket fails. The Pusher app key is public and used by the official Kick website.

### Rumble
Web scraping via `aiohttp` + `beautifulsoup4`. Rumble doesn't have a public chat API. CSS selectors may need updating if Rumble changes their page structure.

### Facebook
Uses the Facebook Graph API `/comments` edge with the `stream` filter. Requires a Page Access Token with `pages_read_engagement` permission. Comments appear with ~5s delay.

### Instagram
No public API exists for Instagram Live chat. The adapter runs in **mock mode** by default, generating realistic demo messages. For production, consider Restream or StreamElements which have Meta-approved integrations.

---

## Architecture

```
main.py              FastAPI app + WebSocket broadcast
chat_manager.py      Adapter orchestrator + message queue + dedup
adapters/
  __init__.py        BaseAdapter abstract class
  youtube_pytchat.py YouTube via pytchat (thread executor)
  twitch.py          Twitch IRC over WebSocket
  kick.py            Kick Pusher WebSocket + REST fallback
  rumble.py          Rumble HTML scraping
  facebook.py        Facebook Graph API polling
  instagram.py       Mock/experimental adapter
static/
  css/overlay.css    Transparent OBS overlay styles
  css/dashboard.css  Admin dashboard styles
  js/overlay.js      WebSocket client + message renderer
  js/dashboard.js    Dashboard controls + status polling
templates/
  overlay.html       OBS browser source page
  dashboard.html     Admin UI
```

---

## License

MIT
"# chatobs" 
