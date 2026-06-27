# Instagram Live Chat Adapter — Setup Guide

## Overview

The Instagram adapter uses **Playwright** (headless Chromium) to scrape live chat from an Instagram Live page in real time. This is the only viable approach since Meta does not offer a public Instagram Live chat API.

---

## Installation

### 1. Install Python dependency

```bash
pip install playwright==1.44.0
# or with all deps:
pip install -r requirements.txt
```

### 2. Install Chromium browser binary

```bash
playwright install chromium
```

On **Render.com** or other headless Linux hosts, also install system dependencies:

```bash
playwright install-deps chromium
# or equivalently:
apt-get install -y libnss3 libatk-bridge2.0-0 libdrm2 libxkbcommon0 \
  libxcomposite1 libxdamage1 libxrandr2 libgbm1 libpango-1.0-0 \
  libcairo2 libasound2
```

### 3. Configure environment variables

Copy `.env.example` to `.env` and fill in:

```env
INSTAGRAM_ENABLED=true
IG_LIVE_URL=https://www.instagram.com/therock/live/
IG_SESSION_COOKIE=<your_session_id_value>
```

---

## Authentication

### Option A — Session Cookie (Recommended)

Session cookies are more stable than password login and avoid 2FA challenges.

**How to get your session cookie:**

1. Open a browser and log into [instagram.com](https://www.instagram.com)
2. Open **DevTools** (F12) → **Application** → **Cookies** → `https://www.instagram.com`
3. Find the cookie named **`sessionid`**
4. Copy its **Value** (a long alphanumeric string)
5. Set in `.env`:

```env
IG_SESSION_COOKIE=your_sessionid_value_here
```

You can also provide the full cookie as a JSON array (Playwright format):

```env
IG_SESSION_COOKIE=[{"name":"sessionid","value":"...","domain":".instagram.com","path":"/","httpOnly":true,"secure":true}]
```

**Session cookies typically last 90 days.** Refresh them if login stops working.

### Option B — Username / Password

```env
IG_USERNAME=your_instagram_username
IG_PASSWORD=your_instagram_password
```

> ⚠️ **Warning:** Instagram may trigger 2FA or a login challenge when logging in from an unknown server IP. Use a dedicated "bot" account and prefer the session cookie approach.

---

## Render.com Deployment

Add these to your Render service **Build Command**:

```bash
pip install -r requirements.txt && playwright install chromium && playwright install-deps chromium
```

Or add a `render.yaml` build step:

```yaml
services:
  - type: web
    name: streamchat-overlay
    buildCommand: pip install -r requirements.txt && playwright install chromium && playwright install-deps chromium
    startCommand: python main.py
```

Set all `IG_*` environment variables in the Render dashboard under **Environment**.

---

## How It Works

```
Instagram Live Page
       │
       ▼
Playwright headless Chromium
       │  (logs in via cookie or u/p)
       ▼
Navigate to IG_LIVE_URL
       │
       ▼
Inject MutationObserver JS into chat container
       │
       ▼  (every ~1.5 seconds)
Drain queued messages → normalize → emit to ChatManager queue
       │
       ▼
WebSocket broadcast → OBS overlay
```

### Anti-detection measures
- Randomized human-like typing delays during login
- Webdriver flag suppressed via `add_init_script`
- Realistic User-Agent and browser headers
- Rate-limiting on media resource requests
- Exponential backoff on reconnect (5s → 10s → 20s → … → 120s max)

### Dynamic class name handling
Instagram frequently changes CSS class names. The adapter uses **multiple fallback selectors** for every element (container, username, message text, avatar). If Instagram changes their DOM, update the selector lists at the top of `adapters/instagram.py`.

---

## Environment Variables Reference

| Variable | Required | Description |
|---|---|---|
| `INSTAGRAM_ENABLED` | Yes | Set to `true` to activate the adapter |
| `IG_LIVE_URL` | Yes (real mode) | Full URL to the Instagram Live page |
| `IG_SESSION_COOKIE` | Yes (recommended) | Session cookie value or JSON |
| `IG_USERNAME` | Alt auth | Instagram username |
| `IG_PASSWORD` | Alt auth | Instagram password |
| `INSTAGRAM_MOCK_MODE` | No | Set `true` for demo messages without a real stream |

---

## Mock Mode

Set `INSTAGRAM_MOCK_MODE=true` (or enable without credentials) to run a demo that generates realistic fake messages. Useful for testing your overlay UI without an active Instagram Live stream.

---

## Dynamic URL Update (API)

While the server is running, you can point the adapter at a new live URL without restarting:

```bash
curl -X POST http://localhost:8000/api/instagram/update \
  -H "Content-Type: application/json" \
  -d '{"live_url": "https://www.instagram.com/newstreamer/live/"}'
```

---

## Troubleshooting

| Symptom | Cause | Fix |
|---|---|---|
| `playwright not installed` | Playwright not in venv | `pip install playwright && playwright install chromium` |
| Redirected to login page | Session cookie expired | Get a fresh `sessionid` cookie |
| Login challenge / 2FA | New IP detected | Use session cookie instead of u/p |
| `container_not_found` after 5 attempts | Not on a live page, or IG changed DOM | Verify URL is a live page; update selectors |
| No messages appearing | Observer not attached | Check logs for `MutationObserver registered` |
| High CPU usage | Poll interval too low | Increase `POLL_INTERVAL_MS` in adapter |
