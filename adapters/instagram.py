"""
Instagram Live Chat Adapter — Real browser automation via Playwright.

Scrapes Instagram Live chat in real time using a headless Chromium session.
Supports login via session cookie (preferred) or username/password.

Environment variables:
    IG_USERNAME         Instagram account username
    IG_PASSWORD         Instagram account password
    IG_SESSION_COOKIE   Serialized session cookie JSON (preferred over u/p)
    IG_LIVE_URL         Full URL to the Instagram Live page
    INSTAGRAM_ENABLED   Set to "true" to enable this adapter (default: false)
    INSTAGRAM_MOCK_MODE Set to "true" to run in demo/mock mode (default: false)

Playwright install (required once):
    playwright install chromium
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import random
import re
import time
from typing import Optional

from adapters import BaseAdapter

logger = logging.getLogger("streamchat.adapter.instagram")

# ---------------------------------------------------------------------------
# Playwright availability guard
# ---------------------------------------------------------------------------
try:
    from playwright.async_api import (
        async_playwright,
        Browser,
        BrowserContext,
        Page,
        Playwright,
    )
    PLAYWRIGHT_AVAILABLE = True
except ImportError:
    PLAYWRIGHT_AVAILABLE = False
    logger.warning(
        "[Instagram] playwright not installed. "
        "Run: pip install playwright && playwright install chromium"
    )

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

# How long to wait between DOM polls when no MutationObserver events fire
POLL_INTERVAL_MS = 1500

# Seconds between full page health-checks (detects stale/crashed pages)
HEALTH_CHECK_INTERVAL = 30

# Max backoff in seconds for reconnect attempts
MAX_BACKOFF = 120

# How many messages to track for dedup (rolling window)
SEEN_CACHE_MAX = 3000

# Playwright timeouts
NAV_TIMEOUT_MS = 60_000
SELECTOR_TIMEOUT_MS = 20_000

# Instagram live page URL template
IG_LIVE_URL_TEMPLATE = "https://www.instagram.com/{username}/live/"

# ---------------------------------------------------------------------------
# CSS / JS selector strategies (ordered by reliability)
# Instagram frequently changes class names — we try multiple selectors.
# ---------------------------------------------------------------------------

# Chat comment container (the scroll pane holding all messages)
CHAT_CONTAINER_SELECTORS = [
    "[data-testid='live-chat-comment-list']",
    "ul[role='list']",
    "div[class*='LiveVideoComments']",
    "div[class*='liveVideoComments']",
    "div[class*='x1qjc9v5']",   # Instagram utility class (changes often)
    "div[class*='commentList']",
]

# Individual comment row
COMMENT_ROW_SELECTORS = [
    "li[role='listitem']",
    "div[role='listitem']",
    "li[class*='comment']",
    "div[class*='Comment']",
    "div[class*='comment']",
]

# Username within a comment
USERNAME_SELECTORS = [
    "span[class*='username']",
    "a[role='link'] > span",
    "span[class*='Username']",
    "strong",
    "b",
]

# Message text within a comment
MESSAGE_SELECTORS = [
    "span[class*='commentText']",
    "span[class*='CommentText']",
    "span[dir='auto']:not([class*='username']):not([class*='Username'])",
    "span[class*='x1lliihq']:last-of-type",
]

# Avatar image
AVATAR_SELECTORS = [
    "img[alt*='profile']",
    "img[draggable='false']",
    "img[class*='Avatar']",
    "img[class*='avatar']",
]

# JS injected into the page to observe chat mutations
_MUTATION_OBSERVER_JS = """
(function() {
    if (window.__streamchat_observer) return 'already_registered';

    window.__streamchat_queue = [];
    window.__streamchat_observer = null;

    function findChatContainer() {
        const selectors = %s;
        for (const sel of selectors) {
            const el = document.querySelector(sel);
            if (el) return el;
        }
        return null;
    }

    function extractMessages(container) {
        const rowSelectors = %s;
        let rows = [];
        for (const sel of rowSelectors) {
            rows = Array.from(container.querySelectorAll(sel));
            if (rows.length) break;
        }

        return rows.map(row => {
            // Username
            const unameSelectors = %s;
            let username = '';
            for (const sel of unameSelectors) {
                const el = row.querySelector(sel);
                if (el && el.textContent.trim()) {
                    username = el.textContent.trim();
                    break;
                }
            }

            // Message text
            const msgSelectors = %s;
            let message = '';
            for (const sel of msgSelectors) {
                const el = row.querySelector(sel);
                if (el && el.textContent.trim() && el.textContent.trim() !== username) {
                    message = el.textContent.trim();
                    break;
                }
            }

            // Fallback: grab all spans and pick the non-username one
            if (!message) {
                const spans = Array.from(row.querySelectorAll('span'));
                for (const s of spans) {
                    const t = s.textContent.trim();
                    if (t && t !== username && t.length > 0) {
                        message = t;
                        break;
                    }
                }
            }

            // Avatar
            const avSelectors = %s;
            let avatar = '';
            for (const sel of avSelectors) {
                const img = row.querySelector(sel);
                if (img && img.src) {
                    avatar = img.src;
                    break;
                }
            }

            // Badges (e.g. verified check, moderator)
            const badges = [];
            const svgs = row.querySelectorAll('svg[aria-label]');
            svgs.forEach(svg => {
                const label = svg.getAttribute('aria-label') || '';
                if (label) badges.push(label.toLowerCase());
            });

            return { username, message, avatar, badges, ts: Date.now() };
        }).filter(m => m.username && m.message);
    }

    function startObserving() {
        const container = findChatContainer();
        if (!container) return false;

        if (window.__streamchat_observer) {
            window.__streamchat_observer.disconnect();
        }

        window.__streamchat_observer = new MutationObserver(() => {
            const msgs = extractMessages(container);
            msgs.forEach(m => window.__streamchat_queue.push(m));
        });

        window.__streamchat_observer.observe(container, {
            childList: true,
            subtree: true,
            characterData: true,
        });

        // Initial snapshot
        const msgs = extractMessages(container);
        msgs.forEach(m => window.__streamchat_queue.push(m));

        return true;
    }

    window.__streamchat_startObserving = startObserving;
    const ok = startObserving();
    return ok ? 'registered' : 'container_not_found';
})();
""" % (
    json.dumps(CHAT_CONTAINER_SELECTORS),
    json.dumps(COMMENT_ROW_SELECTORS),
    json.dumps(USERNAME_SELECTORS),
    json.dumps(MESSAGE_SELECTORS),
    json.dumps(AVATAR_SELECTORS),
)

# JS to drain the accumulated queue
_DRAIN_QUEUE_JS = """
(function() {
    if (!window.__streamchat_queue) return [];
    const msgs = window.__streamchat_queue.splice(0);
    return msgs;
})();
"""

# JS to check if the live stream is still active
_LIVE_CHECK_JS = """
(function() {
    const liveIndicators = [
        document.querySelector('[data-testid*="live"]'),
        document.querySelector('[aria-label*="Live"]'),
        document.querySelector('[class*="LiveBadge"]'),
        document.querySelector('span[class*="live"]'),
    ];
    const hasLive = liveIndicators.some(el => el !== null);

    const ended = !!(
        document.querySelector('[class*="LiveEnded"]') ||
        document.querySelector('[class*="liveEnded"]') ||
        document.title.toLowerCase().includes('ended')
    );

    const chatContainer = document.querySelector(
        %s
    );

    return {
        hasLive,
        ended,
        hasChatContainer: !!chatContainer,
        url: window.location.href,
        title: document.title,
    };
})();
""" % json.dumps(CHAT_CONTAINER_SELECTORS[0])

# ---------------------------------------------------------------------------
# Mock data for fallback / demo mode
# ---------------------------------------------------------------------------
_MOCK_USERNAMES = [
    "ig_viewer_42", "pink_fan_girl", "reels_lover", "daily_scroller",
    "insta_user_99", "photo_enthusiast", "lifestyle_vibes", "social_watcher",
    "moment_captured", "reel_repeat", "ig_lurker_007", "content_fan_2025",
    "gramm_addict", "insta_regular", "storywatcher99", "liveviewer_2025",
]
_MOCK_MESSAGES = [
    "❤️ Love this stream!",
    "🔥🔥🔥",
    "Hello from Instagram!",
    "First time watching, this is great!",
    "Keep it up! 💯",
    "Watching from 📱",
    "This is amazing ✨",
    "Hi everyone 👋",
    "Best stream today!",
    "Sharing this with my followers 📲",
    "So good! Following now 🙌",
    "🎉🎉🎉",
    "Can you see me in chat?",
    "Long time follower, first time in live 🙏",
    "You're doing amazing!",
    "POV: can't stop watching 👀",
]


class InstagramAdapter(BaseAdapter):
    """
    Instagram Live Chat adapter.

    Modes:
      1. REAL (default when IG_LIVE_URL + credentials are set):
         Uses Playwright headless Chromium to scrape chat in real time.
      2. MOCK (fallback / demo):
         Emits realistic-looking fake messages at random intervals.
    """

    def __init__(
        self,
        live_url: Optional[str] = None,
        username: Optional[str] = None,
        password: Optional[str] = None,
        session_cookie: Optional[str] = None,
        mock_mode: Optional[bool] = None,
        poll_interval: float = POLL_INTERVAL_MS / 1000,
        mock_interval: float = 12.0,
    ):
        super().__init__("instagram")

        self.live_url = live_url or os.getenv("IG_LIVE_URL", "")
        self.ig_username = username or os.getenv("IG_USERNAME", "")
        self.ig_password = password or os.getenv("IG_PASSWORD", "")
        self.session_cookie_raw = session_cookie or os.getenv("IG_SESSION_COOKIE", "")
        self.poll_interval = poll_interval
        self.mock_interval = mock_interval

        # Decide mode
        if mock_mode is not None:
            self._mock_mode = mock_mode
        else:
            env_mock = os.getenv("INSTAGRAM_MOCK_MODE", "false").lower() == "true"
            has_creds = bool(
                (self.ig_username and self.ig_password) or self.session_cookie_raw
            )
            has_url = bool(self.live_url)
            self._mock_mode = env_mock or not (has_creds and has_url and PLAYWRIGHT_AVAILABLE)

        # Playwright state
        self._playwright: Optional[Playwright] = None
        self._browser: Optional[Browser] = None
        self._context: Optional[BrowserContext] = None
        self._page: Optional[Page] = None

        # Dedup
        self._seen: set[str] = set()
        self._seen_order: list[str] = []

        # Reconnect state
        self._backoff = 5.0

        if self._mock_mode:
            logger.warning(
                "[Instagram] Running in MOCK mode — demo messages only. "
                "Set IG_LIVE_URL, IG_USERNAME/IG_PASSWORD or IG_SESSION_COOKIE "
                "to enable real scraping."
            )
        else:
            logger.info(
                f"[Instagram] Real mode enabled. "
                f"Live URL: {self.live_url} | "
                f"Auth: {'cookie' if self.session_cookie_raw else 'credentials'}"
            )

    # ───────────────────────────── BaseAdapter interface ────────────────────

    async def _connect(self):
        if self._mock_mode:
            logger.info("[Instagram] Mock adapter connected.")
            return

        if not PLAYWRIGHT_AVAILABLE:
            logger.error(
                "[Instagram] Playwright not installed. Falling back to mock mode. "
                "Fix: pip install playwright && playwright install chromium"
            )
            self._mock_mode = True
            return

        await self._launch_browser()

    async def _listen(self):
        if self._mock_mode:
            await self._mock_listen()
            return

        await self._real_listen()

    async def stop(self):
        self._running = False
        await self._teardown_browser()
        logger.info("[Instagram] Adapter stopped.")

    # ───────────────────────────── Browser lifecycle ─────────────────────────

    async def _launch_browser(self):
        """Start Playwright and open a headless Chromium session."""
        logger.info("[Instagram] Launching headless Chromium...")
        try:
            self._playwright = await async_playwright().start()
            self._browser = await self._playwright.chromium.launch(
                headless=True,
                args=[
                    "--no-sandbox",
                    "--disable-setuid-sandbox",
                    "--disable-blink-features=AutomationControlled",
                    "--disable-dev-shm-usage",
                    "--disable-gpu",
                    "--disable-infobars",
                    "--window-size=1280,900",
                ],
            )

            # Build context with realistic browser fingerprint
            self._context = await self._browser.new_context(
                viewport={"width": 1280, "height": 900},
                user_agent=(
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/124.0.0.0 Safari/537.36"
                ),
                locale="en-US",
                timezone_id="America/New_York",
                extra_http_headers={
                    "Accept-Language": "en-US,en;q=0.9",
                },
            )

            # Stealth: hide webdriver flag
            await self._context.add_init_script("""
                Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
                Object.defineProperty(navigator, 'languages', { get: () => ['en-US', 'en'] });
                Object.defineProperty(navigator, 'plugins', { get: () => [1, 2, 3] });
            """)

            self._page = await self._context.new_page()

            # Suppress non-critical resource loading to reduce fingerprint
            await self._page.route(
                "**/*.{png,jpg,jpeg,gif,webp,woff,woff2,svg,ico}",
                lambda route: route.abort()
                if self._should_block_resource(route.request.url)
                else route.continue_(),
            )

            logger.info("[Instagram] Browser launched successfully.")
        except Exception as exc:
            logger.error(f"[Instagram] Failed to launch browser: {exc}")
            raise

    def _should_block_resource(self, url: str) -> bool:
        """Block heavy media but allow avatars (cdninstagram.com)."""
        if "cdninstagram.com" in url or "fbcdn.net" in url:
            return False  # Allow profile pictures
        blocked_extensions = (".mp4", ".webm", ".mov", ".avi", ".woff", ".woff2")
        return any(url.endswith(ext) for ext in blocked_extensions)

    async def _teardown_browser(self):
        """Clean up Playwright resources."""
        try:
            if self._page and not self._page.is_closed():
                await self._page.close()
        except Exception:
            pass
        try:
            if self._context:
                await self._context.close()
        except Exception:
            pass
        try:
            if self._browser:
                await self._browser.close()
        except Exception:
            pass
        try:
            if self._playwright:
                await self._playwright.stop()
        except Exception:
            pass
        self._page = None
        self._context = None
        self._browser = None
        self._playwright = None

    # ───────────────────────────── Authentication ────────────────────────────

    async def _authenticate(self) -> bool:
        """
        Log into Instagram.
        Tries session cookie first; falls back to username/password.
        Returns True on success.
        """
        if self.session_cookie_raw:
            return await self._auth_via_cookie()
        elif self.ig_username and self.ig_password:
            return await self._auth_via_credentials()
        else:
            logger.error("[Instagram] No credentials provided (IG_USERNAME/IG_PASSWORD or IG_SESSION_COOKIE)")
            return False

    async def _auth_via_cookie(self) -> bool:
        """Inject a saved session cookie to skip the login form."""
        try:
            # Session cookie can be a JSON list (Playwright format) or
            # a single "sessionid=<value>" string
            if self.session_cookie_raw.strip().startswith("["):
                cookies = json.loads(self.session_cookie_raw)
            elif "sessionid=" in self.session_cookie_raw:
                session_id = re.search(
                    r"sessionid=([^;]+)", self.session_cookie_raw
                )
                if not session_id:
                    logger.error("[Instagram] Could not parse sessionid from IG_SESSION_COOKIE")
                    return False
                cookies = [
                    {
                        "name": "sessionid",
                        "value": session_id.group(1),
                        "domain": ".instagram.com",
                        "path": "/",
                        "httpOnly": True,
                        "secure": True,
                        "sameSite": "Lax",
                    }
                ]
            else:
                # Treat as raw session ID value
                cookies = [
                    {
                        "name": "sessionid",
                        "value": self.session_cookie_raw.strip(),
                        "domain": ".instagram.com",
                        "path": "/",
                        "httpOnly": True,
                        "secure": True,
                        "sameSite": "Lax",
                    }
                ]

            await self._context.add_cookies(cookies)
            logger.info("[Instagram] Session cookie injected.")

            # Verify the cookie is valid by visiting IG
            await self._page.goto(
                "https://www.instagram.com/",
                wait_until="domcontentloaded",
                timeout=NAV_TIMEOUT_MS,
            )
            await asyncio.sleep(2)

            # Check if we're still on the login page
            if "/accounts/login" in self._page.url:
                logger.error("[Instagram] Session cookie appears invalid (redirected to login).")
                return False

            logger.info("[Instagram] Cookie auth successful.")
            return True

        except Exception as exc:
            logger.error(f"[Instagram] Cookie auth error: {exc}")
            return False

    async def _auth_via_credentials(self) -> bool:
        """Log in via the Instagram login form."""
        try:
            logger.info("[Instagram] Logging in via username/password...")
            await self._page.goto(
                "https://www.instagram.com/accounts/login/",
                wait_until="networkidle",
                timeout=NAV_TIMEOUT_MS,
            )
            await asyncio.sleep(random.uniform(1.5, 3.0))

            # Accept cookies dialog if it appears
            try:
                cookie_btn = self._page.get_by_role("button", name=re.compile(r"allow|accept|ok", re.I))
                if await cookie_btn.count() > 0:
                    await cookie_btn.first.click()
                    await asyncio.sleep(1)
            except Exception:
                pass

            # Fill in credentials
            username_input = self._page.locator("input[name='username']")
            password_input = self._page.locator("input[name='password']")

            await username_input.wait_for(timeout=SELECTOR_TIMEOUT_MS)
            await username_input.fill("")
            await asyncio.sleep(random.uniform(0.3, 0.7))

            # Type character by character to mimic human input
            for char in self.ig_username:
                await username_input.type(char, delay=random.uniform(50, 150))

            await asyncio.sleep(random.uniform(0.5, 1.0))

            await password_input.fill("")
            for char in self.ig_password:
                await password_input.type(char, delay=random.uniform(50, 150))

            await asyncio.sleep(random.uniform(0.5, 1.5))

            # Submit
            await self._page.keyboard.press("Enter")

            # Wait for navigation (success or challenge)
            await asyncio.sleep(5)

            current_url = self._page.url

            if "/challenge/" in current_url or "/checkpoint/" in current_url:
                logger.error(
                    "[Instagram] Login challenge required (2FA / suspicious login). "
                    "Use IG_SESSION_COOKIE instead of username/password."
                )
                return False

            if "/accounts/login/" in current_url:
                logger.error("[Instagram] Login failed — still on login page. Check credentials.")
                return False

            # Handle 'Save Login Info' dialog
            try:
                not_now = self._page.get_by_role("button", name=re.compile(r"not now|skip", re.I))
                if await not_now.count() > 0:
                    await not_now.first.click()
                    await asyncio.sleep(1)
            except Exception:
                pass

            # Handle 'Turn on Notifications' dialog
            try:
                not_now2 = self._page.get_by_role("button", name=re.compile(r"not now|skip", re.I))
                if await not_now2.count() > 0:
                    await not_now2.first.click()
                    await asyncio.sleep(1)
            except Exception:
                pass

            logger.info("[Instagram] Credential login successful.")
            return True

        except Exception as exc:
            logger.error(f"[Instagram] Credential auth error: {exc}")
            return False

    # ───────────────────────────── Real scraping ─────────────────────────────

    async def _real_listen(self):
        """Main real-time scraping loop."""
        while self._running:
            try:
                await self._scrape_session()
            except asyncio.CancelledError:
                break
            except Exception as exc:
                logger.error(f"[Instagram] Scraping session crashed: {exc}")

            if not self._running:
                break

            logger.info(f"[Instagram] Reconnecting in {self._backoff:.0f}s...")
            await asyncio.sleep(self._backoff)
            self._backoff = min(self._backoff * 2, MAX_BACKOFF)

            # Restart browser for a fresh session
            await self._teardown_browser()
            await asyncio.sleep(2)
            await self._launch_browser()

    async def _scrape_session(self):
        """
        One complete scraping session:
        1. Authenticate
        2. Navigate to the Live page
        3. Inject MutationObserver
        4. Poll for new messages until stream ends or error
        """
        # Auth
        ok = await self._authenticate()
        if not ok:
            logger.error("[Instagram] Authentication failed. Cannot scrape.")
            await asyncio.sleep(30)
            return

        # Navigate to live page
        logger.info(f"[Instagram] Navigating to live page: {self.live_url}")
        try:
            await self._page.goto(
                self.live_url,
                wait_until="domcontentloaded",
                timeout=NAV_TIMEOUT_MS,
            )
        except Exception as exc:
            logger.error(f"[Instagram] Navigation failed: {exc}")
            return

        # Wait for page to hydrate
        await asyncio.sleep(5)

        # Check we're on a live page
        live_state = await self._check_live_state()
        if live_state.get("ended"):
            logger.warning("[Instagram] Live stream appears to have already ended.")
            return

        # Inject observer
        await self._inject_observer()

        # Reset backoff — we connected successfully
        self._backoff = 5.0

        logger.info("[Instagram] Chat observer active. Listening for messages...")

        health_check_at = time.time() + HEALTH_CHECK_INTERVAL

        while self._running:
            await asyncio.sleep(self.poll_interval)

            try:
                # Drain queued messages from JS side
                messages = await self._page.evaluate(_DRAIN_QUEUE_JS)
                for raw in messages:
                    await self._process_raw_message(raw)

                # Periodic health check
                if time.time() >= health_check_at:
                    health_check_at = time.time() + HEALTH_CHECK_INTERVAL
                    ok = await self._health_check()
                    if not ok:
                        logger.warning("[Instagram] Health check failed — reconnecting...")
                        return

            except Exception as exc:
                logger.error(f"[Instagram] Poll error: {exc}")
                # Don't immediately give up — try to re-inject observer
                try:
                    await self._inject_observer()
                except Exception:
                    return

    async def _inject_observer(self):
        """Inject the MutationObserver JS into the page, with retry."""
        for attempt in range(5):
            try:
                result = await self._page.evaluate(_MUTATION_OBSERVER_JS)
                if result == "registered":
                    logger.info("[Instagram] MutationObserver registered on chat container.")
                    return
                elif result == "already_registered":
                    logger.debug("[Instagram] Observer already registered.")
                    return
                else:
                    logger.warning(
                        f"[Instagram] Observer inject attempt {attempt + 1}: {result}"
                    )
                    # Chat container might not have loaded yet — wait
                    await asyncio.sleep(2 + attempt * 1.5)

                    # Try scrolling to trigger lazy-load
                    try:
                        await self._page.mouse.wheel(0, 300)
                    except Exception:
                        pass

            except Exception as exc:
                logger.warning(f"[Instagram] Observer inject error (attempt {attempt + 1}): {exc}")
                await asyncio.sleep(2)

        # Final fallback: try re-navigating
        logger.error("[Instagram] Could not find chat container after 5 attempts. Is this a live page?")

    async def _check_live_state(self) -> dict:
        try:
            return await self._page.evaluate(_LIVE_CHECK_JS) or {}
        except Exception:
            return {}

    async def _health_check(self) -> bool:
        """
        Returns True if the page is still healthy and the stream is live.
        Returns False if we should reconnect.
        """
        try:
            if self._page.is_closed():
                logger.warning("[Instagram] Page was closed.")
                return False

            state = await self._check_live_state()

            if state.get("ended"):
                logger.info("[Instagram] Live stream has ended.")
                return False

            # Re-inject observer if container disappeared (e.g. chat scrolled off DOM)
            if not state.get("hasChatContainer"):
                logger.warning("[Instagram] Chat container not found — re-injecting observer.")
                await self._inject_observer()

            return True

        except Exception as exc:
            logger.error(f"[Instagram] Health check error: {exc}")
            return False

    async def _process_raw_message(self, raw: dict):
        """Normalize and emit a message extracted from the DOM."""
        username = (raw.get("username") or "").strip()
        message = (raw.get("message") or "").strip()

        if not username or not message:
            return

        # Deduplicate
        fp = f"{username}:{message}"
        if fp in self._seen:
            return
        self._seen.add(fp)
        self._seen_order.append(fp)
        if len(self._seen_order) > SEEN_CACHE_MAX:
            oldest = self._seen_order.pop(0)
            self._seen.discard(oldest)

        # Build badges list
        badges = [b for b in (raw.get("badges") or []) if isinstance(b, str)]
        if "verified" in " ".join(badges).lower():
            normalized_badges = ["verified"]
        else:
            normalized_badges = badges

        # Detect pinned messages
        msg_lower = message.lower()
        is_pinned = "📌" in message or "pinned" in msg_lower

        await self._emit({
            "username": username,
            "message": message,
            "platform": "instagram",
            "timestamp": raw.get("ts", time.time() * 1000) / 1000,
            "avatar": raw.get("avatar", ""),
            "badges": normalized_badges,
            "is_sub": False,
            "is_mod": any("mod" in b for b in normalized_badges),
            "is_pinned": is_pinned,
        })

    # ───────────────────────────── Mock mode ─────────────────────────────────

    async def _mock_listen(self):
        """Emit realistic fake messages for demo/testing purposes."""
        # Initial burst
        await asyncio.sleep(2)
        for _ in range(random.randint(3, 6)):
            await self._emit_mock_message()
            await asyncio.sleep(random.uniform(0.3, 1.2))

        # Ongoing trickle
        while self._running:
            interval = random.uniform(self.mock_interval * 0.5, self.mock_interval * 2.0)
            await asyncio.sleep(interval)
            if self._running:
                await self._emit_mock_message()

    async def _emit_mock_message(self):
        username = random.choice(_MOCK_USERNAMES)
        message = random.choice(_MOCK_MESSAGES)

        if random.random() < 0.15:
            message += f" ({'❤️' * random.randint(1, 4)})"

        badges = []
        if random.random() < 0.05:
            badges = ["verified"]

        await self._emit({
            "username": username,
            "message": message,
            "platform": "instagram",
            "timestamp": time.time(),
            "avatar": "",
            "badges": badges,
            "is_sub": False,
            "is_mod": False,
        })
