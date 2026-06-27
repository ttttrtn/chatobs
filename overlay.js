/**
 * StreamChat Overlay — OBS Browser Source Script
 * Lightweight, no framework dependencies.
 * Connects to WebSocket, renders messages with smooth animations.
 */

(function () {
  "use strict";

  // ── Config ──────────────────────────────────────────────────────────────────
  const CONFIG = {
    maxMessages: 20,          // Max bubbles visible at once
    fadeOutDelay: 30_000,     // Remove messages after 30s (0 = never)
    reconnectDelay: 3_000,    // ms before reconnect
    maxReconnectDelay: 60_000,
    wsPath: "/ws",
  };

  // Platform icons (emoji fallbacks — no external CDN needed)
  const PLATFORM_ICONS = {
    youtube:   "▶️",
    twitch:    "💜",
    kick:      "🟢",
    rumble:    "🟠",
    facebook:  "🔵",
    instagram: "📸",
    system:    "⚙️",
  };

  // ── State ────────────────────────────────────────────────────────────────────
  let ws = null;
  let reconnectDelay = CONFIG.reconnectDelay;
  let reconnectTimer = null;
  const feed = document.getElementById("chat-feed");

  // ── WebSocket ────────────────────────────────────────────────────────────────

  function connect() {
    const protocol = location.protocol === "https:" ? "wss:" : "ws:";
    const url = `${protocol}//${location.host}${CONFIG.wsPath}`;

    ws = new WebSocket(url);

    ws.onopen = () => {
      reconnectDelay = CONFIG.reconnectDelay;
      // Send keepalive ping every 20s
      clearInterval(ws._pingInterval);
      ws._pingInterval = setInterval(() => {
        if (ws && ws.readyState === WebSocket.OPEN) {
          ws.send("ping");
        }
      }, 20_000);
    };

    ws.onmessage = (event) => {
      if (event.data === "pong") return;
      try {
        const msg = JSON.parse(event.data);
        renderMessage(msg);
      } catch (e) {
        console.warn("[StreamChat] Bad message:", event.data);
      }
    };

    ws.onerror = () => {};

    ws.onclose = () => {
      clearInterval(ws._pingInterval);
      scheduleReconnect();
    };
  }

  function scheduleReconnect() {
    clearTimeout(reconnectTimer);
    reconnectTimer = setTimeout(() => {
      connect();
      reconnectDelay = Math.min(reconnectDelay * 1.5, CONFIG.maxReconnectDelay);
    }, reconnectDelay);
  }

  // ── Rendering ────────────────────────────────────────────────────────────────

  function renderMessage(msg) {
    const platform = (msg.platform || "system").toLowerCase();
    const username = escHtml(msg.username || "Unknown");
    const messageText = escHtml(msg.message || "");
    const badges = msg.badges || [];
    const avatar = msg.avatar || "";
    const isSuperchat = msg.is_superchat || false;
    const superAmount = msg.superchat_amount || "";
    const isMod = msg.is_mod || false;
    const isSub = msg.is_sub || false;

    // Build element
    const el = document.createElement("div");
    el.className = `chat-msg platform-${platform}${isSuperchat ? " is-superchat" : ""}`;

    // ── Header ──
    const header = document.createElement("div");
    header.className = "msg-header";

    // Avatar or placeholder
    if (avatar) {
      const img = document.createElement("img");
      img.className = "msg-avatar";
      img.src = avatar;
      img.alt = "";
      img.loading = "lazy";
      img.onerror = () => {
        img.replaceWith(buildAvatarPlaceholder(username, platform));
      };
      header.appendChild(img);
    } else {
      header.appendChild(buildAvatarPlaceholder(username, platform));
    }

    // Platform icon
    const icon = document.createElement("span");
    icon.className = "platform-icon";
    icon.textContent = PLATFORM_ICONS[platform] || "💬";
    icon.setAttribute("title", platform);
    header.appendChild(icon);

    // Username
    const nameEl = document.createElement("span");
    nameEl.className = "msg-username";
    nameEl.textContent = username;
    header.appendChild(nameEl);

    // Badges
    if (badges.length > 0) {
      const badgeContainer = document.createElement("span");
      badgeContainer.className = "msg-badges";
      for (const badge of badges) {
        const b = document.createElement("span");
        b.className = `badge ${getBadgeClass(badge)}`;
        b.textContent = badge;
        badgeContainer.appendChild(b);
      }
      header.appendChild(badgeContainer);
    }

    // Superchat amount
    if (isSuperchat && superAmount) {
      const amt = document.createElement("span");
      amt.className = "superchat-amount";
      amt.textContent = `💰 ${escHtml(superAmount)}`;
      header.appendChild(amt);
    }

    el.appendChild(header);

    // ── Body ──
    const body = document.createElement("div");
    body.className = "msg-body";
    body.textContent = messageText;
    el.appendChild(body);

    // Append to feed
    feed.appendChild(el);

    // Trim old messages
    trimMessages();

    // Auto-remove after delay
    if (CONFIG.fadeOutDelay > 0) {
      setTimeout(() => removeMessage(el), CONFIG.fadeOutDelay);
    }
  }

  function buildAvatarPlaceholder(username, platform) {
    const el = document.createElement("div");
    el.className = "msg-avatar-placeholder";
    el.textContent = (username || "?")[0].toUpperCase();
    return el;
  }

  function getBadgeClass(badge) {
    const map = {
      mod: "badge-mod",
      moderator: "badge-mod",
      owner: "badge-owner",
      broadcaster: "badge-broadcaster",
      member: "badge-member",
      subscriber: "badge-subscriber",
      verified: "badge-verified",
      vip: "badge-vip",
    };
    return map[badge.toLowerCase()] || "badge-default";
  }

  function trimMessages() {
    const msgs = feed.querySelectorAll(".chat-msg:not(.exiting)");
    if (msgs.length > CONFIG.maxMessages) {
      const excess = msgs.length - CONFIG.maxMessages;
      for (let i = 0; i < excess; i++) {
        removeMessage(msgs[i]);
      }
    }
  }

  function removeMessage(el) {
    if (!el || !el.parentNode || el.classList.contains("exiting")) return;
    el.classList.add("exiting");
    el.addEventListener("animationend", () => el.remove(), { once: true });
    // Fallback removal
    setTimeout(() => el.remove(), 400);
  }

  // ── Utilities ────────────────────────────────────────────────────────────────

  function escHtml(str) {
    const div = document.createElement("div");
    div.textContent = str;
    return div.innerHTML;
  }

  // ── Boot ─────────────────────────────────────────────────────────────────────
  connect();

})();
