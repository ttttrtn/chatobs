(function () {
  "use strict";

  const CONFIG = {
    maxMessages: 25,
    fadeOutDelay: 30_000,
    reconnectDelay: 3_000,
    maxReconnectDelay: 60_000,
    wsPath: "/ws",
  };

  const PLATFORM_SVGS = {
    youtube: `<svg viewBox="0 0 24 24" fill="#FF0000" xmlns="http://www.w3.org/2000/svg"><path d="M23.5 6.2a3 3 0 0 0-2.1-2.1C19.5 3.5 12 3.5 12 3.5s-7.5 0-9.4.6A3 3 0 0 0 .5 6.2 31.5 31.5 0 0 0 0 12a31.5 31.5 0 0 0 .5 5.8 3 3 0 0 0 2.1 2.1c1.9.6 9.4.6 9.4.6s7.5 0 9.4-.6a3 3 0 0 0 2.1-2.1A31.5 31.5 0 0 0 24 12a31.5 31.5 0 0 0-.5-5.8zM9.7 15.5V8.5l6.3 3.5-6.3 3.5z"/></svg>`,
    twitch: `<svg viewBox="0 0 24 24" fill="#9146FF" xmlns="http://www.w3.org/2000/svg"><path d="M11.6 6H13v4.5h-1.4V6zm3.8 0H17v4.5h-1.4V6zM2.4 0L1 3.4V21h5.8v3h3.4l3-3H17l6-6V0H2.4zm18.2 13.8-3 3h-4.5l-3 3v-3H5.4V2h15.2v11.8z"/></svg>`,
    kick: `<svg viewBox="0 0 24 24" fill="#53FC18" xmlns="http://www.w3.org/2000/svg"><path d="M2 2h4v8l5-8h5l-6 9 6 11h-5L6 13v9H2V2z"/></svg>`,
    rumble: `<svg viewBox="0 0 24 24" fill="#FF6700" xmlns="http://www.w3.org/2000/svg"><path d="M12 2C6.48 2 2 6.48 2 12s4.48 10 10 10 10-4.48 10-10S17.52 2 12 2zm-1 14.5v-9l7 4.5-7 4.5z"/></svg>`,
    facebook: `<svg viewBox="0 0 24 24" fill="#1877F2" xmlns="http://www.w3.org/2000/svg"><path d="M24 12.07C24 5.41 18.63 0 12 0S0 5.41 0 12.07C0 18.1 4.39 23.1 10.13 24v-8.44H7.08v-3.49h3.04V9.41c0-3.02 1.8-4.7 4.54-4.7 1.31 0 2.68.24 2.68.24v2.97h-1.51c-1.49 0-1.95.93-1.95 1.88v2.27h3.32l-.53 3.5h-2.79V24C19.61 23.1 24 18.1 24 12.07z"/></svg>`,
    instagram: `<svg viewBox="0 0 24 24" xmlns="http://www.w3.org/2000/svg"><defs><linearGradient id="ig" x1="0%" y1="100%" x2="100%" y2="0%"><stop offset="0%" style="stop-color:#f09433"/><stop offset="25%" style="stop-color:#e6683c"/><stop offset="50%" style="stop-color:#dc2743"/><stop offset="75%" style="stop-color:#cc2366"/><stop offset="100%" style="stop-color:#bc1888"/></linearGradient></defs><path fill="url(#ig)" d="M12 2.163c3.204 0 3.584.012 4.85.07 3.252.148 4.771 1.691 4.919 4.919.058 1.265.069 1.645.069 4.849 0 3.205-.012 3.584-.069 4.849-.149 3.225-1.664 4.771-4.919 4.919-1.266.058-1.644.07-4.85.07-3.204 0-3.584-.012-4.849-.07-3.26-.149-4.771-1.699-4.919-4.92-.058-1.265-.07-1.644-.07-4.849 0-3.204.013-3.583.07-4.849.149-3.227 1.664-4.771 4.919-4.919 1.266-.057 1.645-.069 4.849-.069zM12 0C8.741 0 8.333.014 7.053.072 2.695.272.273 2.69.073 7.052.014 8.333 0 8.741 0 12c0 3.259.014 3.668.072 4.948.2 4.358 2.618 6.78 6.98 6.98C8.333 23.986 8.741 24 12 24c3.259 0 3.668-.014 4.948-.072 4.354-.2 6.782-2.618 6.979-6.98.059-1.28.073-1.689.073-4.948 0-3.259-.014-3.667-.072-4.947-.196-4.354-2.617-6.78-6.979-6.98C15.668.014 15.259 0 12 0zm0 5.838a6.162 6.162 0 1 0 0 12.324 6.162 6.162 0 0 0 0-12.324zM12 16a4 4 0 1 1 0-8 4 4 0 0 1 0 8zm6.406-11.845a1.44 1.44 0 1 0 0 2.881 1.44 1.44 0 0 0 0-2.881z"/></svg>`,
    system: `<svg viewBox="0 0 24 24" fill="#AAAAAA" xmlns="http://www.w3.org/2000/svg"><path d="M12 2a10 10 0 1 0 0 20A10 10 0 0 0 12 2zm1 15h-2v-6h2v6zm0-8h-2V7h2v2z"/></svg>`,
  };

  let ws = null;
  let reconnectDelay = CONFIG.reconnectDelay;
  const feed = document.getElementById("chat-feed");

  function connect() {
    const protocol = location.protocol === "https:" ? "wss:" : "ws:";
    ws = new WebSocket(`${protocol}//${location.host}${CONFIG.wsPath}`);

    ws.onopen = () => {
      reconnectDelay = CONFIG.reconnectDelay;
      clearInterval(ws._ping);
      ws._ping = setInterval(() => {
        if (ws.readyState === WebSocket.OPEN) ws.send("ping");
      }, 20_000);
    };

    ws.onmessage = (e) => {
      if (e.data === "pong") return;
      try { renderMessage(JSON.parse(e.data)); } catch (_) {}
    };

    ws.onerror = () => {};
    ws.onclose = () => {
      clearInterval(ws._ping);
      setTimeout(() => {
        connect();
        reconnectDelay = Math.min(reconnectDelay * 1.5, CONFIG.maxReconnectDelay);
      }, reconnectDelay);
    };
  }

  function renderMessage(msg) {
    const platform = (msg.platform || "system").toLowerCase();
    const username = msg.username || "Unknown";
    const messageText = msg.message || "";
    const badges = msg.badges || [];
    const isSuperchat = msg.is_superchat || false;
    const superAmount = msg.superchat_amount || "";

    const el = document.createElement("div");
    el.className = `chat-msg platform-${platform}`;

    // All inline: [icon] Username [badges]: message text
    const content = document.createElement("div");
    content.className = "msg-content";

    // Platform icon — small, inline before username
    const iconWrap = document.createElement("span");
    iconWrap.className = "platform-icon";
    iconWrap.innerHTML = PLATFORM_SVGS[platform] || PLATFORM_SVGS.system;
    content.appendChild(iconWrap);

    // Username
    const nameEl = document.createElement("span");
    nameEl.className = "msg-username";
    nameEl.textContent = " " + username;
    content.appendChild(nameEl);

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
      content.appendChild(badgeContainer);
    }

    // Colon + message
    const textEl = document.createElement("span");
    textEl.className = "msg-text";
    textEl.textContent = ": " + messageText;
    content.appendChild(textEl);

    if (isSuperchat && superAmount) {
      const amt = document.createElement("span");
      amt.className = "superchat-amount";
      amt.textContent = ` 💰 ${superAmount}`;
      content.appendChild(amt);
    }

    el.appendChild(content);
    feed.appendChild(el);
    feed.scrollTop = feed.scrollHeight;
    trimMessages();

    if (CONFIG.fadeOutDelay > 0) {
      setTimeout(() => removeMessage(el), CONFIG.fadeOutDelay);
    }
  }

  function getBadgeClass(badge) {
    const map = {
      mod: "badge-mod", moderator: "badge-mod",
      owner: "badge-owner", broadcaster: "badge-broadcaster",
      member: "badge-member", subscriber: "badge-subscriber",
      verified: "badge-verified", vip: "badge-vip",
    };
    return map[badge.toLowerCase()] || "badge-default";
  }

  function trimMessages() {
    const msgs = feed.querySelectorAll(".chat-msg:not(.exiting)");
    if (msgs.length > CONFIG.maxMessages) {
      const excess = msgs.length - CONFIG.maxMessages;
      for (let i = 0; i < excess; i++) removeMessage(msgs[i]);
    }
  }

  function removeMessage(el) {
    if (!el?.parentNode || el.classList.contains("exiting")) return;
    el.classList.add("exiting");
    el.addEventListener("animationend", () => el.remove(), { once: true });
    setTimeout(() => el.remove(), 400);
  }

  connect();
})();
