/**
 * StreamChat Dashboard JS
 * Manages adapter controls, live message preview, and status polling.
 */

(function () {
  "use strict";

  let ws = null;
  let msgCount = 0;
  const recentFeed = document.getElementById("recent-feed");
  const adapterList = document.getElementById("adapter-list");
  const wsStatusDot = document.getElementById("ws-status-dot");
  const wsStatusLabel = document.getElementById("ws-status-label");
  const msgCountEl = document.getElementById("msg-count");

  // Set OBS URL hint
  document.getElementById("obs-url").textContent = location.origin + "/";

  // ── WebSocket ─────────────────────────────────────────────────────────────

  function connectWS() {
    const protocol = location.protocol === "https:" ? "wss:" : "ws:";
    const url = `${protocol}//${location.host}/ws`;
    ws = new WebSocket(url);

    ws.onopen = () => {
      wsStatusDot.classList.remove("disconnected");
      wsStatusLabel.textContent = "Connected";
    };

    ws.onmessage = (event) => {
      if (event.data === "pong") return;
      try {
        const msg = JSON.parse(event.data);
        addRecentMessage(msg);
        msgCount++;
        msgCountEl.textContent = `${msgCount.toLocaleString()} messages`;
      } catch (e) {}
    };

    ws.onclose = () => {
      wsStatusDot.classList.add("disconnected");
      wsStatusLabel.textContent = "Disconnected";
      setTimeout(connectWS, 3000);
    };
  }

  // ── Recent message feed ───────────────────────────────────────────────────

  function addRecentMessage(msg) {
    const empty = recentFeed.querySelector(".empty-state");
    if (empty) empty.remove();

    const el = document.createElement("div");
    el.className = "recent-msg";
    el.setAttribute("data-platform", msg.platform || "system");

    el.innerHTML = `
      <div class="r-user">${esc(msg.username)} <small style="font-weight:400;opacity:.5">[${esc(msg.platform)}]</small></div>
      <div class="r-text">${esc(msg.message)}</div>
    `;

    recentFeed.prepend(el);

    // Keep max 100 in list
    const all = recentFeed.querySelectorAll(".recent-msg");
    if (all.length > 100) {
      all[all.length - 1].remove();
    }
  }

  // ── Status polling ────────────────────────────────────────────────────────

  async function pollStatus() {
    try {
      const resp = await fetch("/api/status");
      if (!resp.ok) return;
      const data = await resp.json();
      renderAdapters(data.adapters || {});
    } catch (e) {}
  }

  function renderAdapters(adapters) {
    adapterList.innerHTML = "";

    const platforms = ["youtube", "twitch", "kick", "rumble", "facebook", "instagram"];
    const allPlatforms = new Set([...platforms, ...Object.keys(adapters)]);

    for (const platform of allPlatforms) {
      const info = adapters[platform];
      const isRunning = info?.running ?? false;
      const count = info?.messages_received ?? 0;

      const card = document.createElement("div");
      card.className = "adapter-card";
      card.innerHTML = `
        <div class="adapter-dot ${isRunning ? "" : "off"}"></div>
        <div class="adapter-info">
          <div class="adapter-name">${capitalize(platform)}</div>
          <div class="adapter-stat">${isRunning ? `${count} msgs` : "Not connected"}</div>
        </div>
        ${isRunning ? `<button class="adapter-stop-btn" onclick="stopAdapter('${platform}')">Stop</button>` : ""}
      `;
      adapterList.appendChild(card);
    }
  }

  // ── Adapter controls ──────────────────────────────────────────────────────

  window.updateYouTube = async function () {
    const videoId = document.getElementById("yt-video-id").value.trim();
    if (!videoId) return alert("Enter a YouTube video ID");
    const resp = await fetch("/api/youtube/update", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ video_id: videoId }),
    });
    const data = await resp.json();
    alert(data.status === "updated" ? `✅ YouTube updated: ${videoId}` : `Error: ${JSON.stringify(data)}`);
    pollStatus();
  };

  window.startTwitch = async function () {
    const channel = document.getElementById("twitch-channel").value.trim();
    if (!channel) return alert("Enter a Twitch channel name");
    const resp = await fetch("/api/adapter/twitch/start", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ channel }),
    });
    const data = await resp.json();
    alert(data.status === "started" ? `✅ Twitch connected: #${channel}` : `Error: ${JSON.stringify(data)}`);
    pollStatus();
  };

  window.startKick = async function () {
    const channel = document.getElementById("kick-channel").value.trim();
    if (!channel) return alert("Enter a Kick channel name");
    const resp = await fetch("/api/adapter/kick/start", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ channel }),
    });
    const data = await resp.json();
    alert(data.status === "started" ? `✅ Kick connected: ${channel}` : `Error: ${JSON.stringify(data)}`);
    pollStatus();
  };

  window.startRumble = async function () {
    const stream_url = document.getElementById("rumble-url").value.trim();
    if (!stream_url) return alert("Enter a Rumble stream URL");
    const resp = await fetch("/api/adapter/rumble/start", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ stream_url }),
    });
    const data = await resp.json();
    alert(data.status === "started" ? `✅ Rumble connected` : `Error: ${JSON.stringify(data)}`);
    pollStatus();
  };

  window.stopAdapter = async function (platform) {
    await fetch(`/api/adapter/${platform}/stop`, { method: "POST" });
    pollStatus();
  };

  // ── Helpers ───────────────────────────────────────────────────────────────

  function esc(str) {
    const d = document.createElement("div");
    d.textContent = str || "";
    return d.innerHTML;
  }

  function capitalize(s) {
    return s.charAt(0).toUpperCase() + s.slice(1);
  }

  // ── Boot ──────────────────────────────────────────────────────────────────
  connectWS();
  pollStatus();
  setInterval(pollStatus, 5000);

})();
