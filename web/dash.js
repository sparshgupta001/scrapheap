(() => {
  "use strict";

  const TILE_COUNT = 1024;
  const GRID_SIZE = 32;
  const TILE_PIXELS = 64;
  const IMAGE_PIXELS = GRID_SIZE * TILE_PIXELS;
  const MAX_TILE_FETCHES = 8;
  const PALETTE = [
    "#ff6b6b", "#42d9f5", "#ffd166", "#9b8cff", "#52e0a1",
    "#ff9f43", "#f06de2", "#8bd450", "#56a7ff", "#ff8270",
    "#d0a2ff", "#e5d94d", "#37d6c2", "#ff75aa", "#b4dc55",
    "#76a9ff"
  ];

  const imageCanvas = document.getElementById("image-canvas");
  const imageContext = imageCanvas.getContext("2d", { alpha: false });
  const offscreen = document.createElement("canvas");
  offscreen.width = IMAGE_PIXELS;
  offscreen.height = IMAGE_PIXELS;
  const offscreenContext = offscreen.getContext("2d", { alpha: false });
  const tileStates = new Array(TILE_COUNT).fill("Q");
  const tilePixels = new Array(TILE_COUNT).fill(null);
  const tileMetadata = new Map();
  const averageCountedTasks = new Set();
  const requeueRedUntil = new Map();
  const devices = new Map();
  const deviceAverages = new Map();
  const fetchQueue = [];
  const queuedFetches = new Set();
  const fetchingTiles = new Set();
  const retryTimers = new Map();
  const chartSamples = [];
  const chartMarkers = [];
  const tickerItems = [];
  let activeFetches = 0;
  let lastSnapshot = null;
  let tintEnabled = false;
  let websocket = null;
  let reconnectDelay = 500;
  let toastTimer = 0;
  let flashTimer = 0;
  let pointerTile = -1;

  function display(value) {
    return value === undefined || value === null || value === "" || Number.isNaN(value) ? "-" : String(value);
  }

  function colorForDevice(id) {
    if (!id) return "#8190a3";
    let hash = 0;
    for (let index = 0; index < id.length; index++) {
      hash = (hash * 31 + id.charCodeAt(index)) | 0;
    }
    return PALETTE[(hash >>> 0) % PALETTE.length];
  }

  function setConnection(connected) {
    const state = document.querySelector(".connection-state");
    state.classList.toggle("connected", connected);
    document.getElementById("connection-label").textContent = connected ? "connected" : "disconnected";
  }

  function paintChecker(task) {
    const row = Math.floor(task / GRID_SIZE);
    const col = task % GRID_SIZE;
    const x = col * TILE_PIXELS;
    const y = row * TILE_PIXELS;
    const alternate = ((row + col) % 2) === 0;
    offscreenContext.fillStyle = alternate ? "#171d28" : "#111722";
    offscreenContext.fillRect(x, y, TILE_PIXELS, TILE_PIXELS);
  }

  function paintTile(task, shouldBlit = true) {
    if (task < 0 || task >= TILE_COUNT) return;
    const row = Math.floor(task / GRID_SIZE);
    const col = task % GRID_SIZE;
    const x = col * TILE_PIXELS;
    const y = row * TILE_PIXELS;
    const state = tileStates[task];

    if (tilePixels[task]) {
      const image = new ImageData(tilePixels[task], TILE_PIXELS, TILE_PIXELS);
      offscreenContext.putImageData(image, x, y);
      const metadata = tileMetadata.get(task);
      if (tintEnabled && metadata && metadata.device) {
        offscreenContext.globalAlpha = 0.35;
        offscreenContext.fillStyle = colorForDevice(metadata.device);
        offscreenContext.fillRect(x, y, TILE_PIXELS, TILE_PIXELS);
        offscreenContext.globalAlpha = 1;
      }
    } else {
      paintChecker(task);
    }

    if (state === "L") {
      offscreenContext.strokeStyle = "#ffc45c";
      offscreenContext.lineWidth = 3;
      offscreenContext.strokeRect(x + 2, y + 2, TILE_PIXELS - 4, TILE_PIXELS - 4);
    } else if (state === "R") {
      offscreenContext.strokeStyle = Date.now() < (requeueRedUntil.get(task) || 0) ? "#ff5964" : "#d89a55";
      offscreenContext.lineWidth = 4;
      offscreenContext.strokeRect(x + 2, y + 2, TILE_PIXELS - 4, TILE_PIXELS - 4);
    }
    if (shouldBlit) blitImage();
  }

  function blitImage() {
    const context = imageCanvas.getContext("2d", { alpha: false });
    context.imageSmoothingEnabled = false;
    context.clearRect(0, 0, imageCanvas.width, imageCanvas.height);
    context.drawImage(offscreen, 0, 0, imageCanvas.width, imageCanvas.height);
  }

  function repaintVisibleCanvas() {
    const ratio = Math.max(1, window.devicePixelRatio || 1);
    const bounds = imageCanvas.getBoundingClientRect();
    const physicalWidth = Math.max(1, Math.round(bounds.width * ratio));
    const physicalHeight = Math.max(1, Math.round(bounds.height * ratio));
    if (imageCanvas.width !== physicalWidth || imageCanvas.height !== physicalHeight) {
      imageCanvas.width = physicalWidth;
      imageCanvas.height = physicalHeight;
    }
    blitImage();
    drawChart();
  }

  function enqueueTileFetch(task, retry = 0) {
    if (task < 0 || task >= TILE_COUNT || tilePixels[task] || queuedFetches.has(task) ||
        fetchingTiles.has(task) || retryTimers.has(task)) return;
    queuedFetches.add(task);
    fetchQueue.push({ task, retry });
    pumpTileFetches();
  }

  function pumpTileFetches() {
    while (activeFetches < MAX_TILE_FETCHES && fetchQueue.length > 0) {
      const item = fetchQueue.shift();
      queuedFetches.delete(item.task);
      if (tilePixels[item.task]) continue;
      activeFetches++;
      fetchingTiles.add(item.task);
      fetch(`/api/tile/${item.task}`, { cache: "no-store" })
        .then((response) => {
          if (!response.ok) throw new Error(`tile ${item.task}: HTTP ${response.status}`);
          return response.arrayBuffer();
        })
        .then((buffer) => {
          if (buffer.byteLength !== TILE_PIXELS * TILE_PIXELS * 4) {
            throw new Error(`tile ${item.task}: expected 16384 bytes, got ${buffer.byteLength}`);
          }
          tilePixels[item.task] = new Uint8ClampedArray(buffer);
          const row = Math.floor(item.task / GRID_SIZE);
          const col = item.task % GRID_SIZE;
          offscreenContext.putImageData(new ImageData(tilePixels[item.task], TILE_PIXELS, TILE_PIXELS), col * TILE_PIXELS, row * TILE_PIXELS);
          paintTileOverlay(item.task);
          blitImage();
        })
        .catch(() => {
          const delay = Math.min(10000, 350 * (2 ** Math.min(item.retry, 5)));
          const timer = window.setTimeout(() => {
            retryTimers.delete(item.task);
            enqueueTileFetch(item.task, item.retry + 1);
          }, delay);
          retryTimers.set(item.task, timer);
        })
        .finally(() => {
          fetchingTiles.delete(item.task);
          activeFetches--;
          pumpTileFetches();
        });
    }
  }

  function paintTileOverlay(task) {
    const row = Math.floor(task / GRID_SIZE);
    const col = task % GRID_SIZE;
    const x = col * TILE_PIXELS;
    const y = row * TILE_PIXELS;
    const metadata = tileMetadata.get(task);
    if (tintEnabled && metadata && metadata.device) {
      offscreenContext.globalAlpha = 0.35;
      offscreenContext.fillStyle = colorForDevice(metadata.device);
      offscreenContext.fillRect(x, y, TILE_PIXELS, TILE_PIXELS);
      offscreenContext.globalAlpha = 1;
    }
    if (tileStates[task] === "L") {
      offscreenContext.strokeStyle = "#ffc45c";
      offscreenContext.lineWidth = 3;
      offscreenContext.strokeRect(x + 2, y + 2, TILE_PIXELS - 4, TILE_PIXELS - 4);
    } else if (tileStates[task] === "R") {
      offscreenContext.strokeStyle = Date.now() < (requeueRedUntil.get(task) || 0) ? "#ff5964" : "#d89a55";
      offscreenContext.lineWidth = 4;
      offscreenContext.strokeRect(x + 2, y + 2, TILE_PIXELS - 4, TILE_PIXELS - 4);
    }
  }

  function noteTileInfo(info) {
    if (!info || !Number.isInteger(info.task) || info.task < 0 || info.task >= TILE_COUNT) return;
    const alreadyCounted = averageCountedTasks.has(info.task);
    tileMetadata.set(info.task, {
      device: typeof info.device === "string" ? info.device : "",
      computeMs: info.computeMs,
      attempts: info.attempts
    });
    if (!alreadyCounted) {
      averageCountedTasks.add(info.task);
      updateDeviceAverage(info.device, info.computeMs);
    }
    if (tilePixels[info.task]) paintTile(info.task);
    enqueueTileFetch(info.task);
  }

  function updateDeviceAverage(device, computeMs) {
    if (!device || !Number.isFinite(computeMs)) return;
    const current = deviceAverages.get(device) || { sum: 0, count: 0 };
    current.sum += computeMs;
    current.count++;
    deviceAverages.set(device, current);
  }

  function updateSnapshot(snapshot) {
    lastSnapshot = snapshot;
    document.getElementById("tiles-per-sec").textContent = display(snapshot.tilesPerSec);
    document.getElementById("device-count").textContent = Array.isArray(snapshot.devices) ? display(snapshot.devices.length) : "-";
    document.getElementById("done-total").textContent = `${display(snapshot.done)} / ${display(snapshot.total)}`;
    const progress = Number.isFinite(snapshot.done) && Number.isFinite(snapshot.total) && snapshot.total > 0
      ? Math.max(0, Math.min(100, snapshot.done / snapshot.total * 100))
      : 0;
    document.getElementById("progress-fill").style.width = `${progress}%`;
    const verifiedTotal = Number.isFinite(snapshot.verifiedOK) && Number.isFinite(snapshot.verifiedFail)
      ? snapshot.verifiedOK + snapshot.verifiedFail
      : "-";
    const badge = document.getElementById("verified-badge");
    badge.textContent = `Verified: ${display(snapshot.verifiedOK)}/${display(verifiedTotal)}`;
    badge.classList.toggle("good", snapshot.job === "done" && snapshot.verifiedFail === 0);
    badge.classList.toggle("failed", Number(snapshot.verifiedFail) > 0);

    if (Number.isFinite(snapshot.t) && Number.isFinite(snapshot.tilesPerSec)) {
      chartSamples.push({ t: snapshot.t, rate: snapshot.tilesPerSec });
      while (chartSamples.length && snapshot.t - chartSamples[0].t > 60) chartSamples.shift();
      drawChart();
    }
    if (typeof snapshot.tileStates === "string") {
      const length = Math.min(TILE_COUNT, snapshot.tileStates.length);
      let changed = false;
      for (let task = 0; task < length; task++) {
        const next = snapshot.tileStates[task];
        if (next === tileStates[task]) continue;
        tileStates[task] = next;
        paintTile(task, false);
        changed = true;
        if (next === "R") flashRequeuedTile(task);
        if (next === "D" && !tilePixels[task]) enqueueTileFetch(task);
      }
      if (changed) blitImage();
    }
    if (Array.isArray(snapshot.devices)) {
      devices.clear();
      for (const device of snapshot.devices) {
        if (device && typeof device.id === "string") devices.set(device.id, device);
      }
      renderDevices(snapshot.devices);
    }
    document.getElementById("debug-overlay").textContent = JSON.stringify(snapshot, null, 2);
    renderLegend();
  }

  function recordEvent(event) {
    const time = Number.isFinite(event.t) ? event.t : null;
    if (event.type === "device_joined" || event.type === "device_left") {
      chartMarkers.push({ t: time, kind: event.type });
      if (chartSamples.length && Number.isFinite(time)) drawChart();
    }
    if (event.type === "tile_done") {
      const metadata = {
        task: event.task,
        device: event.device,
        computeMs: event.computeMs,
        attempts: event.attempt
      };
      noteTileInfo(metadata);
    }
    if (event.type === "tile_requeued") {
      const task = Number(event.task);
      if (Number.isInteger(task) && task >= 0 && task < TILE_COUNT) {
        tileStates[task] = "R";
        flashRequeuedTile(task);
        paintTile(task);
      }
      const frame = document.querySelector(".image-frame");
      frame.classList.remove("requeue-flash");
      void frame.offsetWidth;
      frame.classList.add("requeue-flash");
      window.clearTimeout(flashTimer);
      flashTimer = window.setTimeout(() => frame.classList.remove("requeue-flash"), 800);
    }

    function flashRequeuedTile(task) {
      const fadeAt = Date.now() + 900;
      requeueRedUntil.set(task, fadeAt);
      window.setTimeout(() => {
        if (requeueRedUntil.get(task) !== fadeAt) return;
        requeueRedUntil.set(task, 0);
        if (tileStates[task] === "R") paintTile(task);
      }, 900);
    }

    const messages = {
      device_joined: `${display(event.label)} joined`,
      device_left: `${display(deviceLabel(event.id))} left`,
      tile_requeued: `Tile ${display(event.task)} requeued (${display(event.reason)})`,
      tile_done: `Tile ${display(event.task)} completed by ${display(deviceLabel(event.device))}`,
      job_done: `Job complete · checksum ${display(event.finalChecksum)}`
    };
    if (!messages[event.type]) return;
    tickerItems.unshift({ t: time, text: messages[event.type], kind: event.type });
    tickerItems.length = Math.min(tickerItems.length, 8);
    renderTicker();
  }

  function deviceLabel(id) {
    const device = devices.get(id);
    return device && device.label ? device.label : id || "-";
  }

  function renderDevices(deviceList) {
    const list = document.getElementById("device-list");
    list.replaceChildren();
    const sorted = [...deviceList].sort((a, b) => (Number(b.tilesDone) || 0) - (Number(a.tilesDone) || 0));
    const maximum = Math.max(1, ...sorted.map((device) => Number(device.tilesDone) || 0));
    for (const device of sorted) {
      const row = document.createElement("div");
      row.className = `device-row${device.state === "gone" ? " gone" : ""}`;
      const name = document.createElement("div");
      name.className = "device-name";
      name.style.color = device.state === "gone" ? "" : colorForDevice(device.id);
      name.textContent = display(device.label);
      const meter = document.createElement("div");
      meter.className = "device-meter";
      const fill = document.createElement("span");
      fill.style.width = `${Math.max(0, Math.min(100, (Number(device.tilesDone) || 0) / maximum * 100))}%`;
      fill.style.background = colorForDevice(device.id);
      if (device.state === "gone") fill.style.opacity = "0.35";
      meter.append(fill);
      const numbers = document.createElement("div");
      numbers.className = "device-numbers";
      numbers.textContent = `${display(device.tilesDone)} · ${display(device.threads)} th · ${display(device.tilesPerSec)}/s`;
      const average = deviceAverages.get(device.id);
      const subline = document.createElement("div");
      subline.className = "device-subline";
      subline.textContent = `avg compute ${average && average.count ? Math.round(average.sum / average.count) : "-"} ms`;
      row.append(name, meter, numbers, subline);
      list.append(row);
    }
    renderLegend();
  }

  function renderLegend() {
    const legend = document.getElementById("device-legend");
    legend.replaceChildren();
    if (!tintEnabled) return;
    for (const device of devices.values()) {
      const item = document.createElement("span");
      item.className = "legend-item";
      const swatch = document.createElement("span");
      swatch.className = "legend-swatch";
      swatch.style.background = colorForDevice(device.id);
      const label = document.createElement("span");
      label.textContent = display(device.label);
      item.append(swatch, label);
      legend.append(item);
    }
  }

  function renderTicker() {
    const list = document.getElementById("event-list");
    list.replaceChildren();
    for (const item of tickerItems) {
      const row = document.createElement("li");
      row.className = `event-item${item.kind === "tile_requeued" ? " requeue" : ""}`;
      const time = document.createElement("span");
      time.className = "event-time";
      time.textContent = Number.isFinite(item.t) ? `${item.t.toFixed(1)}s` : "-";
      const text = document.createElement("span");
      text.textContent = item.text;
      row.append(time, text);
      list.append(row);
    }
  }

  function drawChart() {
    const canvas = document.getElementById("rate-chart");
    if (!canvas) return;
    const ratio = Math.max(1, window.devicePixelRatio || 1);
    const bounds = canvas.getBoundingClientRect();
    const width = Math.max(1, Math.floor(bounds.width * ratio));
    const height = Math.max(1, Math.floor(bounds.height * ratio));
    if (canvas.width !== width || canvas.height !== height) {
      canvas.width = width;
      canvas.height = height;
    }
    const context = canvas.getContext("2d");
    context.clearRect(0, 0, width, height);
    const left = 34 * ratio;
    const right = width - 8 * ratio;
    const top = 8 * ratio;
    const bottom = height - 22 * ratio;
    const samples = chartSamples;
    const newest = samples.length ? samples[samples.length - 1].t : 0;
    const oldest = Math.max(0, newest - 60);
    const maximum = Math.max(1, ...samples.map((sample) => sample.rate));
    context.font = `${10 * ratio}px Segoe UI, Arial`;
    context.strokeStyle = "#2b394a";
    context.fillStyle = "#8a9aaf";
    context.lineWidth = ratio;
    for (let line = 0; line <= 3; line++) {
      const y = top + (bottom - top) * line / 3;
      context.beginPath();
      context.moveTo(left, y);
      context.lineTo(right, y);
      context.stroke();
      context.fillText((maximum * (1 - line / 3)).toFixed(0), 2 * ratio, y + 3 * ratio);
    }
    const timeSpan = Math.max(1, newest - oldest);
    for (const marker of chartMarkers) {
      if (!Number.isFinite(marker.t) || marker.t < oldest || marker.t > newest) continue;
      const x = left + (marker.t - oldest) / timeSpan * (right - left);
      context.strokeStyle = marker.kind === "device_joined" ? "#53d99a" : "#ff777f";
      context.setLineDash([3 * ratio, 3 * ratio]);
      context.beginPath();
      context.moveTo(x, top);
      context.lineTo(x, bottom);
      context.stroke();
    }
    context.setLineDash([]);
    if (samples.length) {
      context.strokeStyle = "#5be49b";
      context.lineWidth = 2 * ratio;
      context.beginPath();
      samples.forEach((sample, index) => {
        const x = left + (sample.t - oldest) / timeSpan * (right - left);
        const y = bottom - sample.rate / maximum * (bottom - top);
        if (index === 0) context.moveTo(x, y);
        else context.lineTo(x, y);
      });
      context.stroke();
    }
    context.fillStyle = "#8292a6";
    context.fillText(`${oldest.toFixed(0)}s`, left, height - 5 * ratio);
    context.fillText(`${newest.toFixed(0)}s`, right - 28 * ratio, height - 5 * ratio);
  }

  function showToast(message) {
    const toast = document.getElementById("toast");
    toast.textContent = message;
    toast.classList.add("visible");
    window.clearTimeout(toastTimer);
    toastTimer = window.setTimeout(() => toast.classList.remove("visible"), 3500);
  }

  async function runAgain(config) {
    try {
      const suffix = config ? `?config=${encodeURIComponent(config)}` : "";
      const response = await fetch(`/api/run-again${suffix}`, { method: "POST" });
      if (!response.ok) throw new Error(`HTTP ${response.status}`);
    } catch (error) {
      showToast(`Could not start run: ${error.message}`);
    }
  }

  function connectWebSocket() {
    const scheme = window.location.protocol === "https:" ? "wss:" : "ws:";
    const socket = new WebSocket(`${scheme}//${window.location.host}/ws/dash`);
    websocket = socket;
    socket.addEventListener("open", () => {
      reconnectDelay = 500;
      setConnection(true);
    });
    socket.addEventListener("message", (message) => {
      try {
        const data = JSON.parse(message.data);
        if (data.type === "snapshot") updateSnapshot(data);
        else recordEvent(data);
      } catch {
        showToast("Received an unreadable dashboard update");
      }
    });
    socket.addEventListener("close", () => {
      if (websocket !== socket) return;
      setConnection(false);
      window.setTimeout(connectWebSocket, reconnectDelay);
      reconnectDelay = Math.min(15000, reconnectDelay * 2);
    });
    socket.addEventListener("error", () => socket.close());
  }

  function showTileTooltip(event) {
    const bounds = imageCanvas.getBoundingClientRect();
    const col = Math.floor((event.clientX - bounds.left) / bounds.width * GRID_SIZE);
    const row = Math.floor((event.clientY - bounds.top) / bounds.height * GRID_SIZE);
    if (row < 0 || row >= GRID_SIZE || col < 0 || col >= GRID_SIZE) return;
    const task = row * GRID_SIZE + col;
    pointerTile = task;
    const metadata = tileMetadata.get(task);
    const tooltip = document.getElementById("tile-tooltip");
    tooltip.innerHTML = "";
    const entries = [
      `Tile ${task} · row ${row}, col ${col}`,
      `Device: ${metadata ? deviceLabel(metadata.device) : "-"}`,
      `Compute: ${metadata && metadata.computeMs !== undefined ? metadata.computeMs : "-"} ms`,
      `Attempts: ${metadata && metadata.attempts !== undefined ? metadata.attempts : "-"}`
    ];
    for (const entry of entries) {
      const line = document.createElement("div");
      line.textContent = entry;
      tooltip.append(line);
    }
    const frame = document.querySelector(".image-frame").getBoundingClientRect();
    tooltip.style.left = `${Math.min(event.clientX - frame.left + 12, frame.width - 240)}px`;
    tooltip.style.top = `${Math.max(0, event.clientY - frame.top + 12)}px`;
    tooltip.hidden = false;
  }

  function installControls() {
    document.getElementById("run-again").addEventListener("click", () => runAgain(""));
    document.getElementById("run-baseline").addEventListener("click", () => runAgain("B"));
    document.getElementById("tint-toggle").addEventListener("click", toggleTint);
    imageCanvas.addEventListener("mousemove", showTileTooltip);
    imageCanvas.addEventListener("mouseleave", () => {
      pointerTile = -1;
      document.getElementById("tile-tooltip").hidden = true;
    });
    window.addEventListener("resize", repaintVisibleCanvas);
    window.addEventListener("keydown", (event) => {
      if (event.target instanceof HTMLInputElement || event.target instanceof HTMLTextAreaElement) return;
      const key = event.key.toLowerCase();
      if (key === "f") {
        if (!document.fullscreenElement) document.documentElement.requestFullscreen().catch(() => {});
        else document.exitFullscreen().catch(() => {});
      } else if (key === "d") {
        const debug = document.getElementById("debug-overlay");
        debug.hidden = !debug.hidden;
      } else if (key === "t") {
        toggleTint();
      }
    });
  }

  function toggleTint() {
    tintEnabled = !tintEnabled;
    const button = document.getElementById("tint-toggle");
    button.setAttribute("aria-pressed", String(tintEnabled));
    for (let task = 0; task < TILE_COUNT; task++) paintTile(task, false);
    blitImage();
    renderLegend();
  }

  async function loadTileMetadata() {
    try {
      const response = await fetch("/api/tiles", { cache: "no-store" });
      if (!response.ok) throw new Error(`HTTP ${response.status}`);
      const list = await response.json();
      if (!Array.isArray(list)) throw new Error("tile metadata response is not an array");
      for (const info of list) noteTileInfo(info);
    } catch (error) {
      showToast(`Could not load completed tile list: ${error.message}`);
    }
  }

  function loadJoinURL() {
    fetch("/api/joinurl", { cache: "no-store" })
      .then((response) => {
        if (!response.ok) throw new Error(`HTTP ${response.status}`);
        return response.json();
      })
      .then((data) => {
        const link = document.getElementById("join-url");
        if (typeof data.url !== "string" || !data.url) throw new Error("URL missing");
        link.textContent = data.url;
        link.href = data.url;
      })
      .catch((error) => {
        document.getElementById("join-url").textContent = `Join URL unavailable: ${error.message}`;
      });
  }

  for (let task = 0; task < TILE_COUNT; task++) paintChecker(task);
  installControls();
  repaintVisibleCanvas();
  loadJoinURL();
  loadTileMetadata();
  connectWebSocket();
})();
