const $ = (id) => document.getElementById(id);

const KEY_WEBSITE_ID = "tiara.website.unique.id";
const KEY_MQTT_CFG = "tiara.mqtt.config";
const KEY_DEVICE_ID = "tiara.device.id";
const KEY_INSTRUMENT_IDS = "tiara.instrument.ids";
const KEY_INSTRUMENT_NAMES = "tiara.instrument.names";
const KEY_DEFAULT_INSTRUMENT_ID = "tiara.default.instrument.id";
// In cloud-hosted mode these are the defaults used by the PWA.
const CLOUD_MQTT_DEFAULTS = {
  BROKER_HOST: "pgiatis.dyndns.org",
  BROKER_PORT: 9001,
  MQTT_USERNAME: "BisinaSystems",
  MQTT_PASSWORD: "BisinaSystems123",
  MQTT_CLIENT_PREFIX: "TIARA",
  PROJECT_TOPIC_ROOT: "tiara"
};

let mqttClient = null;
let reconnectTimer = null;
let mqttConfig = null;
let topicBase = "";
let activeInstrumentId = "";
let gLoadedFromDeviceApi = false;
const graphBuffer = [];
const graphBufferSize = 120;
let graphDrawPending = false;
let lastPacketReceiveMs = 0;

function loadKnownInstrumentIds() {
  try {
    const raw = JSON.parse(localStorage.getItem(KEY_INSTRUMENT_IDS) || "[]");
    if (!Array.isArray(raw)) return [];
    const seen = new Set();
    const out = [];
    for (const value of raw) {
      const id = normalizeUniqueId(value);
      if (!id || seen.has(id)) continue;
      seen.add(id);
      out.push(id);
    }
    return out;
  } catch (_) {
    return [];
  }
}

function saveKnownInstrumentIds(ids) {
  localStorage.setItem(KEY_INSTRUMENT_IDS, JSON.stringify(ids));
}

function getDefaultInstrumentId() {
  return normalizeUniqueId(localStorage.getItem(KEY_DEFAULT_INSTRUMENT_ID) || "");
}

function setDefaultInstrumentId(id) {
  const safeId = normalizeUniqueId(id);
  if (safeId) {
    localStorage.setItem(KEY_DEFAULT_INSTRUMENT_ID, safeId);
  } else {
    localStorage.removeItem(KEY_DEFAULT_INSTRUMENT_ID);
  }
}

function exportInstrumentsToFile() {
  const payload = {
    version: 1,
    exportedAt: new Date().toISOString(),
    ids: loadKnownInstrumentIds(),
    names: loadInstrumentNames(),
    defaultId: getDefaultInstrumentId(),
    activeId: activeInstrumentId || ""
  };

  const blob = new Blob([JSON.stringify(payload, null, 2)], { type: "application/json" });
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url;
  a.download = "tiara-instruments.json";
  document.body.appendChild(a);
  a.click();
  document.body.removeChild(a);
  URL.revokeObjectURL(url);
  setStatus("#4ade80", "Instrument list exported");
}

function applyInstrumentImportPayload(payload) {
  if (!payload || typeof payload !== "object") {
    throw new Error("Invalid import file");
  }

  const importedIds = Array.isArray(payload.ids) ? payload.ids : [];
  const normalizedIds = [];
  const seen = new Set();
  for (const rawId of importedIds) {
    const id = normalizeUniqueId(rawId);
    if (!id || seen.has(id)) continue;
    seen.add(id);
    normalizedIds.push(id);
  }

  if (normalizedIds.length === 0) {
    throw new Error("No valid instrument IDs in import file");
  }

  saveKnownInstrumentIds(normalizedIds);

  const rawNames = payload.names && typeof payload.names === "object" ? payload.names : {};
  const cleanNames = {};
  for (const key of Object.keys(rawNames)) {
    const id = normalizeUniqueId(key);
    const name = String(rawNames[key] || "").trim();
    if (!id || !name) continue;
    if (!normalizedIds.includes(id)) continue;
    cleanNames[id] = name;
  }
  saveInstrumentNames(cleanNames);

  const importedDefault = normalizeUniqueId(payload.defaultId || "");
  if (importedDefault && normalizedIds.includes(importedDefault)) {
    setDefaultInstrumentId(importedDefault);
  } else {
    setDefaultInstrumentId(normalizedIds[0]);
  }

  const importedActive = normalizeUniqueId(payload.activeId || "");
  const nextActive = importedActive && normalizedIds.includes(importedActive)
    ? importedActive
    : (getDefaultInstrumentId() || normalizedIds[0]);

  selectInstrument(nextActive, true);
  renderInstrumentManagerList();
  setStatus("#4ade80", `Imported ${normalizedIds.length} instrument(s)`);
}

function importInstrumentsFromFile(file) {
  if (!file) return;
  const reader = new FileReader();
  reader.onload = () => {
    try {
      const payload = JSON.parse(String(reader.result || "{}"));
      applyInstrumentImportPayload(payload);
    } catch (error) {
      const message = error && error.message ? error.message : "Import failed";
      setStatus("#ef4444", message);
    }
  };
  reader.onerror = () => setStatus("#ef4444", "Failed to read import file");
  reader.readAsText(file);
}

function setCurrentInstrumentAsDefault() {
  if (!activeInstrumentId) return;
  setDefaultInstrumentId(activeInstrumentId);
  renderInstrumentSelector();
  renderInstrumentManagerList();
  setStatus("#4ade80", `Default set to ${getInstrumentDisplayName(activeInstrumentId)}`);
}

function loadInstrumentNames() {
  try {
    const raw = JSON.parse(localStorage.getItem(KEY_INSTRUMENT_NAMES) || "{}");
    if (!raw || typeof raw !== "object") return {};
    const out = {};
    for (const key of Object.keys(raw)) {
      const id = normalizeUniqueId(key);
      const name = String(raw[key] || "").trim();
      if (id && name) {
        out[id] = name;
      }
    }
    return out;
  } catch (_) {
    return {};
  }
}

function saveInstrumentNames(names) {
  localStorage.setItem(KEY_INSTRUMENT_NAMES, JSON.stringify(names));
}

function setInstrumentName(id, name) {
  const safeId = normalizeUniqueId(id);
  if (!safeId) return;

  const names = loadInstrumentNames();
  const nextName = String(name || "").trim();
  if (nextName) {
    names[safeId] = nextName;
  } else {
    delete names[safeId];
  }
  saveInstrumentNames(names);
}

function getInstrumentDisplayName(id) {
  const safeId = normalizeUniqueId(id);
  if (!safeId) return "";
  const names = loadInstrumentNames();
  if (names[safeId]) {
    return `${names[safeId]} (${safeId.toUpperCase()})`;
  }
  return safeId.toUpperCase();
}

function refreshInstrumentNameEditor() {
  const input = $("instrumentNameInput");
  if (!input) return;
  const names = loadInstrumentNames();
  input.value = activeInstrumentId ? (names[activeInstrumentId] || "") : "";
}

function addKnownInstrumentId(value) {
  const id = normalizeUniqueId(value);
  if (!id) return loadKnownInstrumentIds();

  const ids = loadKnownInstrumentIds();
  if (!ids.includes(id)) {
    ids.push(id);
    saveKnownInstrumentIds(ids);
  }
  return ids;
}

function updateInstrumentHash(id) {
  const safeId = normalizeUniqueId(id);
  if (!safeId) return;
  window.history.replaceState({}, "", `${window.location.pathname}${window.location.search}#${encodeURIComponent(safeId)}`);
}

function renderInstrumentSelector() {
  const select = $("instrumentSelect");
  if (!select) return;

  const ids = loadKnownInstrumentIds();
  const defaultId = getDefaultInstrumentId();
  select.innerHTML = "";

  if (ids.length === 0) {
    const opt = document.createElement("option");
    opt.value = "";
    opt.textContent = "No instruments";
    select.appendChild(opt);
    select.disabled = true;
    return;
  }

  select.disabled = false;
  for (const id of ids) {
    const opt = document.createElement("option");
    opt.value = id;
    const defaultTag = id === defaultId ? " [Default]" : "";
    opt.textContent = `${getInstrumentDisplayName(id)}${defaultTag}`;
    select.appendChild(opt);
  }

  if (activeInstrumentId && ids.includes(activeInstrumentId)) {
    select.value = activeInstrumentId;
  }

  refreshInstrumentNameEditor();
}

function selectInstrument(id, reconnect = true) {
  const nextId = normalizeUniqueId(id);
  if (!nextId || !mqttConfig) return;

  addKnownInstrumentId(nextId);
  activeInstrumentId = nextId;
  localStorage.setItem(KEY_WEBSITE_ID, nextId);
  localStorage.setItem(KEY_DEVICE_ID, nextId);

  mqttConfig.WEBSITE_UNIQUE_ID = nextId;
  topicBase = buildTopicBase();
  graphBuffer.length = 0;
  drawCurrentGraph();
  updateInstrumentHash(nextId);
  setDynamicManifest(nextId);
  updateIdentityPanel();
  renderInstrumentSelector();

  if (reconnect) {
    connectMqtt();
  }
}

function addInstrumentFromInput() {
  const input = $("instrumentIdInput");
  if (!input) return;
  const typed = normalizeUniqueId(input.value);
  if (!typed) return;
  selectInstrument(typed, true);
  input.value = "";
}

function saveSelectedInstrumentName() {
  if (!activeInstrumentId) return;
  const input = $("instrumentNameInput");
  if (!input) return;
  setInstrumentName(activeInstrumentId, input.value);
  renderInstrumentSelector();
  updateIdentityPanel();
}

function openInstrumentManagerDialog() {
  renderInstrumentManagerList();
  const dialog = $("instrumentManagerDialog");
  if (dialog) dialog.showModal();
}

function closeInstrumentManagerDialog() {
  const dialog = $("instrumentManagerDialog");
  if (dialog) dialog.close();
}

function moveInstrument(id, direction) {
  const safeId = normalizeUniqueId(id);
  if (!safeId) return;
  const ids = loadKnownInstrumentIds();
  const index = ids.indexOf(safeId);
  if (index < 0) return;

  const target = index + direction;
  if (target < 0 || target >= ids.length) return;

  const temp = ids[index];
  ids[index] = ids[target];
  ids[target] = temp;
  saveKnownInstrumentIds(ids);
  renderInstrumentSelector();
  renderInstrumentManagerList();
}

function renameInstrumentFromDialog(id) {
  const safeId = normalizeUniqueId(id);
  if (!safeId) return;
  const names = loadInstrumentNames();
  const current = names[safeId] || "";
  const entered = prompt("Instrument nickname:", current);
  if (entered === null) return;
  setInstrumentName(safeId, entered);
  renderInstrumentSelector();
  renderInstrumentManagerList();
  updateIdentityPanel();
}

function setDefaultInstrumentFromDialog(id) {
  const safeId = normalizeUniqueId(id);
  if (!safeId) return;
  setDefaultInstrumentId(safeId);
  renderInstrumentSelector();
  renderInstrumentManagerList();
}

function deleteInstrumentFromDialog(id) {
  const safeId = normalizeUniqueId(id);
  if (!safeId) return;
  const display = getInstrumentDisplayName(safeId);
  if (!confirm(`Remove instrument ${display}?`)) return;

  const ids = loadKnownInstrumentIds().filter((item) => item !== safeId);
  saveKnownInstrumentIds(ids);

  const names = loadInstrumentNames();
  delete names[safeId];
  saveInstrumentNames(names);

  if (getDefaultInstrumentId() === safeId) {
    setDefaultInstrumentId(ids[0] || "");
  }

  if (activeInstrumentId === safeId) {
    const fallback = ids[0] || "";
    if (fallback) {
      selectInstrument(fallback, true);
    } else {
      activeInstrumentId = "";
      localStorage.removeItem(KEY_WEBSITE_ID);
      localStorage.removeItem(KEY_DEVICE_ID);
      setStatus("#ef4444", "No instruments saved. Scan a QR code to add one.");
      renderInstrumentSelector();
    }
  } else {
    renderInstrumentSelector();
  }

  renderInstrumentManagerList();
  updateIdentityPanel();
}

function renderInstrumentManagerList() {
  const list = $("instrumentManagerList");
  if (!list) return;

  const ids = loadKnownInstrumentIds();
  const defaultId = getDefaultInstrumentId();
  list.innerHTML = "";

  if (ids.length === 0) {
    const empty = document.createElement("div");
    empty.className = "instrument-manager-item-label";
    empty.textContent = "No instruments saved.";
    list.appendChild(empty);
    return;
  }

  ids.forEach((id) => {
    const row = document.createElement("div");
    row.className = "instrument-manager-item";

    const label = document.createElement("div");
    label.className = "instrument-manager-item-label";
    const name = getInstrumentDisplayName(id);
    const suffix = [];
    if (id === activeInstrumentId) suffix.push("Active");
    if (id === defaultId) suffix.push("Default");
    const state = suffix.length ? ` (${suffix.join(", ")})` : "";
    label.textContent = `${name}${state}`;

    const actions = document.createElement("div");
    actions.className = "instrument-manager-actions";

    const upBtn = document.createElement("button");
    upBtn.textContent = "Up";
    upBtn.addEventListener("click", () => moveInstrument(id, -1));

    const downBtn = document.createElement("button");
    downBtn.textContent = "Down";
    downBtn.addEventListener("click", () => moveInstrument(id, 1));

    const renameBtn = document.createElement("button");
    renameBtn.textContent = "Rename";
    renameBtn.addEventListener("click", () => renameInstrumentFromDialog(id));

    const defaultBtn = document.createElement("button");
    defaultBtn.textContent = "Set Default";
    defaultBtn.addEventListener("click", () => setDefaultInstrumentFromDialog(id));

    const deleteBtn = document.createElement("button");
    deleteBtn.textContent = "Delete";
    deleteBtn.addEventListener("click", () => deleteInstrumentFromDialog(id));

    actions.appendChild(upBtn);
    actions.appendChild(downBtn);
    actions.appendChild(renameBtn);
    actions.appendChild(defaultBtn);
    actions.appendChild(deleteBtn);

    row.appendChild(label);
    row.appendChild(actions);
    list.appendChild(row);
  });
}

function setTextIf(id, value) {
  const el = $(id);
  if (el) el.textContent = value;
}

function setStatus(color, foot) {
  $("statusDot").style.background = color;
  if (foot) setTextIf("foot", foot);
}

function setDynamicManifest(uniqueId) {
  try {
    const startUrl = uniqueId ? `./#${encodeURIComponent(uniqueId)}` : "./";
    const manifest = {
      name: "TIARA-1000",
      short_name: "TIARA",
      display: "standalone",
      start_url: startUrl,
      scope: "./",
      background_color: "#111827",
      theme_color: "#111827",
      icons: []
    };

    const blob = new Blob([JSON.stringify(manifest)], { type: "application/json" });
    const link = document.querySelector("link[rel='manifest']");
    if (link) link.href = URL.createObjectURL(blob);
  } catch (_) {
  }
}

function normalizeUniqueId(value) {
  return String(value || "").trim().toLowerCase().replace(/[^a-z0-9_-]/g, "");
}

function extractUniqueIdFromUrl() {
  const params = new URLSearchParams(window.location.search);

  const fromQuery = normalizeUniqueId(
    params.get("device") || params.get("id") || params.get("unique_id") || ""
  );
  if (fromQuery) return fromQuery;

  const hash = String(window.location.hash || "").trim();
  if (hash.startsWith("#/id/")) {
    const fromHashPath = normalizeUniqueId(hash.substring(5));
    if (fromHashPath) return fromHashPath;
  }
  if (hash.startsWith("#id=")) {
    const fromHashKeyValue = normalizeUniqueId(hash.substring(4));
    if (fromHashKeyValue) return fromHashKeyValue;
  }
  if (hash.startsWith("#")) {
    const fromHash = normalizeUniqueId(hash.substring(1));
    if (fromHash) return fromHash;
  }

  return "";
}

function parseConfigResponse(raw) {
  let savedCfg = {};
  try {
    savedCfg = JSON.parse(localStorage.getItem(KEY_MQTT_CFG) || "{}");
  } catch (_) {
    savedCfg = {};
  }

  const cfg = {
    ...CLOUD_MQTT_DEFAULTS,
    ...savedCfg,
    ...raw
  };

  const urlId = extractUniqueIdFromUrl();
  const savedId = normalizeUniqueId(localStorage.getItem(KEY_WEBSITE_ID) || "");
  const savedDeviceId = normalizeUniqueId(localStorage.getItem(KEY_DEVICE_ID) || "");
  const defaultInstrumentId = getDefaultInstrumentId();
  const deviceConfigId = normalizeUniqueId(cfg.WEBSITE_UNIQUE_ID || cfg.DEVICE_UNIQUE_ID || "");

  // Prefer the current device identity when loaded from device API to avoid stale cross-device topic mismatches.
  const websiteId = urlId || deviceConfigId || savedId || savedDeviceId || defaultInstrumentId;
  if (websiteId) {
    localStorage.setItem(KEY_WEBSITE_ID, websiteId);
    localStorage.setItem(KEY_DEVICE_ID, websiteId);
    addKnownInstrumentId(websiteId);
    activeInstrumentId = websiteId;

    if (!window.location.hash || normalizeUniqueId(window.location.hash.substring(1)) !== websiteId) {
      window.history.replaceState({}, "", `${window.location.pathname}${window.location.search}#${encodeURIComponent(websiteId)}`);
    }
  }

  cfg.WEBSITE_UNIQUE_ID = websiteId;
  cfg.DEVICE_UNIQUE_ID = normalizeUniqueId(cfg.DEVICE_UNIQUE_ID || websiteId);
  if (!cfg.MQTT_CLIENT_PREFIX) cfg.MQTT_CLIENT_PREFIX = "TIARA";
  cfg.PROJECT_TOPIC_ROOT = String(cfg.PROJECT_TOPIC_ROOT || "tiara").replace(/^\/+|\/+$/g, "");
  cfg.BROKER_PORT = Number(cfg.BROKER_PORT || 9001);

  localStorage.setItem(KEY_MQTT_CFG, JSON.stringify({
    BROKER_HOST: String(cfg.BROKER_HOST || "").trim(),
    BROKER_PORT: Number(cfg.BROKER_PORT || 9001),
    MQTT_USERNAME: String(cfg.MQTT_USERNAME || ""),
    MQTT_PASSWORD: String(cfg.MQTT_PASSWORD || ""),
    MQTT_CLIENT_PREFIX: String(cfg.MQTT_CLIENT_PREFIX || "TIARA").trim(),
    PROJECT_TOPIC_ROOT: String(cfg.PROJECT_TOPIC_ROOT || "tiara").trim(),
    WEBSITE_UNIQUE_ID: String(cfg.WEBSITE_UNIQUE_ID || "").trim().toLowerCase(),
    DEVICE_UNIQUE_ID: String(cfg.DEVICE_UNIQUE_ID || "").trim()
  }));

  return cfg;
}

function buildTopicBase() {
  return `${mqttConfig.PROJECT_TOPIC_ROOT}/${mqttConfig.WEBSITE_UNIQUE_ID}`;
}

function mqttWsUrls() {
  const host = String(mqttConfig.BROKER_HOST || "").trim();
  const port = Number(mqttConfig.BROKER_PORT || 9001);

  if (location.protocol === "https:") {
    const urls = [];
    if (port > 0 && port !== 443) {
      urls.push(`wss://${host}:${port}/mqtt`);
    }
    urls.push(`wss://${host}/mqtt`);
    return urls;
  }

  return [`ws://${host}:${port}/mqtt`];
}

function mqttWsUrlsForHostPort(hostIn, portIn) {
  const host = String(hostIn || "").trim();
  const port = Number(portIn || 9001);
  if (!host) return [];

  if (location.protocol === "https:") {
    const urls = [];
    if (port > 0 && port !== 443) {
      urls.push(`wss://${host}:${port}/mqtt`);
    }
    urls.push(`wss://${host}/mqtt`);
    return urls;
  }

  return [`ws://${host}:${port}/mqtt`];
}

function updateIdentityPanel() {
  const site = activeInstrumentId || mqttConfig.WEBSITE_UNIQUE_ID || "--";
  const names = loadInstrumentNames();
  const label = names[site] ? `${names[site]} (${site})` : site;
  const broker = `${mqttConfig.BROKER_HOST || "--"}:${mqttConfig.BROKER_PORT || "--"}`;
  setTextIf("foot", `Instrument: ${label} | Broker: ${broker}`);
}

function buildGraphLegend() {
  const legend = $("graphLegend");
  if (!legend) return;
  legend.innerHTML = "";

  for (let v = 1000; v >= -1000; v -= 200) {
    const span = document.createElement("span");
    span.textContent = String(v);
    legend.appendChild(span);
  }
}

function pushGraphValue(value) {
  if (!Number.isFinite(value)) return;
  graphBuffer.push(value);
  if (graphBuffer.length > graphBufferSize) graphBuffer.shift();
  scheduleGraphDraw();
}

function pushGraphValues(values) {
  let added = 0;
  for (const raw of values) {
    const value = Number(raw);
    if (!Number.isFinite(value)) continue;
    graphBuffer.push(value);
    added++;
  }
  if (added === 0) return;

  const overflow = graphBuffer.length - graphBufferSize;
  if (overflow > 0) {
    graphBuffer.splice(0, overflow);
  }
  scheduleGraphDraw();
}

function scheduleGraphDraw() {
  if (graphDrawPending) return;
  graphDrawPending = true;
  requestAnimationFrame(() => {
    graphDrawPending = false;
    drawCurrentGraph();
  });
}

function drawCurrentGraph() {
  const canvas = $("currentGraph");
  if (!canvas) return;

  const ctx = canvas.getContext("2d");
  const width = canvas.width;
  const height = canvas.height;

  ctx.clearRect(0, 0, width, height);

  // grid lines every 200 units
  ctx.strokeStyle = "#1f3652";
  ctx.lineWidth = 1;
  for (let y = 0; y <= 10; y++) {
    const py = (y / 10) * height;
    ctx.beginPath();
    ctx.moveTo(0, py);
    ctx.lineTo(width, py);
    ctx.stroke();
  }

  // center axis
  ctx.strokeStyle = "#3b6b99";
  ctx.beginPath();
  ctx.moveTo(0, height / 2);
  ctx.lineTo(width, height / 2);
  ctx.stroke();

  // signal trace
  const points = graphBuffer.length;
  if (points === 0) {
    return;
  }

  ctx.strokeStyle = "#ffea63";
  ctx.lineWidth = 2;
  ctx.beginPath();

  if (points === 1) {
    const y = ((1000 - graphBuffer[0]) / 2000) * height;
    ctx.moveTo(0, y);
    ctx.lineTo(width, y);
    ctx.stroke();
    return;
  }

  for (let i = 0; i < points; i++) {
    const value = graphBuffer[i];
    const x = (i / (points - 1)) * width;
    const y = ((1000 - value) / 2000) * height;
    if (i === 0) ctx.moveTo(x, y);
    else ctx.lineTo(x, y);
  }
  ctx.stroke();
}

function publishTopic(path, payload) {
  if (!mqttClient || !mqttClient.connected) return;
  mqttClient.publish(`${topicBase}${path}`, payload);
}

function sendCmd(rawCmd) {
  publishTopic("/control/command", rawCmd);
}

function applyState(data) {
  const cur = Number(data.cur);
  $("cur").textContent = Number.isFinite(cur) ? `${cur.toFixed(3)} ${data.unit || ""}` : "--";
  $("range").textContent = data.range ?? "--";
  $("auto").textContent = data.auto ?? "--";
  $("output").textContent = data.output ? "ON" : "OFF";
  $("source").textContent = data.source ?? "--";

  if (Number.isFinite(Number(data.brightness))) {
    const b = Number(data.brightness);
    $("brightness").value = b;
    $("brightnessValue").textContent = `${b}%`;
  }

  pushGraphValue(cur);
}

function onMqttMessage(topic, payloadBuf) {
  const payload = new TextDecoder().decode(payloadBuf);

  if (topic === `${topicBase}/state/snapshot`) {
    try {
      const data = JSON.parse(payload);
      applyState(data);
    } catch (_) {
    }
    return;
  }

  if (!topic.startsWith(`${topicBase}/sensors/`)) return;
  const name = topic.substring(`${topicBase}/sensors/`.length);

  if (name === "current_measurement") {
    if (Date.now() - lastPacketReceiveMs < 1500) return;
    const currentText = $("cur").textContent;
    const unit = currentText.includes(" ") ? currentText.split(" ").pop() : "";
    const n = Number(payload);
    $("cur").textContent = Number.isFinite(n) ? `${n.toFixed(3)} ${unit}` : "--";
    pushGraphValue(n);
  }
  if (name === "current_packet") {
    try {
      const packet = JSON.parse(payload);
      const values = Array.isArray(packet.values) ? packet.values : [];
      if (values.length > 0) {
        pushGraphValues(values);
        const last = Number(values[values.length - 1]);
        if (Number.isFinite(last)) {
          const currentText = $("cur").textContent;
          const unit = currentText.includes(" ") ? currentText.split(" ").pop() : "";
          $("cur").textContent = `${last.toFixed(3)} ${unit}`;
        }
        lastPacketReceiveMs = Date.now();
      }
    } catch (_) {
    }
  }
  if (name === "current_unit") {
    const currentText = $("cur").textContent;
    const value = currentText.split(" ")[0] || "--";
    $("cur").textContent = `${value} ${payload}`;
  }
  if (name === "range") $("range").textContent = payload;
  if (name === "auto_mode") $("auto").textContent = payload;
  if (name === "output") $("output").textContent = (payload === "1" || payload === "on") ? "ON" : "OFF";
  if (name === "source") $("source").textContent = payload;
  if (name === "brightness") {
    const b = Number(payload);
    if (Number.isFinite(b)) {
      $("brightness").value = b;
      $("brightnessValue").textContent = `${b}%`;
    }
  }
}

function scheduleReconnect() {
  if (reconnectTimer) return;
  reconnectTimer = setTimeout(() => {
    reconnectTimer = null;
    connectMqtt();
  }, 5000);
}

function connectMqtt() {
  if (!mqttConfig || !mqttConfig.BROKER_HOST) {
    setStatus("#ef4444", "MQTT broker host is not configured");
    return;
  }

  const candidateUrls = mqttWsUrls();

  // In GitHub/cloud mode, stale saved config can point to the wrong broker.
  // Append canonical cloud broker endpoints as recovery candidates.
  if (!gLoadedFromDeviceApi) {
    const fallbackUrls = mqttWsUrlsForHostPort(
      CLOUD_MQTT_DEFAULTS.BROKER_HOST,
      CLOUD_MQTT_DEFAULTS.BROKER_PORT
    );
    for (const url of fallbackUrls) {
      if (!candidateUrls.includes(url)) {
        candidateUrls.push(url);
      }
    }
  }

  if (candidateUrls.length === 0) {
    setStatus("#ef4444", "No MQTT endpoint candidates available");
    return;
  }

  const opts = {
    reconnectPeriod: 0,
    username: mqttConfig.MQTT_USERNAME || undefined,
    password: mqttConfig.MQTT_PASSWORD && mqttConfig.MQTT_PASSWORD !== "***" ? mqttConfig.MQTT_PASSWORD : undefined,
    clientId: `${mqttConfig.MQTT_CLIENT_PREFIX || "TIARA"}-WEB-${mqttConfig.WEBSITE_UNIQUE_ID || "site"}`
  };

  const connectAt = (urlIndex) => {
    const endpoint = candidateUrls[urlIndex];
    let connected = false;
    let switched = false;

    setStatus("#f59e0b", `Connecting to broker (${endpoint})...`);

    try {
      if (mqttClient) mqttClient.end(true);
      mqttClient = mqtt.connect(endpoint, opts);

      mqttClient.on("connect", () => {
        connected = true;
        setStatus("#4ade80", `Connected ${topicBase}`);
        mqttClient.subscribe(`${topicBase}/#`);
        publishTopic("/control/read_state", "");
      });

      mqttClient.on("message", onMqttMessage);

      mqttClient.on("close", () => {
        if (!connected && !switched && urlIndex + 1 < candidateUrls.length) {
          switched = true;
          connectAt(urlIndex + 1);
          return;
        }
        setStatus("#94a3b8", "Disconnected; reconnect scheduled");
        scheduleReconnect();
      });

      mqttClient.on("offline", () => {
        if (!connected && !switched && urlIndex + 1 < candidateUrls.length) {
          switched = true;
          connectAt(urlIndex + 1);
          return;
        }
        setStatus("#94a3b8", "MQTT offline");
        scheduleReconnect();
      });

      mqttClient.on("error", (err) => {
        if (!connected && !switched && urlIndex + 1 < candidateUrls.length) {
          switched = true;
          try { mqttClient.end(true); } catch (_) {}
          connectAt(urlIndex + 1);
          return;
        }

        setStatus("#ef4444", `MQTT error: ${err && err.message ? err.message : "unknown"}`);
        try { mqttClient.end(true); } catch (_) {}
        scheduleReconnect();
      });
    } catch (e) {
      if (urlIndex + 1 < candidateUrls.length) {
        connectAt(urlIndex + 1);
        return;
      }
      setStatus("#ef4444", `Connect failed: ${e.message}`);
      scheduleReconnect();
    }
  };

  connectAt(0);
}

async function loadConfig() {
  let raw = {};
  gLoadedFromDeviceApi = false;

  try {
    const res = await fetch("/api/mqtt/config?includeSecrets=1", { cache: "no-store" });
    if (res.ok) {
      raw = await res.json();
      gLoadedFromDeviceApi = true;
    }
  } catch (_) {
  }

  mqttConfig = parseConfigResponse(raw);
  if (activeInstrumentId) {
    mqttConfig.WEBSITE_UNIQUE_ID = activeInstrumentId;
  }
  topicBase = buildTopicBase();
  updateIdentityPanel();
  setDynamicManifest(mqttConfig.WEBSITE_UNIQUE_ID);
  renderInstrumentSelector();

  const openCfgBtn = $("openConfig");
  if (openCfgBtn) {
    openCfgBtn.style.display = gLoadedFromDeviceApi ? "inline-block" : "none";
  }

  if (!gLoadedFromDeviceApi) {
    setStatus("#f59e0b", "Cloud mode: using embedded broker config + URL instrument ID");
  }
}

async function saveConfigFromDialog() {
  const dialog = $("cfgDialog");
  const form = $("cfgForm");
  const fd = new FormData(form);
  const next = {
    BROKER_HOST: String(fd.get("BROKER_HOST") || "").trim(),
    BROKER_PORT: Number(fd.get("BROKER_PORT") || 1883),
    MQTT_USERNAME: String(fd.get("MQTT_USERNAME") || ""),
    MQTT_PASSWORD: String(fd.get("MQTT_PASSWORD") || ""),
    MQTT_CLIENT_PREFIX: String(fd.get("MQTT_CLIENT_PREFIX") || "TIARA").trim(),
    PROJECT_TOPIC_ROOT: String(fd.get("PROJECT_TOPIC_ROOT") || "tiara").trim(),
    WEBSITE_UNIQUE_ID: String(fd.get("WEBSITE_UNIQUE_ID") || "").trim().toLowerCase()
  };

  let savedToDeviceApi = false;
  try {
    const res = await fetch("/api/mqtt/config", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(next)
    });

    if (res.ok) {
      savedToDeviceApi = true;
    } else {
      const txt = await res.text();
      throw new Error(txt || "Failed to save MQTT config");
    }
  } catch (_) {
    savedToDeviceApi = false;
  }

  dialog.close();

  if (savedToDeviceApi) {
    await loadConfig();
  } else {
    mqttConfig = parseConfigResponse(next);
    if (activeInstrumentId) {
      mqttConfig.WEBSITE_UNIQUE_ID = activeInstrumentId;
    }
    topicBase = buildTopicBase();
    updateIdentityPanel();
    setDynamicManifest(mqttConfig.WEBSITE_UNIQUE_ID);
    setStatus("#f59e0b", "Saved in browser (cloud mode); device EEPROM unchanged");
  }

  connectMqtt();
}

function openConfigDialog() {
  if (!gLoadedFromDeviceApi) {
    setStatus("#f59e0b", "Cloud mode uses embedded MQTT credentials");
    return;
  }

  const d = $("cfgDialog");
  const f = $("cfgForm");
  f.BROKER_HOST.value = mqttConfig.BROKER_HOST || "";
  f.BROKER_PORT.value = mqttConfig.BROKER_PORT || 1883;
  f.MQTT_USERNAME.value = mqttConfig.MQTT_USERNAME || "";
  f.MQTT_PASSWORD.value = mqttConfig.MQTT_PASSWORD && mqttConfig.MQTT_PASSWORD !== "***" ? mqttConfig.MQTT_PASSWORD : "";
  f.MQTT_CLIENT_PREFIX.value = mqttConfig.MQTT_CLIENT_PREFIX || "TIARA";
  f.PROJECT_TOPIC_ROOT.value = mqttConfig.PROJECT_TOPIC_ROOT || "tiara";
  f.WEBSITE_UNIQUE_ID.value = mqttConfig.WEBSITE_UNIQUE_ID || "";
  d.showModal();
}

function wireUi() {
  document.querySelectorAll("[data-cmd]").forEach((el) => {
    el.addEventListener("click", () => sendCmd(el.getAttribute("data-cmd")));
  });

  $("outputOn").addEventListener("click", () => publishTopic("/control/set_output", "on"));
  $("outputOff").addEventListener("click", () => publishTopic("/control/set_output", "off"));

  $("applyCurrent").addEventListener("click", () => {
    const value = Number($("setVal").value);
    const unit = $("setUnit").value;
    if (!Number.isFinite(value)) return;
    publishTopic("/control/set_current", JSON.stringify({ value, unit }));
  });

  $("brightness").addEventListener("input", () => {
    const b = Number($("brightness").value);
    $("brightnessValue").textContent = `${b}%`;
    publishTopic("/control/set_brightness", String(b));
  });

  $("openConfig").addEventListener("click", openConfigDialog);
  $("instrumentSelect").addEventListener("change", () => {
    const id = normalizeUniqueId($("instrumentSelect").value);
    if (id) selectInstrument(id, true);
  });
  $("addInstrumentBtn").addEventListener("click", addInstrumentFromInput);
  $("saveInstrumentNameBtn").addEventListener("click", saveSelectedInstrumentName);
  $("openInstrumentManagerBtn").addEventListener("click", openInstrumentManagerDialog);
  $("closeInstrumentManagerBtn").addEventListener("click", closeInstrumentManagerDialog);
  $("setCurrentDefaultBtn").addEventListener("click", setCurrentInstrumentAsDefault);
  $("exportInstrumentsBtn").addEventListener("click", exportInstrumentsToFile);
  $("importInstrumentsBtn").addEventListener("click", () => {
    const picker = $("importInstrumentsInput");
    if (picker) picker.click();
  });
  $("importInstrumentsInput").addEventListener("change", (event) => {
    const target = event.target;
    const file = target && target.files ? target.files[0] : null;
    importInstrumentsFromFile(file);
    if (target) target.value = "";
  });
  $("instrumentNameInput").addEventListener("keydown", (event) => {
    if (event.key === "Enter") {
      event.preventDefault();
      saveSelectedInstrumentName();
    }
  });
  $("instrumentIdInput").addEventListener("keydown", (event) => {
    if (event.key === "Enter") {
      event.preventDefault();
      addInstrumentFromInput();
    }
  });
  $("saveCfg").addEventListener("click", async (event) => {
    event.preventDefault();
    try {
      await saveConfigFromDialog();
    } catch (e) {
      setStatus("#ef4444", e.message || "Failed to save config");
    }
  });
}

async function bootstrap() {
  wireUi();
  buildGraphLegend();
  drawCurrentGraph();

  if ("serviceWorker" in navigator) {
    window.addEventListener("load", () => {
      navigator.serviceWorker.register("./service-worker.js").catch(() => {});
    });
  }

  try {
    await loadConfig();
    if (!mqttConfig || !mqttConfig.WEBSITE_UNIQUE_ID) {
      setStatus("#ef4444", "No instrument ID in URL. Use ...#<unique_id>");
      return;
    }
    const hashId = extractUniqueIdFromUrl();
    if (hashId && hashId !== activeInstrumentId) {
      selectInstrument(hashId, false);
    }

    window.addEventListener("hashchange", () => {
      const nextHashId = extractUniqueIdFromUrl();
      if (nextHashId && nextHashId !== activeInstrumentId) {
        selectInstrument(nextHashId, true);
      }
    });

    connectMqtt();
  } catch (e) {
    setStatus("#ef4444", e.message || "Bootstrap failed");
  }
}

bootstrap();
