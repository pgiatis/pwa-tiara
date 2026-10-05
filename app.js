const $ = (id) => document.getElementById(id);

const KEY_WEBSITE_ID = "tiara.website.unique.id";
const KEY_DEVICE_ID = "tiara.device.id";
const KEY_INSTRUMENT_IDS = "tiara.instrument.ids";
const KEY_INSTRUMENT_NAMES = "tiara.instrument.names";
const KEY_DEFAULT_INSTRUMENT_ID = "tiara.default.instrument.id";
const KEY_GRAPH_RENDER_MODE = "tiara.graph.render.mode";
const BROKER_HOST = "pgiatis.dyndns.org";
const BROKER_PORT_WS = 9001;
const BROKER_USER = "BisinaSystems";
const BROKER_PASS = "BisinaSystems123";
const TOPIC_ROOT = "tiara";

let mqttClient = null;
let reconnectTimer = null;
let websiteUniqueId = "";
let topicBase = "";
let latestState = {};
let currentBuffer = [];
const bufferSize = 512;
let graphDrawPending = false;
let lastPacketReceiveMs = 0;
let graphRenderPointBudget = 180;
let graphInterpolationPxStep = 6;
let currentGraphMode = "smooth";
let graphWindowPoints = 160;
const GRAPH_RENDER_MODES = {
  accuracy: { budget: 320, pxStep: 2, windowPoints: 320 },
  smooth: { budget: 180, pxStep: 6, windowPoints: 160 },
  performance: { budget: 84, pxStep: 12, windowPoints: 72 },
  ideal: { budget: 260, pxStep: 4, windowPoints: 220 }
};
const graphSignalState = {
  waveformType: 0,
  frequency: 0,
  offset: 0,
  samples: 64
};
let instrumentScanActive = false;
let instrumentScanTimer = null;
let instrumentScanKnownIds = new Set();
let instrumentScanDiscoveredIds = new Set();
let instrumentScanFoundIds = new Set();
let instrumentScanClient = null;
let instrumentScanUsesPrimaryClient = false;
let instrumentScanLastResults = [];

const INSTRUMENT_SCAN_TOPICS = [
  `${TOPIC_ROOT}/+/state/snapshot`
];

const TIARA_SNAPSHOT_SIGNATURE_KEYS = [
  "cur",
  "unit",
  "range",
  "auto",
  "output",
  "set_current",
  "set_unit",
  "frequency",
  "waveform_offset",
  "samples"
];

function extractInstrumentIdFromTopic(topic) {
  const rawTopic = String(topic || "");
  const parts = rawTopic.split("/");
  if (parts.length < 2) return "";
  if (parts[0] !== TOPIC_ROOT) return "";
  return normalizeUniqueId(parts[1]);
}

function isLikelyTiaraSnapshot(topic, payloadText) {
  if (typeof topic !== "string" || !topic.endsWith("/state/snapshot")) {
    return false;
  }

  let snapshot = null;
  try {
    snapshot = JSON.parse(String(payloadText || "{}"));
  } catch (_) {
    return false;
  }

  if (!snapshot || typeof snapshot !== "object") {
    return false;
  }

  let matches = 0;
  for (const key of TIARA_SNAPSHOT_SIGNATURE_KEYS) {
    if (Object.prototype.hasOwnProperty.call(snapshot, key)) {
      matches += 1;
    }
  }

  return matches >= 4;
}

function setInstrumentScanButtonState() {
  const btn = $("instrument-scan-btn");
  if (!btn) return;
  btn.disabled = instrumentScanActive;
  btn.textContent = instrumentScanActive ? "Scanning..." : "Scan for Instruments";
}

function setInstrumentScanStatus(message, isError = false) {
  const status = $("instrument-scan-status");
  if (!status) return;
  status.textContent = message;
  status.classList.toggle("error", !!isError);
}

function closeInstrumentScanResultsModal() {
  const modal = $("instrument-scan-results-modal");
  if (modal) modal.style.display = "none";
}

function renderInstrumentScanResultsList() {
  const list = $("instrument-scan-results-list");
  if (!list) return;

  list.innerHTML = "";
  if (instrumentScanLastResults.length === 0) {
    const empty = document.createElement("div");
    empty.className = "instrument-scan-results-empty";
    empty.textContent = "No TIARA instruments were detected in this scan window.";
    list.appendChild(empty);
    return;
  }

  instrumentScanLastResults.forEach((item) => {
    const row = document.createElement("label");
    row.className = "instrument-scan-results-item";

    const checkbox = document.createElement("input");
    checkbox.type = "checkbox";
    checkbox.name = "scan-result-id";
    checkbox.value = item.id;
    checkbox.checked = !item.isKnown;
    checkbox.disabled = item.isKnown;

    const text = document.createElement("span");
    const suffix = item.isKnown ? " (already added)" : "";
    text.textContent = `${item.id.toUpperCase()}${suffix}`;

    row.appendChild(checkbox);
    row.appendChild(text);
    list.appendChild(row);
  });
}

function openInstrumentScanResultsModal() {
  renderInstrumentScanResultsList();
  const modal = $("instrument-scan-results-modal");
  if (modal) modal.style.display = "flex";
}

function addSelectedScannedInstruments() {
  const list = $("instrument-scan-results-list");
  if (!list) return;

  const checked = Array.from(list.querySelectorAll('input[name="scan-result-id"]:checked'));
  const selectedIds = checked
    .map((item) => normalizeUniqueId(item.value))
    .filter((id) => !!id);

  if (selectedIds.length === 0) {
    setInstrumentScanStatus("No instruments selected to add.");
    return;
  }

  let ids = loadKnownInstrumentIds();
  let added = 0;
  for (const id of selectedIds) {
    if (ids.includes(id)) continue;
    ids.push(id);
    added += 1;
  }

  if (added > 0) {
    saveKnownInstrumentIds(ids);
    renderInstrumentSelector();
    renderInstrumentManagerList();
  }

  closeInstrumentScanResultsModal();
  setInstrumentScanStatus(`Added ${added} instrument(s).`);
  setFoot(`Added ${added} instrument(s) from scan`);
}

function setScannedInstrumentSelection(checked) {
  const list = $("instrument-scan-results-list");
  if (!list) return;
  const boxes = Array.from(list.querySelectorAll('input[name="scan-result-id"]'));
  boxes.forEach((box) => {
    box.checked = !!checked;
  });
}

function buildDiscoveryMqttUrls() {
  const candidateUrls = mqttWsUrls();
  const fallbackUrls = mqttWsUrlsForHostPort(BROKER_HOST, BROKER_PORT_WS);
  for (const url of fallbackUrls) {
    if (!candidateUrls.includes(url)) {
      candidateUrls.push(url);
    }
  }
  return candidateUrls;
}

function stopInstrumentScan() {
  if (instrumentScanTimer) {
    clearTimeout(instrumentScanTimer);
    instrumentScanTimer = null;
  }

  if (instrumentScanClient) {
    try {
      instrumentScanClient.unsubscribe(INSTRUMENT_SCAN_TOPICS);
    } catch (_) {
    }

    if (!instrumentScanUsesPrimaryClient) {
      try {
        instrumentScanClient.end(true);
      } catch (_) {
      }
    }
  }

  const foundIds = Array.from(instrumentScanFoundIds).sort();
  const discoveredIds = Array.from(instrumentScanDiscoveredIds).sort();
  const foundCount = foundIds.length;
  instrumentScanActive = false;
  instrumentScanKnownIds = new Set();
  instrumentScanDiscoveredIds = new Set();
  instrumentScanFoundIds = new Set();
  instrumentScanClient = null;
  instrumentScanUsesPrimaryClient = false;
  setInstrumentScanButtonState();

  if (discoveredIds.length > 0) {
    instrumentScanLastResults = discoveredIds.map((id) => ({
      id,
      isKnown: !foundIds.includes(id)
    }));

    setInstrumentScanStatus(`Scan complete: found ${discoveredIds.length} instrument(s), ${foundCount} new.`);
    setFoot(`Scan complete: found ${discoveredIds.length} instrument(s), ${foundCount} new`);
  } else {
    instrumentScanLastResults = [];
    setInstrumentScanStatus("Scan complete: no new instruments detected.");
    setFoot("Scan complete: no new instruments detected");
  }

  openInstrumentScanResultsModal();
}

function processInstrumentScanTopic(topic, payloadText) {
  if (!instrumentScanActive) return;
  if (!isLikelyTiaraSnapshot(topic, payloadText)) return;

  const id = extractInstrumentIdFromTopic(topic);
  if (!id) return;
  instrumentScanDiscoveredIds.add(id);
  if (instrumentScanKnownIds.has(id)) return;
  if (instrumentScanFoundIds.has(id)) return;

  instrumentScanFoundIds.add(id);
  const foundCount = instrumentScanFoundIds.size;
  setInstrumentScanStatus(`Scanning... found ${foundCount} new instrument(s).`);
  setFoot(`Scan: found ${foundCount} new instrument(s)`);
}

function beginInstrumentScanWithClient(client, usePrimaryClient) {
  instrumentScanClient = client;
  instrumentScanUsesPrimaryClient = usePrimaryClient;

  try {
    instrumentScanClient.subscribe(INSTRUMENT_SCAN_TOPICS);
  } catch (_) {
    instrumentScanActive = false;
    instrumentScanClient = null;
    instrumentScanUsesPrimaryClient = false;
    setInstrumentScanButtonState();
    setInstrumentScanStatus("Scan failed: unable to subscribe to discovery topics.", true);
    setFoot("Scan failed: unable to subscribe to discovery topics");
    return;
  }

  setInstrumentScanStatus("Scanning MQTT for TIARA instruments (8s)...");
  setFoot("Scanning MQTT for TIARA instruments (8s)...");
  instrumentScanTimer = setTimeout(stopInstrumentScan, 8000);
}

function startStandaloneInstrumentScan() {
  const candidateUrls = buildDiscoveryMqttUrls();
  if (candidateUrls.length === 0) {
    instrumentScanActive = false;
    setInstrumentScanButtonState();
    setInstrumentScanStatus("Scan failed: no MQTT endpoints available.", true);
    return;
  }

  const opts = {
    reconnectPeriod: 0,
    username: BROKER_USER,
    password: BROKER_PASS,
    clientId: `TIARA-SCAN-${Math.random().toString(16).slice(2, 10)}`
  };

  const tryConnectAt = (urlIndex) => {
    const endpoint = candidateUrls[urlIndex];
    let connected = false;
    let movedNext = false;

    setInstrumentScanStatus(`Connecting scanner to MQTT: ${endpoint}`);

    let scanClient = null;
    try {
      scanClient = mqtt.connect(endpoint, opts);
    } catch (_) {
      if (urlIndex + 1 < candidateUrls.length) {
        tryConnectAt(urlIndex + 1);
        return;
      }
      instrumentScanActive = false;
      setInstrumentScanButtonState();
      setInstrumentScanStatus("Scan failed: could not create MQTT connection.", true);
      return;
    }

    scanClient.on("connect", () => {
      connected = true;
      scanClient.on("message", (topic, payloadBuf) => {
        const payload = new TextDecoder().decode(payloadBuf);
        processInstrumentScanTopic(topic, payload);
      });
      beginInstrumentScanWithClient(scanClient, false);
    });

    const onFailure = () => {
      if (connected) return;
      if (!movedNext && urlIndex + 1 < candidateUrls.length) {
        movedNext = true;
        try { scanClient.end(true); } catch (_) {}
        tryConnectAt(urlIndex + 1);
        return;
      }

      instrumentScanActive = false;
      setInstrumentScanButtonState();
      setInstrumentScanStatus("Scan failed: unable to connect to MQTT broker.", true);
    };

    scanClient.on("error", onFailure);
    scanClient.on("close", onFailure);
    scanClient.on("offline", onFailure);
  };

  tryConnectAt(0);
}

function startInstrumentScan() {
  if (instrumentScanActive) return;

  const knownIds = loadKnownInstrumentIds();
  instrumentScanKnownIds = new Set(knownIds);
  instrumentScanDiscoveredIds = new Set();
  instrumentScanFoundIds = new Set();
  instrumentScanLastResults = [];
  instrumentScanActive = true;
  setInstrumentScanButtonState();
  setInstrumentScanStatus("Preparing scan...");

  if (mqttClient && mqttClient.connected) {
    beginInstrumentScanWithClient(mqttClient, true);
    return;
  }

  startStandaloneInstrumentScan();
}

function loadKnownInstrumentIds() {
  try {
    const parsed = JSON.parse(localStorage.getItem(KEY_INSTRUMENT_IDS) || "[]");
    if (!Array.isArray(parsed)) return [];
    const out = [];
    const seen = new Set();
    for (const value of parsed) {
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
  if (safeId) localStorage.setItem(KEY_DEFAULT_INSTRUMENT_ID, safeId);
  else localStorage.removeItem(KEY_DEFAULT_INSTRUMENT_ID);
}

function exportInstrumentsToFile() {
  const payload = {
    version: 1,
    exportedAt: new Date().toISOString(),
    ids: loadKnownInstrumentIds(),
    names: loadInstrumentNames(),
    defaultId: getDefaultInstrumentId(),
    activeId: websiteUniqueId || ""
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
  setFoot("Instrument list exported");
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

  switchInstrument(nextActive, true);
  renderInstrumentManagerList();
  setFoot(`Imported ${normalizedIds.length} instrument(s)`);
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
      setFoot(message);
    }
  };
  reader.onerror = () => setFoot("Failed to read import file");
  reader.readAsText(file);
}

function setCurrentInstrumentAsDefault() {
  if (!websiteUniqueId) return;
  setDefaultInstrumentId(websiteUniqueId);
  renderInstrumentSelector();
  renderInstrumentManagerList();
  setFoot(`Default set to ${getInstrumentDisplayName(websiteUniqueId)}`);
}

function loadInstrumentNames() {
  try {
    const parsed = JSON.parse(localStorage.getItem(KEY_INSTRUMENT_NAMES) || "{}");
    if (!parsed || typeof parsed !== "object") return {};
    const out = {};
    for (const key of Object.keys(parsed)) {
      const id = normalizeUniqueId(key);
      const name = String(parsed[key] || "").trim();
      if (id && name) out[id] = name;
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
  if (nextName) names[safeId] = nextName;
  else delete names[safeId];
  saveInstrumentNames(names);
}

function getInstrumentDisplayName(id) {
  const safeId = normalizeUniqueId(id);
  if (!safeId) return "";
  const names = loadInstrumentNames();
  if (names[safeId]) return `${names[safeId]} (${safeId.toUpperCase()})`;
  return safeId.toUpperCase();
}

function refreshInstrumentNameEditor() {
  const input = $("instrument-name-input");
  if (!input) return;
  const names = loadInstrumentNames();
  input.value = websiteUniqueId ? (names[websiteUniqueId] || "") : "";
}

function updateConnectionSummary() {
  const label = $("connection-active-id");
  if (!label) return;
  if (!websiteUniqueId) {
    label.textContent = "No instrument selected";
    return;
  }
  label.textContent = getInstrumentDisplayName(websiteUniqueId);
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

function renderInstrumentSelector() {
  const select = $("instrument-select");
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
    updateConnectionSummary();
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

  if (websiteUniqueId && ids.includes(websiteUniqueId)) {
    select.value = websiteUniqueId;
  }

  refreshInstrumentNameEditor();
  updateConnectionSummary();
}

function updateInstrumentHash(id) {
  const safeId = normalizeUniqueId(id);
  if (!safeId) return;
  window.history.replaceState({}, "", `${window.location.pathname}${window.location.search}#${encodeURIComponent(safeId)}`);
}

function switchInstrument(id, reconnect = true) {
  const nextId = normalizeUniqueId(id);
  if (!nextId) return;

  addKnownInstrumentId(nextId);
  websiteUniqueId = nextId;
  topicBase = `${TOPIC_ROOT}/${websiteUniqueId}`;
  localStorage.setItem(KEY_WEBSITE_ID, websiteUniqueId);
  localStorage.setItem(KEY_DEVICE_ID, websiteUniqueId);

  currentBuffer = [];
  drawCurrentGraph();
  latestState = {
    ip: `piot-${websiteUniqueId}.local`,
    ssid: "--"
  };

  updateInstrumentHash(websiteUniqueId);
  renderInstrumentSelector();

  if (reconnect) {
    connectMqtt();
  }
}

function addInstrumentFromInput() {
  const input = $("instrument-input");
  if (!input) return;
  const typed = normalizeUniqueId(input.value);
  if (!typed) return;
  switchInstrument(typed, true);
  input.value = "";
}

function saveSelectedInstrumentName() {
  if (!websiteUniqueId) return;
  const input = $("instrument-name-input");
  if (!input) return;
  setInstrumentName(websiteUniqueId, input.value);
  renderInstrumentSelector();
  setFoot(`Connected ${topicBase}`);
}

function openInstrumentManager() {
  renderInstrumentManagerList();
  const modal = $("instrument-manager-modal");
  if (modal) modal.style.display = "flex";
}

function closeInstrumentManager() {
  const modal = $("instrument-manager-modal");
  if (modal) modal.style.display = "none";
}

function openConnectionModal() {
  const modal = $("connection-modal");
  if (modal) modal.style.display = "flex";
}

function closeConnectionModal() {
  const modal = $("connection-modal");
  if (modal) modal.style.display = "none";
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
  const current = loadInstrumentNames()[safeId] || "";
  const entered = prompt("Instrument nickname:", current);
  if (entered === null) return;
  setInstrumentName(safeId, entered);
  renderInstrumentSelector();
  renderInstrumentManagerList();
  setFoot(`Connected ${topicBase}`);
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

  if (websiteUniqueId === safeId) {
    const fallback = ids[0] || "";
    if (fallback) {
      switchInstrument(fallback, true);
    } else {
      websiteUniqueId = "";
      topicBase = "";
      localStorage.removeItem(KEY_WEBSITE_ID);
      localStorage.removeItem(KEY_DEVICE_ID);
      setHardwareLed(false);
      setFoot("No instruments saved. Scan a QR code to add one.");
      renderInstrumentSelector();
    }
  } else {
    renderInstrumentSelector();
  }

  renderInstrumentManagerList();
}

function renderInstrumentManagerList() {
  const list = $("instrument-manager-list");
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
    const suffix = [];
    if (id === websiteUniqueId) suffix.push("Active");
    if (id === defaultId) suffix.push("Default");
    const state = suffix.length ? ` (${suffix.join(", ")})` : "";
    label.textContent = `${getInstrumentDisplayName(id)}${state}`;

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

function normalizeUniqueId(value) {
  return String(value || "").trim().toLowerCase().replace(/[^a-z0-9_-]/g, "");
}

function getUniqueIdFromUrl() {
  const params = new URLSearchParams(window.location.search);
  const queryId = normalizeUniqueId(params.get("device") || params.get("id") || params.get("unique_id"));
  if (queryId) return queryId;

  const hash = String(window.location.hash || "").trim();
  if (hash.startsWith("#")) {
    return normalizeUniqueId(hash.substring(1));
  }

  return "";
}

function setHardwareLed(connected) {
  const led = $("hw-led");
  if (!led) return;
  if (connected) {
    led.style.background = "#3c3";
    led.style.boxShadow = "0 0 6px #0f0";
  } else {
    led.style.background = "#d11";
    led.style.boxShadow = "0 0 6px #f00";
  }
}

function setFoot(message) {
  const foot = $("foot");
  if (foot) foot.textContent = message;
}

function mqttWsUrl() {
  if (location.protocol === "https:") {
    return `wss://${BROKER_HOST}/mqtt`;
  }
  return `ws://${BROKER_HOST}:${BROKER_PORT_WS}/mqtt`;
}

function mqttWsUrls() {
  if (location.protocol === "https:") {
    return [
      `wss://${BROKER_HOST}:${BROKER_PORT_WS}/mqtt`,
      `wss://${BROKER_HOST}/mqtt`
    ];
  }
  return [`ws://${BROKER_HOST}:${BROKER_PORT_WS}/mqtt`];
}

function mqttWsUrlsForHostPort(hostIn, portIn) {
  const host = String(hostIn || "").trim();
  const port = Number(portIn || BROKER_PORT_WS);
  if (!host) return [];

  if (location.protocol === "https:") {
    return [
      `wss://${host}:${port}/mqtt`,
      `wss://${host}/mqtt`
    ];
  }

  return [`ws://${host}:${port}/mqtt`];
}

function publishTopic(path, payload) {
  if (!mqttClient || !mqttClient.connected) return;
  mqttClient.publish(`${topicBase}${path}`, payload);
}

function sendCmd(cmd) {
  publishTopic("/control/command", cmd);
}

window.sendCmd = sendCmd;

function buildGraphLegend() {
  const legend = $("graph-legend");
  if (!legend) return;
  legend.innerHTML = "";
  const top = 1000;
  const bottom = -1000;
  const step = 200;
  for (let v = top; v >= bottom; v -= step) {
    const span = document.createElement("span");
    span.textContent = String(v);
    legend.appendChild(span);
  }
}

function scheduleGraphDraw() {
  if (graphDrawPending) return;
  graphDrawPending = true;
  requestAnimationFrame(() => {
    graphDrawPending = false;
    drawCurrentGraph();
  });
}

function pushCurrentSample(value) {
  if (!Number.isFinite(value)) return;
  currentBuffer.push(value);
  if (currentBuffer.length > bufferSize) currentBuffer.shift();
  scheduleGraphDraw();
}

function pushCurrentSamples(values) {
  let added = 0;
  for (const raw of values) {
    const value = Number(raw);
    if (!Number.isFinite(value)) continue;
    currentBuffer.push(value);
    added++;
  }
  if (added === 0) return;

  const overflow = currentBuffer.length - bufferSize;
  if (overflow > 0) {
    currentBuffer.splice(0, overflow);
  }
  scheduleGraphDraw();
}

function setGraphRenderMode(mode, persist = true) {
  const safeMode = GRAPH_RENDER_MODES[mode] ? mode : "smooth";
  const cfg = GRAPH_RENDER_MODES[safeMode];
  graphRenderPointBudget = cfg.budget;
  graphInterpolationPxStep = cfg.pxStep;
  graphWindowPoints = cfg.windowPoints;
  currentGraphMode = safeMode;

  if (persist) {
    localStorage.setItem(KEY_GRAPH_RENDER_MODE, safeMode);
  }

  const select = $("graph-mode");
  if (select && select.value !== safeMode) {
    select.value = safeMode;
  }

  scheduleGraphDraw();
}

function initGraphRenderMode() {
  const saved = localStorage.getItem(KEY_GRAPH_RENDER_MODE) || "smooth";
  setGraphRenderMode(saved, false);
}

function clampGraphValue(value) {
  return Math.max(-1000, Math.min(1000, value));
}

function normalizeWaveformType(rawType) {
  const t = Number(rawType);
  return Number.isFinite(t) ? Math.max(0, Math.min(3, Math.round(t))) : 0;
}

function waveformValue(type, phase) {
  const frac = phase - Math.floor(phase);
  switch (type) {
    case 1:
      return Math.sin(frac * Math.PI * 2);
    case 2:
      return 1 - 4 * Math.abs(frac - 0.5);
    case 3:
      return frac < 0.5 ? 1 : -1;
    default:
      return 0;
  }
}

function buildIdealRenderPoints(values, width, height) {
  if (values.length < 2) return buildRenderPoints(values, width, height);

  let minVal = Infinity;
  let maxVal = -Infinity;
  for (const raw of values) {
    const value = clampGraphValue(Number(raw));
    if (!Number.isFinite(value)) continue;
    if (value < minVal) minVal = value;
    if (value > maxVal) maxVal = value;
  }

  if (!Number.isFinite(minVal) || !Number.isFinite(maxVal)) {
    return buildRenderPoints(values, width, height);
  }

  const amplitude = Math.max((maxVal - minVal) / 2, 0.5);
  const baseCenter = (maxVal + minVal) / 2;
  const offsetPct = Number.isFinite(Number(graphSignalState.offset)) ? Number(graphSignalState.offset) : 0;
  const center = clampGraphValue(baseCenter + amplitude * (offsetPct / 100));
  const periodSamples = Math.max(8, Math.round(Number(graphSignalState.samples) || 64));
  const cyclesAcrossWindow = Math.max(1, values.length / periodSamples);
  const frequency = Number(graphSignalState.frequency);
  const phaseOffset = Number.isFinite(frequency) && frequency > 0
    ? (Date.now() / 1000 * frequency) % 1
    : 0;
  const waveformType = normalizeWaveformType(graphSignalState.waveformType);

  const outputPoints = Math.max(280, Math.floor(width));
  const out = [];
  for (let i = 0; i < outputPoints; i++) {
    const x = (i / (outputPoints - 1)) * width;
    const phase = phaseOffset + (i / (outputPoints - 1)) * cyclesAcrossWindow;
    const v = clampGraphValue(center + amplitude * waveformValue(waveformType, phase));
    const y = ((1000 - v) / 2000) * height;
    out.push({ x, y });
  }
  return out;
}

function buildRenderIndices(totalPoints, maxPoints) {
  if (totalPoints <= maxPoints) {
    return Array.from({ length: totalPoints }, (_, i) => i);
  }

  const out = [0];
  const lastIndex = totalPoints - 1;
  const step = lastIndex / (maxPoints - 1);

  for (let i = 1; i < maxPoints - 1; i++) {
    const idx = Math.round(i * step);
    if (idx > out[out.length - 1] && idx < lastIndex) {
      out.push(idx);
    }
  }

  out.push(lastIndex);
  return out;
}

function buildRenderPoints(values, width, height) {
  const points = values.length;
  if (points === 0) return [];

  const drawCount = Math.max(2, Math.min(graphRenderPointBudget, points));
  const indices = buildRenderIndices(points, drawCount);
  const anchors = indices.map((idx) => {
    const x = points > 1 ? (idx / (points - 1)) * width : 0;
    const value = clampGraphValue(Number(values[idx]));
    const y = ((1000 - value) / 2000) * height;
    return { x, y };
  });

  if (anchors.length < 2) return anchors;

  const interpolated = [anchors[0]];
  for (let i = 0; i < anchors.length - 1; i++) {
    const a = anchors[i];
    const b = anchors[i + 1];
    const segmentPx = Math.abs(b.x - a.x);
    const steps = Math.max(1, Math.ceil(segmentPx / graphInterpolationPxStep));

    for (let s = 1; s <= steps; s++) {
      const t = s / steps;
      interpolated.push({
        x: a.x + (b.x - a.x) * t,
        y: a.y + (b.y - a.y) * t
      });
    }
  }

  return interpolated;
}

function drawCurrentGraph() {
  const canvas = $("graph-canvas");
  if (!canvas) return;
  const ctx = canvas.getContext("2d");
  ctx.clearRect(0, 0, canvas.width, canvas.height);

  // Draw a light reference grid so waveform changes are easier to read.
  const gridTop = 1000;
  const gridBottom = -1000;
  const gridStep = 200;
  const gridRange = gridTop - gridBottom;

  ctx.save();
  ctx.strokeStyle = "rgba(158, 201, 255, 0.16)";
  ctx.lineWidth = 1;

  for (let v = gridTop; v >= gridBottom; v -= gridStep) {
    const y = ((gridTop - v) / gridRange) * canvas.height;
    ctx.beginPath();
    ctx.moveTo(0, y);
    ctx.lineTo(canvas.width, y);
    ctx.stroke();
  }

  const verticalDivisions = 8;
  for (let i = 0; i <= verticalDivisions; i++) {
    const x = (i / verticalDivisions) * canvas.width;
    ctx.beginPath();
    ctx.moveTo(x, 0);
    ctx.lineTo(x, canvas.height);
    ctx.stroke();
  }

  const zeroY = ((gridTop - 0) / gridRange) * canvas.height;
  ctx.strokeStyle = "rgba(158, 201, 255, 0.3)";
  ctx.beginPath();
  ctx.moveTo(0, zeroY);
  ctx.lineTo(canvas.width, zeroY);
  ctx.stroke();
  ctx.restore();

  const sourceValues = currentBuffer.slice(-graphWindowPoints);
  const points = sourceValues.length;
  if (points === 0) return;

  const renderPoints = currentGraphMode === "ideal"
    ? buildIdealRenderPoints(sourceValues, canvas.width, canvas.height)
    : buildRenderPoints(sourceValues, canvas.width, canvas.height);
  if (renderPoints.length === 0) return;

  ctx.strokeStyle = "#3cf";
  ctx.lineWidth = 2;
  ctx.beginPath();
  if (renderPoints.length === 1) {
    const y = renderPoints[0].y;
    ctx.moveTo(0, y);
    ctx.lineTo(canvas.width, y);
    ctx.stroke();
    return;
  }

  for (let i = 0; i < renderPoints.length; i++) {
    const { x, y } = renderPoints[i];
    if (i === 0) ctx.moveTo(x, y);
    else ctx.lineTo(x, y);
  }
  ctx.stroke();
}

function drawWaveformGlyph(type) {
  const canvas = $("waveform-glyph");
  if (!canvas) return;
  canvas._waveformType = type;

  const ctx = canvas.getContext("2d");
  ctx.clearRect(0, 0, canvas.width, canvas.height);
  ctx.strokeStyle = "#ffff00";
  ctx.lineWidth = 2;

  switch (Number(type)) {
    case 0:
      ctx.beginPath();
      ctx.moveTo(2, canvas.height / 2);
      ctx.lineTo(canvas.width - 2, canvas.height / 2);
      ctx.stroke();
      break;
    case 1:
      ctx.beginPath();
      for (let x = 0; x < canvas.width; x++) {
        const y = canvas.height / 2 - Math.sin((x / canvas.width) * 2 * Math.PI) * (canvas.height / 2 - 2);
        if (x === 0) ctx.moveTo(x, y);
        else ctx.lineTo(x, y);
      }
      ctx.stroke();
      break;
    case 2:
      ctx.beginPath();
      ctx.moveTo(2, canvas.height / 2);
      ctx.lineTo(canvas.width / 4, 2);
      ctx.lineTo((canvas.width * 3) / 4, canvas.height - 2);
      ctx.lineTo(canvas.width - 2, canvas.height / 2);
      ctx.stroke();
      break;
    case 3:
      ctx.beginPath();
      ctx.moveTo(2, canvas.height / 2);
      ctx.lineTo(2, 2);
      ctx.lineTo(canvas.width / 2, 2);
      ctx.lineTo(canvas.width / 2, canvas.height - 2);
      ctx.lineTo(canvas.width - 2, canvas.height - 2);
      ctx.lineTo(canvas.width - 2, canvas.height / 2);
      ctx.stroke();
      break;
    default:
      break;
  }
}

function updateOutputButton(state) {
  const btn = $("output-btn");
  if (!btn) return;
  if (state) {
    btn.innerText = "Output ON";
    btn.className = "btn green";
    btn.dataset.state = "on";
  } else {
    btn.innerText = "Output OFF";
    btn.className = "btn off";
    btn.dataset.state = "off";
  }
}

function updateSourceButton(source) {
  const btn = $("source-btn");
  if (!btn) return;
  if (source === "int") {
    btn.innerHTML = '<span style="color:#ff0;font-weight:bold;">Int</span><span style="color:#fff;">/Ext</span>';
  } else {
    btn.innerHTML = '<span style="color:#fff;">Int/</span><span style="color:#ff0;font-weight:bold;">Ext</span>';
  }
}

function applyState(data) {
  latestState = { ...latestState, ...data };

  if (typeof data.waveform_type !== "undefined") {
    graphSignalState.waveformType = normalizeWaveformType(data.waveform_type);
  }
  if (typeof data.frequency !== "undefined") {
    const f = Number(data.frequency);
    if (Number.isFinite(f)) graphSignalState.frequency = f;
  }
  if (typeof data.waveform_offset !== "undefined") {
    const o = Number(data.waveform_offset);
    if (Number.isFinite(o)) graphSignalState.offset = o;
  }
  if (typeof data.samples !== "undefined") {
    const s = Number(data.samples);
    if (Number.isFinite(s) && s > 0) graphSignalState.samples = s;
  }

  if (typeof data.cur !== "undefined") {
    const cur = Number(data.cur);
    $("current-value").innerText = Number.isFinite(cur) ? cur.toFixed(2) : "--";
    if (Number.isFinite(cur)) {
      pushCurrentSample(cur);
    }
  }
  if (typeof data.unit !== "undefined") $("current-unit").innerText = data.unit || "--";
  if (typeof data.range !== "undefined") {
    $("tia-range").innerText = data.range || "--";
    $("gen-range").innerText = data.range || "--";
  }
  if (typeof data.auto !== "undefined") $("auto-mode").innerText = data.auto || "--";
  if (typeof data.output !== "undefined") {
    const isOn = !!data.output;
    $("output-state").innerText = isOn ? "ON" : "OFF";
    updateOutputButton(isOn);
  }
  if (typeof data.power !== "undefined") {
    latestState.power = !!data.power;
  }
  if (typeof data.fw_version !== "undefined") {
    latestState.fw_version = data.fw_version;
  }
  if (typeof data.serial !== "undefined") {
    latestState.serial = data.serial;
  }
  if (typeof data.set_current !== "undefined") {
    const sc = Number(data.set_current);
    $("set-current-value").innerText = Number.isFinite(sc) ? sc.toFixed(2) : "--";
  }
  if (typeof data.set_unit !== "undefined") $("set-current-unit").innerText = data.set_unit || "--";
  if (typeof data.frequency !== "undefined") $("freq-value").innerText = data.frequency;
  if (typeof data.waveform_offset !== "undefined") $("offset-value").innerText = data.waveform_offset;
  if (typeof data.samples !== "undefined") $("samples-value").innerText = data.samples;
  if (typeof data.waveform_type !== "undefined") drawWaveformGlyph(data.waveform_type);
  if (typeof data.source !== "undefined") updateSourceButton(data.source);
  if (typeof data.brightness !== "undefined") {
    const b = Number(data.brightness);
    if (Number.isFinite(b)) {
      $("brightness").value = b;
      $("brightness-label").innerText = `${b}%`;
    }
  }
}

function onMqttMessage(topic, payloadBuf) {
  const payload = new TextDecoder().decode(payloadBuf);
  processInstrumentScanTopic(topic, payload);

  if (topic === `${topicBase}/state/snapshot`) {
    try {
      applyState(JSON.parse(payload));
    } catch (_) {
    }
    return;
  }

  if (!topic.startsWith(`${topicBase}/sensors/`)) return;
  const name = topic.substring(`${topicBase}/sensors/`.length);

  if (name === "current_measurement") {
    if (Date.now() - lastPacketReceiveMs < 1500) return;
    const n = Number(payload);
    applyState({ cur: Number.isFinite(n) ? n : undefined });
  } else if (name === "current_packet") {
    try {
      const packet = JSON.parse(payload);
      const values = Array.isArray(packet.values) ? packet.values : [];
      if (values.length > 0) {
        pushCurrentSamples(values);
        const last = Number(values[values.length - 1]);
        if (Number.isFinite(last)) {
          $("current-value").innerText = last.toFixed(2);
        }
        lastPacketReceiveMs = Date.now();
      }
    } catch (_) {
    }
  } else if (name === "current_unit") {
    applyState({ unit: payload });
  } else if (name === "range") {
    applyState({ range: payload });
  } else if (name === "auto_mode") {
    applyState({ auto: payload });
  } else if (name === "output") {
    applyState({ output: payload === "1" || payload.toLowerCase() === "on" });
  } else if (name === "power") {
    applyState({ power: payload === "1" || payload.toLowerCase() === "on" });
  } else if (name === "source") {
    applyState({ source: payload });
  } else if (name === "set_current") {
    applyState({ set_current: Number(payload) });
  } else if (name === "set_unit") {
    applyState({ set_unit: payload });
  } else if (name === "waveform_type") {
    applyState({ waveform_type: Number(payload) });
  } else if (name === "frequency") {
    applyState({ frequency: payload });
  } else if (name === "waveform_offset") {
    applyState({ waveform_offset: payload });
  } else if (name === "samples") {
    applyState({ samples: payload });
  } else if (name === "brightness") {
    applyState({ brightness: Number(payload) });
  } else if (name === "rssi") {
    applyState({ rssi: Number(payload) });
  } else if (name === "ssid") {
    applyState({ ssid: payload });
  } else if (name === "ip_address") {
    applyState({ ip: payload });
  } else if (name === "firmware_version") {
    applyState({ fw_version: payload });
  } else if (name === "serial_number") {
    applyState({ serial: Number(payload) });
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
  if (!websiteUniqueId) {
    setFoot("No instrument ID in URL. Use ...#<unique_id>");
    setHardwareLed(false);
    return;
  }

  const candidateUrls = mqttWsUrls();

  // Include known cloud endpoints as safety fallbacks.
  const fallbackUrls = mqttWsUrlsForHostPort(BROKER_HOST, BROKER_PORT_WS);
  for (const url of fallbackUrls) {
    if (!candidateUrls.includes(url)) {
      candidateUrls.push(url);
    }
  }

  if (candidateUrls.length === 0) {
    setHardwareLed(false);
    setFoot("No MQTT endpoints available");
    return;
  }

  const opts = {
    reconnectPeriod: 0,
    username: BROKER_USER,
    password: BROKER_PASS,
    clientId: `TIARA-WEB-${websiteUniqueId}`
  };

  if (location.protocol === "https:") {
    setFoot(`HTTPS page detected, using secure MQTT: ${mqttWsUrl()}`);
  }

  const connectAt = (urlIndex) => {
    const endpoint = candidateUrls[urlIndex];
    let connected = false;
    let switched = false;

    setFoot(`Connecting MQTT: ${endpoint}`);

    try {
      if (mqttClient) mqttClient.end(true);
      mqttClient = mqtt.connect(endpoint, opts);

      mqttClient.on("connect", () => {
        connected = true;
        setHardwareLed(true);
        setFoot(`Connected ${topicBase}`);
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
        setHardwareLed(false);
        setFoot("MQTT disconnected; retrying...");
        scheduleReconnect();
      });

      mqttClient.on("offline", () => {
        if (!connected && !switched && urlIndex + 1 < candidateUrls.length) {
          switched = true;
          connectAt(urlIndex + 1);
          return;
        }
        setHardwareLed(false);
        setFoot("MQTT offline; retrying...");
        scheduleReconnect();
      });

      mqttClient.on("error", (err) => {
        if (!connected && !switched && urlIndex + 1 < candidateUrls.length) {
          switched = true;
          try { mqttClient.end(true); } catch (_) {}
          connectAt(urlIndex + 1);
          return;
        }

        setHardwareLed(false);
        const message = err && err.message ? err.message : "unknown";
        setFoot(`MQTT error (${endpoint}): ${message}`);
        try { mqttClient.end(true); } catch (_) {}
        scheduleReconnect();
      });
    } catch (e) {
      if (urlIndex + 1 < candidateUrls.length) {
        connectAt(urlIndex + 1);
        return;
      }
      setHardwareLed(false);
      setFoot(`Connect failed: ${e.message}`);
      scheduleReconnect();
    }
  };

  connectAt(0);
}

function toggleOutput() {
  const btn = $("output-btn");
  const isOn = btn && btn.dataset.state === "on";
  publishTopic("/control/set_output", isOn ? "off" : "on");
}

function setBrightness(value) {
  $("brightness-label").innerText = `${value}%`;
  publishTopic("/control/set_brightness", String(value));
}

function showInfo() {
  const instrumentId = getInstrumentDisplayName(websiteUniqueId || "--");
  const infoIp = latestState.ip || (websiteUniqueId ? `piot-${websiteUniqueId}.local` : "--");
  const infoSsid = latestState.ssid || "--";
  const hasRssi = typeof latestState.rssi !== "undefined" && latestState.rssi !== null && latestState.rssi !== "";
  const infoRssi = hasRssi ? latestState.rssi : "--";
  const outputState = $("output-state").innerText || (latestState.output ? "ON" : "OFF");
  const powerState = latestState.power !== undefined ? (latestState.power ? "On" : "Off") : "--";
  const currentText = $("current-value").innerText || "--";
  const currentUnit = $("current-unit").innerText || "--";
  const setCurrentText = $("set-current-value").innerText || "--";
  const setCurrentUnit = $("set-current-unit").innerText || "--";
  const firmwareVersion = latestState.fw_version || "--";
  const serialNumber = latestState.serial !== undefined ? String(latestState.serial) : "--";
  const waveformText = $("waveform-btn") ? $("waveform-btn").textContent : "Constant";
  const freqText = $("freq-value").innerText || "--";
  const offsetText = $("offset-value").innerText || "--";
  const sampleText = $("samples-value").innerText || "--";

  const html = [
    `<b>Firmware:</b> ${firmwareVersion}`,
    `<b>Instrument ID:</b> ${instrumentId}`,
    `<b>Serial #:</b> ${serialNumber}`,
    `<b>IP Address:</b> ${infoIp}`,
    `<b>Connected To:</b> ${infoSsid}`,
    `<b>Signal:</b> ${infoRssi} dBm`,
    `<b>Current:</b> ${currentText} ${currentUnit}`,
    `<b>Set Current:</b> ${setCurrentText} ${setCurrentUnit}`,
    `<b>Range:</b> ${$("tia-range").innerText || "--"}`,
    `<b>Auto Mode:</b> ${$("auto-mode").innerText || "--"}`,
    `<b>Power:</b> ${powerState}`,
    `<b>Output:</b> ${outputState}`,
    `<b>Waveform:</b> ${waveformText}`,
    `<b>Frequency:</b> ${freqText} Hz`,
    `<b>Offset:</b> ${offsetText}%`,
    `<b>Samples:</b> ${sampleText}`
  ].join("<br>");

  $("info-content").innerHTML = html;
  $("info-modal").style.display = "flex";
}

function hideInfoModal() {
  $("info-modal").style.display = "none";
}

function navigateNetwork() {
  window.open(`http://piot-${websiteUniqueId}.local/network`, "_blank");
}

function showFirmwareUpdateModal() {
  alert("Firmware update is available from the embedded local UI only.");
}

function showSpiffsManagerModal() {
  alert("SPIFFS manager is available from the embedded local UI only.");
}

function showSetCurrentDialog() {
  const currentValues = {
    value: parseFloat($("set-current-value").textContent) || 0,
    unit: $("set-current-unit").textContent || "uA",
    waveform: Number($("waveform-glyph")._waveformType || 0),
    frequency: parseFloat($("freq-value").textContent) || 100,
    offset: parseFloat($("offset-value").textContent) || 0,
    samples: parseInt($("samples-value").textContent, 10) || 64
  };

  const waveformLabels = ["Constant", "Sinewave", "Triangle", "Square"];
  const frequencyValues = ["0.1", "0.2", "0.5", "1", "2", "5", "10", "20", "50", "100", "200", "300", "500", "700", "1k"];
  const frequencyActual = [0.1, 0.2, 0.5, 1, 2, 5, 10, 20, 50, 100, 200, 300, 500, 700, 1000];
  const offsetValues = ["-100%", "-75%", "-50%", "-25%", "-10%", "-5%", "0%", "+5%", "+10%", "+25%", "+50%", "+75%", "+100%"];
  const offsetActual = [-100, -75, -50, -25, -10, -5, 0, 5, 10, 25, 50, 75, 100];
  const samplesValues = ["8", "12", "16", "24", "32", "48", "64", "96", "128", "192", "256"];
  const samplesActual = [8, 12, 16, 24, 32, 48, 64, 96, 128, 192, 256];

  let freqIndex = frequencyActual.findIndex((f) => Math.abs(f - currentValues.frequency) < 0.01);
  if (freqIndex < 0) freqIndex = 9;
  let offsetIndex = offsetActual.findIndex((o) => Math.abs(o - currentValues.offset) < 0.01);
  if (offsetIndex < 0) offsetIndex = 6;
  let samplesIndex = samplesActual.findIndex((s) => s === currentValues.samples);
  if (samplesIndex < 0) samplesIndex = 6;

  let currentInput = currentValues.value === 0 ? "0" : currentValues.value.toFixed(2);
  let inputCleared = false;
  $("settings-current-value").textContent = currentInput;

  function updateUnitButtons() {
    document.querySelectorAll(".unit-btn").forEach((btn) => {
      btn.classList.toggle("active", btn.dataset.unit === currentValues.unit);
    });
  }

  function updateWaveformButton() {
    $("waveform-btn").textContent = waveformLabels[currentValues.waveform];
  }

  function updateFrequencyButton() {
    $("freq-btn").querySelector("span").textContent = frequencyValues[freqIndex];
  }

  function updateOffsetButton() {
    $("offset-btn").querySelector("span").textContent = offsetValues[offsetIndex];
  }

  function updateSamplesButton() {
    $("samples-btn").querySelector("span").textContent = samplesValues[samplesIndex];
  }

  document.querySelectorAll(".keypad-btn").forEach((btn) => {
    btn.onclick = () => {
      const key = btn.dataset.key;
      if (key === "CLR") {
        currentInput = "0";
        inputCleared = true;
      } else if (key === "+/-") {
        if (currentInput.startsWith("-")) currentInput = currentInput.substring(1);
        else if (currentInput !== "0") currentInput = `-${currentInput}`;
      } else if (key === "M+" || key === "MEM") {
        return;
      } else if (key === ".") {
        if (!inputCleared && (currentInput === "0" || currentInput === currentValues.value.toFixed(2))) {
          currentInput = "";
          inputCleared = true;
        }
        if (!currentInput.includes(".")) currentInput += ".";
      } else {
        if (!inputCleared && (currentInput === "0" || currentInput === currentValues.value.toFixed(2))) {
          currentInput = "";
          inputCleared = true;
        }
        currentInput = currentInput === "0" ? key : `${currentInput}${key}`;
      }
      $("settings-current-value").textContent = currentInput;
    };
  });

  $("settings-bksp").onclick = () => {
    if (currentInput.length > 0) currentInput = currentInput.slice(0, -1);
    if (currentInput.length === 0 || currentInput === "-") currentInput = "0";
    $("settings-current-value").textContent = currentInput;
  };

  $("waveform-btn").onclick = () => {
    currentValues.waveform = (currentValues.waveform + 1) % 4;
    updateWaveformButton();
  };
  $("freq-btn").onclick = () => {
    freqIndex = (freqIndex + 1) % frequencyValues.length;
    currentValues.frequency = frequencyActual[freqIndex];
    updateFrequencyButton();
  };
  $("offset-btn").onclick = () => {
    offsetIndex = (offsetIndex + 1) % offsetValues.length;
    currentValues.offset = offsetActual[offsetIndex];
    updateOffsetButton();
  };
  $("samples-btn").onclick = () => {
    samplesIndex = (samplesIndex + 1) % samplesValues.length;
    currentValues.samples = samplesActual[samplesIndex];
    updateSamplesButton();
  };

  document.querySelectorAll(".unit-btn").forEach((btn) => {
    btn.onclick = () => {
      currentValues.unit = btn.dataset.unit;
      updateUnitButtons();
    };
  });

  $("settings-set").onclick = () => {
    const value = parseFloat(currentInput);
    if (!Number.isFinite(value)) {
      alert("Invalid number format");
      return;
    }
    if (Math.abs(value) > 1000) {
      alert(`Value must be between -1000 and 1000 ${currentValues.unit}`);
      return;
    }

    sendCmd(`set_current:${value}:${currentValues.unit}`);
    sendCmd(`waveform:${currentValues.waveform}`);
    sendCmd(`freq:${currentValues.frequency}`);
    sendCmd(`offset:${currentValues.offset}`);
    sendCmd(`samples:${currentValues.samples}`);
    $("settings-modal").style.display = "none";
  };

  $("settings-cancel").onclick = () => {
    $("settings-modal").style.display = "none";
  };

  updateUnitButtons();
  updateWaveformButton();
  updateFrequencyButton();
  updateOffsetButton();
  updateSamplesButton();
  $("settings-modal").style.display = "flex";
}

function showSetCurrent() {
  showSetCurrentDialog();
}

window.toggleOutput = toggleOutput;
window.setBrightness = setBrightness;
window.showInfo = showInfo;
window.hideInfoModal = hideInfoModal;
window.navigateNetwork = navigateNetwork;
window.showSetCurrent = showSetCurrent;
window.showFirmwareUpdateModal = showFirmwareUpdateModal;
window.showSpiffsManagerModal = showSpiffsManagerModal;

function bootstrap() {
  const urlId = getUniqueIdFromUrl();
  const savedId = normalizeUniqueId(localStorage.getItem(KEY_WEBSITE_ID));
  const defaultId = getDefaultInstrumentId();
  websiteUniqueId = urlId || savedId || defaultId;

  if (websiteUniqueId) {
    addKnownInstrumentId(websiteUniqueId);
    localStorage.setItem(KEY_WEBSITE_ID, websiteUniqueId);
    localStorage.setItem(KEY_DEVICE_ID, websiteUniqueId);
    updateInstrumentHash(websiteUniqueId);
  }

  topicBase = `${TOPIC_ROOT}/${websiteUniqueId}`;
  latestState.ip = websiteUniqueId ? `piot-${websiteUniqueId}.local` : "--";
  latestState.ssid = "--";

  renderInstrumentSelector();

  const openConnectionBtn = $("connection-open-btn");
  if (openConnectionBtn) openConnectionBtn.addEventListener("click", openConnectionModal);

  const closeConnectionBtn = $("connection-close-btn");
  if (closeConnectionBtn) closeConnectionBtn.addEventListener("click", closeConnectionModal);

  const connectionModal = $("connection-modal");
  if (connectionModal) {
    connectionModal.addEventListener("click", (event) => {
      if (event.target === connectionModal) {
        closeConnectionModal();
      }
    });
  }

  const scanResultsModal = $("instrument-scan-results-modal");
  if (scanResultsModal) {
    scanResultsModal.addEventListener("click", (event) => {
      if (event.target === scanResultsModal) {
        closeInstrumentScanResultsModal();
      }
    });
  }

  const scanAddSelectedBtn = $("instrument-scan-add-selected-btn");
  if (scanAddSelectedBtn) scanAddSelectedBtn.addEventListener("click", addSelectedScannedInstruments);

  const scanCancelBtn = $("instrument-scan-cancel-btn");
  if (scanCancelBtn) scanCancelBtn.addEventListener("click", closeInstrumentScanResultsModal);

  const scanSelectAllBtn = $("instrument-scan-select-all-btn");
  if (scanSelectAllBtn) {
    scanSelectAllBtn.addEventListener("click", () => setScannedInstrumentSelection(true));
  }

  const scanClearAllBtn = $("instrument-scan-clear-all-btn");
  if (scanClearAllBtn) {
    scanClearAllBtn.addEventListener("click", () => setScannedInstrumentSelection(false));
  }

  const select = $("instrument-select");
  if (select) {
    select.addEventListener("change", () => {
      const selected = normalizeUniqueId(select.value);
      if (selected) switchInstrument(selected, true);
    });
  }

  const addBtn = $("instrument-add-btn");
  if (addBtn) addBtn.addEventListener("click", addInstrumentFromInput);

  const saveNameBtn = $("instrument-name-save-btn");
  if (saveNameBtn) saveNameBtn.addEventListener("click", saveSelectedInstrumentName);

  const defaultBtn = $("instrument-default-btn");
  if (defaultBtn) defaultBtn.addEventListener("click", setCurrentInstrumentAsDefault);

  const exportBtn = $("instrument-export-btn");
  if (exportBtn) exportBtn.addEventListener("click", exportInstrumentsToFile);

  const importBtn = $("instrument-import-btn");
  if (importBtn) {
    importBtn.addEventListener("click", () => {
      const picker = $("instrument-import-input");
      if (picker) picker.click();
    });
  }

  const importInput = $("instrument-import-input");
  if (importInput) {
    importInput.addEventListener("change", (event) => {
      const target = event.target;
      const file = target && target.files ? target.files[0] : null;
      importInstrumentsFromFile(file);
      if (target) target.value = "";
    });
  }

  const manageBtn = $("instrument-manage-btn");
  if (manageBtn) manageBtn.addEventListener("click", openInstrumentManager);

  const scanBtn = $("instrument-scan-btn");
  if (scanBtn) scanBtn.addEventListener("click", startInstrumentScan);

  const manageCloseBtn = $("instrument-manager-close-btn");
  if (manageCloseBtn) manageCloseBtn.addEventListener("click", closeInstrumentManager);

  const graphMode = $("graph-mode");
  if (graphMode) {
    graphMode.addEventListener("change", () => {
      setGraphRenderMode(graphMode.value, true);
    });
  }

  const input = $("instrument-input");
  if (input) {
    input.addEventListener("keydown", (event) => {
      if (event.key === "Enter") {
        event.preventDefault();
        addInstrumentFromInput();
      }
    });
  }

  const nameInput = $("instrument-name-input");
  if (nameInput) {
    nameInput.addEventListener("keydown", (event) => {
      if (event.key === "Enter") {
        event.preventDefault();
        saveSelectedInstrumentName();
      }
    });
  }

  if ("serviceWorker" in navigator) {
    window.addEventListener("load", () => {
      navigator.serviceWorker.register("./service-worker.js?v=20261005a").catch(() => {});
    });
  }

  buildGraphLegend();
  initGraphRenderMode();
  drawCurrentGraph();
  drawWaveformGlyph(0);
  updateOutputButton(false);
  updateSourceButton("ext");

  window.addEventListener("hashchange", () => {
    const hashId = getUniqueIdFromUrl();
    if (hashId && hashId !== websiteUniqueId) {
      switchInstrument(hashId, true);
    }
  });

  connectMqtt();
  setInstrumentScanButtonState();
}

bootstrap();
