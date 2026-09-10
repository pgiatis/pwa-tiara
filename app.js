const $ = (id) => document.getElementById(id);

const KEY_WEBSITE_ID = "tiara.website.unique.id";
const KEY_DEVICE_ID = "tiara.device.id";
const BROKER_HOST = "pgiatis.dyndns.org";
const BROKER_PORT_WS = 9001;
const BROKER_USER = "BisinaSystems";
const BROKER_PASS = "BisinaSystems123";
const TOPIC_ROOT = "tiara";
const CLOUD_PWA_BASE_URL = "https://pgiatis.github.io/pwa-tiara/";

let mqttClient = null;
let reconnectTimer = null;
let websiteUniqueId = "";
let topicBase = "";
let latestState = {};
let currentBuffer = [];
const bufferSize = 200;

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

function buildPwaInstallUrl() {
  const id = websiteUniqueId || normalizeUniqueId(localStorage.getItem(KEY_WEBSITE_ID));
  if (!id) return CLOUD_PWA_BASE_URL;
  return `${CLOUD_PWA_BASE_URL}#${encodeURIComponent(id)}`;
}

function showPwaQrModal() {
  const installUrl = buildPwaInstallUrl();
  const qrImage = $("pwa-qr-image");
  const qrUrlLabel = $("pwa-qr-url");
  const modal = $("pwa-qr-modal");
  if (!qrImage || !qrUrlLabel || !modal) return;

  qrImage.src = `https://api.qrserver.com/v1/create-qr-code/?size=220x220&data=${encodeURIComponent(installUrl)}`;
  qrUrlLabel.textContent = installUrl;
  modal.style.display = "flex";
}

function hidePwaQrModal() {
  const modal = $("pwa-qr-modal");
  if (modal) modal.style.display = "none";
}

function mqttWsUrl() {
  if (location.protocol === "https:") {
    return `wss://${BROKER_HOST}/mqtt`;
  }
  return `ws://${BROKER_HOST}:${BROKER_PORT_WS}/mqtt`;
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

function drawCurrentGraph() {
  const canvas = $("graph-canvas");
  if (!canvas) return;
  const ctx = canvas.getContext("2d");
  ctx.clearRect(0, 0, canvas.width, canvas.height);

  ctx.strokeStyle = "#3cf";
  ctx.lineWidth = 2;
  ctx.beginPath();
  for (let i = 0; i < bufferSize; i++) {
    const val = currentBuffer[i] !== undefined ? currentBuffer[i] : 0;
    const y = ((1000 - val) / 2000) * canvas.height;
    const x = (i / (bufferSize - 1)) * canvas.width;
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

  if (typeof data.cur !== "undefined") {
    const cur = Number(data.cur);
    $("current-value").innerText = Number.isFinite(cur) ? cur.toFixed(2) : "--";
    if (Number.isFinite(cur)) {
      currentBuffer.push(cur);
      if (currentBuffer.length > bufferSize) currentBuffer.shift();
      drawCurrentGraph();
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
  if (typeof data.rssi !== "undefined") $("wifi-rssi").innerText = data.rssi;
  if (typeof data.ssid !== "undefined") $("wifi-ssid").innerText = data.ssid || "--";
  if (typeof data.ip !== "undefined") $("ip-address").innerText = data.ip || `piot-${websiteUniqueId}.local`;
}

function onMqttMessage(topic, payloadBuf) {
  const payload = new TextDecoder().decode(payloadBuf);

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
    const n = Number(payload);
    applyState({ cur: Number.isFinite(n) ? n : undefined });
  } else if (name === "current_unit") {
    applyState({ unit: payload });
  } else if (name === "range") {
    applyState({ range: payload });
  } else if (name === "auto_mode") {
    applyState({ auto: payload });
  } else if (name === "output") {
    applyState({ output: payload === "1" || payload.toLowerCase() === "on" });
  } else if (name === "source") {
    applyState({ source: payload });
  } else if (name === "set_current") {
    applyState({ set_current: Number(payload) });
  } else if (name === "set_unit") {
    applyState({ set_unit: payload });
  } else if (name === "frequency") {
    applyState({ frequency: payload });
  } else if (name === "waveform_offset") {
    applyState({ waveform_offset: payload });
  } else if (name === "samples") {
    applyState({ samples: payload });
  } else if (name === "brightness") {
    applyState({ brightness: Number(payload) });
  } else if (name === "ip_address") {
    // keep placeholder for info modal
    latestState.ip = payload;
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

  const opts = {
    reconnectPeriod: 0,
    username: BROKER_USER,
    password: BROKER_PASS,
    clientId: `TIARA-WEB-${websiteUniqueId}`
  };

  if (location.protocol === "https:") {
    setFoot(`HTTPS page detected, using secure MQTT: ${mqttWsUrl()}`);
  }

  setFoot("Connecting to MQTT broker...");

  try {
    if (mqttClient) mqttClient.end(true);
    mqttClient = mqtt.connect(mqttWsUrl(), opts);

    mqttClient.on("connect", () => {
      setHardwareLed(true);
      setFoot(`Connected ${topicBase}`);
      mqttClient.subscribe(`${topicBase}/#`);
      publishTopic("/control/read_state", "");
    });

    mqttClient.on("message", onMqttMessage);
    mqttClient.on("close", () => {
      setHardwareLed(false);
      setFoot("MQTT disconnected; retrying...");
      scheduleReconnect();
    });
    mqttClient.on("offline", () => {
      setHardwareLed(false);
      setFoot("MQTT offline; retrying...");
      scheduleReconnect();
    });
    mqttClient.on("error", (err) => {
      setHardwareLed(false);
      const message = err && err.message ? err.message : "unknown";
      setFoot(`MQTT error (${mqttWsUrl()}): ${message}`);
      try { mqttClient.end(true); } catch (_) {}
      scheduleReconnect();
    });
  } catch (e) {
    setHardwareLed(false);
    setFoot(`Connect failed: ${e.message}`);
    scheduleReconnect();
  }
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
  const html = [
    `<b>Instrument ID:</b> ${websiteUniqueId || "--"}`,
    `<b>MQTT Endpoint:</b> ${mqttWsUrl()}`,
    `<b>Topic Base:</b> ${topicBase || "--"}`,
    `<b>Current:</b> ${$("current-value").innerText} ${$("current-unit").innerText}`,
    `<b>Set Current:</b> ${$("set-current-value").innerText} ${$("set-current-unit").innerText}`,
    `<b>Range:</b> ${$("tia-range").innerText}`,
    `<b>Auto Mode:</b> ${$("auto-mode").innerText}`,
    `<b>Output:</b> ${$("output-state").innerText}`,
    `<b>Frequency:</b> ${$("freq-value").innerText} Hz`,
    `<b>Offset:</b> ${$("offset-value").innerText}%`,
    `<b>Samples:</b> ${$("samples-value").innerText}`,
    `<b>Signal:</b> ${$("wifi-rssi").innerText} dBm`
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
window.showPwaQrModal = showPwaQrModal;
window.hidePwaQrModal = hidePwaQrModal;

function bootstrap() {
  websiteUniqueId = getUniqueIdFromUrl() || normalizeUniqueId(localStorage.getItem(KEY_WEBSITE_ID));
  if (websiteUniqueId) {
    localStorage.setItem(KEY_WEBSITE_ID, websiteUniqueId);
    localStorage.setItem(KEY_DEVICE_ID, websiteUniqueId);
    const currentHash = normalizeUniqueId((window.location.hash || "").replace("#", ""));
    if (currentHash !== websiteUniqueId) {
      window.history.replaceState({}, "", `${window.location.pathname}${window.location.search}#${encodeURIComponent(websiteUniqueId)}`);
    }
  }

  topicBase = `${TOPIC_ROOT}/${websiteUniqueId}`;
  $("ip-address").textContent = websiteUniqueId ? `piot-${websiteUniqueId}.local` : "--";
  $("wifi-ssid").textContent = "--";

  if ("serviceWorker" in navigator) {
    window.addEventListener("load", () => {
      navigator.serviceWorker.register("./service-worker.js").catch(() => {});
    });
  }

  buildGraphLegend();
  drawCurrentGraph();
  drawWaveformGlyph(0);
  updateOutputButton(false);
  updateSourceButton("ext");
  connectMqtt();
}

bootstrap();
