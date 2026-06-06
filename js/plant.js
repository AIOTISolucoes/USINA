// ======================================================
// CONTROLE DE ACESSO POR ROLE
// ======================================================
function _getUserRole() {
  try {
    const u = JSON.parse(localStorage.getItem("user") || "{}");
    return u.role_key || "viewer";
  } catch { return "viewer"; }
}
function _canSendCommand() {
  if (typeof canSendCommand === "function") return canSendCommand();
  const r = _getUserRole();
  return ["superuser", "operator", "admin_customer"].includes(r);
}

// ======================================================
// ESTADO ÚNICO DA USINA (FONTE DA VERDADE NO FRONT)
// ======================================================
let PLANT_STATE = {
  name: "—",
  rated_power_kwp: 0,
  active_power_kw: 0,
  capacity_percent: 0,
  inverter_total: 0,
  inverter_online: 0,
  pr_percent: 0,
  capacity_ac: null,
};

// ======================================================
// CONFIG (ONLINE/OFFLINE)
// ======================================================
const MINUTE_MS = 60 * 1000;

const INVERTER_OFFLINE_AFTER_MINUTES = 25;
const STRING_STALE_AFTER_MINUTES = 25;

const INVERTER_ONLINE_AFTER_MS = INVERTER_OFFLINE_AFTER_MINUTES * MINUTE_MS;
const INVERTER_NO_COMM_AFTER_MS = INVERTER_OFFLINE_AFTER_MINUTES * MINUTE_MS;
const STRING_STALE_AFTER_MS = STRING_STALE_AFTER_MINUTES * MINUTE_MS;

// ======================================================
// FUNÇÕES AUXILIARES
// ======================================================
function asNumber(value, fallback = 0) {
  if (value === null || value === undefined) return fallback;
  const s = typeof value === "string" ? value.replace(",", ".").trim() : value;
  const parsed = Number(s);
  return Number.isFinite(parsed) ? parsed : fallback;
}

function formatNumberPtBR(v) {
  const n = Number(v);
  if (!Number.isFinite(n)) return "—";
  return new Intl.NumberFormat("pt-BR").format(n);
}

function formatKwhPtBR(v) {
  const n = Number(v);
  if (!Number.isFinite(n)) return "—";
  return `${formatNumberPtBR(n)} kWh`;
}

function formatKwPtBR(v) {
  const n = Number(v);
  if (!Number.isFinite(n)) return "—";
  return `${formatNumberPtBR(n)} kW`;
}

function formatWm2PtBR(v) {
  const n = Number(v);
  if (!Number.isFinite(n)) return "—";
  return `${formatNumberPtBR(n)} W/m²`;
}

function buildLastNDaysLabels(n) {
  const labels = [];
  const today = new Date();
  for (let i = n - 1; i >= 0; i--) {
    const d = new Date(today);
    d.setDate(today.getDate() - i);
    const dd = String(d.getDate()).padStart(2, "0");
    const mm = String(d.getMonth() + 1).padStart(2, "0");
    labels.push(`${dd}/${mm}`);
  }
  return labels;
}

function looksLikeDayOnlyLabel(label) {
  const s = String(label ?? "").trim();
  if (!s) return false;
  if (s.includes("/") || s.includes("-")) return false;
  return /^\d{1,2}$/.test(s);
}

function hasDuplicateLabels(labels) {
  const set = new Set();
  for (const l of labels) {
    const key = String(l);
    if (set.has(key)) return true;
    set.add(key);
  }
  return false;
}

function valueOrDash(v) {
  return v === null || v === undefined || v === "" ? "—" : v;
}

function fmtDatePtBR(iso) {
  if (!iso) return "—";
  const d = new Date(iso);
  return isNaN(d.getTime()) ? "—" : d.toLocaleString("pt-BR");
}

function numFixedOrDash(v, digits = 1) {
  // ✅ IMPORTANT: 0 deve aparecer como "0.0", não "—"
  const n = Number(typeof v === "string" ? v.replace(",", ".") : v);
  return Number.isFinite(n) ? n.toFixed(digits) : "—";
}

function normalizePercentMaybe(v) {
  const n = Number(typeof v === "string" ? v.replace(",", ".") : v);
  if (!Number.isFinite(n)) return null;
  if (n <= 1.0) return n * 100;
  return n;
}

function fmtAmp(v) {
  const n = Number(typeof v === "string" ? v.replace(",", ".") : v);
  if (!Number.isFinite(n)) return "—";
  return `${n.toFixed(2)} A`;
}

// ======================================================
// HELPERS — INVERTER STATUS (inverter_status / state_operation / working_status)
// ======================================================
function getInverterStatusInfo(inv) {
  const raw = inv?.inverter_status ?? inv?.state_operation;
  const code = Number(raw);
  if (!Number.isFinite(code)) return { code: null, label: "—", cls: "inv-st-unknown" };
  switch (code) {
    case 0: return { code, label: "Off",     cls: "inv-st-off" };
    case 1: return { code, label: "Standby", cls: "inv-st-standby" };
    case 2: return { code, label: "Run",     cls: "inv-st-run" };
    case 3: return { code, label: "Fault",   cls: "inv-st-fault" };
    default: return { code, label: `Desconhecido (${code})`, cls: "inv-st-unknown" };
  }
}

function getWorkingStatus(inv) {
  const raw = inv?.working_status;
  if (raw == null || raw === "") return "—";
  return String(raw);
}

// ======================================================
// ✅ HELPERS NUMÉRICOS PARA MENSAL (UNIDADE + OUTLIER)
// ======================================================
function median(arr) {
  const a = arr.filter(x => Number.isFinite(x)).slice().sort((x, y) => x - y);
  if (!a.length) return 0;
  const mid = Math.floor(a.length / 2);
  return a.length % 2 ? a[mid] : (a[mid - 1] + a[mid]) / 2;
}

function p95(arr) {
  const a = arr.filter(x => Number.isFinite(x)).slice().sort((x, y) => x - y);
  if (!a.length) return 0;
  const idx = Math.max(0, Math.min(a.length - 1, Math.floor((a.length - 1) * 0.95)));
  return a[idx];
}

/**
 * Converte Wh->kWh se detectar escala absurda.
 * - Para 30 dias de uma usina ~2MW, MTD em kWh não deveria ir pra milhões.
 */
function maybeConvertWhToKwh(dailyArr, mtdArr) {
  const maxCum = Math.max(...(mtdArr || []), 0);
  const looksLikeWh = maxCum > 500000; // gatilho conservador
  if (!looksLikeWh) return { daily: dailyArr, mtd: mtdArr, converted: false };
  return {
    daily: dailyArr.map(v => v / 1000),
    mtd: mtdArr.map(v => v / 1000),
    converted: true
  };
}

/**
 * Trata outliers: se um dia é MUITO maior que o normal, capamos pra não destruir o gráfico.
 * Regra: se v > max(mediana*25, p95*4) => cap = max(mediana*10, p95*1.5)
 */
function capMonthlyOutliers(dailyArr) {
  const daily = dailyArr.map(v => Number(v) || 0);

  const med = median(daily);
  const q95 = p95(daily);

  const spikeThreshold = Math.max(med * 25, q95 * 4);
  const capValue = Math.max(med * 10, q95 * 1.5);

  let changed = false;
  const capped = daily.map(v => {
    if (med <= 0 && q95 <= 0) return v;
    if (v > spikeThreshold && capValue > 0) {
      changed = true;
      return capValue;
    }
    return v;
  });

  return { daily: capped, changed, med, q95, spikeThreshold, capValue };
}

// ======================================================
// ✅ NORMALIZA PAYLOAD DIÁRIO (00:00 até último dado do dia)
// - Filtra apenas pontos do dia atual quando houver timestamp por ponto
// - Preenche faltas com 0 para evitar buracos visuais
// ======================================================
function normalizeDailyPayload(payload) {
  if (!payload) return payload;

  const labelsRaw = Array.isArray(payload.labels) ? payload.labels.slice() : [];

  const powerRaw =
    Array.isArray(payload.activePower) ? payload.activePower.slice() :
    Array.isArray(payload.active_power_kw) ? payload.active_power_kw.slice() :
    Array.isArray(payload.power_kw) ? payload.power_kw.slice() :
    [];

  const irrRaw =
    Array.isArray(payload.irradiance) ? payload.irradiance.slice() :
    Array.isArray(payload.irradiance_wm2) ? payload.irradiance_wm2.slice() :
    [];

  const expectedRaw =
    Array.isArray(payload.expectedPower) ? payload.expectedPower.slice() :
    Array.isArray(payload.expected_power_kw) ? payload.expected_power_kw.slice() :
    Array.isArray(payload.expected_power) ? payload.expected_power.slice() :
    [];

  if (!labelsRaw.length) return payload;

  // timestamp por ponto (se existir), para filtrar estritamente o DIA ATUAL
  const pointTsRaw =
    Array.isArray(payload.timestamps) ? payload.timestamps :
    Array.isArray(payload.ts) ? payload.ts :
    Array.isArray(payload.point_timestamps) ? payload.point_timestamps :
    Array.isArray(payload.time) ? payload.time :
    null;

  const hasPointTimestamps = Array.isArray(pointTsRaw) && pointTsRaw.length > 0;

  // "HH:mm" -> minutos desde 00:00
  const toMin = (hhmm) => {
    const m = String(hhmm || "").trim().match(/^(\d{1,2}):(\d{2})$/);
    if (!m) return null;
    const hh = Number(m[1]), mm = Number(m[2]);
    if (!Number.isFinite(hh) || !Number.isFinite(mm)) return null;
    if (hh < 0 || hh > 23 || mm < 0 || mm > 59) return null;
    return hh * 60 + mm;
  };

  const dateKeyInFortaleza = (d) => {
    if (!(d instanceof Date) || Number.isNaN(d.getTime())) return null;
    return d.toLocaleDateString("en-CA", { timeZone: "America/Fortaleza" });
  };

  const todayKeyFortaleza = new Date().toLocaleDateString("en-CA", { timeZone: "America/Fortaleza" });

  const points = [];
  for (let i = 0; i < labelsRaw.length; i++) {
    const minute = toMin(labelsRaw[i]);
    if (minute == null) continue;

    if (hasPointTimestamps) {
      const ts = pointTsRaw[i];
      const d = ts ? new Date(ts) : null;
      const key = dateKeyInFortaleza(d);
      if (!key || key !== todayKeyFortaleza) continue;
    }

    points.push({
      minute,
      power: powerRaw[i] != null ? asNumber(powerRaw[i], 0) : 0,
      irr: irrRaw[i] != null ? asNumber(irrRaw[i], 0) : 0,
      expected: expectedRaw[i] != null ? asNumber(expectedRaw[i], 0) : 0
    });
  }

  if (!points.length) {
    return {
      ...payload,
      labels: [],
      activePower: [],
      irradiance: [],
      expectedPower: []
    };
  }

  const mins = points.map(p => p.minute).sort((a, b) => a - b);

  // detecta o passo (1,5,10,15...) olhando o menor diff positivo
  let step = 5; // fallback
  if (mins.length >= 3) {
    const diffs = [];
    for (let i = 1; i < mins.length; i++) {
      const d = mins[i] - mins[i - 1];
      if (d > 0 && d <= 60) diffs.push(d);
    }
    if (diffs.length) step = Math.max(1, Math.min(...diffs));
  }

  const mapP = new Map();
  const mapI = new Map();
  const mapE = new Map();
  points.forEach(p => {
    mapP.set(p.minute, p.power);
    mapI.set(p.minute, p.irr);
    mapE.set(p.minute, p.expected);
  });

  // começa SEMPRE em 00:00 e termina no último minuto que chegou dado hoje
  const lastMin = Math.max(...mins);

  const labels = [];
  const activePower = [];
  const irradiance = [];
  const expectedPower = [];

  for (let m = 0; m <= lastMin; m += step) {
    const hh = String(Math.floor(m / 60)).padStart(2, "0");
    const mm = String(m % 60).padStart(2, "0");
    labels.push(`${hh}:${mm}`);

    // sem buraco visual: minutos faltantes viram 0
    activePower.push(mapP.has(m) ? mapP.get(m) : 0);
    irradiance.push(mapI.has(m) ? mapI.get(m) : 0);
    expectedPower.push(mapE.has(m) ? mapE.get(m) : 0);
  }

  return {
    ...payload,
    labels,
    activePower,
    irradiance,
    expectedPower
  };
}


// ======================================================
// SÉRIES REAIS (API)
// ======================================================
let DAILY = null;
let MONTHLY = null;
let ACTIVE_ALARMS = [];
let PLANT_ALARMS_MENU_OPEN = false;
let INVERTERS_REALTIME = [];
let RELAY_REALTIME = null; // ✅ NEW
let MULTIMETER_REALTIME = null;
window.INVERTERS_REALTIME = INVERTERS_REALTIME;
window.RELAY_REALTIME = RELAY_REALTIME;
window.MULTIMETER_REALTIME = MULTIMETER_REALTIME;
let OPEN_INVERTER_REAL_ID = null;
const STRINGS_REFRESH_SEQ_MAP = new Map();
let IS_REFRESHING_PLANT = false;
let INVERTER_EXTRAS_BY_ID = new Map(); // inverter_id (string) -> objeto inv completo

let PLANT_CATALOG = {
  inverters: [],
  hasRelay: false
};

let RELAY_SUPPORTED = null; // null = desconhecido / true / false
let MULTIMETER_SUPPORTED = null; // null = desconhecido / true / false

let PLANT_CAPABILITIES = {
  hasRelay: null,
  hasTransformer: null,
  hasMultimeter: null,
  relayDeviceId: null,
  transformerDeviceId: null,
  multimeterDeviceId: null,
  breakers: [],          // [{id, level, name, cabin_id, device_id}]
};

/* ── Breaker helpers ── */
function getBreaker(level, cabinId, deviceId) {
  const bk = PLANT_CAPABILITIES.breakers;
  if (!bk || !bk.length) return null;
  if (level === 'djmt') return bk.find(b => b.level === 'djmt') || null;
  if (level === 'djbt') return bk.find(b => b.level === 'djbt' && b.cabin_id === cabinId) || null;
  if (level === 'djinv') return bk.find(b => b.level === 'djinv' && b.device_id === deviceId) || null;
  return null;
}
function hasBreaker(level, cabinId, deviceId) {
  return getBreaker(level, cabinId, deviceId) != null;
}
function getBreakerName(level, cabinId, deviceId, fallback) {
  const b = getBreaker(level, cabinId, deviceId);
  return b?.name || fallback;
}

const API_BASE = "https://jgeg9i0js1.execute-api.us-east-1.amazonaws.com";
const PLANT_REFRESH_INTERVAL_MS = 30000;
const PLANT_ID = new URLSearchParams(window.location.search).get("plant_id");

function normalizeApiBody(data) {
  if (data && data.body) {
    return typeof data.body === "string" ? JSON.parse(data.body) : data.body;
  }
  return data;
}

function normalizeAlarmState(value) {
  const s = String(value ?? "").trim().toUpperCase();
  if (!s) return "UNKNOWN";
  if (["ACTIVE", "ACTIVO", "ATIVO", "OPEN", "ABERTO"].includes(s)) return "ACTIVE";
  if (["CLEARED", "CLEAR", "RESOLVED", "RESOLVIDO", "INACTIVE", "FECHADO", "CLOSED"].includes(s)) return "CLEARED";
  return s;
}

function normalizeAlarmSeverity(value) {
  const s = String(value ?? "").trim().toLowerCase();
  if (!s) return "info";
  if (["critical", "critico", "crítico", "high", "alta"].includes(s)) return "critical";
  if (["warning", "warn", "media", "média", "medium"].includes(s)) return "warning";
  if (["minor", "low", "baixa", "info", "informational"].includes(s)) return "info";
  return s;
}

function dedupePlantAlarms(alarms) {
  const map = new Map();
  (Array.isArray(alarms) ? alarms : []).forEach((alarm) => {
    const key = String(
      alarm?.event_row_id ??
      alarm?.alarm_id ??
      alarm?.id ??
      `${alarm?.event_code ?? "evt"}:${alarm?.timestamp ?? alarm?.started_at ?? ""}`
    );
    if (!map.has(key)) map.set(key, alarm);
  });
  return Array.from(map.values());
}

function hasActivePlantAlarms() {
  return Array.isArray(ACTIVE_ALARMS) && ACTIVE_ALARMS.length > 0;
}

const DEVICE_COMMAND_STATE = new Map();
let DEVICE_COMMAND_MENU_OPEN_KEY = null;

function getDeviceKey(deviceType, deviceId) {
  return `${String(deviceType || "")}:${String(deviceId ?? "")}`;
}

function getDevicePersistentState(deviceType, deviceId, fallback = "off") {
  return DEVICE_COMMAND_STATE.get(getDeviceKey(deviceType, deviceId)) || fallback;
}

function setDevicePersistentState(deviceType, deviceId, state) {
  if (state !== "on" && state !== "off") return;
  DEVICE_COMMAND_STATE.set(getDeviceKey(deviceType, deviceId), state);
}

function isTruthyFlag(value) {
  if (value === true) return true;
  if (value === false || value == null) return false;
  const s = String(value).trim().toLowerCase();
  return ["1", "true", "on", "online", "yes", "y", "sim"].includes(s);
}

function isFalsyFlag(value) {
  if (value === false) return true;
  if (value === true || value == null) return false;
  const s = String(value).trim().toLowerCase();
  return ["0", "false", "off", "offline", "no", "n", "nao", "não"].includes(s);
}

function normalizeCommunicationFault(value) {
  if (value === null || value === undefined || value === "") return null;
  const n = Number(String(value).replace(",", ".").trim());
  return Number.isFinite(n) ? n : null;
}

function commFaultMeansOnline(value) {
  const n = normalizeCommunicationFault(value);
  if (n === null) return null;
  if (n === 192) return true;
  if (n === 28) return false;
  return null;
}

function relayOnlineFromPayload(relayItem) {
  const eventRaw = relayItem?.event?.raw ?? {};

  const commCandidates = [
    relayItem?.communication_fault,
    relayItem?.event?.communication_fault,
    eventRaw?.communication_fault
  ];

  for (const c of commCandidates) {
    const commDecision = commFaultMeansOnline(c);
    if (commDecision !== null) return commDecision;
  }

  const statusCandidates = [
    relayItem?.is_online,
    relayItem?.online,
    relayItem?.isOnline,
    relayItem?.event?.is_online,
    eventRaw?.is_online,
    eventRaw?.online,
    eventRaw?.status_online
  ];

  for (const candidate of statusCandidates) {
    if (isTruthyFlag(candidate)) return true;
    if (isFalsyFlag(candidate)) return false;
  }

  return false;
}

function relayStateFromPayload(relayItem) {
  if (!relayItem) return "off";

  if (relayItem?.relay_on === true) return "on";
  if (relayItem?.relay_on === false) return "off";

  const eventRaw = relayItem?.event?.raw ?? {};
  const commCandidates = [
    relayItem?.communication_fault,
    relayItem?.event?.communication_fault,
    eventRaw?.communication_fault,
    relayItem?.analog?.communication_fault
  ];

  for (const c of commCandidates) {
    const decision = commFaultMeansOnline(c);
    if (decision === true) return "on";
    if (decision === false) return "off";
  }

  if (relayItem?.is_online === true || relayItem?.online === true) return "on";
  if (relayItem?.is_online === false || relayItem?.online === false) return "off";

  return "off";
}

function multimeterOnlineFromPayload(item) {
  const analog = item?.analog ?? {};
  const data = item?.data ?? {};

  const commCandidates = [
    item?.communication_fault,
    analog?.communication_fault,
    data?.communication_fault
  ];

  for (const c of commCandidates) {
    const commDecision = commFaultMeansOnline(c);
    if (commDecision !== null) return commDecision;
  }

  const statusCandidates = [
    item?.is_online,
    item?.online,
    item?.isOnline,
    item?.status_online,
    analog?.is_online,
    data?.is_online
  ];

  for (const candidate of statusCandidates) {
    if (isTruthyFlag(candidate)) return true;
    if (isFalsyFlag(candidate)) return false;
  }

  return false;
}

function renderDeviceCommandControl(deviceType, deviceId, currentState = "off") {
  const safeType = String(deviceType || "");
  const safeId = String(deviceId ?? "");
  const state = getDevicePersistentState(safeType, safeId, currentState);
  const stateClass = state === "on" ? "is-on" : "is-off";
  const key = getDeviceKey(safeType, safeId);
  return `
    <div class="device-command-control ${stateClass}" data-device-key="${key}" data-device-type="${safeType}" data-device-id="${safeId}">
      <button type="button" class="device-command-trigger" data-device-key="${key}" data-device-type="${safeType}" data-device-id="${safeId}" aria-label="Comandos do dispositivo">
        <span class="device-command-switch" aria-hidden="true">
          <svg class="device-command-switch-track" viewBox="0 0 72 40" preserveAspectRatio="none" focusable="false" aria-hidden="true">
            <rect class="device-command-switch-track__outer" x="2" y="4" width="68" height="32" rx="16"></rect>
            <rect class="device-command-switch-track__inner" x="4.5" y="6.5" width="63" height="27" rx="13.5"></rect>
            <path class="device-command-switch-track__pulse" d="M14 21h10l4-6 6 12 5-8h18"></path>
          </svg>
          <span class="device-command-switch-thumb">
            <svg class="device-command-switch-glyph" viewBox="0 0 24 24" focusable="false" aria-hidden="true">
              <use href="#device-command-switch-glyph"></use>
            </svg>
          </span>
        </span>
      </button>
      <div class="device-command-popover" data-device-key="${key}">
        <button type="button" class="device-command-option device-command-option--on" data-device-type="${safeType}" data-device-id="${safeId}" data-device-key="${key}" data-action="on"><span class="dot"></span><span>ON</span></button>
        <button type="button" class="device-command-option device-command-option--off" data-device-type="${safeType}" data-device-id="${safeId}" data-device-key="${key}" data-action="off"><span class="dot"></span><span>OFF</span></button>
        <button type="button" class="device-command-option device-command-option--reset" data-device-type="${safeType}" data-device-id="${safeId}" data-device-key="${key}" data-action="reset"><span class="dot"></span><span>RESET</span></button>
      </div>
    </div>
  `;
}

function applyDeviceVisualState(deviceType, deviceId, state) {
  const key = getDeviceKey(deviceType, deviceId);
  document.querySelectorAll(`.device-command-control[data-device-key="${key}"]`).forEach((el) => {
    el.classList.remove("is-on", "is-off", "is-reset-flash");
    el.classList.add(state === "on" ? "is-on" : "is-off");
  });
}

function closeAllDeviceCommandMenus() {
  DEVICE_COMMAND_MENU_OPEN_KEY = null;
  document.querySelectorAll(".device-command-control.is-open").forEach((el) => {
    el.classList.remove("is-open");
  });
}

function ensureDeviceCommandModals() {
  if (document.getElementById("deviceCommandAuthModal")) return;
  const wrap = document.createElement("div");
  wrap.innerHTML = `
    <div id="deviceCommandAuthModal" class="device-command-modal hidden">
      <div class="device-command-modal-card">
        <h3>Autenticação</h3>
        <p id="deviceCommandAuthLabel">Confirme usuário e senha</p>
        <input id="deviceCommandUser" type="text" placeholder="Usuário" />
        <input id="deviceCommandPass" type="password" placeholder="Senha" />
        <div class="device-command-modal-actions">
          <button id="deviceCommandCancelBtn" type="button">Cancelar</button>
          <button id="deviceCommandConfirmBtn" type="button">Confirmar</button>
        </div>
      </div>
    </div>
    <div id="deviceCommandRunModal" class="device-command-modal hidden">
      <div class="device-command-modal-card">
        <h3 id="deviceCommandRunTitle">Executando comando</h3>
        <p id="deviceCommandRunSub">Processando...</p>
        <div class="device-command-progress-wrap"><div id="deviceCommandProgressBar"></div></div>
        <div id="deviceCommandProgressPct">0%</div>
        <p id="deviceCommandRunResult"></p>
      </div>
    </div>
  `;
  document.body.appendChild(wrap);
}

function ensureCommandConsoleModal() {
  if (document.getElementById("cmdConsoleOverlay")) return;
  const el = document.createElement("div");
  el.innerHTML = `
    <div id="cmdConsoleOverlay" class="cmd-console-overlay hidden" role="dialog" aria-modal="true" aria-label="Console de Comandos">
      <div class="cmd-console">
        <div class="cmd-console__header">
          <div class="cmd-console__title-group">
            <div class="cmd-console__icon"><i class="fa-solid fa-terminal"></i></div>
            <div>
              <div class="cmd-console__label">Console de Comandos</div>
              <div class="cmd-console__device-name" id="cmdConsoleDeviceName">—</div>
            </div>
          </div>
          <button class="cmd-console__close" id="cmdConsoleClose" aria-label="Fechar console">
            <i class="fa-solid fa-xmark"></i>
          </button>
        </div>

        <div class="cmd-console__state-row">
          <span class="cmd-console__state-dot is-unknown" id="cmdConsoleStateDot"></span>
          <span class="cmd-console__state-text" id="cmdConsoleStateText">Estado desconhecido</span>
        </div>

        <div class="cmd-console__cmds">
          <button class="cmd-console__cmd cmd-console__cmd--on" id="cmdConsoleBtnOn">
            <i class="fa-solid fa-power-off"></i>
            <span class="cmd-console__cmd-label">ON</span>
            <span class="cmd-console__cmd-desc">Ligar equipamento</span>
          </button>
          <button class="cmd-console__cmd cmd-console__cmd--off" id="cmdConsoleBtnOff">
            <i class="fa-solid fa-stop"></i>
            <span class="cmd-console__cmd-label">OFF</span>
            <span class="cmd-console__cmd-desc">Desligar equipamento</span>
          </button>
          <button class="cmd-console__cmd cmd-console__cmd--reset" id="cmdConsoleBtnReset">
            <i class="fa-solid fa-rotate"></i>
            <span class="cmd-console__cmd-label">RESET</span>
            <span class="cmd-console__cmd-desc">Reiniciar equipamento</span>
          </button>
        </div>

        <!-- Setar potência -->
        <div class="cmd-console__power-section">
          <div class="cmd-console__power-label">
            <i class="fa-solid fa-sliders"></i>
            Setar Potência Ativa
          </div>
          <div class="cmd-console__power-row">
            <input
              id="cmdConsolePowerInput"
              class="cmd-console__power-input"
              type="number"
              min="0"
              step="0.1"
              placeholder="kW"
              aria-label="Potência em kW"
            />
            <span class="cmd-console__power-unit">kW</span>
            <button class="cmd-console__power-btn" id="cmdConsoleBtnSetPower">
              <i class="fa-solid fa-paper-plane"></i>
              Setar
            </button>
          </div>
        </div>

        <!-- Feedback de execução -->
        <div class="cmd-console__feedback hidden" id="cmdConsoleFeedback">
          <div class="cmd-console__feedback-inner">
            <span class="cmd-console__feedback-icon" id="cmdConsoleFeedbackIcon"></span>
            <span class="cmd-console__feedback-text" id="cmdConsoleFeedbackText"></span>
          </div>
        </div>

        <!-- Alarmes do dispositivo -->
        <div class="cmd-console__alarms-section">
          <div class="cmd-console__alarms-title">
            <i class="fa-solid fa-triangle-exclamation"></i>
            Alarmes do dispositivo
          </div>
          <div id="cmdConsoleAlarmList" class="cmd-console__alarm-list">
            <div class="cmd-console__alarm-empty">Nenhum alarme ativo</div>
          </div>
        </div>

        <div class="cmd-console__footer">
          <i class="fa-solid fa-lock"></i>
          Todos os comandos requerem autenticação antes de serem executados.
        </div>
      </div>
    </div>
  `;
  document.body.appendChild(el);

  document.getElementById("cmdConsoleOverlay").addEventListener("click", (e) => {
    if (e.target === e.currentTarget) closeCommandConsole();
  });
  document.addEventListener("keydown", (e) => {
    if (e.key === "Escape") closeCommandConsole();
  });
  document.getElementById("cmdConsoleClose").addEventListener("click", closeCommandConsole);
}

function openCommandConsole({ deviceType, deviceId }) {
  ensureCommandConsoleModal();
  ensureDeviceCommandModals();

  const overlay   = document.getElementById("cmdConsoleOverlay");
  const nameEl    = document.getElementById("cmdConsoleDeviceName");
  const dotEl     = document.getElementById("cmdConsoleStateDot");
  const textEl    = document.getElementById("cmdConsoleStateText");
  const feedbackEl= document.getElementById("cmdConsoleFeedback");
  const alarmList = document.getElementById("cmdConsoleAlarmList");
  if (!overlay) return;

  const typeLabel = String(deviceType || "").toUpperCase();
  nameEl.textContent = `${typeLabel} — ID ${deviceId}`;

  const state = getDevicePersistentState(deviceType, deviceId, "off");
  dotEl.className = "cmd-console__state-dot " + (state === "on" ? "is-on" : "is-off");
  textEl.textContent = state === "on" ? "ESTADO ATUAL: LIGADO" : "ESTADO ATUAL: DESLIGADO";

  // Limpa feedback anterior
  if (feedbackEl) feedbackEl.classList.add("hidden");

  // Alarmes filtrados para este dispositivo
  if (alarmList) {
    const deviceAlarms = Array.isArray(ACTIVE_ALARMS)
      ? ACTIVE_ALARMS.filter(a => {
          const aid = String(a?.device_id ?? a?.deviceId ?? "");
          return aid && aid === String(deviceId);
        })
      : [];
    if (deviceAlarms.length === 0) {
      alarmList.innerHTML = `<div class="cmd-console__alarm-empty">Nenhum alarme ativo</div>`;
    } else {
      alarmList.innerHTML = deviceAlarms.map(a => {
        const msg = a.event_name
          || (a.event_code != null ? `Evento ${a.event_code}` : null)
          || a.message || a.description || a.alarm_message
          || "Alarme sem descrição";
        const ts = a.started_at ?? a.timestamp ?? a.created_at ?? null;
        const timeStr = ts
          ? new Date(ts).toLocaleString("pt-BR", { day:"2-digit", month:"2-digit", hour:"2-digit", minute:"2-digit" })
          : "—";
        const devLabel = a.device_name ? ` · ${a.device_name}` : "";
        return `
        <div class="cmd-console__alarm-item" title="${msg}">
          <span class="cmd-console__alarm-dot"></span>
          <span class="cmd-console__alarm-msg">${msg}${devLabel}</span>
          <span class="cmd-console__alarm-time">${timeStr}</span>
        </div>`;
      }).join("");
    }
  }

  // Reclona botões para limpar handlers anteriores
  ["cmdConsoleBtnOn", "cmdConsoleBtnOff", "cmdConsoleBtnReset", "cmdConsoleBtnSetPower"].forEach(id => {
    const btn = document.getElementById(id);
    if (btn) { const c = btn.cloneNode(true); btn.parentNode.replaceChild(c, btn); }
  });

  // Limpa input de potência
  const pwrInput = document.getElementById("cmdConsolePowerInput");
  if (pwrInput) pwrInput.value = "";

  const dispatch = (action, value) => {
    closeCommandConsole();
    openCommandAuthFlow({ deviceType, deviceId, action, value });
  };
  document.getElementById("cmdConsoleBtnOn").addEventListener("click",    () => dispatch("on"));
  document.getElementById("cmdConsoleBtnOff").addEventListener("click",   () => dispatch("off"));
  document.getElementById("cmdConsoleBtnReset").addEventListener("click", () => dispatch("reset"));
  document.getElementById("cmdConsoleBtnSetPower").addEventListener("click", () => {
    const input = document.getElementById("cmdConsolePowerInput");
    const val = input ? parseFloat(input.value) : NaN;
    if (isNaN(val) || val < 0) {
      if (input) input.focus();
      return;
    }
    dispatch("set_power", val);
  });

  overlay.classList.remove("hidden");
  document.body.style.overflow = "hidden";
}

function closeCommandConsole() {
  const overlay = document.getElementById("cmdConsoleOverlay");
  if (overlay) overlay.classList.add("hidden");
  document.body.style.overflow = "";
}

function openCommandAuthFlow({ deviceType, deviceId, action, value }) {
  ensureDeviceCommandModals();
  const auth       = document.getElementById("deviceCommandAuthModal");
  const authLabel  = document.getElementById("deviceCommandAuthLabel");
  const cancelBtn  = document.getElementById("deviceCommandCancelBtn");
  const confirmBtn = document.getElementById("deviceCommandConfirmBtn");
  const userInput  = document.getElementById("deviceCommandUser");
  const passInput  = document.getElementById("deviceCommandPass");
  if (!auth || !confirmBtn || !cancelBtn || !userInput || !passInput) return;

  // Limpa campos e mostra modal de autenticação
  userInput.value = "";
  passInput.value = "";
  const actionLabel = action === "set_power"
    ? `SET POWER → ${value} kW`
    : action.toUpperCase();
  authLabel.textContent = `${String(deviceType).toUpperCase()} ${deviceId} • ${actionLabel}`;
  auth.classList.remove("hidden");

  const closeAuth = () => auth.classList.add("hidden");
  cancelBtn.onclick = closeAuth;

  // Função auxiliar para exibir feedback no console (reabre se fechado)
  function showConsoleFeedback({ success, message }) {
    ensureCommandConsoleModal();
    const overlay     = document.getElementById("cmdConsoleOverlay");
    const feedbackEl  = document.getElementById("cmdConsoleFeedback");
    const iconEl      = document.getElementById("cmdConsoleFeedbackIcon");
    const textEl      = document.getElementById("cmdConsoleFeedbackText");
    const nameEl      = document.getElementById("cmdConsoleDeviceName");
    const dotEl       = document.getElementById("cmdConsoleStateDot");
    const stateTextEl = document.getElementById("cmdConsoleStateText");

    if (overlay) { overlay.classList.remove("hidden"); document.body.style.overflow = "hidden"; }
    if (nameEl) nameEl.textContent = `${String(deviceType).toUpperCase()} — ID ${deviceId}`;

    // Atualiza estado visual se bem-sucedido
    if (success && (action === "on" || action === "off")) {
      setDevicePersistentState(deviceType, deviceId, action);
      applyDeviceVisualState(deviceType, deviceId, action);
      if (dotEl) dotEl.className = "cmd-console__state-dot " + (action === "on" ? "is-on" : "is-off");
      if (stateTextEl) stateTextEl.textContent = `ESTADO ATUAL: ${action === "on" ? "LIGADO" : "DESLIGADO"}`;
    }

    if (feedbackEl && iconEl && textEl) {
      feedbackEl.classList.remove("hidden", "is-success", "is-error");
      feedbackEl.classList.add(success ? "is-success" : "is-error");
      iconEl.innerHTML = success
        ? `<i class="fa-solid fa-circle-check"></i>`
        : `<i class="fa-solid fa-circle-xmark"></i>`;
      textEl.textContent = message;
    }
  }

  confirmBtn.onclick = async () => {
    const username = userInput.value.trim();
    const password = passInput.value;

    if (!username || !password) {
      authLabel.textContent = "Preencha usuário e senha.";
      return;
    }

    confirmBtn.disabled = true;
    confirmBtn.textContent = "Aguarde...";

    try {
      if (!PLANT_ID) throw new Error("plant_id não encontrado na URL");

      const headers = buildAuthHeaders();
      const res = await fetch(`${API_BASE}/plants/${PLANT_ID}/devices/${deviceId}/command`, {
        method: "POST",
        headers,
        body: JSON.stringify({
          action,
          username,
          password,
          requested_by: username,
          ...(action === "set_power" && value != null ? { value } : {}),
        }),
      });

      let data = {};
      try { data = await res.json(); } catch (_) {}

      closeAuth();

      if (res.ok && data.ok) {
        // Mostra o console com a barra de espera do CLP
        _startClpWait({ deviceType, deviceId, action, value, commandId: data.command_id });
      } else {
        const errMsg = data.error
          || (res.status === 401 ? "Credenciais inválidas." : `Falha ao executar comando. (${res.status})`)
        showConsoleFeedback({ success: false, message: errMsg });
      }
    } catch (err) {
      closeAuth();
      showConsoleFeedback({ success: false, message: `Erro: ${err.message}` });
    } finally {
      confirmBtn.disabled = false;
      confirmBtn.textContent = "Confirmar";
    }
  };
}

// ─── Aguarda confirmação do CLP (polling 3s, timeout 2min) ───────────────────
function _startClpWait({ deviceType, deviceId, action, value, commandId }) {
  ensureCommandConsoleModal();

  const TOTAL_MS   = 120_000;   // 2 minutos
  const POLL_MS    = 3_000;     // a cada 3s
  const START      = Date.now();

  // Abre (ou mantém aberto) o console de comandos
  const overlay     = document.getElementById("cmdConsoleOverlay");
  const feedbackEl  = document.getElementById("cmdConsoleFeedback");
  const nameEl      = document.getElementById("cmdConsoleDeviceName");
  const dotEl       = document.getElementById("cmdConsoleStateDot");
  const stateTextEl = document.getElementById("cmdConsoleStateText");

  if (!overlay || !feedbackEl) return;
  if (nameEl) nameEl.textContent = `${String(deviceType).toUpperCase()} — ID ${deviceId}`;

  // Atualiza bolinha de estado imediatamente (MQTT já foi enviado)
  if (action === "on" || action === "off") {
    setDevicePersistentState(deviceType, deviceId, action);
    applyDeviceVisualState(deviceType, deviceId, action);
    if (dotEl)       dotEl.className  = "cmd-console__state-dot " + (action === "on" ? "is-on" : "is-off");
    if (stateTextEl) stateTextEl.textContent = `ESTADO ATUAL: ${action === "on" ? "LIGADO" : "DESLIGADO"}`;
  }

  overlay.classList.remove("hidden");
  document.body.style.overflow = "hidden";

  // Monta o painel de espera dentro de cmdConsoleFeedback
  feedbackEl.classList.remove("hidden", "is-success", "is-error", "is-warning", "is-waiting");
  feedbackEl.classList.add("is-waiting");
  feedbackEl.innerHTML = `
    <div class="cmd-console__clp-header">
      <span class="cmd-console__clp-spinner"></span>
      <span>Aguardando confirmação do CLP…</span>
    </div>
    <div class="cmd-console__clp-bar-wrap">
      <div class="cmd-console__clp-bar-fill" id="clpBarFill"></div>
    </div>
    <div class="cmd-console__clp-bar-label">
      <span id="clpBarStatus">Comando enviado via MQTT</span>
      <span id="clpBarTimer">2:00</span>
    </div>
  `;

  const fillEl   = document.getElementById("clpBarFill");
  const timerEl  = document.getElementById("clpBarTimer");
  const statusEl = document.getElementById("clpBarStatus");

  let pollTimer   = null;
  let animFrame   = null;
  let finished    = false;

  function formatTime(ms) {
    const secs = Math.max(0, Math.ceil(ms / 1000));
    const m    = Math.floor(secs / 60);
    const s    = secs % 60;
    return `${m}:${String(s).padStart(2, "0")}`;
  }

  function tick() {
    if (finished) return;
    const elapsed = Date.now() - START;
    const pct     = Math.min(100, (elapsed / TOTAL_MS) * 100);
    if (fillEl)  fillEl.style.width  = pct + "%";
    if (timerEl) timerEl.textContent = formatTime(TOTAL_MS - elapsed);
    animFrame = requestAnimationFrame(tick);
  }
  animFrame = requestAnimationFrame(tick);

  function finish(type, message) {
    finished = true;
    cancelAnimationFrame(animFrame);
    clearTimeout(pollTimer);

    feedbackEl.classList.remove("is-waiting", "is-success", "is-error", "is-warning");

    if (type === "success") {
      feedbackEl.classList.add("is-success");
      feedbackEl.innerHTML = `
        <div class="cmd-console__feedback-inner">
          <span class="cmd-console__feedback-icon"><i class="fa-solid fa-circle-check"></i></span>
          <span class="cmd-console__feedback-text">${message}</span>
        </div>`;
    } else if (type === "error") {
      feedbackEl.classList.add("is-error");
      feedbackEl.innerHTML = `
        <div class="cmd-console__feedback-inner">
          <span class="cmd-console__feedback-icon"><i class="fa-solid fa-circle-xmark"></i></span>
          <span class="cmd-console__feedback-text">${message}</span>
        </div>`;
    } else {
      // warning (timeout)
      feedbackEl.classList.add("is-warning");
      feedbackEl.innerHTML = `
        <div class="cmd-console__feedback-inner">
          <span class="cmd-console__feedback-icon"><i class="fa-solid fa-triangle-exclamation"></i></span>
          <span class="cmd-console__feedback-text">${message}</span>
        </div>`;
    }
  }

  async function poll() {
    if (finished) return;

    const elapsed = Date.now() - START;
    if (elapsed >= TOTAL_MS) {
      finish("warning", "Sem resposta do CLP em 2 minutos. Verifique o equipamento.");
      return;
    }

    // Se não há command_id, apenas aguarda timeout (CLP ack ainda não implementado)
    if (!commandId) {
      pollTimer = setTimeout(poll, POLL_MS);
      return;
    }

    try {
      const res  = await fetch(`${API_BASE}/plants/${PLANT_ID}/commands/${commandId}/status`, {
        headers: buildAuthHeaders(),
      });
      const data = res.ok ? await res.json() : {};

      if (statusEl && data.status) {
        const labels = { PENDING:"Aguardando envio…", SENT:"Enviado ao equipamento…", SUCCESS:"Enviado via MQTT", CLP_OK:"CLP confirmou", CLP_FAILED:"CLP reportou falha", FAILED:"Falha no envio" };
        statusEl.textContent = labels[data.status] || data.status;
      }

      if (data.clp_ok === true) {
        const msg = data.clp_message || (action === "set_power"
          ? `Potência setada para ${value} kW confirmada pelo CLP.`
          : `Comando ${action.toUpperCase()} confirmado pelo CLP.`);
        finish("success", msg);
        return;
      }

      if (data.clp_ok === false) {
        const msg = data.clp_message || "O CLP reportou falha ao executar o comando.";
        finish("error", msg);
        return;
      }
    } catch (_) {
      // erro de rede — continua tentando
    }

    pollTimer = setTimeout(poll, POLL_MS);
  }

  pollTimer = setTimeout(poll, POLL_MS);
}

function wireDeviceCommandButtons(rootEl) {
  if (!rootEl) return;
  rootEl.querySelectorAll(".device-command-trigger").forEach((btn) => {
    if (btn.dataset.wiredCmdTrigger === "true") return;
    btn.dataset.wiredCmdTrigger = "true";
    btn.addEventListener("click", (e) => {
      e.preventDefault();
      e.stopPropagation();
      closeAllDeviceCommandMenus();
      openCommandConsole({
        deviceType: btn.dataset.deviceType || "",
        deviceId: btn.dataset.deviceId || "",
      });
    });
  });

  rootEl.querySelectorAll(".device-command-option").forEach((btn) => {
    if (btn.dataset.wiredCmdOption === "true") return;
    btn.dataset.wiredCmdOption = "true";
    btn.addEventListener("click", (e) => {
      e.preventDefault();
      e.stopPropagation();
      const deviceType = btn.dataset.deviceType || "";
      const deviceId = btn.dataset.deviceId || "";
      const action = btn.dataset.action || "";
      closeAllDeviceCommandMenus();
      openCommandAuthFlow({ deviceType, deviceId, action });
    });
  });
}

function renderAlarmMenuButton() {
  const btn = document.getElementById("plantAlarmMenuButton");
  const count = document.getElementById("plantAlarmMenuCount");
  const panel = document.getElementById("plantAlarmMenuPanel");
  const empty = document.getElementById("plantAlarmMenuEmptyState");
  const icon = btn?.querySelector(".plant-alarm-menu-icon");

  if (!btn) return;

  const hasAlarms = hasActivePlantAlarms();

  btn.classList.toggle("is-clean", !hasAlarms);
  btn.classList.toggle("is-alert", hasAlarms);
  btn.setAttribute("aria-expanded", PLANT_ALARMS_MENU_OPEN ? "true" : "false");

  if (count) {
    count.textContent = String(ACTIVE_ALARMS.length || 0);
    count.style.display = hasAlarms ? "inline-flex" : "none";
  }

  if (icon) {
    icon.innerHTML = hasAlarms
      ? `<svg viewBox="0 0 24 24" aria-hidden="true"><path d="M12 7v6" stroke="currentColor" stroke-width="2.2" stroke-linecap="round"/><circle cx="12" cy="17" r="1.4" fill="currentColor"/><circle cx="12" cy="12" r="9" fill="none" stroke="currentColor" stroke-width="1.8"/></svg>`
      : `<svg viewBox="0 0 24 24" aria-hidden="true"><path d="M6 12.5l4 4 8-9" fill="none" stroke="currentColor" stroke-width="2.2" stroke-linecap="round" stroke-linejoin="round"/><circle cx="12" cy="12" r="9" fill="none" stroke="currentColor" stroke-width="1.6"/></svg>`;
  }

  if (panel) {
    panel.classList.toggle("open", PLANT_ALARMS_MENU_OPEN);
  }

  if (empty) {
    empty.style.display = hasAlarms ? "none" : "";
  }
}

function setPlantAlarmMenuOpen(open) {
  PLANT_ALARMS_MENU_OPEN = !!open;
  renderAlarmMenuButton();
}

function setupPlantAlarmMenu() {
  const btn = document.getElementById("plantAlarmMenuButton");
  const panel = document.getElementById("plantAlarmMenuPanel");
  const closeBtn = document.getElementById("plantAlarmMenuClose");

  if (!btn || !panel) return;
  if (btn.dataset.wiredAlarmMenu === "true") return;
  btn.dataset.wiredAlarmMenu = "true";

  btn.addEventListener("click", (e) => {
    e.stopPropagation();
    setPlantAlarmMenuOpen(!PLANT_ALARMS_MENU_OPEN);
  });

  panel.addEventListener("click", (e) => {
    e.stopPropagation();
  });

  closeBtn?.addEventListener("click", (e) => {
    e.stopPropagation();
    setPlantAlarmMenuOpen(false);
  });

  document.addEventListener("click", () => {
    if (PLANT_ALARMS_MENU_OPEN) setPlantAlarmMenuOpen(false);
  });

  document.addEventListener("keydown", (e) => {
    if (e.key === "Escape" && PLANT_ALARMS_MENU_OPEN) {
      setPlantAlarmMenuOpen(false);
    }
  });
}

function sortPlantAlarmsDesc(alarms) {
  return (Array.isArray(alarms) ? alarms : []).slice().sort((a, b) => {
    const ta = Date.parse(a?.started_at ?? a?.timestamp ?? "") || 0;
    const tb = Date.parse(b?.started_at ?? b?.timestamp ?? "") || 0;
    return tb - ta;
  });
}

function getUserContext() {
  try {
    const user = JSON.parse(localStorage.getItem("user"));
    return {
      customer_id: user?.customer_id ?? null,
      is_superuser: user?.is_superuser ?? false,
      username: user?.username ?? null,
      user_id: user?.id ?? user?.user_id ?? null,
    };
  } catch {
    return { customer_id: null, is_superuser: false, username: null, user_id: null };
  }
}

function buildAuthHeaders() {
  const ctx = getUserContext();
  const headers = { "Content-Type": "application/json" };
  if (ctx.customer_id) headers["X-Customer-Id"] = ctx.customer_id;
  if (ctx.is_superuser) headers["X-Is-Superuser"] = "true";
  return headers;
}

function buildWriteAuthHeaders() {
  const ctx = getUserContext();
  const headers = { "Content-Type": "application/json" };
  if (ctx.customer_id) headers["X-Customer-Id"] = ctx.customer_id;
  if (ctx.is_superuser) headers["X-Is-Superuser"] = "true";
  if (ctx.username) headers["X-Username"] = ctx.username;
  if (ctx.user_id) headers["X-User-Id"] = String(ctx.user_id);
  return headers;
}

// ======================================================
// ✅ PREFERÊNCIAS — STRINGS DESABILITADAS (LOCALSTORAGE)
// ======================================================
function getPrefKey() {
  const ctx = getUserContext();
  const customer = ctx.customer_id ?? "anon";
  return `scada:strings_disabled:${customer}:${PLANT_ID}`;
}

function readDisabledPrefs() {
  try {
    return JSON.parse(localStorage.getItem(getPrefKey())) || {};
  } catch {
    return {};
  }
}

function isDisabledPref(inverterRealId, stringIndex) {
  const prefs = readDisabledPrefs();
  return !!prefs?.[String(inverterRealId)]?.[String(stringIndex)];
}

function setDisabledPref(inverterRealId, stringIndex, disabled) {
  const prefs = readDisabledPrefs();
  const invKey = String(inverterRealId);
  const sKey = String(stringIndex);

  prefs[invKey] = prefs[invKey] || {};
  if (disabled) prefs[invKey][sKey] = true;
  else delete prefs[invKey][sKey];

  localStorage.setItem(getPrefKey(), JSON.stringify(prefs));
}

// ======================================================
// FETCH — TEMPO REAL, WEATHER, ALARMES, ENERGIA
// ======================================================
let _lastGoodRealtime = null;

async function fetchPlantRealtime(plantId) {
  const res = await fetch(`${API_BASE}/plants/${plantId}/realtime`, {
    headers: buildAuthHeaders()
  });
  if (!res.ok) {
    console.warn(`[realtime] HTTP ${res.status} — mantendo estado anterior`);
    return _lastGoodRealtime;
  }
  const data = normalizeApiBody(await res.json());
  if (data && !data.error) _lastGoodRealtime = data;
  return data;
}

async function fetchActiveAlarms(plantId) {
  const res = await fetch(`${API_BASE}/plants/${plantId}/alarms/active`, {
    headers: buildAuthHeaders()
  });
  const data = normalizeApiBody(await res.json());
  const items = Array.isArray(data) ? data : (Array.isArray(data?.items) ? data.items : []);
  const normalized = items.map((alarm) => ({
    ...alarm,
    state: normalizeAlarmState(alarm?.state ?? alarm?.alarm_state ?? alarm?.status),
    severity: normalizeAlarmSeverity(alarm?.severity ?? alarm?.alarm_severity ?? alarm?.level)
  }));
  return sortPlantAlarmsDesc(
    dedupePlantAlarms(
      normalized.filter((alarm) => alarm.state === "ACTIVE" && alarm.acknowledged !== true)
    )
  );
}

async function acknowledgePlantAlarm(alarm) {
  const alarmId = alarm?.alarm_id ?? alarm?.id ?? alarm?.event_row_id;
  if (!alarmId) throw new Error("alarm_id ausente para ACK");

  const user = (() => {
    try {
      return JSON.parse(localStorage.getItem("user")) || {};
    } catch {
      return {};
    }
  })();

  const response = await fetch(`${API_BASE}/alarms/${alarmId}/ack`, {
    method: "POST",
    headers: buildAuthHeaders(),
    body: JSON.stringify({
      event_row_id: alarm?.event_row_id ?? null,
      power_plant_id: alarm?.power_plant_id ?? alarm?.plant_id ?? PLANT_ID ?? null,
      acknowledged_by: user?.name ?? user?.email ?? user?.username ?? "frontend",
      acknowledgment_note: "Reconhecido via operação SCADA"
    })
  });

  if (!response.ok) {
    const errorText = await response.text().catch(() => "");
    throw new Error(`Falha ao reconhecer alarme (${response.status}): ${errorText}`);
  }

  return true;
}

async function fetchTrackersRealtime(plantId) {
  const res = await fetch(`${API_BASE}/plants/${plantId}/trackers/realtime`, {
    headers: buildAuthHeaders()
  });

  if (res.status === 404) {
    console.warn("[trackers/realtime] 404");
    return { items: [], plant_center: null, plant_bounds: null };
  }

  if (!res.ok) {
    console.warn(`[trackers/realtime] HTTP ${res.status}`);
    return [];
  }

  const raw = await res.json();
  const data = normalizeApiBody(raw);
  if (Array.isArray(data)) return { items: data, plant_center: null, plant_bounds: null };
  const items =
    Array.isArray(data?.items) ? data.items :
    Array.isArray(data?.trackers) ? data.trackers :
    Array.isArray(data?.item) ? data.item : [];
  return {
    items,
    plant_center: data?.plant_center ?? null,
    plant_bounds: data?.plant_bounds ?? null
  };
}

async function fetchDailyEnergy(plantId) {
  const res = await fetch(`${API_BASE}/plants/${plantId}/energy/daily`, {
    headers: buildAuthHeaders()
  });
  const data = await res.json();
  return normalizeApiBody(data);
}

async function fetchMonthlyEnergy(plantId) {
  const res = await fetch(`${API_BASE}/plants/${plantId}/energy/monthly`, {
    headers: buildAuthHeaders()
  });
  const data = await res.json();
  return normalizeApiBody(data);
}

async function safeFetchRelayIfSupported(plantId) {
  if (RELAY_SUPPORTED === false) return null;

  const url = `${API_BASE}/plants/${plantId}/relay/realtime`;
  const res = await fetch(url, { headers: buildAuthHeaders() });

  if (res.status === 404) {
    RELAY_SUPPORTED = false;
    return null;
  }

  if (!res.ok) {
    console.warn(`[relay/realtime] HTTP ${res.status} em ${url} — mantendo estado anterior`);
    return RELAY_REALTIME ?? null;
  }

  RELAY_SUPPORTED = true;
  const payload = normalizeApiBody(await res.json());
  return payload?.item ?? null;
}

async function safeFetchMultimeterIfSupported(plantId) {
  if (MULTIMETER_SUPPORTED === false) return null;

  const url = `${API_BASE}/plants/${plantId}/multimeter/realtime`;
  const res = await fetch(url, { headers: buildAuthHeaders() });

  if (res.status === 404) {
    MULTIMETER_SUPPORTED = false;
    return null;
  }

  if (!res.ok) {
    console.warn(`[multimeter/realtime] HTTP ${res.status} em ${url} — mantendo estado anterior`);
    return MULTIMETER_REALTIME ?? null;
  }

  MULTIMETER_SUPPORTED = true;
  const payload = normalizeApiBody(await res.json());
  return payload?.item ?? payload ?? null;
}

async function fetchPlantCapabilities(plantId) {
  try {
    const res = await fetch(`${API_BASE}/plants/${plantId}/devices/catalog`, {
      headers: buildAuthHeaders()
    });
    if (!res.ok) return;
    const data = normalizeApiBody(await res.json());
    PLANT_CAPABILITIES.hasRelay       = !!data.has_relay;
    PLANT_CAPABILITIES.hasTransformer = !!data.has_transformer;
    PLANT_CAPABILITIES.hasMultimeter  = !!data.has_multimeter;
    PLANT_CAPABILITIES.relayDeviceId       = data.relay_device_id != null ? String(data.relay_device_id) : null;
    PLANT_CAPABILITIES.transformerDeviceId = data.transformer_device_id != null ? String(data.transformer_device_id) : null;
    PLANT_CAPABILITIES.multimeterDeviceId  = data.multimeter_device_id != null ? String(data.multimeter_device_id) : null;
    PLANT_CAPABILITIES.breakers            = Array.isArray(data.breakers) ? data.breakers : [];
  } catch (e) {
    console.warn('[fetchPlantCapabilities]', e);
  }
}

function setRelaySectionVisible(visible) {
  const relaySection = document.getElementById("relaySection");
  if (relaySection) relaySection.style.display = visible ? "" : "none";
  const btn = document.getElementById("navBtnRelay");
  if (btn) btn.style.display = visible ? "" : "none";
}

function setMultimeterSectionVisible(visible) {
  const section = document.getElementById("multimeterSection");
  if (section) section.style.display = visible ? "" : "none";
  const btn = document.getElementById("navBtnMultimeter");
  if (btn) btn.style.display = visible ? "" : "none";
}

function setTrackersSectionVisible(visible) {
  const section = document.getElementById("trackersSection");
  const btn = document.getElementById("trackersMenuToggle");
  if (!section) return;
  section.classList.toggle("trackers-hidden", !visible);
  if (btn) {
    btn.classList.toggle("on", visible);
    btn.setAttribute("aria-expanded", visible ? "true" : "false");
    btn.style.display = visible ? "" : "none";
  }
}

function setTrackersCollapsed(collapsed) {
  const section = document.getElementById("trackersSection");
  const tabToggleEl = document.getElementById("trackersTabToggle");
  if (!section) return;

  section.classList.toggle("is-collapsed", !!collapsed);

  if (tabToggleEl) {
    tabToggleEl.setAttribute("aria-expanded", collapsed ? "false" : "true");
  }
}

// ✅ realtime por inversor
async function fetchInvertersRealtime(plantId) {
  const candidates = [
    `${API_BASE}/plants/${plantId}/inverters/realtime`,
    `${API_BASE}/plants/${plantId}/inverters`
  ];

  for (const url of candidates) {
    const res = await fetch(url, { headers: buildAuthHeaders() });
    if (res.ok) {
      const data = normalizeApiBody(await res.json());
      return Array.isArray(data) ? data : (data?.items || []);
    }

    if (res.status === 404) continue;
    console.warn(`[inverters realtime] HTTP ${res.status} em ${url} — mantendo estado anterior`);
    return INVERTERS_REALTIME ?? [];
  }

  console.warn("[inverters realtime] nenhum endpoint disponível -> mantendo estático");
  return INVERTERS_REALTIME ?? [];
}

// config (enabled/has_data)
async function fetchInverterStrings(plantId, inverterRealId) {
  const url = `${API_BASE}/plants/${plantId}/inverters/${inverterRealId}/strings`;
  const res = await fetch(url, { headers: buildAuthHeaders() });

  if (!res.ok) {
    console.warn(`[strings] ${res.status} em ${url}`);
    return null;
  }
  return normalizeApiBody(await res.json());
}

// medida (current_a)
async function fetchInverterStringsRealtime(plantId, inverterRealId) {
  const url = `${API_BASE}/plants/${plantId}/inverters/${inverterRealId}/strings/realtime`;
  const res = await fetch(url, { headers: buildAuthHeaders() });

  if (!res.ok) {
    console.warn(`[strings/realtime] ${res.status} em ${url}`);
    return null;
  }
  return normalizeApiBody(await res.json());
}

async function patchInverterString(plantId, inverterRealId, stringIndex, enabled) {
  const url = `${API_BASE}/plants/${plantId}/inverters/${inverterRealId}/strings/${stringIndex}`;
  const res = await fetch(url, {
    method: "PATCH",
    headers: buildWriteAuthHeaders(),
    body: JSON.stringify({ enabled })
  });

  if (!res.ok) {
    throw new Error(`PATCH string falhou: HTTP ${res.status}`);
  }
  return normalizeApiBody(await res.json());
}

function getInvTsMs(inv) {
  const iso =
    inv.last_reading_at ??
    inv.last_reading_ts ??
    inv.last_ts ??
    inv.timestamp ??
    inv.event_ts ??
    null;

  const ms = iso ? new Date(iso).getTime() : NaN;
  return Number.isFinite(ms) ? ms : 0;
}

function parseTsToMs(anyTs) {
  if (!anyTs) return 0;
  const ms = Date.parse(anyTs);
  return Number.isFinite(ms) ? ms : 0;
}

function dedupInvertersById(list) {
  const map = new Map();

  for (const inv of (list || [])) {
    const id = inv.inverter_id ?? inv.device_id ?? inv.id;
    if (id == null) continue;

    const ts = parseTsToMs(
      inv.last_ts ??
      inv.timestamp ??
      inv.event_ts ??
      inv.ts ??
      inv.last_reading_at ??
      inv.last_reading_ts
    );

    const prev = map.get(id);
    const prevTs = prev ? parseTsToMs(
      prev.last_ts ??
      prev.timestamp ??
      prev.event_ts ??
      prev.ts ??
      prev.last_reading_at ??
      prev.last_reading_ts
    ) : -1;

    if (!prev || ts >= prevTs) map.set(id, inv);
  }

  return [...map.values()];
}

function computeInverterChipsByTelemetry(invertersRaw) {
  const inverters = dedupInvertersById(invertersRaw);
  const now = Date.now();

  let noComm = 0;
  let gen = 0;
  let off = 0;

  for (const inv of inverters) {
    const lastMs = parseTsToMs(
      inv.last_ts ??
      inv.timestamp ??
      inv.event_ts ??
      inv.ts ??
      inv.last_reading_at ??
      inv.last_reading_ts
    );
    const age = lastMs ? (now - lastMs) : Number.POSITIVE_INFINITY;

    if (age > INVERTER_NO_COMM_AFTER_MS) {
      noComm++;
      continue;
    }

    const working =
      inv.working === true ||
      inv.status === "working" ||
      inv.is_working === true;

    if (working) gen++;
    else off++;
  }

  const total = inverters.length;
  gen = Math.min(gen, total);
  off = Math.min(off, total);
  noComm = Math.min(noComm, total);

  return { total, gen, off, noComm };
}

function setChipCount(id, value) {
  const el = document.getElementById(id);
  if (el) el.textContent = String(value);
}

function refreshInverterStatusChips(invertersRaw) {
  const { total, gen, off, noComm } = computeInverterChipsByTelemetry(invertersRaw);

  setChipCount("countGen", gen);
  setChipCount("countNoComm", noComm);
  setChipCount("countOff", off);

  console.log("[INV CHIPS]", { plantId: PLANT_ID, total, gen, off, noComm });
}

function getInverterRealId(inv) {
  return inv?.device_id ?? inv?.inverter_id ?? inv?.deviceId ?? inv?.id ?? null;
}

function getInverterDisplayName(inv, fallbackIndex = 0) {
  return (
    inv?.device_name ??
    inv?.inverter_name ??
    inv?.name ??
    `Inversor ${fallbackIndex + 1}`
  );
}

function getInverterSvgModern() {
  return `
    <svg class="inv-icon" viewBox="0 0 140 140" xmlns="http://www.w3.org/2000/svg" aria-hidden="true">
      <defs>
        <linearGradient id="invS" x1="0" y1="0" x2="1" y2="1">
          <stop offset="0" stop-color="rgba(255,255,255,0.35)"/>
          <stop offset="1" stop-color="rgba(255,255,255,0.10)"/>
        </linearGradient>
        <filter id="invSoft" x="-30%" y="-30%" width="160%" height="160%">
          <feDropShadow dx="0" dy="8" stdDeviation="8" flood-color="rgba(0,0,0,0.55)"/>
        </filter>
      </defs>

      <g filter="url(#invSoft)">
        <rect x="18" y="18" width="104" height="104" rx="18"
              fill="rgba(0,0,0,0)"
              stroke="url(#invS)" stroke-width="3"/>
      </g>

      <path d="M42 88 H78" stroke="rgba(233,255,243,0.62)" stroke-width="4" stroke-linecap="round"/>
      <path d="M42 98 H78" stroke="rgba(233,255,243,0.35)" stroke-width="4" stroke-linecap="round" stroke-dasharray="8 7"/>

      <path d="M74 56
               C80 42, 88 42, 94 56
               C100 70, 108 70, 114 56"
            fill="none" stroke="rgba(233,255,243,0.62)" stroke-width="4"
            stroke-linecap="round" stroke-linejoin="round"/>
    </svg>
  `;
}

function ensureInverterRowsFromRealtime(inverters) {
  const container = document.getElementById("invertersContainer");
  if (!container) return;

  const preservedOpenId = OPEN_INVERTER_REAL_ID;
  const uniq = dedupInvertersById(Array.isArray(inverters) ? inverters : []);

  const sortByName = (a, b) => {
    const an = String(getInverterDisplayName(a, 0) || "");
    const bn = String(getInverterDisplayName(b, 0) || "");
    if (an && bn) return an.localeCompare(bn, "pt-BR", { numeric: true, sensitivity: "base" });
    return Number(getInverterRealId(a) || 0) - Number(getInverterRealId(b) || 0);
  };

  uniq.sort(sortByName);

  const nextIds = uniq
    .map(inv => getInverterRealId(inv))
    .filter(id => id != null)
    .map(id => String(id));

  // Signature includes cabin assignment so regrouping triggers re-render
  const nextSignature = uniq
    .map(inv => `${getInverterRealId(inv)}:${inv.cabin_id ?? ""}`)
    .join("|");

  if (LAST_INVERTER_ROWS_SIGNATURE === nextSignature && container.children.length > 0) {
    if (preservedOpenId != null && !nextIds.includes(String(preservedOpenId))) {
      OPEN_INVERTER_REAL_ID = null;
    }
    return;
  }

  LAST_INVERTER_ROWS_SIGNATURE = nextSignature;
  container.innerHTML = "";

  // Helper: create and append a single inverter row+panel into a parent element
  const appendInverterRowAndPanel = (parent, inv, idx) => {
    const realId = getInverterRealId(inv);
    if (realId == null) return;

    const title = getInverterDisplayName(inv, idx);

    const row = document.createElement("div");
    row.className = "inverter-toggle inverter-row";
    row.dataset.inverterRealId = String(realId);
    row.innerHTML = `
      <span class="status-dot"></span>
      <span class="inverter-name">${title}<i class="arrow fa-solid fa-chevron-down"></i></span>
      <div class="inv-metrics-grid">
        <span class="inv-metric" data-label="Power">—</span>
        <span class="inv-metric" data-label="Efficiency">—</span>
        <span class="inv-metric" data-label="Temp">—</span>
        <span class="inv-metric" data-label="Freq">—</span>
        <span class="inv-metric" data-label="PR">—</span>
        <span class="inv-metric inv-metric--wide" data-label="Leitura">—</span>
      </div>
      <span class="device-command-cell">
        ${_canSendCommand() ? renderDeviceCommandControl("inverter", realId, isOnlineByFreshness(inv) && !isZeroSnapshot(inv) ? "on" : "off") : ""}
      </span>
    `;

    const panel = document.createElement("div");
    panel.className = "inverter-strings";
    panel.id = `strings-${realId}`;
    panel.innerHTML = `
      <div class="inv-flow" data-inverter-real-id="${realId}">
        <svg class="inv-flow-arrows" viewBox="0 0 1000 260" preserveAspectRatio="none" aria-hidden="true">
          <defs>
            <filter id="arrowGlow" x="-40%" y="-40%" width="180%" height="180%">
              <feGaussianBlur stdDeviation="3.8" result="b"/>
              <feMerge>
                <feMergeNode in="b"/>
                <feMergeNode in="SourceGraphic"/>
              </feMerge>
            </filter>

            <marker id="arrowHead" viewBox="0 0 10 10" refX="9" refY="5"
                    markerWidth="9" markerHeight="9" orient="auto">
              <path d="M0,0 L10,5 L0,10 Z" fill="rgba(57,229,140,0.95)"/>
            </marker>
          </defs>

          <path class="arrow-path arrow-path--ac"
                d="M 500 88
                   C 390 66, 250 66, 128 90
                   C 92 98, 62 112, 46 136
                   C 34 154, 34 176, 50 192"
                marker-end="url(#arrowHead)"/>

          <path class="arrow-path arrow-path--dc"
                d="M 520 108
                   C 630 86, 770 86, 892 110
                   C 928 118, 958 132, 974 156
                   C 986 174, 986 196, 970 212"
                marker-end="url(#arrowHead)"/>
        </svg>

        <div class="inv-center">
          ${getInverterSvgModern()}
          <div class="inv-center-tags">
            <span class="inv-tag">AC</span>
            <span class="inv-tag">DC</span>
          </div>
        </div>

        <div class="inv-side inv-side--ac">
          <div class="inv-side-title">AC</div>
          <div class="inv-side-row" data-row="ac"></div>
        </div>

        <div class="inv-side inv-side--dc">
          <div class="inv-side-title">DC</div>
          <div class="inv-side-row" data-row="dc"></div>
        </div>
      </div>

      <div class="strings-grid" data-inverter-real-id="${realId}"></div>
    `;

    parent.appendChild(row);
    parent.appendChild(panel);
    wireDeviceCommandButtons(row);
    const inferredState = isOnlineByFreshness(inv) && !isZeroSnapshot(inv) ? "on" : "off";
    applyDeviceVisualState("inverter", String(realId), getDevicePersistentState("inverter", String(realId), inferredState));
  };

  const hasCabins = uniq.some(inv => inv.cabin_id != null);

  if (!hasCabins) {
    // Flat rendering — comportamento original
    uniq.forEach((inv, idx) => appendInverterRowAndPanel(container, inv, idx));
  } else {
    // Agrupar por cabin_id
    const groupMap = new Map();
    const noCabin = [];

    uniq.forEach(inv => {
      const cabinId = inv.cabin_id;
      if (cabinId == null) {
        noCabin.push(inv);
      } else {
        if (!groupMap.has(cabinId)) {
          groupMap.set(cabinId, {
            name: inv.section_name ?? inv.cabin_name ?? inv.cabin_code ?? `Cabine ${cabinId}`,
            displayOrder: inv.cabin_display_order ?? 999,
            inverters: []
          });
        }
        groupMap.get(cabinId).inverters.push(inv);
      }
    });

    const sortedGroups = Array.from(groupMap.values())
      .sort((a, b) => a.displayOrder - b.displayOrder);

    if (noCabin.length > 0) {
      sortedGroups.push({ name: "Sem cabine", displayOrder: 9999, inverters: noCabin });
    }

    let globalIdx = 0;
    sortedGroups.forEach(group => {
      const groupEl = document.createElement("div");
      groupEl.className = "cabin-group";
      groupEl.dataset.cabinCollapsed = "false";

      const header = document.createElement("div");
      header.className = "cabin-group-header";
      header.innerHTML = `
        <svg class="cabin-group-header__icon" xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" width="18" height="18" fill="none" stroke="currentColor" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round">
          <rect x="2" y="7" width="20" height="13" rx="1"/>
          <path d="M16 7V5a2 2 0 0 0-2-2h-4a2 2 0 0 0-2 2v2"/>
          <line x1="12" y1="12" x2="12" y2="16"/>
          <line x1="10" y1="14" x2="14" y2="14"/>
        </svg>
        <span class="cabin-group-header__name">${group.name}</span>
        <span class="cabin-group-header__count">${group.inverters.length} inversor${group.inverters.length !== 1 ? "es" : ""}</span>
        <svg class="cabin-group-header__chevron" xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" width="14" height="14" fill="none" stroke="currentColor" stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round">
          <polyline points="6 9 12 15 18 9"/>
        </svg>
      `;

      const body = document.createElement("div");
      body.className = "cabin-group-body";

      header.addEventListener("click", () => {
        const collapsed = groupEl.dataset.cabinCollapsed === "true";
        groupEl.dataset.cabinCollapsed = collapsed ? "false" : "true";
        body.classList.toggle("is-collapsed", !collapsed);
      });

      groupEl.appendChild(header);
      groupEl.appendChild(body);

      group.inverters.forEach(inv => {
        appendInverterRowAndPanel(body, inv, globalIdx++);
      });

      container.appendChild(groupEl);
    });
  }

  if (preservedOpenId != null) {
    const row = container.querySelector(`.inverter-toggle[data-inverter-real-id="${preservedOpenId}"]`);
    const panel = document.getElementById(`strings-${preservedOpenId}`);
    if (row && panel) {
      row.classList.add("open");
      panel.classList.add("open");
      panel.style.opacity = "1";
      panel.style.maxHeight = panel.scrollHeight + "px";
    } else {
      OPEN_INVERTER_REAL_ID = null;
    }
  }
}

// ======================================================
// MODO UNIFILAR — SINGLE LINE DIAGRAM
// ======================================================
let _plantChartsPlaceholder = null;

// Estado do modo unifilar
let UNIF_MODE = "overview"; // "overview" | "cabin"
let UNIF_CABIN_IDX = 0;
let UNIF_GROUPS = [];
let UNIF_SIDE_COLLAPSED = false;
let UNIF_ACTIVE_CABIN_FILTER = null;
let UNIF_SEARCH_TEXT = "";
let UNIF_TRANSITION_RUNNING = false;
let UNIF_MODAL_SEQ = 0;

/* ── Estado global de zoom/pan (persiste entre refreshes) ── */
let _unifZoom = null;   // null = não inicializado ainda
let _unifPanX = 0;
let _unifPanY = 0;
let _unifZoomInited = false; // true após primeiro cálculo de initZoom
let _unifEvtAbort = null;    // AbortController para limpar listeners de window

function cabinMapEscape(value) {
  return String(value ?? "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&#39;");
}

function cabinMapFormat(value, digits = 1, unit = "") {
  const n = Number(typeof value === "string" ? value.replace(",", ".") : value);
  if (!Number.isFinite(n)) return "—";
  return `${n.toFixed(digits)}${unit ? ` ${unit}` : ""}`;
}

function cabinMapReadInvMetric(inv, keys) {
  for (const key of keys) {
    const value = inv?.[key];
    if (value !== null && value !== undefined && value !== "") return value;
  }
  return null;
}

function isCabineMapVisible() {
  const mapView = document.getElementById("cabineMapView");
  return !!mapView && getComputedStyle(mapView).display !== "none";
}
function isUnifilarVisible() { return isCabineMapVisible(); }

function resizePlantChartsSoon() {
  const resize = () => {
    try { dailyChartInstance?.resize?.(); } catch (err) { console.warn("[dailyChart] resize erro:", err); }
    try { monthlyChartInstance?.resize?.(); } catch (err) { console.warn("[monthlyChart] resize erro:", err); }
  };
  requestAnimationFrame(() => {
    resize();
    setTimeout(resize, 80);
  });
}

function getPlantChartsGrid() {
  return document.querySelector(".plant-charts-grid");
}

function ensurePlantChartsPlaceholder(chartsGrid) {
  if (!chartsGrid || _plantChartsPlaceholder) return;
  _plantChartsPlaceholder = document.createComment("plant-charts-home");
  chartsGrid.parentNode.insertBefore(_plantChartsPlaceholder, chartsGrid.nextSibling);
}

function movePlantChartsIntoCabineMap() {
  const dock = document.getElementById("cabineMapChartDock");
  const chartsGrid = getPlantChartsGrid();
  if (!dock || !chartsGrid || dock.contains(chartsGrid)) return;
  ensurePlantChartsPlaceholder(chartsGrid);
  // Move para o dock que fica ABAIXO das cabines (no stage), não sobreposto
  dock.appendChild(chartsGrid);
  chartsGrid.classList.add("plant-charts-grid--cabine-map");
  resizePlantChartsSoon();
}

function autoFitCabineMap() { /* substituído pelo modo unifilar */ }

function movePlantChartsToList() {
  const chartsGrid = getPlantChartsGrid();
  if (!chartsGrid || !_plantChartsPlaceholder?.parentNode) return;
  _plantChartsPlaceholder.parentNode.insertBefore(chartsGrid, _plantChartsPlaceholder);
  chartsGrid.classList.remove("plant-charts-grid--cabine-map");
  resizePlantChartsSoon();
}

function initCabineMapDragZoom() { /* substituído pelo modo unifilar */ }

const CABINE_STRINGS_CACHE_TTL_MS = 60 * 1000;
const CABINE_STRINGS_CACHE = new Map();
const CABINE_STRINGS_PENDING = new Map();
let CABINE_STRINGS_BALLOON_SEQ = 0;
let CABINE_MAP_STRUCTURE_SIGNATURE = "";
let CABINE_STRINGS_ESC_HANDLER = null;

async function loadCabineStringsPayload(inverterRealId, { force = false } = {}) {
  const id = String(inverterRealId ?? "");
  if (!id || !PLANT_ID) return null;

  const cached = CABINE_STRINGS_CACHE.get(id);
  if (!force && cached && Date.now() - cached.ts < CABINE_STRINGS_CACHE_TTL_MS) {
    return cached.payload;
  }

  if (!force && CABINE_STRINGS_PENDING.has(id)) {
    return CABINE_STRINGS_PENDING.get(id);
  }

  const req = Promise.all([
    fetchInverterStrings(PLANT_ID, id),
    fetchInverterStringsRealtime(PLANT_ID, id)
  ])
    .then(([cfg, rt]) => {
      const payload = mergeStringsPayload(cfg, rt, id);
      CABINE_STRINGS_CACHE.set(id, { ts: Date.now(), payload });
      return payload;
    })
    .catch(err => {
      console.warn("[cabine strings] erro ao carregar strings:", err);
      return null;
    })
    .finally(() => CABINE_STRINGS_PENDING.delete(id));

  CABINE_STRINGS_PENDING.set(id, req);
  return req;
}

function renderCabineStringsBalloonRows(payload, inverterRealId) {
  const strings = Array.isArray(payload?.strings) ? payload.strings : [];
  const inverterOnline = getInverterOnlineStateById(inverterRealId);

  const visible = strings.filter(s =>
    s.exists_in_api === true &&
    s.effective_enabled !== false
  );

  if (!visible.length) {
    return {
      count: 0,
      html: `<div class="csb-empty">Nenhuma string monitorada</div>`
    };
  }

  const maxAmp = visible.reduce((m, s) => Math.max(m, asNumber(s.current_a, 0)), 0) || 10;

  const html = visible
    .slice()
    .sort((a, b) => Number(a.string_index) - Number(b.string_index))
    .map(s => {
      const amp = s.current_a != null ? asNumber(s.current_a, 0) : null;
      const pct = amp != null ? Math.min(100, (amp / maxAmp) * 100) : 0;
      const inAlarm = isStringInAlarm(s, inverterOnline);
      const noData = s.has_data !== true;
      const disabled = s.effective_enabled === false;

      const stClass = disabled ? "csb-str--disabled"
        : inAlarm ? "csb-str--alarm"
        : noData ? "csb-str--nodata"
        : "csb-str--ok";

      const statusTxt = disabled ? "Desabilitada"
        : inAlarm ? (s.alarm_reason || s.alarm_state || "Alarme")
        : noData ? "Sem dados"
        : "OK";

      const ampTxt = amp != null ? `${amp.toFixed(2)} A` : "—";

      return `
        <div class="csb-str-row ${stClass}">
          <span class="csb-str-idx">S${s.string_index}</span>
          <div class="csb-str-bar-wrap" title="${ampTxt}">
            <div class="csb-str-bar" style="width:${pct.toFixed(1)}%"></div>
          </div>
          <span class="csb-str-amp">${ampTxt}</span>
          <span class="csb-str-status">${statusTxt}</span>
        </div>`;
    }).join("");

  return { count: visible.length, html };
}

// ======================================================
// BALLOON DE STRINGS (popup ao clicar no card do inversor)
// ======================================================

function closeCabineStringsBalloon(immediate = false) {
  CABINE_STRINGS_BALLOON_SEQ++;
  if (CABINE_STRINGS_ESC_HANDLER) {
    document.removeEventListener("keydown", CABINE_STRINGS_ESC_HANDLER);
    CABINE_STRINGS_ESC_HANDLER = null;
  }
  const el = document.getElementById("cabineStringsBalloon");
  if (!el) return;
  if (immediate) {
    el.remove();
    return;
  }
  el.classList.remove("csb-backdrop--visible");
  setTimeout(() => { if (el.parentNode) el.parentNode.removeChild(el); }, 220);
}

async function openCabineStringsBalloon(anchorEl, inv, invName) {
  closeCabineStringsBalloon(true);

  const realId = getInverterRealId(inv);
  if (realId == null) return;

  const seq = ++CABINE_STRINGS_BALLOON_SEQ;

  const rect = anchorEl.getBoundingClientRect();
  const vw = window.innerWidth;
  const vh = window.innerHeight;

  const BALLOON_W = 370;
  const MARGIN = 14;
  const preferRight = rect.right + MARGIN + BALLOON_W <= vw;
  const rawBx = preferRight ? rect.right + MARGIN : rect.left - MARGIN - BALLOON_W;
  const bx = Math.max(12, Math.min(rawBx, vw - BALLOON_W - 12));
  const BALLOON_EST_H = 260;
  let by = rect.top + rect.height / 2 - BALLOON_EST_H / 2;
  by = Math.max(12, Math.min(by, vh - BALLOON_EST_H - 12));
  const tailY = Math.max(16, Math.min(BALLOON_EST_H - 32, rect.top + rect.height / 2 - by));

  const tailSide = preferRight ? "tail-left" : "tail-right";

  const backdrop = document.createElement("div");
  backdrop.id = "cabineStringsBalloon";
  backdrop.className = "csb-backdrop";
  backdrop.innerHTML = `
    <div class="csb-balloon csb-balloon--${tailSide}"
         style="top:${by.toFixed(0)}px; left:${bx.toFixed(0)}px; --csb-tail-y:${tailY.toFixed(0)}px; width:${BALLOON_W}px">
      <div class="csb-header">
        <div class="csb-header-icon">
          <svg viewBox="0 0 56 56" fill="none" width="16" height="16">
            <rect x="3" y="10" width="50" height="34" rx="5"
                  stroke="rgba(57,229,140,.8)" stroke-width="2" fill="rgba(57,229,140,.07)"/>
            <path d="M30 14 L26 25 H30 L25 42 L35 23 H31 L35 14 Z"
                  fill="rgba(57,229,140,.9)"/>
          </svg>
        </div>
        <span class="csb-inv-name">${cabinMapEscape(invName)}</span>
        <div class="csb-header-pills">
          <span class="csb-pill" id="csbStringCount">Carregando...</span>
        </div>
        <button class="csb-close" aria-label="Fechar">&times;</button>
      </div>
      <div class="csb-body">
        <div class="csb-col-labels">
          <span></span><span>Corrente</span><span>Amp</span><span>Status</span>
        </div>
        <div class="csb-strings-grid" id="csbStringsGrid">
          <div class="csb-empty">Carregando strings...</div>
        </div>
      </div>
    </div>`;

  backdrop.addEventListener("pointerdown", (e) => {
    if (!e.target.closest(".csb-balloon") || e.target.closest(".csb-close")) {
      closeCabineStringsBalloon();
    }
  });

  CABINE_STRINGS_ESC_HANDLER = function escHandler(e) {
    if (e.key === "Escape") {
      closeCabineStringsBalloon();
    }
  };
  document.addEventListener("keydown", CABINE_STRINGS_ESC_HANDLER);

  document.body.appendChild(backdrop);
  requestAnimationFrame(() => backdrop.classList.add("csb-backdrop--visible"));

  const payload = await loadCabineStringsPayload(realId);
  if (seq !== CABINE_STRINGS_BALLOON_SEQ) return;

  const grid = document.getElementById("csbStringsGrid");
  const count = document.getElementById("csbStringCount");
  const rendered = renderCabineStringsBalloonRows(payload, realId);

  if (grid) grid.innerHTML = rendered.html;
  if (count) count.textContent = `${rendered.count} string${rendered.count !== 1 ? "s" : ""}`;
}

function buildAcdcChip(label, value, digits, unit) {
  const n = Number(typeof value === "string" ? value.replace(",", ".") : value);
  const txt = Number.isFinite(n) ? `${n.toFixed(digits)}${unit ? " " + unit : ""}` : "—";
  return `<div class="cabine-acdc-chip">
    <span class="cabine-acdc-chip__lbl">${label}</span>
    <span class="cabine-acdc-chip__val">${txt}</span>
  </div>`;
}

function buildCabineCard(inv, idx = 0) {
  const realId = getInverterRealId(inv);
  const safeRealId = realId == null ? "" : String(realId);
  const name = getInverterDisplayName(inv, idx);
  const isOnline = isOnlineByFreshness(inv) && !isZeroSnapshot(inv);
  const hasAlarm = !!(inv?.alarm || inv?.fault || inv?.warning || inv?.alarm_active);

  const powerKw = cabinMapReadInvMetric(inv, ["active_power_kw", "power_kw", "power", "active_power"]);
  const effPct = cabinMapReadInvMetric(inv, ["efficiency_pct", "efficiency", "eff_pct"]);
  const tempC = cabinMapReadInvMetric(inv, ["temperature_internal_c", "temperature_c", "temp_c", "temperature_current", "temperature"]);
  const freqHz = cabinMapReadInvMetric(inv, ["frequency_hz", "freq_hz", "frequency"]);
  const prRaw = cabinMapReadInvMetric(inv, ["performance_ratio", "pr", "pr_ratio", "performance"]);
  const lastTs = cabinMapReadInvMetric(inv, ["last_reading_at", "last_reading_ts", "last_ts", "timestamp", "event_ts"]);
  const ratedKw = cabinMapReadInvMetric(inv, ["rated_power_kw", "capacity_kw", "rated_kw", "nominal_power_kw", "rated_power"]);

  const maxKw = Math.max(1, asNumber(ratedKw, 100));
  const powerNum = asNumber(powerKw, 0);
  const barPct = powerKw != null ? Math.min(100, Math.max(0, (powerNum / maxKw) * 100)) : 0;
  const prPct = prRaw != null ? normalizePercentMaybe(prRaw) : null;
  const loadPct = powerKw != null ? barPct : null;
  const boltOpacity = isOnline ? Math.max(0.25, barPct / 100) : 0.12;
  const boltColor = isOnline
    ? `rgba(57,229,140,${boltOpacity.toFixed(2)})`
    : "rgba(120,140,130,.2)";
  const _cabStInfo = getInverterStatusInfo(inv);
  const stateText = hasAlarm ? "Alarme" : _cabStInfo.label !== "—" ? _cabStInfo.label : (isOnline ? "Online" : "Offline");
  const safeName = cabinMapEscape(name);
  const safeTitle = cabinMapEscape(inv?.name || inv?.device_name || name);

  const card = document.createElement("div");
  card.className = `cabine-inv-card ${isOnline ? "is-online" : "is-offline"}${hasAlarm ? " has-alarm" : ""}`;
  card.dataset.inverterRealId = safeRealId;
  card.style.setProperty("--cabine-power-pct", `${barPct.toFixed(1)}%`);

  card.innerHTML = `
    <div class="cabine-inv-card__top">
      <div class="cabine-inv-card__status ${_cabStInfo.cls}"></div>
      <span class="cabine-inv-card__name" title="${safeTitle}">${safeName}</span>
      <span class="cabine-inv-card__state">${stateText}</span>
      ${realId != null ? `<button class="unif-dj-btn cabine-dj-btn" data-dj-inv-id="${safeRealId}"
              title="Disjuntor: ${safeName}" onclick="event.stopPropagation()">
        ${unifSVGDisjuntor(hasAlarm ? true : isOnline ? false : null, 'small')}
      </button>` : ''}
      ${_canSendCommand() && realId != null
        ? `<div class="cabine-inv-card__cmd">${renderDeviceCommandControl("inverter", realId, isOnline ? "on" : "off")}</div>`
        : ""}
    </div>

    <div class="cabine-inv-card__icon-row">
      <svg viewBox="0 0 56 56" width="52" height="52" fill="none"
           class="cabine-inv-svg" aria-hidden="true">
        <defs>
          <filter id="invGlow${safeRealId}" x="-30%" y="-30%" width="160%" height="160%">
            <feGaussianBlur stdDeviation="${isOnline ? "2.2" : "0"}" result="b"/>
            <feMerge><feMergeNode in="b"/><feMergeNode in="SourceGraphic"/></feMerge>
          </filter>
        </defs>
        <!-- Corpo do inversor -->
        <rect x="3" y="10" width="50" height="34" rx="5"
              stroke="${isOnline ? "rgba(57,229,140,.65)" : "rgba(90,110,100,.28)"}"
              stroke-width="1.6"
              fill="${isOnline ? "rgba(57,229,140,.05)" : "rgba(0,0,0,.25)"}"
              filter="url(#invGlow${safeRealId})"/>
        <!-- Painel solar DC -->
        <rect x="7" y="15" width="14" height="10" rx="1.5"
              fill="${isOnline ? "rgba(57,229,140,.16)" : "rgba(50,70,60,.08)"}"/>
        <line x1="7" y1="20" x2="21" y2="20"
              stroke="${isOnline ? "rgba(57,229,140,.35)" : "rgba(50,70,60,.12)"}" stroke-width=".7"/>
        <line x1="14" y1="15" x2="14" y2="25"
              stroke="${isOnline ? "rgba(57,229,140,.35)" : "rgba(50,70,60,.12)"}" stroke-width=".7"/>
        <path d="M21 20 L26 20" stroke="${isOnline ? "rgba(57,229,140,.5)" : "rgba(70,90,80,.2)"}"
              stroke-width="1" stroke-dasharray="2 1.5"/>
        <text x="8" y="31" font-size="4.5" fill="${isOnline ? "rgba(57,229,140,.55)" : "rgba(100,120,110,.25)"}"
              font-family="monospace" font-weight="700">DC IN</text>
        <!-- Raio central -->
        <path d="M30 14 L26 25 H30 L25 42 L35 23 H31 L35 14 Z"
              fill="${boltColor}" class="cabine-bolt-path"
              opacity="${isOnline ? "1" : "0.18"}"/>
        <!-- Ondas AC -->
        <path d="M40 20 Q42 16.5 44 20 Q46 23.5 48 20"
              stroke="${isOnline ? "rgba(57,229,140,.6)" : "rgba(80,100,90,.2)"}"
              stroke-width="1.4" fill="none" stroke-linecap="round"/>
        <path d="M40 26 Q42 22.5 44 26 Q46 29.5 48 26"
              stroke="${isOnline ? "rgba(57,229,140,.4)" : "rgba(80,100,90,.15)"}"
              stroke-width="1.1" fill="none" stroke-linecap="round"/>
        <path d="M40 23 L37 23" stroke="${isOnline ? "rgba(57,229,140,.5)" : "rgba(70,90,80,.2)"}"
              stroke-width="1" stroke-dasharray="2 1.5"/>
        <text x="39" y="35" font-size="4.5" fill="${isOnline ? "rgba(57,229,140,.55)" : "rgba(100,120,110,.25)"}"
              font-family="monospace" font-weight="700">AC OUT</text>
        <!-- LED de status -->
        <circle cx="46" cy="40" r="2.5"
                fill="${isOnline ? "#39e58c" : (hasAlarm ? "#ef9f27" : "#334433")}"
                style="${isOnline ? "filter:drop-shadow(0 0 5px rgba(57,229,140,.95));" : hasAlarm ? "filter:drop-shadow(0 0 4px rgba(239,159,39,.8));" : ""}"/>
      </svg>

      <div class="cabine-inv-card__power-bar-wrap">
        <div class="cabine-inv-card__power-readout">
          <span class="cabine-inv-card__power-label">${powerKw != null ? cabinMapFormat(powerKw, 0, "kW") : "—"}</span>
          <span class="cabine-inv-card__load-label">${loadPct != null ? `${cabinMapFormat(loadPct, 0)}%` : ""}</span>
        </div>
        <div class="cabine-inv-card__power-bar-track">
          <div class="cabine-inv-card__power-bar" style="width:${barPct.toFixed(1)}%;"></div>
        </div>
      </div>
    </div>

    <div class="cabine-inv-card__metrics">
      <div class="cabine-inv-metric">
        <span class="cabine-inv-metric__lbl">Efic.</span>
        <span class="cabine-inv-metric__val">${effPct != null ? cabinMapFormat(effPct, 1, "%") : "—"}</span>
      </div>
      <div class="cabine-inv-metric">
        <span class="cabine-inv-metric__lbl">Temp</span>
        <span class="cabine-inv-metric__val ${tempC != null && asNumber(tempC, 0) > 70 ? "val-warn" : ""}">
          ${tempC != null ? `${cabinMapFormat(tempC, 1)}&deg;C` : "—"}
        </span>
      </div>
      <div class="cabine-inv-metric">
        <span class="cabine-inv-metric__lbl">Freq</span>
        <span class="cabine-inv-metric__val">${freqHz != null ? cabinMapFormat(freqHz, 2, "Hz") : "—"}</span>
      </div>
      <div class="cabine-inv-metric">
        <span class="cabine-inv-metric__lbl">PR</span>
        <span class="cabine-inv-metric__val">${prPct != null ? cabinMapFormat(prPct, 1, "%") : "—"}</span>
      </div>
      <div class="cabine-inv-metric">
        <span class="cabine-inv-metric__lbl">Cap.</span>
        <span class="cabine-inv-metric__val">${ratedKw != null ? cabinMapFormat(ratedKw, 0, "kW") : "—"}</span>
      </div>
      <div class="cabine-inv-metric">
        <span class="cabine-inv-metric__lbl">Leit.</span>
        <span class="cabine-inv-metric__val">${fmtDatePtBR(lastTs)}</span>
      </div>
      <div class="cabine-inv-metric">
        <span class="cabine-inv-metric__lbl">Status</span>
        <span class="cabine-inv-metric__val ${_cabStInfo.cls}">${_cabStInfo.label}</span>
      </div>
    </div>

    ${(inv?.strings_rt && Array.isArray(inv.strings_rt) && inv.strings_rt.length > 0) ? `
    <div class="cabine-inv-card__strings">
      <div class="cabine-inv-strings-head">
        <svg width="10" height="10" viewBox="0 0 10 10" fill="none">
          <rect x="1" y="1" width="3" height="8" rx="0.8"
                stroke="${isOnline ? "rgba(57,229,140,.7)" : "rgba(80,100,90,.3)"}" stroke-width=".8"/>
          <rect x="6" y="1" width="3" height="8" rx="0.8"
                stroke="${isOnline ? "rgba(57,229,140,.7)" : "rgba(80,100,90,.3)"}" stroke-width=".8"/>
        </svg>
        <span>Strings</span>
      </div>
      <div class="cabine-inv-strings-grid">
        ${inv.strings_rt.slice(0, 20).map(s => {
          const amp = s.current_a != null ? `${Number(s.current_a).toFixed(1)}A` : "—";
          const inAlarm = (s.in_alarm || s.alarm) && isOnline;
          const nodata = !s.has_data;
          return `<div class="cabine-string-chip ${inAlarm ? "chip-alarm" : nodata ? "chip-nodata" : "chip-ok"}">
            <span class="chip-idx">S${s.string_index}</span>
            <span class="chip-amp">${amp}</span>
          </div>`;
        }).join("")}
        ${inv.strings_rt.length > 20 ? `<div class="cabine-string-chip chip-nodata"><span class="chip-idx">+${inv.strings_rt.length - 20}</span></div>` : ""}
      </div>
    </div>` : ""}

    <div class="cabine-inv-card__acdc">
      <div class="cabine-acdc-section">
        <span class="cabine-acdc-label">AC</span>
        <div class="cabine-acdc-chips">
          ${buildAcdcChip("V AB", inv?.line_voltage_ab_v ?? inv?.line_voltage_ab, 0, "V")}
          ${buildAcdcChip("Ia",   inv?.current_phase_a_a ?? inv?.current_phase_a, 2, "A")}
          ${buildAcdcChip("FP",   inv?.power_factor, 3, "")}
        </div>
      </div>
      <div class="cabine-acdc-section">
        <span class="cabine-acdc-label">DC</span>
        <div class="cabine-acdc-chips">
          ${buildAcdcChip("P DC",   inv?.power_dc_kw, 2, "kW")}
          ${buildAcdcChip("V str",  inv?.string_voltage_v, 0, "V")}
          ${buildAcdcChip("R isol", inv?.resistance_insulation_mohm, 1, "MΩ")}
        </div>
      </div>
    </div>
  `;

  wireDeviceCommandButtons(card);
  card.style.cursor = "pointer";

  return card;
}

function initCabineMapCardClicks() {
  /* Clicks em cards de inversores são tratados pelo Unifilar modal — sem ação aqui */
}

// ======================================================
// MODO UNIFILAR — FUNÇÕES PRINCIPAIS
// ======================================================

function buildUnifGroups(invertersRaw) {
  const uniq = dedupInvertersById(Array.isArray(invertersRaw) ? invertersRaw : []);
  uniq.sort((a, b) => {
    const an = String(getInverterDisplayName(a, 0) || "");
    const bn = String(getInverterDisplayName(b, 0) || "");
    return an.localeCompare(bn, "pt-BR", { numeric: true, sensitivity: "base" });
  });
  const hasCabins = uniq.some(inv => inv.cabin_id != null);
  if (!hasCabins) return [{ id: "all", name: "Inversores", displayOrder: 0, inverters: uniq }];
  const groupMap = new Map();
  const noCabin = [];
  uniq.forEach(inv => {
    const cabinId = inv.cabin_id;
    if (cabinId == null) { noCabin.push(inv); return; }
    if (!groupMap.has(cabinId)) {
      groupMap.set(cabinId, {
        id: cabinId,
        name: inv.section_name ?? inv.cabin_name ?? inv.cabin_code ?? `Cabine ${cabinId}`,
        displayOrder: asNumber(inv.cabin_display_order, 999),
        inverters: []
      });
    }
    groupMap.get(cabinId).inverters.push(inv);
  });
  const groups = [...Array.from(groupMap.values()).sort((a, b) => a.displayOrder - b.displayOrder)];
  if (noCabin.length) groups.push({ id: "none", name: "Sem cabine", displayOrder: 9999, inverters: noCabin });
  return groups;
}

/* ── SVGs dos equipamentos ── */
function unifSVGTransformer() {
  return `<svg viewBox="0 0 48 80" fill="none" class="unif-equip-svg unif-equip-svg--trafo" width="48" height="80">
    <defs>
      <filter id="trafoGlw" x="-40%" y="-40%" width="180%" height="180%">
        <feGaussianBlur stdDeviation="2" result="b"/>
        <feMerge><feMergeNode in="b"/><feMergeNode in="SourceGraphic"/></feMerge>
      </filter>
    </defs>
    <g filter="url(#trafoGlw)">
      <!-- Linha de entrada (topo) -->
      <line x1="24" y1="0" x2="24" y2="14" stroke="rgba(245,200,66,.7)" stroke-width="2" stroke-linecap="round"/>
      <!-- Círculo AT (alta tensão - amarelo) -->
      <circle cx="24" cy="28" r="14" stroke="rgba(245,200,66,.85)" stroke-width="1.8" fill="rgba(245,200,66,.05)"/>
      <!-- Triângulo Delta dentro do círculo AT -->
      <polygon points="24,18 15,34 33,34" stroke="rgba(245,200,66,.7)" stroke-width="1.3" fill="none" stroke-linejoin="round"/>
      <!-- Círculo BT (baixa tensão - verde) -->
      <circle cx="24" cy="50" r="14" stroke="rgba(127,208,85,.85)" stroke-width="1.8" fill="rgba(127,208,85,.05)"/>
      <!-- Y (estrela) dentro do círculo BT -->
      <line x1="24" y1="42" x2="24" y2="50" stroke="rgba(127,208,85,.7)" stroke-width="1.3" stroke-linecap="round"/>
      <line x1="24" y1="50" x2="17" y2="58" stroke="rgba(127,208,85,.7)" stroke-width="1.3" stroke-linecap="round"/>
      <line x1="24" y1="50" x2="31" y2="58" stroke="rgba(127,208,85,.7)" stroke-width="1.3" stroke-linecap="round"/>
      <circle cx="24" cy="50" r="1.5" fill="rgba(127,208,85,.6)"/>
      <!-- Linha de saída (base) -->
      <line x1="24" y1="64" x2="24" y2="80" stroke="rgba(127,208,85,.7)" stroke-width="2" stroke-linecap="round"/>
    </g>
  </svg>`;
}

function unifSVGCC() {
  return `<svg viewBox="0 0 40 40" fill="none" class="unif-equip-svg" width="36" height="36">
    <defs>
      <filter id="ccGlw" x="-30%" y="-30%" width="160%" height="160%">
        <feGaussianBlur stdDeviation="1.8" result="b"/>
        <feMerge><feMergeNode in="b"/><feMergeNode in="SourceGraphic"/></feMerge>
      </filter>
    </defs>
    <g filter="url(#ccGlw)">
      <!-- Caixa exterior -->
      <rect x="2" y="2" width="36" height="36" rx="4"
            stroke="rgba(127,208,85,.7)" stroke-width="1.5" fill="rgba(127,208,85,.04)"/>
      <!-- Barras condutoras verticais -->
      <line x1="10" y1="8"  x2="10" y2="32" stroke="rgba(127,208,85,.75)" stroke-width="1.6" stroke-linecap="round"/>
      <line x1="16" y1="8"  x2="16" y2="32" stroke="rgba(127,208,85,.6)"  stroke-width="1.4" stroke-linecap="round"/>
      <line x1="22" y1="8"  x2="22" y2="32" stroke="rgba(127,208,85,.45)" stroke-width="1.2" stroke-linecap="round"/>
      <line x1="28" y1="8"  x2="28" y2="32" stroke="rgba(127,208,85,.32)" stroke-width="1.1" stroke-linecap="round"/>
      <!-- Barra de barramento horizontal -->
      <line x1="8" y1="14" x2="32" y2="14" stroke="rgba(127,208,85,.5)" stroke-width=".9" stroke-linecap="round" stroke-dasharray="2 2"/>
      <line x1="8" y1="26" x2="32" y2="26" stroke="rgba(127,208,85,.5)" stroke-width=".9" stroke-linecap="round" stroke-dasharray="2 2"/>
      <!-- Ponto de entrada -->
      <circle cx="36" cy="20" r="2.5" fill="rgba(127,208,85,.85)"/>
    </g>
  </svg>`;
}

function unifSVGQGBT(isOnline) {
  const cMain = isOnline === true  ? "rgba(127,208,85,.88)"
              : isOnline === false ? "rgba(255,80,80,.72)"
              :                     "rgba(127,208,85,.45)";
  const cDot  = isOnline === true  ? "#7fd055"
              : isOnline === false ? "#ff4444"
              :                     "#334433";
  const glow  = isOnline === true  ? "filter:drop-shadow(0 0 5px rgba(127,208,85,.95));"
              : isOnline === false ? "filter:drop-shadow(0 0 5px rgba(255,80,80,.8));"
              :                     "";
  return `<svg viewBox="0 0 40 40" fill="none" class="unif-equip-svg" width="36" height="36">
    <defs>
      <filter id="qgbtGlw" x="-30%" y="-30%" width="160%" height="160%">
        <feGaussianBlur stdDeviation="1.6" result="b"/>
        <feMerge><feMergeNode in="b"/><feMergeNode in="SourceGraphic"/></feMerge>
      </filter>
    </defs>
    <g filter="url(#qgbtGlw)">
      <rect x="2" y="2" width="36" height="36" rx="4"
            stroke="${cMain}" stroke-width="1.5" fill="rgba(127,208,85,.03)"/>
      <!-- Chave seccionadora (linha de força) -->
      <line x1="12" y1="30" x2="26" y2="30" stroke="${cMain}" stroke-width="2" stroke-linecap="round"/>
      <!-- Contato móvel inclinado -->
      <line x1="12" y1="30" x2="18" y2="18" stroke="${cMain}" stroke-width="2" stroke-linecap="round"/>
      <!-- Contato fixo superior -->
      <line x1="18" y1="16" x2="26" y2="16" stroke="${cMain}" stroke-width="1.5" stroke-linecap="round" stroke-dasharray="3 2" opacity=".5"/>
      <!-- Ponto de articulação -->
      <circle cx="12" cy="30" r="2" fill="${cMain}"/>
      <!-- Ponto de entrada saída -->
      <circle cx="26" cy="30" r="2" fill="${cMain}"/>
      <!-- LED de status (canto superior direito) -->
      <circle cx="32" cy="8" r="3.5" fill="${cDot}" style="${glow}"/>
    </g>
  </svg>`;
}

function unifSVGSA() {
  return `<svg viewBox="0 0 34 34" fill="none" class="unif-equip-svg" width="30" height="30">
    <defs>
      <filter id="saGlw" x="-30%" y="-30%" width="160%" height="160%">
        <feGaussianBlur stdDeviation="1.4" result="b"/>
        <feMerge><feMergeNode in="b"/><feMergeNode in="SourceGraphic"/></feMerge>
      </filter>
    </defs>
    <g filter="url(#saGlw)">
      <circle cx="17" cy="17" r="14"
              stroke="rgba(127,208,85,.6)" stroke-width="1.4"
              fill="rgba(127,208,85,.04)" stroke-dasharray="4 3"/>
      <!-- Raio (serviços auxiliares) -->
      <path d="M17 7 L14 16 H18 L13 27 L23 16 H19 L22 7 Z"
            fill="rgba(127,208,85,.72)"/>
    </g>
  </svg>`;
}

/* ── Wire status helper ── */
function getWireClass(device, isOnline) {
  if (!device) return 'wire--idle';
  if (isOnline === false) return 'wire--fault';
  const quality = device?.quality ?? device?.data?.quality ?? null;
  if (quality === 28) return 'wire--fault';
  if (isOnline === true) return 'wire--active';
  return 'wire--idle';
}

/* ── SVG relay v2 ── */
function unifSVGRelayV2(isOnline) {
  const c = isOnline ? 'rgba(57,229,140,' : 'rgba(239,68,68,';
  return `<svg viewBox="0 0 52 64" width="52" height="64" fill="none" class="unif-device-svg">
    <line x1="26" y1="0" x2="26" y2="10" stroke="${c}0.55)" stroke-width="2" stroke-linecap="round"/>
    <rect x="6" y="10" width="40" height="36" rx="5"
          fill="${isOnline ? 'rgba(57,229,140,0.06)' : 'rgba(239,68,68,0.05)'}"
          stroke="${c}0.5)" stroke-width="1.5"/>
    <path d="M12 22 Q16 18 20 22 Q24 26 28 22 Q32 18 36 22 Q40 26 44 22"
          stroke="${c}0.6)" stroke-width="1.2" fill="none"/>
    <path d="M12 30 Q16 26 20 30 Q24 34 28 30 Q32 26 36 30 Q40 34 44 30"
          stroke="${c}0.4)" stroke-width="1" fill="none"/>
    <circle cx="26" cy="38" r="5" stroke="${c}0.7)" stroke-width="1.2"/>
    <path d="M26 34 L24 38 H27 L22 42 L30 37 H27 L28 34 Z"
          fill="${c}0.8)" opacity="0.9"/>
    <line x1="26" y1="46" x2="26" y2="56" stroke="${c}0.55)" stroke-width="2" stroke-linecap="round"/>
    <circle cx="42" cy="14" r="2.5"
            fill="${isOnline ? '#39e58c' : '#ef4444'}"
            style="filter:drop-shadow(0 0 4px ${isOnline ? 'rgba(57,229,140,0.9)' : 'rgba(239,68,68,0.7)'})"/>
  </svg>`;
}

/* ── SVG Cubículo de Proteção (conforme diagrama técnico IEC) ── */
function unifSVGCubiculo(relayOnline) {
  const isOn = relayOnline === true;
  const c  = isOn ? 'rgba(57,229,140,' : 'rgba(239,68,68,';
  const cY = 'rgba(245,200,66,';
  const cW = 'rgba(255,255,255,';

  return `<svg viewBox="0 0 110 210" width="154" height="294" fill="none" class="unif-cubiculo-svg">
    <line x1="55" y1="0" x2="55" y2="10" stroke="${c}0.55)" stroke-width="2" stroke-linecap="round"/>
    <rect x="6" y="10" width="98" height="188" rx="4"
          fill="${c}${isOn ? '0.03' : '0.07'})" stroke="${c}0.55)" stroke-width="1.4" stroke-dasharray="5 3"
          style="filter:drop-shadow(0 0 ${isOn ? '6' : '4'}px ${isOn ? 'rgba(57,229,140,0.25)' : 'rgba(239,68,68,0.2)'})"/>
    <text x="55" y="22" text-anchor="middle" font-size="5.2"
          fill="${cW}0.55)" font-family="monospace" font-weight="600" letter-spacing=".6">CUBÍCULO DE PROTEÇÃO</text>
    <rect x="30" y="27" width="50" height="12" rx="2"
          fill="${c}0.04)" stroke="${c}0.55)" stroke-width="1"/>
    <line x1="42" y1="30" x2="42" y2="36" stroke="${c}0.55)" stroke-width="1.6" stroke-linecap="round"/>
    <line x1="50" y1="30" x2="50" y2="36" stroke="${c}0.55)" stroke-width="1.6" stroke-linecap="round"/>
    <line x1="58" y1="30" x2="58" y2="36" stroke="${c}0.55)" stroke-width="1.6" stroke-linecap="round"/>
    <text x="84" y="36" font-size="4.5" fill="${c}0.55)" font-family="monospace">PR</text>
    <text x="84" y="41" font-size="3.5" fill="${cW}0.35)" font-family="monospace">12kV/10kA</text>
    <line x1="55" y1="39" x2="55" y2="48" stroke="${c}0.6)" stroke-width="1.5"/>
    <circle cx="55" cy="50" r="2" fill="${c}0.7)" stroke="${c}0.8)" stroke-width="0.8"/>
    <line x1="55" y1="52" x2="55" y2="55" stroke="${c}0.6)" stroke-width="1.2"/>
    <line x1="55" y1="55" x2="62" y2="48" stroke="${c}0.7)" stroke-width="1.5" stroke-linecap="round"/>
    <circle cx="55" cy="58" r="2" fill="${c}0.7)" stroke="${c}0.8)" stroke-width="0.8"/>
    <text x="84" y="55" font-size="4.5" fill="${c}0.55)" font-family="monospace">S01</text>
    <text x="84" y="60" font-size="3.5" fill="${cW}0.35)" font-family="monospace">15kV 400A</text>
    <line x1="55" y1="60" x2="55" y2="68" stroke="${c}0.6)" stroke-width="1.5"/>
    <circle cx="55" cy="74" r="6" stroke="${cY}0.6)" stroke-width="1" fill="${cY}0.03)"/>
    <circle cx="55" cy="82" r="6" stroke="${cY}0.6)" stroke-width="1" fill="${cY}0.03)"/>
    <text x="46" y="76" font-size="4" fill="${cY}0.5)" font-family="monospace" font-weight="600">Y</text>
    <text x="46" y="84" font-size="4" fill="${cY}0.5)" font-family="monospace" font-weight="600">Y</text>
    <text x="84" y="79" font-size="4.5" fill="${cY}0.55)" font-family="monospace">TP</text>
    <text x="84" y="84" font-size="3.5" fill="${cW}0.35)" font-family="monospace">700VA</text>
    <line x1="55" y1="88" x2="55" y2="96" stroke="${c}0.6)" stroke-width="1.5"/>
    <circle cx="55" cy="102" r="6" stroke="${cY}0.6)" stroke-width="1" fill="${cY}0.03)"/>
    <line x1="50" y1="102" x2="60" y2="102" stroke="${cY}0.5)" stroke-width="0.9"/>
    <polygon points="59,100 62,102 59,104" fill="${cY}0.45)"/>
    <text x="84" y="105" font-size="4.5" fill="${cY}0.55)" font-family="monospace">TC</text>
    <line x1="49" y1="102" x2="35" y2="102" stroke="${c}0.5)" stroke-width="1" stroke-dasharray="2 2"/>
    <line x1="35" y1="102" x2="35" y2="118" stroke="${c}0.5)" stroke-width="1" stroke-dasharray="2 2"/>
    <line x1="55" y1="108" x2="55" y2="116" stroke="${c}0.6)" stroke-width="1.5"/>
    <circle cx="35" cy="126" r="8" stroke="${c}0.55)" stroke-width="1.3" fill="${c}0.04)"/>
    <text x="35" y="129.5" text-anchor="middle" font-size="9" font-weight="700"
          fill="${c}0.7)" font-family="monospace">P</text>
    <circle cx="46" cy="120" r="2.2"
            fill="${isOn ? '#39e58c' : '#ef4444'}"
            style="filter:drop-shadow(0 0 3px ${isOn ? 'rgba(57,229,140,0.8)' : 'rgba(239,68,68,0.7)'})"/>
    <text x="18" y="129" font-size="3.8" fill="${c}0.55)" font-family="monospace">RELÉ</text>
    <line x1="43" y1="126" x2="55" y2="126" stroke="${c}0.5)" stroke-width="1" stroke-dasharray="2 2"/>
    <rect x="40" y="116" width="30" height="22" rx="3"
          fill="${c}0.04)" stroke="${c}0.6)" stroke-width="1"/>
    <line x1="55" y1="116" x2="55" y2="121" stroke="${c}0.8)" stroke-width="1.8" stroke-linecap="round"/>
    <circle cx="55" cy="121" r="1.5" fill="${c}0.65)"/>
    <line x1="55" y1="121" x2="55" y2="132" stroke="${c}0.85)" stroke-width="1.5" stroke-linecap="round"/>
    <line x1="55" y1="132" x2="55" y2="138" stroke="${c}0.8)" stroke-width="1.8" stroke-linecap="round"/>
    <text x="64" y="131" font-size="5" font-weight="700" fill="${c}0.65)" font-family="monospace">52</text>
    <text x="84" y="126" font-size="4.5" fill="${c}0.55)" font-family="monospace">DJ-02</text>
    <text x="84" y="131" font-size="3.5" fill="${cW}0.35)" font-family="monospace">630A</text>
    <text x="84" y="136" font-size="3.5" fill="${cW}0.35)" font-family="monospace">17.5kV</text>
    <line x1="55" y1="138" x2="55" y2="152" stroke="${c}0.6)" stroke-width="1.5"/>
    <rect x="30" y="152" width="50" height="10" rx="2"
          fill="${c}0.03)" stroke="${c}0.5)" stroke-width="0.8"/>
    <line x1="42" y1="155" x2="42" y2="159" stroke="${c}0.55)" stroke-width="1.3" stroke-linecap="round"/>
    <line x1="50" y1="155" x2="50" y2="159" stroke="${c}0.55)" stroke-width="1.3" stroke-linecap="round"/>
    <line x1="58" y1="155" x2="58" y2="159" stroke="${c}0.55)" stroke-width="1.3" stroke-linecap="round"/>
    <text x="84" y="160" font-size="4.5" fill="${c}0.55)" font-family="monospace">PR</text>
    <line x1="55" y1="162" x2="55" y2="210" stroke="${c}0.55)" stroke-width="2" stroke-linecap="round"/>
  </svg>`;
}

/* ── SVG medidor v2 (tamanho normal, na espinha) ── */
function unifSVGMeterV2(isOnline) {
  const c = isOnline ? 'rgba(57,229,140,' : 'rgba(239,68,68,';
  return `<svg viewBox="0 0 52 64" width="52" height="64" fill="none" class="unif-device-svg">
    <line x1="26" y1="0" x2="26" y2="12" stroke="${c}0.55)" stroke-width="2" stroke-linecap="round"/>
    <rect x="5" y="12" width="42" height="34" rx="4"
          fill="${isOnline ? 'rgba(57,229,140,0.06)' : 'rgba(239,68,68,0.05)'}"
          stroke="${c}0.5)" stroke-width="1.5"/>
    <rect x="10" y="17" width="32" height="18" rx="2"
          fill="rgba(0,0,0,0.4)" stroke="${c}0.25)" stroke-width="0.7"/>
    <line x1="13" y1="22" x2="39" y2="22" stroke="${c}0.45)" stroke-width="0.6"/>
    <line x1="13" y1="26" x2="35" y2="26" stroke="${c}0.3)" stroke-width="0.6"/>
    <line x1="13" y1="30" x2="38" y2="30" stroke="${c}0.4)" stroke-width="0.6"/>
    <text x="26" y="23" text-anchor="middle" font-size="5"
          fill="${c}0.7)" font-family="monospace" font-weight="700">METER</text>
    <line x1="26" y1="46" x2="26" y2="56" stroke="${c}0.55)" stroke-width="2" stroke-linecap="round"/>
    <circle cx="40" cy="42" r="2.2"
            fill="${isOnline ? '#39e58c' : '#ef4444'}"
            style="filter:drop-shadow(0 0 3px ${isOnline ? 'rgba(57,229,140,0.8)' : 'rgba(239,68,68,0.6)'})"/>
  </svg>`;
}

/* ── SVG medidor PEQUENO (ao lado do DJ Geral) ── */
function unifSVGMeterSmall(isOnline) {
  const c = isOnline ? 'rgba(57,229,140,' : 'rgba(239,68,68,';
  return `<svg viewBox="0 0 32 28" width="46" height="40" fill="none">
    <line x1="0" y1="14" x2="5" y2="14" stroke="${c}0.55)" stroke-width="1.5" stroke-linecap="round"/>
    <rect x="5" y="3" width="22" height="22" rx="2"
          fill="${isOnline ? 'rgba(57,229,140,0.05)' : 'rgba(239,68,68,0.04)'}"
          stroke="${c}0.6)" stroke-width="1.2"/>
    <rect x="9" y="7" width="14" height="14" rx="1.5"
          fill="rgba(0,0,0,0.35)" stroke="${c}0.35)" stroke-width="0.8"/>
    <line x1="11" y1="11" x2="21" y2="11" stroke="${c}0.4)" stroke-width="0.5"/>
    <line x1="11" y1="14" x2="19" y2="14" stroke="${c}0.3)" stroke-width="0.5"/>
    <line x1="11" y1="17" x2="20" y2="17" stroke="${c}0.35)" stroke-width="0.5"/>
    <circle cx="24" cy="22" r="1.5"
            fill="${isOnline ? '#39e58c' : '#ef4444'}"
            style="filter:drop-shadow(0 0 2px ${isOnline ? 'rgba(57,229,140,0.8)' : 'rgba(239,68,68,0.6)'})"/>
  </svg>`;
}

/* ── SVG relé PEQUENO (ao lado do DJMT) ── */
function unifSVGRelaySmall(isOnline) {
  const c = isOnline ? 'rgba(57,229,140,' : 'rgba(239,68,68,';
  return `<svg viewBox="0 0 32 28" width="46" height="40" fill="none">
    <line x1="0" y1="14" x2="5" y2="14" stroke="${c}0.55)" stroke-width="1.5" stroke-linecap="round"/>
    <rect x="5" y="3" width="22" height="22" rx="2"
          fill="${isOnline ? 'rgba(57,229,140,0.05)' : 'rgba(239,68,68,0.04)'}"
          stroke="${c}0.6)" stroke-width="1.2"/>
    <text x="16" y="13" text-anchor="middle" font-size="5.5" font-weight="700"
          fill="${c}0.7)" font-family="monospace">RELÉ</text>
    <text x="16" y="20" text-anchor="middle" font-size="4.5"
          fill="${c}0.5)" font-family="monospace">PROT.</text>
    <circle cx="24" cy="22" r="1.5"
            fill="${isOnline ? '#39e58c' : '#ef4444'}"
            style="filter:drop-shadow(0 0 2px ${isOnline ? 'rgba(57,229,140,0.8)' : 'rgba(239,68,68,0.6)'})"/>
  </svg>`;
}

/* ── SVG disjuntor (IEC) — SEM linhas de extensão ── */
function unifSVGDisjuntor(isTripped, size) {
  const lg  = size === 'large';
  const vW  = lg ? 40 : 26;
  const vH  = lg ? 34 : 22;
  const w   = lg ? 52 : 32;
  const h   = lg ? 46 : 32;
  const cx  = vW / 2;
  const bX  = lg ? 4 : 2, bY = lg ? 2 : 1;
  const bW  = vW - bX * 2;
  const bH  = lg ? 30 : 20;

  const c   = isTripped === true  ? 'rgba(239,68,68,'
            : isTripped === false ? 'rgba(57,229,140,'
            :                      'rgba(245,200,66,';
  const led = isTripped === true  ? '#ef4444'
            : isTripped === false ? '#39e58c'
            :                      '#f5c842';
  const glo = isTripped === true  ? 'rgba(239,68,68,.8)'
            : isTripped === false ? 'rgba(57,229,140,.8)'
            :                      'rgba(245,200,66,.8)';
  const bgFill = isTripped === true  ? 'rgba(239,68,68,.05)'
               : isTripped === false ? 'rgba(57,229,140,.05)'
               :                      'rgba(245,200,66,.04)';

  const ctY2 = bY + (lg ? 10 : 7);
  const cbY1 = bY + bH - (lg ? 10 : 7);
  const armX2 = isTripped ? cx + (lg ? 8 : 5) : cx;
  const sw = lg ? 2.5 : 2;

  return `<svg viewBox="0 0 ${vW} ${vH}" width="${w}" height="${h}" fill="none">
    <rect x="${bX}" y="${bY}" width="${bW}" height="${bH}" rx="${lg ? 4 : 3}"
          fill="${bgFill}" stroke="${c}.55)" stroke-width="1.5"/>
    ${lg ? `<text x="${cx}" y="${bY + bH - 3}" text-anchor="middle" font-size="5.5" font-family="monospace" font-weight="700" fill="${c}.4)">DJ</text>` : ''}
    <line x1="${cx}" y1="${bY + 2}" x2="${cx}" y2="${ctY2}" stroke="${c}.85)" stroke-width="${sw}" stroke-linecap="round"/>
    <circle cx="${cx}" cy="${ctY2}" r="${lg ? 2 : 1.5}" fill="${c}.75)"/>
    <line x1="${cx}" y1="${ctY2}" x2="${armX2}" y2="${cbY1}" stroke="${c}.9)" stroke-width="${lg ? 2 : 1.8}" stroke-linecap="round"/>
    <line x1="${cx}" y1="${cbY1}" x2="${cx}" y2="${bY + bH - 2}" stroke="${c}.85)" stroke-width="${sw}" stroke-linecap="round"/>
    <circle cx="${bX + bW - (lg ? 5 : 4)}" cy="${bY + (lg ? 4 : 3)}" r="${lg ? 2.5 : 2}"
            fill="${led}" style="filter:drop-shadow(0 0 3px ${glo})"/>
  </svg>`;
}

/* ── Slot de inversor no overview (com líquido de eficiência) ── */
function buildUnifInvSlotHTML(inv) {
  const realId   = getInverterRealId(inv);
  const safeId   = String(realId != null ? realId : Math.random().toString(36).slice(2)).replace(/[^a-zA-Z0-9_\-]/g, '_');
  const isOnline = isOnlineByFreshness(inv) && !isZeroSnapshot(inv);
  const hasAlarm = !!(inv?.alarm || inv?.fault || inv?.warning || inv?.alarm_active);
  const powerKw  = asNumber(inv.active_power_kw ?? inv.power_kw ?? 0, 0);
  const ratedKw  = asNumber(inv.rated_power_kw ?? 100, 100);
  const effRaw   = asNumber(inv.efficiency_pct ?? inv.dc_efficiency ?? null, null);
  const effPct   = effRaw ?? (ratedKw > 0 ? Math.min(100, (powerKw / ratedKw) * 100) : 0);
  const nm       = cabinMapEscape(getInverterDisplayName(inv, 0));

  // Corpo do inversor: x=3 y=10 w=50 h=34 → clip interno x=4 y=11 w=48 h=32
  const LIQUID_TOP   = 11;
  const LIQUID_H_MAX = 32;
  const liquidH  = Math.max(0, (effPct / 100) * LIQUID_H_MAX);
  const liquidY  = LIQUID_TOP + LIQUID_H_MAX - liquidH;
  const slotCls  = hasAlarm ? 'has-alarm' : isOnline ? 'is-online' : 'is-offline';
  const boltClr  = hasAlarm ? '#ef9f27' : isOnline ? '#39e58c' : '#334433';
  const liqClr   = isOnline ? (hasAlarm ? '#ef9f27' : '#39e58c') : '#ef4444';
  const liqAlpha = isOnline ? '0.55' : '0.2';
  const wireClass = getWireClass(inv, isOnline);

  return `<div class="unif-inv-slot ${slotCls}"
               data-inv-id="${safeId}"
               title="${nm}: ${powerKw.toFixed(0)} kW · Efic: ${effPct.toFixed(1)}%"
               onclick="typeof openCabineDetailForInverter==='function'&&openCabineDetailForInverter('${safeId}')">
    <svg width="2" height="12" style="overflow:visible">
      <line x1="1" y1="0" x2="1" y2="12" class="${wireClass}" stroke-width="2"/>
    </svg>
    ${_canSendCommand() ? `<button class="unif-dj-btn" data-dj-inv-id="${safeId}" title="Disjuntor: ${nm}"
            onclick="event.stopPropagation()">
      ${unifSVGDisjuntor(hasAlarm ? true : isOnline ? false : null, 'small')}
    </button>` : ''}
    <svg viewBox="0 0 56 56" width="52" height="52" fill="none" class="cabine-inv-svg" aria-hidden="true">
      <defs>
        <filter id="ig${safeId}" x="-30%" y="-30%" width="160%" height="160%">
          <feGaussianBlur stdDeviation="${isOnline ? '2' : '0'}" result="b"/>
          <feMerge><feMergeNode in="b"/><feMergeNode in="SourceGraphic"/></feMerge>
        </filter>
        <clipPath id="ilc${safeId}">
          <rect x="4" y="11" width="48" height="32" rx="4"/>
        </clipPath>
        <linearGradient id="ilg${safeId}" x1="0" y1="0" x2="0" y2="1">
          <stop offset="0%" stop-color="${liqClr}" stop-opacity="0.7"/>
          <stop offset="100%" stop-color="${liqClr}" stop-opacity="0.3"/>
        </linearGradient>
      </defs>
      <rect x="3" y="10" width="50" height="34" rx="5"
            stroke="${isOnline ? 'rgba(57,229,140,.65)' : hasAlarm ? 'rgba(239,159,39,.5)' : 'rgba(90,110,100,.28)'}"
            stroke-width="1.6"
            fill="${isOnline ? 'rgba(57,229,140,.04)' : 'rgba(0,0,0,.25)'}"
            filter="url(#ig${safeId})"/>
      <g clip-path="url(#ilc${safeId})">
        <rect class="inv-liquid" x="4" y="${liquidY.toFixed(1)}" width="48" height="${liquidH.toFixed(1)}"
              fill="url(#ilg${safeId})" opacity="${liqAlpha}"/>
        ${isOnline && liquidH > 4 ? `<path class="inv-liquid-wave"
              d="M4,${liquidY.toFixed(1)} Q18,${(liquidY-2.5).toFixed(1)} 28,${liquidY.toFixed(1)} Q42,${(liquidY+2.5).toFixed(1)} 52,${liquidY.toFixed(1)} L52,${(liquidY+5).toFixed(1)} Q42,${(liquidY+7.5).toFixed(1)} 28,${(liquidY+5).toFixed(1)} Q18,${(liquidY+2.5).toFixed(1)} 4,${(liquidY+5).toFixed(1)} Z"
              fill="${liqClr}" opacity="0.35"/>` : ''}
      </g>
      <rect x="7" y="15" width="14" height="10" rx="1.5"
            fill="${isOnline ? 'rgba(57,229,140,.16)' : 'rgba(50,70,60,.08)'}"/>
      <line x1="7" y1="20" x2="21" y2="20"
            stroke="${isOnline ? 'rgba(57,229,140,.35)' : 'rgba(50,70,60,.12)'}" stroke-width=".7"/>
      <line x1="14" y1="15" x2="14" y2="25"
            stroke="${isOnline ? 'rgba(57,229,140,.35)' : 'rgba(50,70,60,.12)'}" stroke-width=".7"/>
      <path d="M21 20 L26 20" stroke="${isOnline ? 'rgba(57,229,140,.5)' : 'rgba(70,90,80,.2)'}"
            stroke-width="1" stroke-dasharray="2 1.5"/>
      <path d="M30 14 L26 25 H30 L25 42 L35 23 H31 L35 14 Z"
            fill="${boltClr}" opacity="${isOnline ? '1' : '0.18'}"/>
      <path d="M40 20 Q42 16.5 44 20 Q46 23.5 48 20"
            stroke="${isOnline ? 'rgba(57,229,140,.6)' : 'rgba(80,100,90,.2)'}"
            stroke-width="1.4" fill="none" stroke-linecap="round"/>
      <path d="M40 26 Q42 22.5 44 26 Q46 29.5 48 26"
            stroke="${isOnline ? 'rgba(57,229,140,.4)' : 'rgba(80,100,90,.15)'}"
            stroke-width="1.1" fill="none" stroke-linecap="round"/>
      <circle cx="46" cy="40" r="2.5"
              fill="${isOnline ? '#39e58c' : hasAlarm ? '#ef9f27' : '#334433'}"
              style="${isOnline ? 'filter:drop-shadow(0 0 5px rgba(57,229,140,.95));' : hasAlarm ? 'filter:drop-shadow(0 0 4px rgba(239,159,39,.8));' : ''}"/>
    </svg>
    <span class="unif-inv-name">${nm}</span>
    <span class="unif-inv-val">${powerKw > 0 ? powerKw.toFixed(0) + ' kW' : '—'}</span>
  </div>`;
}

/* ── Coluna individual de inversor no overview (unifilar real) ── */
/* ── SVG do inversor (sem slot wrapper) ── */
function buildUnifInvSVGOnly(inv, safeId) {
  const isOnline = isOnlineByFreshness(inv) && !isZeroSnapshot(inv);
  const hasAlarm = !!(inv?.alarm || inv?.fault || inv?.warning || inv?.alarm_active);
  const powerKw  = asNumber(inv.active_power_kw ?? inv.power_kw ?? 0, 0);
  const ratedKw  = asNumber(inv.rated_power_kw ?? 100, 100);
  const effRaw   = asNumber(inv.efficiency_pct ?? inv.dc_efficiency ?? null, null);
  const effPct   = effRaw ?? (ratedKw > 0 ? Math.min(100, (powerKw / ratedKw) * 100) : 0);
  const boltClr  = hasAlarm ? '#ef9f27' : isOnline ? '#39e58c' : '#334433';
  const liqClr   = isOnline ? (hasAlarm ? '#ef9f27' : '#39e58c') : '#ef4444';
  const liqAlpha = isOnline ? '0.55' : '0.2';

  const LIQUID_TOP   = 11;
  const LIQUID_H_MAX = 32;
  const liquidH  = Math.max(0, (effPct / 100) * LIQUID_H_MAX);
  const liquidY  = LIQUID_TOP + LIQUID_H_MAX - liquidH;

  return `<svg viewBox="0 0 56 56" width="44" height="44" fill="none" class="cabine-inv-svg" aria-hidden="true">
    <defs>
      <filter id="ig${safeId}" x="-30%" y="-30%" width="160%" height="160%">
        <feGaussianBlur stdDeviation="${isOnline ? '2' : '0'}" result="b"/>
        <feMerge><feMergeNode in="b"/><feMergeNode in="SourceGraphic"/></feMerge>
      </filter>
      <clipPath id="ilc${safeId}">
        <rect x="4" y="11" width="48" height="32" rx="4"/>
      </clipPath>
      <linearGradient id="ilg${safeId}" x1="0" y1="0" x2="0" y2="1">
        <stop offset="0%" stop-color="${liqClr}" stop-opacity="0.7"/>
        <stop offset="100%" stop-color="${liqClr}" stop-opacity="0.3"/>
      </linearGradient>
    </defs>
    <rect x="3" y="10" width="50" height="34" rx="5"
          stroke="${isOnline ? 'rgba(57,229,140,.65)' : hasAlarm ? 'rgba(239,159,39,.5)' : 'rgba(90,110,100,.28)'}"
          stroke-width="1.6"
          fill="${isOnline ? 'rgba(57,229,140,.04)' : 'rgba(0,0,0,.25)'}"
          filter="url(#ig${safeId})"/>
    <g clip-path="url(#ilc${safeId})">
      <rect class="inv-liquid" x="4" y="${liquidY.toFixed(1)}" width="48" height="${liquidH.toFixed(1)}"
            fill="url(#ilg${safeId})" opacity="${liqAlpha}"/>
      ${isOnline && liquidH > 4 ? `<path class="inv-liquid-wave"
            d="M4,${liquidY.toFixed(1)} Q18,${(liquidY-2.5).toFixed(1)} 28,${liquidY.toFixed(1)} Q42,${(liquidY+2.5).toFixed(1)} 52,${liquidY.toFixed(1)} L52,${(liquidY+5).toFixed(1)} Q42,${(liquidY+7.5).toFixed(1)} 28,${(liquidY+5).toFixed(1)} Q18,${(liquidY+2.5).toFixed(1)} 4,${(liquidY+5).toFixed(1)} Z"
            fill="${liqClr}" opacity="0.35"/>` : ''}
    </g>
    <rect x="7" y="15" width="14" height="10" rx="1.5"
          fill="${isOnline ? 'rgba(57,229,140,.16)' : 'rgba(50,70,60,.08)'}"/>
    <line x1="7" y1="20" x2="21" y2="20"
          stroke="${isOnline ? 'rgba(57,229,140,.35)' : 'rgba(50,70,60,.12)'}" stroke-width=".7"/>
    <line x1="14" y1="15" x2="14" y2="25"
          stroke="${isOnline ? 'rgba(57,229,140,.35)' : 'rgba(50,70,60,.12)'}" stroke-width=".7"/>
    <path d="M21 20 L26 20" stroke="${isOnline ? 'rgba(57,229,140,.5)' : 'rgba(70,90,80,.2)'}"
          stroke-width="1" stroke-dasharray="2 1.5"/>
    <path d="M30 14 L26 25 H30 L25 42 L35 23 H31 L35 14 Z"
          fill="${boltClr}" opacity="${isOnline ? '1' : '0.18'}"/>
    <path d="M40 20 Q42 16.5 44 20 Q46 23.5 48 20"
          stroke="${isOnline ? 'rgba(57,229,140,.6)' : 'rgba(80,100,90,.2)'}"
          stroke-width="1.4" fill="none" stroke-linecap="round"/>
    <path d="M40 26 Q42 22.5 44 26 Q46 29.5 48 26"
          stroke="${isOnline ? 'rgba(57,229,140,.4)' : 'rgba(80,100,90,.15)'}"
          stroke-width="1.1" fill="none" stroke-linecap="round"/>
    <circle cx="46" cy="40" r="2.5"
            fill="${isOnline ? '#39e58c' : hasAlarm ? '#ef9f27' : '#334433'}"
            style="${isOnline ? 'filter:drop-shadow(0 0 5px rgba(57,229,140,.95));' : hasAlarm ? 'filter:drop-shadow(0 0 4px rgba(239,159,39,.8));' : ''}"/>
  </svg>`;
}

/* ── Branch de cabine: DJ → label → ramificação → inversores ── */
function _buildCabineBranch(group, idx, canCmd, isSingle) {
  const invs    = group.inverters || [];
  const online  = invs.filter(i => isOnlineByFreshness(i) && !isZeroSnapshot(i)).length;
  const alarm   = invs.filter(i => !!(i?.alarm||i?.fault||i?.warning||i?.alarm_active)).length;
  const totalKw = invs.reduce((s,i) => s + asNumber(i.active_power_kw??i.power_kw??0,0), 0);
  const cabWire = online > 0 ? 'wire--active' : alarm > 0 ? 'wire--fault' : 'wire--idle';
  const djSt    = alarm > 0 ? true : online > 0 ? false : null;
  const offAttr = online === 0 && alarm === 0 ? ' data-offline="1"' : '';

  const invsHTML = invs.map(inv => {
    const rid    = getInverterRealId(inv);
    const sid    = String(rid!=null?rid:Math.random().toString(36).slice(2)).replace(/[^a-zA-Z0-9_\-]/g,'_');
    const isOn   = isOnlineByFreshness(inv) && !isZeroSnapshot(inv);
    const hasAlm = !!(inv?.alarm||inv?.fault||inv?.warning||inv?.alarm_active);
    const pKw    = asNumber(inv.active_power_kw??inv.power_kw??0,0);
    const nm     = cabinMapEscape(getInverterDisplayName(inv,0));
    const wc     = getWireClass(inv, isOn);
    const cls    = hasAlm?'has-alarm':isOn?'is-online':'is-offline';
    const djS    = hasAlm?true:isOn?false:null;

    return `<div class="unif-branch-inv ${cls}" data-inv-id="${sid}" data-cabin-id="${cabinMapEscape(String(group.id))}">
      <svg class="unif-vw" width="2" height="18"><line x1="1" y1="0" x2="1" y2="18" class="${wc}" stroke-width="1.5"/></svg>
      ${canCmd
        ? `<button class="unif-dj-btn" data-dj-inv-id="${sid}" title="DJ: ${nm}" onclick="event.stopPropagation()">${unifSVGDisjuntor(djS,'small')}</button>`
        : `<div class="unif-dj-sym">${unifSVGDisjuntor(djS,'small')}</div>`}
      <svg class="unif-vw" width="2" height="10"><line x1="1" y1="0" x2="1" y2="10" class="${wc}" stroke-width="1.5"/></svg>
      ${buildUnifInvSVGOnly(inv, sid)}
      <span class="unif-inv-name">${nm}</span>
      <span class="unif-inv-val">${pKw>0?pKw.toFixed(0)+' kW':'—'}</span>
    </div>`;
  }).join('');

  // When there's only 1 cabin (or 0), the per-cabin DJ is redundant (DJBT above covers it).
  // Multi-cabin: show the per-cabin DJ + optional meter arm.
  if (isSingle) {
    return `<div class="unif-branch"${offAttr} data-cabine-idx="${idx}">
      <svg class="unif-vw" width="2" height="14"><line x1="1" y1="0" x2="1" y2="14" class="${cabWire}" stroke-width="1.5"/></svg>
      <span class="unif-branch-label">${cabinMapEscape(group.name)}</span>
      <span class="unif-branch-stats">${online}/${invs.length} · ${totalKw.toFixed(0)} kW</span>
      <div class="unif-branch-fork">${invsHTML}</div>
    </div>`;
  }

  const cabMeterItem   = Array.isArray(MULTIMETER_REALTIME) ? MULTIMETER_REALTIME[0] : MULTIMETER_REALTIME;
  const cabMeterOnline = cabMeterItem ? multimeterOnlineFromPayload(cabMeterItem) : null;
  const showCabMeter   = PLANT_CAPABILITIES.hasMultimeter === true;

  const cabMeterArm = showCabMeter ? `
    <div class="unif-cabin-meter-arm">
      <svg class="unif-hconn" width="28" height="2"><line x1="0" y1="1" x2="28" y2="1" class="${cabWire}" stroke-width="1.2"/></svg>
      <div class="unif-meter-side unif-cabin-meter-node" data-unif-device="multimeter">
        ${unifSVGMeterSmall(cabMeterOnline === true)}
      </div>
    </div>` : '';

  return `<div class="unif-branch"${offAttr} data-cabine-idx="${idx}">
    <svg class="unif-vw" width="2" height="28"><line x1="1" y1="0" x2="1" y2="28" class="${cabWire}" stroke-width="2"/></svg>
    <div class="unif-branch-dj-wrap">
      <div class="unif-branch-dj">${unifSVGDisjuntor(djSt,'small')}</div>
      ${cabMeterArm}
    </div>
    <svg class="unif-vw" width="2" height="14"><line x1="1" y1="0" x2="1" y2="14" class="${cabWire}" stroke-width="1.5"/></svg>
    <span class="unif-branch-label">${cabinMapEscape(group.name)}</span>
    <span class="unif-branch-stats">${online}/${invs.length} · ${totalKw.toFixed(0)} kW</span>
    <div class="unif-branch-fork">${invsHTML}</div>
  </div>`;
}

/* ── Navegar para detalhe de cabine a partir do inversor ── */
function openCabineDetailForInverter(invId) {
  const idx = UNIF_GROUPS.findIndex(g =>
    g.inverters.some(inv => String(getInverterRealId(inv)) === String(invId))
  );
  if (idx < 0) return;
  UNIF_CABIN_IDX = idx;
  setUnifMode('cabin', idx);
  setTimeout(() => {
    const el = document.querySelector(`[data-inverter-real-id="${invId}"]`);
    if (el) el.scrollIntoView({ behavior: 'smooth', block: 'center' });
  }, 150);
}

/* ── Console de comando em massa (Disjuntor Geral) ── */
function openBulkCommandConsole() {
  if (!_canSendCommand()) return;
  document.getElementById('bulkCmdOverlay')?.remove();

  const allInvs = UNIF_GROUPS.flatMap(g =>
    g.inverters.map(inv => ({
      id:       String(getInverterRealId(inv) ?? ''),
      name:     getInverterDisplayName(inv, 0),
      cabin:    g.name,
      isOnline: isOnlineByFreshness(inv) && !isZeroSnapshot(inv),
      hasAlarm: !!(inv?.alarm || inv?.fault || inv?.warning || inv?.alarm_active)
    }))
  );
  if (!allInvs.length) return;

  const overlay = document.createElement('div');
  overlay.id = 'bulkCmdOverlay';
  overlay.className = 'bulk-cmd-overlay';

  const invRows = allInvs.map(inv => {
    const sc = inv.hasAlarm ? 'alarm' : inv.isOnline ? 'online' : 'offline';
    const sl = inv.hasAlarm ? 'Alarme' : inv.isOnline ? 'Online' : 'Offline';
    return `<label class="bulk-inv-row">
      <input type="checkbox" class="bulk-inv-chk" value="${cabinMapEscape(inv.id)}" checked/>
      <span class="bulk-inv-info">
        <span class="bulk-inv-name">${cabinMapEscape(inv.name)}</span>
        <span class="bulk-inv-cabin">${cabinMapEscape(inv.cabin)}</span>
      </span>
      <span class="bulk-inv-badge bulk-inv-badge--${sc}">${sl}</span>
    </label>`;
  }).join('');

  overlay.innerHTML = `
    <div class="bulk-cmd-console">
      <div class="bulk-cmd-header">
        <div class="bulk-cmd-title-wrap">
          ${unifSVGDisjuntor(null, 'large')}
          <div>
            <div class="bulk-cmd-title">${cabinMapEscape(getBreakerName('djbt', null, null, 'DJBT'))} — Disjuntor Geral</div>
            <div class="bulk-cmd-subtitle">Todos os inversores</div>
          </div>
        </div>
        <button class="bulk-cmd-close" id="bulkCmdClose" type="button" aria-label="Fechar">×</button>
      </div>

      <div class="bulk-cmd-body" id="bulkCmdBody">
        <div class="bulk-select-bar">
          <button class="bulk-sel-btn" id="bulkSelAll" type="button">Sel. todos</button>
          <button class="bulk-sel-btn" id="bulkSelNone" type="button">Desmarcar</button>
          <span class="bulk-count" id="bulkCount">${allInvs.length} selecionados</span>
        </div>
        <div class="bulk-inv-list">${invRows}</div>
      </div>

      <div class="bulk-cmd-auth" id="bulkCmdAuth" style="display:none">
        <p class="bulk-auth-label" id="bulkAuthLabel">Autenticação necessária</p>
        <input class="bulk-auth-input" type="text" id="bulkAuthUser" placeholder="Usuário" autocomplete="username"/>
        <input class="bulk-auth-input" type="password" id="bulkAuthPass" placeholder="Senha" autocomplete="current-password"/>
        <div class="bulk-progress-wrap" id="bulkProgressWrap" style="display:none">
          <div class="bulk-progress-bar"><div class="bulk-progress-fill" id="bulkProgressFill"></div></div>
          <span class="bulk-progress-text" id="bulkProgressText">Enviando...</span>
        </div>
        <div class="bulk-auth-actions">
          <button class="bulk-auth-cancel" id="bulkAuthCancel" type="button">Voltar</button>
          <button class="bulk-auth-confirm" id="bulkAuthConfirm" type="button">Confirmar</button>
        </div>
      </div>

      <div class="bulk-cmd-footer" id="bulkCmdFooter">
        <button class="bulk-cmd-btn bulk-cmd-btn--on"    data-bulk-action="on"    type="button">
          <svg width="13" height="13" viewBox="0 0 14 14" fill="none"><path d="M7 1.5a5.5 5.5 0 1 1 0 11 5.5 5.5 0 0 1 0-11zm0 1.5v3.5" stroke="currentColor" stroke-width="1.6" stroke-linecap="round"/></svg>
          LIGAR
        </button>
        <button class="bulk-cmd-btn bulk-cmd-btn--off"   data-bulk-action="off"   type="button">
          <svg width="13" height="13" viewBox="0 0 14 14" fill="none"><rect x="2.5" y="2.5" width="9" height="9" rx="1.8" stroke="currentColor" stroke-width="1.5"/></svg>
          DESLIGAR
        </button>
        <button class="bulk-cmd-btn bulk-cmd-btn--reset" data-bulk-action="reset" type="button">
          <svg width="13" height="13" viewBox="0 0 14 14" fill="none"><path d="M2 7A5 5 0 1 1 7 12" stroke="currentColor" stroke-width="1.6" stroke-linecap="round"/><path d="M2 4.5v2.5H4.5" stroke="currentColor" stroke-width="1.4" stroke-linecap="round" stroke-linejoin="round"/></svg>
          RESETAR
        </button>
      </div>
    </div>`;

  document.body.appendChild(overlay);
  requestAnimationFrame(() => overlay.classList.add('bulk-cmd-visible'));

  let pendingAction = null;

  const closeBulk = () => {
    overlay.classList.remove('bulk-cmd-visible');
    setTimeout(() => overlay.remove(), 220);
  };

  document.getElementById('bulkCmdClose').addEventListener('click', closeBulk);
  overlay.addEventListener('pointerdown', e => { if (!e.target.closest('.bulk-cmd-console')) closeBulk(); });

  const updateCount = () => {
    const n = overlay.querySelectorAll('.bulk-inv-chk:checked').length;
    document.getElementById('bulkCount').textContent = `${n} selecionado${n !== 1 ? 's' : ''}`;
  };
  document.getElementById('bulkSelAll').addEventListener('click',  () => { overlay.querySelectorAll('.bulk-inv-chk').forEach(c => c.checked = true);  updateCount(); });
  document.getElementById('bulkSelNone').addEventListener('click', () => { overlay.querySelectorAll('.bulk-inv-chk').forEach(c => c.checked = false); updateCount(); });
  overlay.querySelectorAll('.bulk-inv-chk').forEach(c => c.addEventListener('change', updateCount));

  overlay.querySelectorAll('.bulk-cmd-btn[data-bulk-action]').forEach(btn => {
    btn.addEventListener('click', () => {
      const selected = [...overlay.querySelectorAll('.bulk-inv-chk:checked')].map(c => c.value);
      if (!selected.length) return;
      const act = btn.dataset.bulkAction;
      const actLbl = act === 'on' ? 'LIGAR' : act === 'off' ? 'DESLIGAR' : 'RESETAR';
      pendingAction = { action: act, ids: selected };
      document.getElementById('bulkAuthLabel').textContent =
        `${actLbl} ${selected.length} inversor${selected.length !== 1 ? 'es' : ''} — autentique-se`;
      document.getElementById('bulkAuthUser').value = '';
      document.getElementById('bulkAuthPass').value = '';
      document.getElementById('bulkProgressWrap').style.display = 'none';
      document.getElementById('bulkAuthCancel').disabled = false;
      document.getElementById('bulkAuthConfirm').disabled = false;
      document.getElementById('bulkAuthConfirm').textContent = 'Confirmar';
      document.getElementById('bulkCmdBody').style.display   = 'none';
      document.getElementById('bulkCmdFooter').style.display = 'none';
      document.getElementById('bulkCmdAuth').style.display   = '';
      setTimeout(() => document.getElementById('bulkAuthUser')?.focus(), 50);
    });
  });

  document.getElementById('bulkAuthCancel').addEventListener('click', () => {
    document.getElementById('bulkCmdAuth').style.display   = 'none';
    document.getElementById('bulkCmdBody').style.display   = '';
    document.getElementById('bulkCmdFooter').style.display = '';
    pendingAction = null;
  });

  document.getElementById('bulkAuthConfirm').addEventListener('click', async () => {
    if (!pendingAction) return;
    const username = (document.getElementById('bulkAuthUser').value || '').trim();
    const password = document.getElementById('bulkAuthPass').value || '';
    if (!username || !password) {
      document.getElementById('bulkAuthLabel').textContent = 'Preencha usuário e senha.';
      return;
    }
    const { action, ids } = pendingAction;
    const progressWrap = document.getElementById('bulkProgressWrap');
    const fillEl       = document.getElementById('bulkProgressFill');
    const textEl       = document.getElementById('bulkProgressText');
    const confirmBtn   = document.getElementById('bulkAuthConfirm');
    const cancelBtn    = document.getElementById('bulkAuthCancel');

    progressWrap.style.display = '';
    confirmBtn.disabled = true;
    cancelBtn.disabled  = true;
    confirmBtn.textContent = 'Aguarde...';

    let done = 0, errors = 0;
    const total = ids.length;
    const headers = typeof buildAuthHeaders === 'function' ? buildAuthHeaders() : { 'Content-Type': 'application/json' };

    for (const deviceId of ids) {
      textEl.textContent = `${action.toUpperCase()} → ${done + 1}/${total}`;
      fillEl.style.width = `${Math.round((done / total) * 100)}%`;
      try {
        const res = await fetch(`${API_BASE}/plants/${PLANT_ID}/devices/${deviceId}/command`, {
          method: 'POST',
          headers,
          body: JSON.stringify({ action, username, password, requested_by: username })
        });
        if (!res.ok) errors++;
        if (action === 'on' || action === 'off') {
          setDevicePersistentState('inverter', deviceId, action);
          applyDeviceVisualState('inverter', deviceId, action);
        }
      } catch (_) { errors++; }
      done++;
    }

    fillEl.style.width = '100%';
    textEl.textContent = errors === 0
      ? `${done} comando${done !== 1 ? 's' : ''} enviado${done !== 1 ? 's' : ''} com sucesso!`
      : `${done - errors}/${done} enviados (${errors} erro${errors !== 1 ? 's' : ''})`;

    setTimeout(closeBulk, 1800);
  });
}

/* ── Overview: constrói HTML do diagrama completo ── */
function buildUnifilarOverviewHTML(groups, relayData, multimeterData) {
  const relayItem   = Array.isArray(relayData)     ? relayData[0]     : relayData;
  const meterItem   = Array.isArray(multimeterData) ? multimeterData[0] : multimeterData;
  const relayOnline = relayItem  ? relayOnlineFromPayload(relayItem)      : null;
  const meterOnline = meterItem  ? multimeterOnlineFromPayload(meterItem) : null;

  const meterPowerRaw = meterItem
    ? pickDeviceMetricValue(meterItem, meterItem?.analog ?? meterItem?.data ?? {}, ["active_power_kw","p_kw","power_kw"])
    : null;
  const meterPowerStr = meterPowerRaw != null ? `${cabinMapFormat(meterPowerRaw, 1)} kW` : '—';
  const meterName     = meterItem?.device_name || meterItem?.name || 'Medidor';

  const relayPowerRaw = relayItem
    ? pickDeviceMetricValue(relayItem, relayItem?.analog ?? {}, ["active_power_kw","power_kw","active_power","power"])
    : null;
  const relayPowerStr = relayPowerRaw != null ? `${cabinMapFormat(relayPowerRaw, 1)} kW` : '—';
  const relayName     = relayItem?.device_name || relayItem?.name || 'Relé';

  const anyOn    = groups.some(g => g.inverters.some(i => isOnlineByFreshness(i) && !isZeroSnapshot(i)));
  const wireMain = anyOn ? 'wire--active' : 'wire--idle';
  const wireMeter = meterItem ? getWireClass(meterItem, meterOnline) : wireMain;
  const wireRelay = relayItem ? getWireClass(relayItem, relayOnline) : wireMain;

  const meterBadge = meterOnline === true  ? '<span class="unif-badge unif-badge--online" id="unifMeterBadge">Online</span>'
                   : meterOnline === false ? '<span class="unif-badge unif-badge--offline" id="unifMeterBadge">Offline</span>'
                   : '';
  const relayBadge = relayOnline === true  ? '<span class="unif-badge unif-badge--online" id="unifRelayBadge">Online</span>'
                   : relayOnline === false ? '<span class="unif-badge unif-badge--offline" id="unifRelayBadge">Offline</span>'
                   : '';

  const canCmd = _canSendCommand();

  // Capabilities (null = unknown → show by default)
  const hasRelay       = PLANT_CAPABILITIES.hasRelay !== false;
  const hasTransformer = PLANT_CAPABILITIES.hasTransformer !== false;
  // Show multimeter if catalog confirmed it exists (regardless of realtime),
  // OR catalog not yet loaded and there's realtime data (backward compat)
  const hasMultimeter  = PLANT_CAPABILITIES.hasMultimeter === true
    || (PLANT_CAPABILITIES.hasMultimeter !== false && meterItem != null);
  // Fallback to realtime payload device_id when catalog not yet loaded
  const relayDevId = PLANT_CAPABILITIES.relayDeviceId
    ?? (relayItem?.device_id != null ? String(relayItem.device_id) : null)
    ?? (relayItem?.relay_id  != null ? String(relayItem.relay_id)  : null);

  // DJMT state: independent of relay — based on whether the plant has active power
  // Green (closed) = plant producing, Yellow (unknown) = no data, Red (open) = commanded off
  const djmtBkId     = getBreaker('djmt', null, null)?.id;
  const djmtCmdState = djmtBkId ? getDevicePersistentState("breaker", String(djmtBkId), null) : null;
  const djmtTripped  = djmtCmdState === "off" ? true : anyOn ? false : null;
  const djmtOff      = djmtCmdState === "off"; // propagate to downstream wires

  // DJBT wire cascade: if no inverters are active, mark as idle
  const wireBelowDjbt = anyOn ? wireMain : 'wire--idle';

  // Wire below DJMT (affects everything downstream if DJMT is commanded off)
  const wireBelowDjmt = djmtOff ? 'wire--idle' : wireMain;

  const isSingle    = groups.length <= 1;
  const cabinesHTML = groups.map((g, i) => _buildCabineBranch(g, i, canCmd, isSingle)).join('');

  return `<div class="unif-diagram unif-diagram--v2" id="unifDiagramRoot">

    <div class="unif-zoom-bar">
      <button class="unif-zoom-btn" id="unifZoomIn" title="+">+</button>
      <button class="unif-zoom-btn" id="unifZoomReset" title="Reset">\u27F2</button>
      <button class="unif-zoom-btn" id="unifZoomOut" title="\u2212">\u2212</button>
    </div>

    <div class="unif-pan-area" id="unifPanArea">
    <div class="unif-zoomable" id="unifZoomable">
    <div class="unif-spine">

      <!-- REDE -->
      <div class="unif-node-row">
        <svg viewBox="0 0 40 28" width="46" height="34" fill="none" opacity=".4">
          <line x1="20" y1="0" x2="20" y2="28" stroke="rgba(255,255,255,.4)" stroke-width="1.5"/>
          <line x1="8" y1="7" x2="32" y2="7" stroke="rgba(255,255,255,.4)" stroke-width="1.5"/>
          <line x1="4" y1="14" x2="36" y2="14" stroke="rgba(255,255,255,.4)" stroke-width="1.5"/>
          <line x1="8" y1="7" x2="4" y2="28" stroke="rgba(255,255,255,.2)" stroke-width="1"/>
          <line x1="32" y1="7" x2="36" y2="28" stroke="rgba(255,255,255,.2)" stroke-width="1"/>
        </svg>
        <span class="unif-grid-lbl">REDE</span>
      </div>

      <!-- Fio: REDE → DJMT -->
      <svg class="unif-vw" width="2" height="20"><line x1="1" y1="0" x2="1" y2="20" class="${wireMain}" stroke-width="2"/></svg>

      <!-- DJMT + Relé de Proteção ao lado -->
      <div class="unif-djmt-row">
        <div class="unif-spine-device unif-spine-dj${canCmd ? ' is-clickable' : ''}"
             id="unifNodeDjmt"
             ${canCmd && djmtBkId ? `data-dj-breaker-id="${djmtBkId}"` : ''}>
          ${unifSVGDisjuntor(djmtTripped, 'large')}
          <span class="unif-node-lbl unif-lbl-dj">${cabinMapEscape(getBreakerName('djmt', null, null, 'DJMT'))}</span>
          ${canCmd ? '<span class="unif-dj-hint">Comandar</span>' : ''}
        </div>
        ${hasRelay ? `
        <div class="unif-relay-arm">
          <svg class="unif-hconn" width="48" height="2"><line x1="0" y1="1" x2="48" y2="1" class="${wireRelay}" stroke-width="1.5"/></svg>
          <div class="unif-meter-side ${relayOnline === false ? 'is-offline' : ''}"
               id="unifNodeRelayArm" data-unif-device="relay">
            ${unifSVGRelaySmall(relayOnline === true)}
            <span class="unif-meter-lbl">${cabinMapEscape(relayName)}</span>
            <span class="unif-meter-val" id="unifRelayArmPower">${relayPowerStr}</span>
            ${relayBadge}
          </div>
        </div>` : ''}
      </div>

      <!-- Fio: DJMT → Trafo/DJBT (com X se DJMT aberto) -->
      ${djmtOff
        ? `<div class="unif-wire-break" style="height:${hasTransformer?14:22}px">
             <svg width="14" height="${hasTransformer?14:22}" style="overflow:visible;display:block;margin:0 auto">
               <line x1="7" y1="0" x2="7" y2="${Math.floor((hasTransformer?14:22)/2)-5}" stroke="rgba(239,68,68,0.35)" stroke-width="2" stroke-linecap="round"/>
               <text x="7" y="${Math.floor((hasTransformer?14:22)/2)+4}" text-anchor="middle" font-size="10" fill="rgba(239,68,68,0.75)" font-weight="700">✕</text>
               <line x1="7" y1="${Math.floor((hasTransformer?14:22)/2)+7}" x2="7" y2="${hasTransformer?14:22}" stroke="rgba(239,68,68,0.35)" stroke-width="2" stroke-linecap="round"/>
             </svg>
           </div>`
        : hasTransformer
          ? `<svg class="unif-vw" width="2" height="14"><line x1="1" y1="0" x2="1" y2="14" class="${wireBelowDjmt}" stroke-width="2"/></svg>`
          : `<svg class="unif-vw" width="2" height="22"><line x1="1" y1="0" x2="1" y2="22" class="${wireBelowDjmt}" stroke-width="2"/></svg>`
      }

      ${hasTransformer ? `
      <!-- TRANSFORMADOR -->
      <div class="unif-node-row">
        <div class="unif-node--trafo-v2" data-unif-device="transformer">
          ${unifSVGTransformer()}
          <span class="unif-node-lbl">TR-01</span>
        </div>
      </div>

      <!-- Fio: Trafo → DJBT -->
      <svg class="unif-vw" width="2" height="22"><line x1="1" y1="0" x2="1" y2="22" class="${wireBelowDjmt}" stroke-width="2"/></svg>
      ` : ''}

      <!-- DJBT centralizado + MEDIDOR ao lado -->
      <div class="unif-dj-meter-row">
        <div class="unif-spine-device unif-spine-dj${canCmd ? ' is-clickable' : ''}"
             id="unifDjGeral"
             ${canCmd ? 'onclick="typeof openBulkCommandConsole===\'function\'&&openBulkCommandConsole()"' : ''}>
          ${unifSVGDisjuntor(anyOn ? false : null, 'large')}
          <span class="unif-node-lbl unif-lbl-dj">${cabinMapEscape(getBreakerName('djbt', groups.length === 1 ? groups[0]?.id : null, null, 'DJBT'))}</span>
          ${canCmd ? '<span class="unif-dj-hint">Comandar</span>' : ''}
        </div>
        ${hasMultimeter ? `
        <div class="unif-meter-arm">
          <svg class="unif-hconn" width="48" height="2"><line x1="0" y1="1" x2="48" y2="1" class="${wireMeter}" stroke-width="1.5"/></svg>
          <div class="unif-meter-side ${meterOnline === false ? 'is-offline' : ''}"
               id="unifNodeMeter" data-unif-device="multimeter">
            ${unifSVGMeterSmall(meterOnline === true)}
            <span class="unif-meter-lbl">${cabinMapEscape(meterName)}</span>
            <span class="unif-meter-val" id="unifMeterPower">${meterPowerStr}</span>
            ${meterBadge}
          </div>
        </div>` : ''}
      </div>

      <!-- Fio: DJBT → Cabines (com X se sem produção) -->
      <svg class="unif-vw" width="2" height="24"><line x1="1" y1="0" x2="1" y2="24" class="${wireBelowDjbt}" stroke-width="2"/></svg>

      <!-- CABINES (árvore) -->
      <div class="unif-tree">${cabinesHTML}</div>

    </div>
    </div>
    </div>
  </div>`;
}

/* ── Inicialização: toggle Lista / Unifilar ── */
function initInvViewToggle() {
  const btnList    = document.getElementById("invBtnList");
  const btnMap     = document.getElementById("invBtnMap");
  const listSection = document.getElementById("invertersListSection");
  const unifView   = document.getElementById("cabineMapView");
  if (!btnList || !btnMap || !listSection || !unifView) return;
  if (btnList.dataset.unifReady === "true") return;
  btnList.dataset.unifReady = "true";

  const switchView = (toMap) => {
    document.body.classList.toggle("plant-map-mode", toMap);
    btnList.classList.toggle("is-active", !toMap);
    btnMap.classList.toggle("is-active", toMap);
    listSection.style.display = toMap ? "none" : "";
    unifView.style.display    = toMap ? "flex" : "none";

    if (!toMap) { movePlantChartsToList(); return; }

    movePlantChartsToList();
    initUnifilarControls();
    buildUnifilarOverview();
    renderUnifilarSidePanel();
    renderUnifilarStatsBar();
  };

  btnList.addEventListener("click", () => switchView(false));
  btnMap.addEventListener("click",  () => switchView(true));
  switchView(false);
}

function initUnifilarControls() {
  const btn = document.getElementById("unifilarBtnOverview");
  if (!btn || btn.dataset.unifCtrl) return;
  btn.dataset.unifCtrl = "1";

  document.getElementById("unifilarBtnOverview")?.addEventListener("click", () => setUnifMode("overview"));
  document.getElementById("unifilarBtnCabin")?.addEventListener("click",   () => setUnifMode("cabin"));
  document.getElementById("unifilarPrev")?.addEventListener("click",        () => navigateUnifCabin(-1));
  document.getElementById("unifilarNext")?.addEventListener("click",        () => navigateUnifCabin(1));
  document.getElementById("unifilarCollapseBtn")?.addEventListener("click", toggleUnifSidePanel);

  const searchEl = document.getElementById("unifilarSearch");
  if (searchEl) {
    searchEl.addEventListener("input", () => {
      UNIF_SEARCH_TEXT = searchEl.value.toLowerCase().trim();
      renderUnifilarSidePanel();
    });
  }
}

function setUnifMode(mode, cabinIdx) {
  if (UNIF_TRANSITION_RUNNING) return;
  UNIF_MODE = mode;

  const btnOv    = document.getElementById("unifilarBtnOverview");
  const btnCab   = document.getElementById("unifilarBtnCabin");
  const cabNav   = document.getElementById("unifilarCabinNav");
  const overview = document.getElementById("unifilarOverview");
  const detail   = document.getElementById("unifilarCabinDetail");

  btnOv?.classList.toggle("is-active", mode === "overview");
  btnCab?.classList.toggle("is-active", mode === "cabin");

  if (mode === "overview") {
    if (cabNav) cabNav.style.display = "none";
    if (detail) detail.style.display = "none";
    if (overview) overview.style.display = "";
    buildUnifilarOverview();
    UNIF_ACTIVE_CABIN_FILTER = null;
    renderUnifilarSidePanel();
  } else {
    if (cabinIdx != null) UNIF_CABIN_IDX = cabinIdx;
    if (cabNav) cabNav.style.display = "flex";
    if (overview) overview.style.display = "none";
    if (detail) detail.style.display = "";
    renderUnifCabinDetail(UNIF_CABIN_IDX, null);
  }
}

function navigateUnifCabin(dir) {
  if (UNIF_TRANSITION_RUNNING || !UNIF_GROUPS.length) return;
  UNIF_CABIN_IDX = ((UNIF_CABIN_IDX + dir) + UNIF_GROUPS.length) % UNIF_GROUPS.length;
  renderUnifCabinDetail(UNIF_CABIN_IDX, dir > 0 ? "left" : "right");
  UNIF_ACTIVE_CABIN_FILTER = String(UNIF_GROUPS[UNIF_CABIN_IDX]?.id ?? "");
  renderUnifilarSidePanel();
}

function renderUnifCabinDetail(idx, direction) {
  const detail = document.getElementById("unifilarCabinDetail");
  const label  = document.getElementById("unifilarCabinLabel");
  if (!detail || !UNIF_GROUPS.length) return;

  const group = UNIF_GROUPS[idx] || UNIF_GROUPS[0];
  if (!group) return;

  if (label) label.textContent = `${group.name} (${group.inverters.length} inv.)`;

  const container = document.createElement("div");
  container.className = "unif-cabin-cards";
  const idxRef = { value: 0 };
  group.inverters.forEach(inv => container.appendChild(buildCabineCard(inv, idxRef.value++)));

  /* Disjuntor individual na visão de cabine */
  container.querySelectorAll(".cabine-dj-btn[data-dj-inv-id]").forEach(btn => {
    btn.addEventListener("click", (e) => {
      e.stopPropagation();
      if (!_canSendCommand()) return;
      const id = btn.dataset.djInvId;
      if (id) openCommandConsole({ deviceType: "inverter", deviceId: id });
    });
  });

  /* Wires click -> modal */
  container.querySelectorAll(".cabine-inv-card[data-inverter-real-id]").forEach(card => {
    card.addEventListener("click", (e) => {
      if (e.target.closest("button,a,.device-command-control")) return;
      const id  = card.dataset.inverterRealId;
      const inv = INVERTER_EXTRAS_BY_ID.get(String(id)) ||
        dedupInvertersById(INVERTERS_REALTIME).find(x => String(getInverterRealId(x)) === String(id));
      if (inv) openUnifDeviceModal(inv, "inverter");
    });
  });

  if (direction && detail.firstElementChild) {
    UNIF_TRANSITION_RUNNING = true;
    const outClass = direction === "left" ? "unif-slide-out-left" : "unif-slide-out-right";
    const inClass  = direction === "left" ? "unif-slide-in-right" : "unif-slide-in-left";
    detail.firstElementChild.classList.add(outClass);
    container.classList.add(inClass);
    detail.appendChild(container);
    setTimeout(() => {
      detail.querySelector("." + outClass)?.remove();
      container.classList.remove(inClass);
      UNIF_TRANSITION_RUNNING = false;
    }, 380);
  } else {
    detail.innerHTML = "";
    detail.appendChild(container);
  }
}

function buildUnifilarOverview() {
  const el = document.getElementById("unifilarOverview");
  if (!el) return;

  /* Limpa listeners de window da renderização anterior */
  if (_unifEvtAbort) _unifEvtAbort.abort();
  _unifEvtAbort = new AbortController();
  const sig = _unifEvtAbort.signal;

  UNIF_GROUPS = buildUnifGroups(INVERTERS_REALTIME);
  el.innerHTML = buildUnifilarOverviewHTML(UNIF_GROUPS, RELAY_REALTIME, MULTIMETER_REALTIME);

  // ── Zoom + Pan (usa CSS zoom para resolução nítida) ──
  const panArea  = document.getElementById('unifPanArea');
  const zoomable = document.getElementById('unifZoomable');
  if (panArea && zoomable) {

    const applyTransform = () => {
      const z = _unifZoom || 0.5;
      zoomable.style.zoom = z;
      zoomable.style.transform = `translate(${_unifPanX / z}px, ${_unifPanY / z}px)`;
    };

    const calcFitZoom = () => {
      const areaW = panArea.clientWidth  || 800;
      const areaH = panArea.clientHeight || 500;
      zoomable.style.zoom = 1;
      zoomable.style.transform = 'none';
      const contentW = zoomable.scrollWidth  || 800;
      const contentH = zoomable.scrollHeight || 500;
      let fit = Math.min(areaW / contentW, areaH / contentH, 1);
      fit = Math.max(0.30, Math.min(1, fit));
      return Math.round(fit * 100) / 100;
    };

    /* Re-render: aplica zoom/pan IMEDIATAMENTE (síncrono) → sem flicker */
    if (_unifZoomInited) {
      applyTransform();
    } else {
      /* Primeira renderização: mede o conteúdo, calcula zoom auto-fit e centraliza */
      requestAnimationFrame(() => {
        requestAnimationFrame(() => {
          const fitZ = calcFitZoom(); // deixa zoom=1 para medir scrollWidth/Height corretos
          const z = Math.min(1, fitZ * 1.3);
          _unifZoom = z;
          // Mede com zoom=1 (calcFitZoom já aplicou zoom=1)
          const contentW = zoomable.scrollWidth  || 800;
          const contentH = zoomable.scrollHeight || 500;
          const areaW    = panArea.clientWidth   || 800;
          const areaH    = panArea.clientHeight  || 500;
          // Centraliza o diagrama na área de pan
          _unifPanX = Math.round((areaW - contentW * z) / 2);
          _unifPanY = Math.max(0, Math.round((areaH - contentH * z) / 2));
          _unifZoomInited = true;
          applyTransform();
        });
      });
    }

    let _dragging = false, _startX = 0, _startY = 0, _startPanX = 0, _startPanY = 0;

    document.getElementById('unifZoomIn')?.addEventListener('click', () => {
      _unifZoom = Math.min(3.0, (_unifZoom || 0.5) + 0.15);
      applyTransform();
    });
    document.getElementById('unifZoomOut')?.addEventListener('click', () => {
      _unifZoom = Math.max(0.25, (_unifZoom || 0.5) - 0.15);
      applyTransform();
    });
    document.getElementById('unifZoomReset')?.addEventListener('click', () => {
      const resetZ = calcFitZoom(); // deixa zoom=1 para medir scrollWidth/Height
      _unifZoom = resetZ;
      const contentW = zoomable.scrollWidth  || 800;
      const contentH = zoomable.scrollHeight || 500;
      const areaW    = panArea.clientWidth   || 800;
      const areaH    = panArea.clientHeight  || 500;
      _unifPanX = Math.round((areaW - contentW * resetZ) / 2);
      _unifPanY = Math.max(0, Math.round((areaH - contentH * resetZ) / 2));
      applyTransform();
    });

    panArea.addEventListener('wheel', (e) => {
      e.preventDefault();
      const delta = e.deltaY > 0 ? -0.06 : 0.06;
      _unifZoom = Math.max(0.25, Math.min(3.0, (_unifZoom || 0.5) + delta));
      applyTransform();
    }, { passive: false });

    panArea.addEventListener('mousedown', (e) => {
      if (e.target.closest('.unif-dj-btn, .unif-zoom-btn, .unif-branch-inv, [data-unif-device]')) return;
      _dragging = true;
      _startX = e.clientX; _startY = e.clientY;
      _startPanX = _unifPanX; _startPanY = _unifPanY;
      panArea.style.cursor = 'grabbing';
      e.preventDefault();
    });
    window.addEventListener('mousemove', (e) => {
      if (!_dragging) return;
      _unifPanX = _startPanX + (e.clientX - _startX);
      _unifPanY = _startPanY + (e.clientY - _startY);
      applyTransform();
    }, { signal: sig });
    window.addEventListener('mouseup', () => {
      if (!_dragging) return;
      _dragging = false;
      if (panArea) panArea.style.cursor = 'grab';
    }, { signal: sig });

    let _lastTouchDist = 0;
    panArea.addEventListener('touchstart', (e) => {
      if (e.touches.length === 1) {
        const t = e.touches[0];
        if (e.target.closest('.unif-dj-btn, .unif-zoom-btn, .unif-branch-inv, [data-unif-device]')) return;
        _dragging = true;
        _startX = t.clientX; _startY = t.clientY;
        _startPanX = _unifPanX; _startPanY = _unifPanY;
      } else if (e.touches.length === 2) {
        _dragging = false;
        _lastTouchDist = Math.hypot(
          e.touches[0].clientX - e.touches[1].clientX,
          e.touches[0].clientY - e.touches[1].clientY
        );
      }
    }, { passive: true });
    panArea.addEventListener('touchmove', (e) => {
      if (e.touches.length === 1 && _dragging) {
        _unifPanX = _startPanX + (e.touches[0].clientX - _startX);
        _unifPanY = _startPanY + (e.touches[0].clientY - _startY);
        applyTransform();
      } else if (e.touches.length === 2) {
        const dist = Math.hypot(
          e.touches[0].clientX - e.touches[1].clientX,
          e.touches[0].clientY - e.touches[1].clientY
        );
        if (_lastTouchDist > 0) {
          const scale = dist / _lastTouchDist;
          _unifZoom = Math.max(0.25, Math.min(3.0, (_unifZoom || 0.5) * scale));
          applyTransform();
        }
        _lastTouchDist = dist;
      }
    }, { passive: true });
    panArea.addEventListener('touchend', () => { _dragging = false; _lastTouchDist = 0; });
  }

  // ── Click: inversores nas branches ──
  el.querySelectorAll('.unif-branch-inv[data-inv-id]').forEach(s => {
    s.addEventListener('click', () => {
      if (s.dataset.invId && typeof openCabineDetailForInverter === 'function')
        openCabineDetailForInverter(s.dataset.invId);
    });
  });

  // ── Click: nós de equipamento na espinha (trafo, medidor, cubículo) ──
  el.querySelectorAll("[data-unif-device]").forEach(node => {
    node.addEventListener("click", () => openUnifDeviceModalById(node.dataset.unifDevice, null));
  });

  // ── Click: disjuntor individual → command console do inversor ──
  el.querySelectorAll(".unif-dj-btn").forEach(btn => {
    btn.addEventListener("click", (e) => {
      e.stopPropagation();
      const id = btn.dataset.djInvId;
      if (id) openCommandConsole({ deviceType: "inverter", deviceId: id });
    });
  });

  // ── Click: DJMT → command console do disjuntor geral (breaker) ──
  el.querySelectorAll("[data-dj-breaker-id]").forEach(btn => {
    btn.addEventListener("click", (e) => {
      e.stopPropagation();
      const id = btn.dataset.djBreakerId;
      if (id && _canSendCommand()) openCommandConsole({ deviceType: "breaker", deviceId: id });
    });
  });
}

function refreshCabineMapCards(invertersRaw) {
  if (!isUnifilarVisible()) return;
  UNIF_GROUPS = buildUnifGroups(invertersRaw);
  if (UNIF_MODE === "overview") {
    buildUnifilarOverview();
  } else {
    UNIF_CABIN_IDX = Math.min(UNIF_CABIN_IDX, Math.max(0, UNIF_GROUPS.length - 1));
    renderUnifCabinDetail(UNIF_CABIN_IDX, null);
  }
  renderUnifilarSidePanel();
  renderUnifilarStatsBar();
}

function updateCabineRelayNode(relayData) {
  if (!isUnifilarVisible() || UNIF_MODE !== "overview") return;
  const item  = Array.isArray(relayData) ? relayData[0] : relayData;
  const relay = document.getElementById("unifNodeRelayArm");
  const val   = document.getElementById("unifRelayArmPower");
  const badge = document.getElementById("unifRelayBadge");
  if (!relay) return;
  const online = item ? relayOnlineFromPayload(item) : false;
  const pKw    = item ? pickDeviceMetricValue(item, item?.analog ?? {}, ["active_power_kw","power_kw","active_power","power"]) : null;
  relay.classList.toggle("is-online",  online);
  relay.classList.toggle("is-offline", !online && item != null);
  if (val)   val.textContent   = pKw != null ? `${cabinMapFormat(pKw, 1)} kW` : "—";
  if (badge) badge.textContent = online ? "Online" : "Offline";
}

function updateCabineMeterNode(multimeterData) {
  if (!isUnifilarVisible() || UNIF_MODE !== "overview") return;
  const item = Array.isArray(multimeterData) ? multimeterData[0] : multimeterData;
  const meter = document.getElementById("unifNodeMeter");
  const val   = document.getElementById("unifMeterPower");
  if (!meter) return;
  const online = item ? multimeterOnlineFromPayload(item) : false;
  const pKw    = item ? pickDeviceMetricValue(item, item?.analog ?? item?.data ?? {}, ["active_power_kw","p_kw","power_kw"]) : null;
  meter.classList.toggle("is-online",  online);
  meter.classList.toggle("is-offline", !online && item != null);
  if (val) val.textContent = pKw != null ? `${cabinMapFormat(pKw, 1)} kW` : "—";
}

/* ── Painel lateral de dispositivos ── */
function renderUnifilarSidePanel() {
  const listEl   = document.getElementById("unifilarSideList");
  const footerEl = document.getElementById("unifilarSideFooter");
  if (!listEl) return;

  const alarms  = Array.isArray(ACTIVE_ALARMS) ? ACTIVE_ALARMS : [];
  const devices = buildUnifDeviceList(UNIF_ACTIVE_CABIN_FILTER, UNIF_SEARCH_TEXT || "");

  listEl.innerHTML = devices.length > 0
    ? devices.map(d => {
        const devAlarms = alarms.filter(a =>
          String(a?.device_id ?? a?.deviceId ?? '') === String(d.id)
        );
        return buildUnifSideRow(d, devAlarms);
      }).join('')
    : `<div class="unif-side-empty">Sem dispositivos</div>`;

  listEl.querySelectorAll(".unif-side-row").forEach(row => {
    row.addEventListener("click", (e) => {
      if (e.target.closest(".unif-side-alarm-badge")) return;
      openUnifDeviceModalById(row.dataset.unifSideType, row.dataset.unifSideId);
    });
  });

  listEl.querySelectorAll(".unif-side-alarm-badge").forEach(badge => {
    badge.addEventListener("click", (e) => {
      e.stopPropagation();
      showUnifAlarmPopover(badge, badge.dataset.deviceId);
    });
  });

  if (footerEl) {
    const online  = devices.filter(d => d.status === "online").length;
    const alarm   = devices.filter(d => d.status === "alarm").length;
    const offline = devices.filter(d => d.status === "offline").length;
    footerEl.innerHTML = `
      <span class="unif-foot-item"><span class="unif-foot-dot unif-foot-dot--online"></span>Online ${online}</span>
      <span class="unif-foot-item"><span class="unif-foot-dot unif-foot-dot--alarm"></span>Alerta ${alarm}</span>
      <span class="unif-foot-item"><span class="unif-foot-dot unif-foot-dot--offline"></span>Offline ${offline}</span>`;
  }
}

function buildUnifDeviceList(cabinFilter, search) {
  const devices = [];
  const q = (search || "").toLowerCase();

  UNIF_GROUPS.forEach(group => {
    if (cabinFilter && String(group.id) !== String(cabinFilter)) return;
    group.inverters.forEach(inv => {
      const id     = String(getInverterRealId(inv) ?? "");
      const name   = getInverterDisplayName(inv, 0);
      const online = isOnlineByFreshness(inv) && !isZeroSnapshot(inv);
      const alarm  = !!(inv?.alarm || inv?.fault || inv?.warning || inv?.alarm_active);
      const rated  = asNumber(inv.rated_power_kw ?? 0, 0);
      if (q && !name.toLowerCase().includes(q)) return;
      devices.push({ id, type: "inverter", name, sub: "Inversor", power: rated > 0 ? `${rated.toFixed(0)} kW` : "—",
        status: alarm ? "alarm" : online ? "online" : "offline", data: inv });
    });
  });

  if (!cabinFilter) {
    const alarms = Array.isArray(ACTIVE_ALARMS) ? ACTIVE_ALARMS : [];
    const meterItem = Array.isArray(MULTIMETER_REALTIME) ? MULTIMETER_REALTIME[0] : MULTIMETER_REALTIME;
    if (meterItem) {
      const mName = meterItem.device_name || meterItem.name || "Medidor";
      const mPow  = pickDeviceMetricValue(meterItem, meterItem?.analog ?? meterItem?.data ?? {}, ["active_power_kw","p_kw","power_kw"]);
      const mId   = String(meterItem.device_id ?? meterItem.multimeter_id ?? "meter");
      const mOnline = multimeterOnlineFromPayload(meterItem);
      const mAlarm  = alarms.some(a => String(a?.device_id ?? a?.deviceId ?? '') === mId);
      if (!q || mName.toLowerCase().includes(q) || "medidor meter multímetro".includes(q))
        devices.push({ id: mId, type: "multimeter",
          name: mName, sub: "Medidor", power: mPow != null ? `${mPow.toFixed(1)} kW` : "—",
          status: mAlarm ? "alarm" : mOnline ? "online" : "offline", data: meterItem });
    }

    const relayItem = Array.isArray(RELAY_REALTIME) ? RELAY_REALTIME[0] : RELAY_REALTIME;
    if (relayItem) {
      const rName = relayItem.device_name || relayItem.name || "Relé";
      const rPow  = pickDeviceMetricValue(relayItem, relayItem?.analog ?? {}, ["active_power_kw","power_kw","active_power","power"]);
      const rId   = String(relayItem.device_id ?? relayItem.relay_id ?? "relay");
      const rOnline = relayOnlineFromPayload(relayItem);
      const rAlarm  = alarms.some(a => String(a?.device_id ?? a?.deviceId ?? '') === rId);
      if (!q || rName.toLowerCase().includes(q) || "relé relay proteção".includes(q))
        devices.push({ id: rId, type: "relay",
          name: rName, sub: "Relé", power: rPow != null ? `${rPow.toFixed(1)} kW` : "—",
          status: rAlarm ? "alarm" : rOnline ? "online" : "offline", data: relayItem });
    }
  }
  return devices;
}

function buildUnifSideRow(d, deviceAlarms = []) {
  const _sideStInfo = d.type === "inverter" ? getInverterStatusInfo(d) : null;
  const sc = d.status === "alarm" ? "alarm" : (_sideStInfo ? _sideStInfo.cls : (d.status === "online" ? "online" : "offline"));
  const sl = d.status === "alarm" ? "Alerta" : (_sideStInfo && _sideStInfo.label !== "—" ? _sideStInfo.label : (d.status === "online" ? "Online" : "Offline"));
  const iconMap = {
    inverter:    `<svg viewBox="0 0 20 20" fill="none"><rect x="1" y="3" width="18" height="14" rx="2" stroke="currentColor" stroke-width="1.2"/><path d="M10 5L8 10h3L7 15l9-7h-4l2-3z" fill="currentColor" opacity=".7"/></svg>`,
    multimeter:  `<svg viewBox="0 0 20 20" fill="none"><rect x="2" y="2" width="16" height="16" rx="2" stroke="currentColor" stroke-width="1.2"/><line x1="6" y1="6" x2="6" y2="14" stroke="currentColor" stroke-width="1.1" opacity=".7"/><line x1="9" y1="6" x2="9" y2="14" stroke="currentColor" stroke-width="1.1" opacity=".7"/><line x1="12" y1="6" x2="12" y2="14" stroke="currentColor" stroke-width="1.1" opacity=".5"/></svg>`,
    transformer: `<svg viewBox="0 0 24 20" fill="none"><circle cx="7" cy="10" r="6" stroke="currentColor" stroke-width="1.3"/><circle cx="17" cy="10" r="6" stroke="currentColor" stroke-width="1.3" opacity=".6"/></svg>`,
    relay:       `<svg viewBox="0 0 20 20" fill="none"><rect x="1" y="4" width="18" height="12" rx="2" stroke="currentColor" stroke-width="1.2"/><line x1="6" y1="14" x2="14" y2="14" stroke="currentColor" stroke-width="1.3" stroke-linecap="round"/><line x1="6" y1="14" x2="9" y2="9" stroke="currentColor" stroke-width="1.3" stroke-linecap="round"/><line x1="6" y1="9" x2="14" y2="9" stroke="currentColor" stroke-width=".8" stroke-dasharray="2 1.5" opacity=".4" stroke-linecap="round"/></svg>`,
    sa:          `<svg viewBox="0 0 20 20" fill="none"><circle cx="10" cy="10" r="8" stroke="currentColor" stroke-width="1.2"/><path d="M10 3L8.5 8.5H11.5L8 17L14.5 10H11.5L13 3Z" fill="currentColor" opacity=".7"/></svg>`,
    rede:        `<svg viewBox="0 0 20 20" fill="none"><line x1="10" y1="1" x2="10" y2="19" stroke="currentColor" stroke-width="1.3" stroke-linecap="round"/><line x1="2" y1="5" x2="18" y2="5" stroke="currentColor" stroke-width="1.2" stroke-linecap="round"/><line x1="4" y1="9" x2="16" y2="9" stroke="currentColor" stroke-width="1" stroke-linecap="round"/><line x1="2" y1="5" x2="6" y2="19" stroke="currentColor" stroke-width="1" stroke-linecap="round" opacity=".6"/><line x1="18" y1="5" x2="14" y2="19" stroke="currentColor" stroke-width="1" stroke-linecap="round" opacity=".6"/><circle cx="2" cy="5" r="1.4" fill="currentColor"/><circle cx="18" cy="5" r="1.4" fill="currentColor"/></svg>`
  };
  const alarmBadge = deviceAlarms.length > 0
    ? `<button class="unif-side-alarm-badge" data-device-id="${d.id}"
              title="${deviceAlarms.length} alarme(s) ativo(s)">
         <svg width="14" height="14" viewBox="0 0 14 14" fill="none">
           <path d="M7 1L1 13h12L7 1z" stroke="#f59e0b" stroke-width="1.3"
                 fill="rgba(245,158,11,.15)" stroke-linejoin="round"/>
           <path d="M7 5.5v3" stroke="#f59e0b" stroke-width="1.3" stroke-linecap="round"/>
           <circle cx="7" cy="10.5" r=".7" fill="#f59e0b"/>
         </svg>
         ${deviceAlarms.length > 1 ? `<span class="unif-side-alarm-count">${deviceAlarms.length}</span>` : ''}
       </button>`
    : '';
  return `<div class="unif-side-row" data-unif-side-type="${d.type}" data-unif-side-id="${d.id}">
    <div class="unif-side-row-icon" style="color:rgba(127,208,85,.65)">${iconMap[d.type] || iconMap.inverter}</div>
    <div class="unif-side-row-info">
      <span class="unif-side-row-name">${cabinMapEscape(d.name)}</span>
      <span class="unif-side-row-sub">${d.sub}</span>
    </div>
    <div class="unif-side-row-right">
      <span class="unif-side-row-power">${d.power}</span>
      <span class="unif-side-status unif-side-status--${sc}">${sl}</span>
    </div>
    <button class="unif-side-row-action" type="button" title="Ver detalhes">⋮</button>
    ${alarmBadge}
  </div>`;
}

/* ── Linha de alarme no painel lateral ── */
function buildUnifAlarmRow(alarm) {
  const deviceType = String(alarm.device_type ?? alarm.type ?? 'inverter');
  const deviceId   = String(alarm.device_id ?? alarm.deviceId ?? '');
  const deviceName = String(alarm.device_name ?? alarm.deviceName ?? deviceId);
  const msg        = String(alarm.event_name ?? alarm.alarm_message ?? alarm.message ?? alarm.event_code ?? '—');
  const rawTs      = alarm.started_at ?? alarm.timestamp ?? alarm.created_at ?? null;
  let timeStr = '';
  if (rawTs) {
    try {
      const d = new Date(rawTs);
      if (!isNaN(d.getTime())) {
        const pad = n => String(n).padStart(2, '0');
        timeStr = `${pad(d.getDate())}/${pad(d.getMonth()+1)} ${pad(d.getHours())}:${pad(d.getMinutes())}`;
      }
    } catch(_) {}
  }
  return `<div class="unif-alarm-row"
    data-unif-alarm-device-type="${deviceType}"
    data-unif-alarm-device-id="${deviceId}">
    <div class="unif-alarm-icon">
      <svg width="16" height="16" viewBox="0 0 16 16" fill="none">
        <path d="M8 1.5L1 14h14L8 1.5z" stroke="#f59e0b" stroke-width="1.4" stroke-linejoin="round" fill="rgba(245,158,11,.12)"/>
        <path d="M8 6v4" stroke="#f59e0b" stroke-width="1.5" stroke-linecap="round"/>
        <circle cx="8" cy="12" r=".85" fill="#f59e0b"/>
      </svg>
    </div>
    <div class="unif-alarm-info">
      <span class="unif-alarm-device">${cabinMapEscape(deviceName)}</span>
      <span class="unif-alarm-msg">${cabinMapEscape(msg)}</span>
    </div>
    ${timeStr ? `<span class="unif-alarm-time">${timeStr}</span>` : ''}
  </div>`;
}

/* ── Popover flutuante de alarmes ── */
function showUnifAlarmPopover(anchorEl, deviceId) {
  document.getElementById("unifAlarmPopover")?.remove();
  const alarms = (Array.isArray(ACTIVE_ALARMS) ? ACTIVE_ALARMS : [])
    .filter(a => String(a?.device_id ?? a?.deviceId ?? '') === String(deviceId));
  if (!alarms.length) return;

  const pop = document.createElement("div");
  pop.id = "unifAlarmPopover";
  pop.className = "unif-alarm-popover";
  pop.innerHTML = `
    <div class="unif-alarm-pop-header">Alarmes ativos (${alarms.length})</div>
    ${alarms.map(a => buildUnifAlarmRow(a)).join('')}`;
  document.body.appendChild(pop);

  const rect = anchorEl.getBoundingClientRect();
  const popW = 260;
  let left = rect.left - popW - 8;
  if (left < 8) left = rect.right + 8;
  pop.style.cssText = `top:${rect.top + window.scrollY}px;left:${left}px;width:${popW}px`;

  const close = (e) => {
    if (!pop.contains(e.target) && e.target !== anchorEl) {
      pop.remove();
      document.removeEventListener("click", close, true);
    }
  };
  setTimeout(() => document.addEventListener("click", close, true), 0);
}

/* ── Barra de estatísticas ── */
function renderUnifilarStatsBar() {
  const el = document.getElementById("unifilarStatsBar");
  if (!el) return;
  const total = UNIF_GROUPS.reduce((s, g) => s + g.inverters.length, 0);
  const dcKwp = UNIF_GROUPS.reduce((s, g) => s + g.inverters.reduce((gs, inv) => gs + asNumber(inv.rated_power_kw ?? 0, 0), 0), 0);
  const acKw  = asNumber(PLANT_STATE.active_power_kw, 0);
  const rated = asNumber(PLANT_STATE.rated_power_kwp, 0);
  el.innerHTML = `
    <div class="unif-stat">
      <svg width="18" height="18" viewBox="0 0 18 18" fill="none"><rect x="1" y="3" width="16" height="12" rx="2" stroke="rgba(127,208,85,.6)" stroke-width="1.2"/></svg>
      <span class="unif-stat-lbl">TOTAL INVERSORES</span><strong class="unif-stat-val">${total}</strong>
    </div>
    <div class="unif-stat">
      <svg width="18" height="18" viewBox="0 0 18 18" fill="none"><rect x="2" y="4" width="14" height="8" rx="1.5" stroke="rgba(127,208,85,.6)" stroke-width="1.2"/><line x1="5" y1="4" x2="4" y2="1" stroke="rgba(127,208,85,.5)" stroke-width="1.1"/><line x1="9" y1="4" x2="9" y2="1" stroke="rgba(127,208,85,.5)" stroke-width="1.1"/><line x1="13" y1="4" x2="14" y2="1" stroke="rgba(127,208,85,.5)" stroke-width="1.1"/></svg>
      <span class="unif-stat-lbl">POTÊNCIA DC TOTAL</span><strong class="unif-stat-val">${dcKwp.toFixed(0)} kWp</strong>
    </div>
    <div class="unif-stat">
      <svg width="18" height="18" viewBox="0 0 18 18" fill="none"><path d="M9 2L7 9h4L5 16l11-7h-4l2-7z" fill="rgba(127,208,85,.7)"/></svg>
      <span class="unif-stat-lbl">POTÊNCIA AC TOTAL</span><strong class="unif-stat-val">${acKw.toFixed(1)} kW</strong>
    </div>
    <div class="unif-stat">
      <svg width="20" height="18" viewBox="0 0 20 18" fill="none"><circle cx="7" cy="9" r="6" stroke="rgba(127,208,85,.6)" stroke-width="1.2"/><circle cx="13" cy="9" r="6" stroke="#f5c842" stroke-width="1.2" opacity=".75"/></svg>
      <span class="unif-stat-lbl">TRANSFORMADOR</span><strong class="unif-stat-val">${rated > 0 ? rated.toFixed(0) + " kVA" : "—"}</strong>
    </div>
    `;
}

/* ── Colapsar painel lateral ── */
function toggleUnifSidePanel() {
  UNIF_SIDE_COLLAPSED = !UNIF_SIDE_COLLAPSED;
  const side = document.getElementById("unifilarSide");
  const svg  = document.getElementById("unifCollapseSVG");
  if (side) side.classList.toggle("is-collapsed", UNIF_SIDE_COLLAPSED);
  if (svg) {
    svg.innerHTML = UNIF_SIDE_COLLAPSED
      ? `<path d="M4 1L7 6L4 11" stroke="currentColor" stroke-width="1.6" stroke-linecap="round" stroke-linejoin="round"/>
         <line x1="11" y1="6" x2="1" y2="6" stroke="currentColor" stroke-width="1.2" stroke-linecap="round" opacity=".4"/>`
      : `<path d="M8 1L11 6L8 11" stroke="currentColor" stroke-width="1.6" stroke-linecap="round" stroke-linejoin="round"/>
         <line x1="1" y1="6" x2="11" y2="6" stroke="currentColor" stroke-width="1.2" stroke-linecap="round" opacity=".4"/>`;
  }
}

/* ── Modal de dispositivo ── */
function openUnifDeviceModalById(type, id) {
  if (type === "inverter") {
    const inv = INVERTER_EXTRAS_BY_ID.get(String(id)) ||
      dedupInvertersById(INVERTERS_REALTIME).find(x => String(getInverterRealId(x)) === String(id));
    if (inv) { openUnifDeviceModal(inv, "inverter"); return; }
  }
  if (type === "relay")      { openUnifDeviceModal(Array.isArray(RELAY_REALTIME)      ? RELAY_REALTIME[0]      : RELAY_REALTIME,      "relay");      return; }
  if (type === "multimeter") { openUnifDeviceModal(Array.isArray(MULTIMETER_REALTIME) ? MULTIMETER_REALTIME[0] : MULTIMETER_REALTIME, "multimeter"); return; }
  openUnifDeviceModal(null, type);
}

function openUnifDeviceModal(data, type) {
  closeUnifDeviceModal(true);
  const seq = ++UNIF_MODAL_SEQ;

  const modal = document.createElement("div");
  modal.id = "unifDeviceModal";
  modal.className = "unif-modal-backdrop";

  const typeLabels = { inverter:"Inversor", relay:"Relé / Proteção", multimeter:"Multimedidor",
    transformer:"Transformador", sa:"Serviços Auxiliares", rede:"Rede", cc:"Caixa Combinadora",
    qgbt:"QGBT MT", weather:"Estação Meteorológica" };
  const typeLabel = typeLabels[type] || type;

  let title = "Dispositivo";
  if (type === "inverter" && data) title = cabinMapEscape(getInverterDisplayName(data, 0));
  else if (type === "relay")       title = "PMT";
  else if (type === "multimeter")  title = "CC 01 / Multimedidor";
  else if (type === "transformer") title = "T01 — Transformador";
  else if (type === "sa")          title = "SA 01";
  else if (type === "rede")        title = "REDE";

  const bodyHTML = buildUnifModalBody(data, type);

  modal.innerHTML = `<div class="unif-modal-card">
    <div class="unif-modal-hdr">
      <div class="unif-modal-title-group">
        <span class="unif-modal-type-badge">${typeLabel}</span>
        <h3 class="unif-modal-title">${title}</h3>
      </div>
      <button class="unif-modal-close" type="button" aria-label="Fechar">×</button>
    </div>
    <div class="unif-modal-body" id="unifModalBody">${bodyHTML}</div>
  </div>`;

  modal.querySelector(".unif-modal-close").addEventListener("click", closeUnifDeviceModal);
  modal.addEventListener("pointerdown", e => { if (!e.target.closest(".unif-modal-card")) closeUnifDeviceModal(); });
  const escFn = e => { if (e.key === "Escape") closeUnifDeviceModal(); };
  document.addEventListener("keydown", escFn);
  modal._escFn = escFn;

  document.body.appendChild(modal);
  requestAnimationFrame(() => modal.classList.add("unif-modal-visible"));

  /* Carrega strings do inversor de forma assíncrona */
  if (type === "inverter" && data) {
    const realId = getInverterRealId(data);
    if (realId != null) {
      loadCabineStringsPayload(realId).then(payload => {
        if (seq !== UNIF_MODAL_SEQ) return;
        const grid  = document.getElementById("unifModalStrGrid");
        const count = document.getElementById("unifModalStrCount");
        if (!grid) return;
        const rendered = renderCabineStringsBalloonRows(payload, realId);
        grid.innerHTML = rendered.html;
        if (count) count.textContent = `${rendered.count} string${rendered.count !== 1 ? "s" : ""}`;
      });
    }
  }

  /* Wires command buttons inside modal */
  setTimeout(() => wireDeviceCommandButtons(modal), 50);
}

function closeUnifDeviceModal(immediate = false) {
  UNIF_MODAL_SEQ++;
  const modal = document.getElementById("unifDeviceModal");
  if (!modal) return;
  if (modal._escFn) document.removeEventListener("keydown", modal._escFn);
  if (immediate) { modal.remove(); return; }
  modal.classList.remove("unif-modal-visible");
  setTimeout(() => modal.parentNode?.removeChild(modal), 230);
}

function buildUnifModalBody(data, type) {
  if (type === "inverter"    && data) return buildUnifInverterBody(data);
  if (type === "relay"       && data) return buildUnifRelayBody(data);
  if (type === "multimeter"  && data) return buildUnifMeterBody(data);
  if (type === "transformer")         return buildUnifTrafoBody();
  return `<div class="unif-modal-empty">Sem dados disponíveis para este dispositivo.</div>`;
}

function buildUnifInverterBody(inv) {
  const realId = getInverterRealId(inv);
  const online  = isOnlineByFreshness(inv) && !isZeroSnapshot(inv);
  const alarm   = !!(inv?.alarm || inv?.fault || inv?.warning || inv?.alarm_active);
  const pKw     = cabinMapReadInvMetric(inv, ["active_power_kw","power_kw","power","active_power"]);
  const eff     = cabinMapReadInvMetric(inv, ["efficiency_pct","efficiency","eff_pct"]);
  const temp    = cabinMapReadInvMetric(inv, ["temperature_internal_c","temperature_c","temp_c","temperature"]);
  const freq    = cabinMapReadInvMetric(inv, ["frequency_hz","freq_hz","frequency"]);
  const prRaw   = cabinMapReadInvMetric(inv, ["performance_ratio","pr","pr_ratio"]);
  const rated   = cabinMapReadInvMetric(inv, ["rated_power_kw","capacity_kw","rated_kw"]);
  const lastTs  = cabinMapReadInvMetric(inv, ["last_reading_at","last_reading_ts","last_ts","timestamp","event_ts"]);
  const prPct   = prRaw != null ? normalizePercentMaybe(prRaw) : null;
  const _unifStInfo = getInverterStatusInfo(inv);
  const sc      = alarm ? "alarm" : _unifStInfo.cls;
  const sl      = alarm ? "Alerta" : _unifStInfo.label !== "—" ? _unifStInfo.label : (online ? "Online" : "Offline");

  const f0 = (v, u) => { const n = Number(typeof v === "string" ? v.replace(",", ".") : v); return Number.isFinite(n) ? `${n.toFixed(0)} ${u}` : "—"; };
  const f2 = (v, u) => { const n = Number(typeof v === "string" ? v.replace(",", ".") : v); return Number.isFinite(n) ? `${n.toFixed(2)} ${u}` : "—"; };

  const vab = inv?.line_voltage_ab_v ?? inv?.line_voltage_ab;
  const vbc = inv?.line_voltage_bc_v ?? inv?.line_voltage_bc;
  const vca = inv?.line_voltage_ca_v ?? inv?.line_voltage_ca;
  const ia  = inv?.current_phase_a_a ?? inv?.current_phase_a;
  const ib  = inv?.current_phase_b_a ?? inv?.current_phase_b;
  const ic  = inv?.current_phase_c_a ?? inv?.current_phase_c;
  const fp  = inv?.power_factor;
  const kvar = inv?.power_reactive_kvar;
  const kva  = inv?.apparent_power_kva;
  const dcKw = inv?.power_dc_kw;
  const strV = inv?.string_voltage_v;
  const iso  = inv?.resistance_insulation_mohm;

  return `<div class="unif-modal-status-bar">
    <span class="unif-modal-state unif-modal-state--${sc}">${sl}</span>
    <span class="unif-modal-last">Última leitura: ${fmtDatePtBR(lastTs)}</span>
  </div>
  <div class="unif-modal-kpis">
    <div class="unif-modal-kpi"><span class="unif-modal-kpi-lbl">Potência</span><strong class="unif-modal-kpi-val">${pKw != null ? cabinMapFormat(pKw, 1, "kW") : "—"}</strong></div>
    <div class="unif-modal-kpi"><span class="unif-modal-kpi-lbl">Eficiência</span><strong class="unif-modal-kpi-val">${eff != null ? cabinMapFormat(eff, 1, "%") : "—"}</strong></div>
    <div class="unif-modal-kpi"><span class="unif-modal-kpi-lbl">Temperatura</span><strong class="unif-modal-kpi-val ${temp != null && asNumber(temp, 0) > 70 ? "val-warn" : ""}">${temp != null ? `${cabinMapFormat(temp, 1)}°C` : "—"}</strong></div>
    <div class="unif-modal-kpi"><span class="unif-modal-kpi-lbl">Frequência</span><strong class="unif-modal-kpi-val">${freq != null ? cabinMapFormat(freq, 2, "Hz") : "—"}</strong></div>
    <div class="unif-modal-kpi"><span class="unif-modal-kpi-lbl">PR</span><strong class="unif-modal-kpi-val">${prPct != null ? cabinMapFormat(prPct, 1, "%") : "—"}</strong></div>
    <div class="unif-modal-kpi"><span class="unif-modal-kpi-lbl">Cap. Nominal</span><strong class="unif-modal-kpi-val">${rated != null ? cabinMapFormat(rated, 0, "kW") : "—"}</strong></div>
    <div class="unif-modal-kpi"><span class="unif-modal-kpi-lbl">Inv Status</span><strong class="unif-modal-kpi-val ${_unifStInfo.cls}">${_unifStInfo.label}</strong></div>
    <div class="unif-modal-kpi"><span class="unif-modal-kpi-lbl">Work Status</span><strong class="unif-modal-kpi-val">${getWorkingStatus(inv)}</strong></div>
  </div>
  <div class="unif-modal-section">
    <div class="unif-modal-section-hdr"><span>Strings</span><span class="unif-modal-count" id="unifModalStrCount">Carregando...</span></div>
    <div class="unif-modal-str-grid" id="unifModalStrGrid"><div class="unif-modal-loading">Carregando strings...</div></div>
  </div>
  <div class="unif-modal-section">
    <div class="unif-modal-section-hdr"><span>AC</span></div>
    <div class="unif-modal-chips">
      <div class="unif-chip"><span class="unif-chip-lbl">V AB</span><span class="unif-chip-val">${f0(vab,"V")}</span></div>
      <div class="unif-chip"><span class="unif-chip-lbl">V BC</span><span class="unif-chip-val">${f0(vbc,"V")}</span></div>
      <div class="unif-chip"><span class="unif-chip-lbl">V CA</span><span class="unif-chip-val">${f0(vca,"V")}</span></div>
      <div class="unif-chip"><span class="unif-chip-lbl">Ia</span><span class="unif-chip-val">${f2(ia,"A")}</span></div>
      <div class="unif-chip"><span class="unif-chip-lbl">Ib</span><span class="unif-chip-val">${f2(ib,"A")}</span></div>
      <div class="unif-chip"><span class="unif-chip-lbl">Ic</span><span class="unif-chip-val">${f2(ic,"A")}</span></div>
      <div class="unif-chip"><span class="unif-chip-lbl">FP</span><span class="unif-chip-val">${(()=>{const n=Number(typeof fp==="string"?fp.replace(",","."):fp);return Number.isFinite(n)?n.toFixed(3):"—"})()}</span></div>
      <div class="unif-chip"><span class="unif-chip-lbl">Q reativa</span><span class="unif-chip-val">${f2(kvar,"kvar")}</span></div>
      <div class="unif-chip"><span class="unif-chip-lbl">S aparente</span><span class="unif-chip-val">${f2(kva,"kVA")}</span></div>
    </div>
  </div>
  <div class="unif-modal-section">
    <div class="unif-modal-section-hdr"><span>DC</span></div>
    <div class="unif-modal-chips">
      <div class="unif-chip"><span class="unif-chip-lbl">P DC</span><span class="unif-chip-val">${f2(dcKw,"kW")}</span></div>
      <div class="unif-chip"><span class="unif-chip-lbl">V string</span><span class="unif-chip-val">${f0(strV,"V")}</span></div>
      <div class="unif-chip"><span class="unif-chip-lbl">R isol.</span><span class="unif-chip-val">${(()=>{const n=Number(typeof iso==="string"?iso.replace(",","."):iso);return Number.isFinite(n)?`${n.toFixed(2)} MΩ`:"—"})()}</span></div>
    </div>
  </div>
  ${_canSendCommand() && realId != null ? `
  <div class="unif-modal-section">
    <div class="unif-modal-section-hdr"><span>Comandos</span></div>
    <div class="unif-modal-cmds">${renderDeviceCommandControl("inverter", realId, online ? "on" : "off")}</div>
  </div>` : ""}`;
}

function buildUnifRelayBody(item) {
  if (!item) return `<div class="unif-modal-empty">Sem dados do relé</div>`;
  const analog  = item?.analog ?? {};
  const ev      = item?.event  ?? {};
  const online  = relayOnlineFromPayload(item);
  const pKw     = pickDeviceMetricValue(item, analog, ["active_power_kw","power_kw","active_power","power"]);
  const pApKva  = pickDeviceMetricValue(item, analog, ["apparent_power_kva","power_apparent_kva","apparent_power"]);
  const pRkvar  = pickDeviceMetricValue(item, analog, ["reactive_power_kvar","power_reactive_kvar","reactive_power"]);
  const vAB     = analog.voltage_ab_v;
  const vBC     = analog.voltage_bc_v;
  const vCA     = analog.voltage_ca_v;
  const iA      = analog.current_a_a;
  const iB      = analog.current_b_a;
  const iC      = analog.current_c_a;
  const relayOn = item?.relay_on;
  const isValid = item?.is_valid;
  const commFlt = item?.communication_fault;
  const evCode  = ev.event_code;
  const evName  = ev.event_name;
  const evSev   = ev.severity;
  const lastTs  = item?.last_update ?? item?.last_reading_at ?? item?.timestamp ?? analog?.timestamp ?? ev?.timestamp;
  const sc      = online ? "online" : "offline";
  const relayId = item?.device_id ?? PLANT_CAPABILITIES.relayDeviceId ?? null;

  const fmt = (v, dec = 1) => v != null ? Number(v).toFixed(dec) : "—";
  const fmtFlag = (v, trueLabel = "SIM", falseLabel = "NÃO") => {
    if (v === true)  return `<span class="unif-relay-flag unif-relay-flag--true">${trueLabel}</span>`;
    if (v === false) return `<span class="unif-relay-flag unif-relay-flag--false">${falseLabel}</span>`;
    return `<span class="unif-relay-flag unif-relay-flag--neutral">—</span>`;
  };

  const hasVoltages = vAB != null || vBC != null || vCA != null;
  const hasCurrents = iA  != null || iB  != null || iC  != null;

  return `
  <div class="unif-modal-status-bar">
    <span class="unif-modal-state unif-modal-state--${sc}">${online ? "Online" : "Offline"}</span>
    <span class="unif-modal-last">Última leitura: ${fmtDatePtBR(lastTs)}</span>
  </div>
  <div class="unif-modal-section-hdr"><span>Potências</span></div>
  <div class="unif-modal-kpis">
    <div class="unif-modal-kpi"><span class="unif-modal-kpi-lbl">Ativa</span><strong class="unif-modal-kpi-val">${pKw != null ? fmt(pKw) + " kW" : "—"}</strong></div>
    <div class="unif-modal-kpi"><span class="unif-modal-kpi-lbl">Aparente</span><strong class="unif-modal-kpi-val">${pApKva != null ? fmt(pApKva) + " kVA" : "—"}</strong></div>
    <div class="unif-modal-kpi"><span class="unif-modal-kpi-lbl">Reativa</span><strong class="unif-modal-kpi-val">${pRkvar != null ? fmt(pRkvar, 2) + " kvar" : "—"}</strong></div>
  </div>
  ${hasVoltages ? `
  <div class="unif-modal-section-hdr" style="margin-top:8px"><span>Tensões</span></div>
  <div class="unif-modal-kpis">
    <div class="unif-modal-kpi"><span class="unif-modal-kpi-lbl">V AB</span><strong class="unif-modal-kpi-val">${fmt(vAB, 0)} V</strong></div>
    <div class="unif-modal-kpi"><span class="unif-modal-kpi-lbl">V BC</span><strong class="unif-modal-kpi-val">${fmt(vBC, 0)} V</strong></div>
    <div class="unif-modal-kpi"><span class="unif-modal-kpi-lbl">V CA</span><strong class="unif-modal-kpi-val">${fmt(vCA, 0)} V</strong></div>
  </div>` : ''}
  ${hasCurrents ? `
  <div class="unif-modal-section-hdr" style="margin-top:8px"><span>Correntes</span></div>
  <div class="unif-modal-kpis">
    <div class="unif-modal-kpi"><span class="unif-modal-kpi-lbl">I A</span><strong class="unif-modal-kpi-val">${fmt(iA)} A</strong></div>
    <div class="unif-modal-kpi"><span class="unif-modal-kpi-lbl">I B</span><strong class="unif-modal-kpi-val">${fmt(iB)} A</strong></div>
    <div class="unif-modal-kpi"><span class="unif-modal-kpi-lbl">I C</span><strong class="unif-modal-kpi-val">${fmt(iC)} A</strong></div>
  </div>` : ''}
  <div class="unif-modal-section-hdr" style="margin-top:8px"><span>Flags de Proteção</span></div>
  <div class="unif-relay-flags">
    <div class="unif-relay-flag-row"><span class="unif-relay-flag-lbl">Relé ON</span>${fmtFlag(relayOn, "ON", "OFF")}</div>
    <div class="unif-relay-flag-row"><span class="unif-relay-flag-lbl">Comun. Válida</span>${fmtFlag(isValid)}</div>
    ${commFlt != null ? `<div class="unif-relay-flag-row"><span class="unif-relay-flag-lbl">Cód. Comunicação</span><span class="unif-relay-flag unif-relay-flag--neutral">${commFlt}</span></div>` : ''}
    ${evCode  != null ? `<div class="unif-relay-flag-row"><span class="unif-relay-flag-lbl">Evento</span><span class="unif-relay-flag unif-relay-flag--neutral">${evCode}${evName ? ` — ${evName}` : ''}</span></div>` : ''}
    ${evSev   != null ? `<div class="unif-relay-flag-row"><span class="unif-relay-flag-lbl">Severidade</span><span class="unif-relay-flag unif-relay-flag--neutral">${evSev}</span></div>` : ''}
  </div>
  ${_canSendCommand() && relayId ? `
  <div class="unif-modal-section">
    <div class="unif-modal-section-hdr"><span>Comandos</span></div>
    <div class="unif-modal-cmds">${renderDeviceCommandControl("relay", relayId, online && relayOn ? "on" : "off")}</div>
  </div>` : ""}`;
}

function buildUnifMeterBody(item) {
  if (!item) return `<div class="unif-modal-empty">Sem dados do multimedidor</div>`;
  const analog   = item?.analog ?? item?.data ?? {};
  const online   = multimeterOnlineFromPayload(item);
  const act      = pickDeviceMetricValue(item, analog, ["active_power_kw","p_kw","power_kw","active_power"]);
  const app      = pickDeviceMetricValue(item, analog, ["apparent_power_kva","power_apparent_kva","apparent_power"]);
  const react    = pickDeviceMetricValue(item, analog, ["reactive_power_kvar","power_reactive_kvar","reactive_power"]);
  const pf       = pickDeviceMetricValue(item, analog, ["power_factor","power_factor_pct","pf"]);
  const lastTs   = item?.last_reading_at ?? item?.last_update ?? item?.timestamp ?? analog?.timestamp ?? data?.timestamp;
  const sc = online ? "online" : "offline";
  return `<div class="unif-modal-status-bar">
    <span class="unif-modal-state unif-modal-state--${sc}">${online ? "Online" : "Offline"}</span>
    <span class="unif-modal-last">Última leitura: ${fmtDatePtBR(lastTs)}</span>
  </div>
  <div class="unif-modal-kpis">
    <div class="unif-modal-kpi"><span class="unif-modal-kpi-lbl">Potência Ativa</span><strong class="unif-modal-kpi-val">${act != null ? cabinMapFormat(act, 1) + " kW" : "—"}</strong></div>
    <div class="unif-modal-kpi"><span class="unif-modal-kpi-lbl">Potência Aparente</span><strong class="unif-modal-kpi-val">${app != null ? cabinMapFormat(app, 1) + " kVA" : "—"}</strong></div>
    <div class="unif-modal-kpi"><span class="unif-modal-kpi-lbl">Potência Reativa</span><strong class="unif-modal-kpi-val">${react != null ? cabinMapFormat(react, 2) + " kvar" : "—"}</strong></div>
    <div class="unif-modal-kpi"><span class="unif-modal-kpi-lbl">Fator de Potência</span><strong class="unif-modal-kpi-val">${pf != null ? cabinMapFormat(pf, 3) : "—"}</strong></div>
  </div>`;
}

function buildUnifTrafoBody() {
  const rated = asNumber(PLANT_STATE.rated_power_kwp, 0);
  return `<div class="unif-modal-kpis">
    <div class="unif-modal-kpi"><span class="unif-modal-kpi-lbl">Capacidade</span><strong class="unif-modal-kpi-val">${rated > 0 ? rated.toFixed(0) + " kVA" : "—"}</strong></div>
    <div class="unif-modal-kpi"><span class="unif-modal-kpi-lbl">Status</span><strong class="unif-modal-kpi-val">Online</strong></div>
    <div class="unif-modal-kpi"><span class="unif-modal-kpi-lbl">Temperatura</span><strong class="unif-modal-kpi-val" id="unifModalTrafoTemp">—</strong></div>
  </div>`;
}

function countOnlineInverters(invertersRaw) {
  const inverters = dedupInvertersById(invertersRaw);
  let online = 0;
  inverters.forEach(inv => {
    if (isOnlineByFreshness(inv) && !isZeroSnapshot(inv)) online++;
  });
  return online;
}

// ======================================================
// RENDER — HEADER DA USINA
// ======================================================
function renderHeaderSummary() {
  const elRated = document.getElementById("headerRatedPower");
  const elActive = document.getElementById("headerActivePower");
  const elCapacity = document.getElementById("headerCapacity");
  const elCapacityAc = document.getElementById("headerCapacityAc");
  if (!elRated || !elActive || !elCapacity) return;

  elRated.textContent = `${asNumber(PLANT_STATE.rated_power_kwp).toFixed(1)} kWp`;
  elActive.textContent = `${asNumber(PLANT_STATE.active_power_kw).toFixed(1)} kW`;
  elCapacity.textContent = `${asNumber(PLANT_STATE.capacity_percent).toFixed(1)} %`;
  if (elCapacityAc) {
    const ac = PLANT_STATE.capacity_ac;
    elCapacityAc.textContent = ac != null && ac > 0 ? `${Number(ac).toFixed(1)} kWp` : "—";
  }
}

// ======================================================
// RENDER — WEATHER
// ======================================================
function renderWeather(data) {
  const hasWeather = !!(data && typeof data === "object");
  const d = hasWeather ? data : {};

  const elIrr = document.getElementById("weatherIrradiance");
  const elAir = document.getElementById("weatherAirTemp");
  const elModule = document.getElementById("weatherModuleTemp");

  if (elIrr) {
    const value = d.irradiance_poa_wm2 ?? d.POA_irradiance ?? null;
    elIrr.textContent = value != null ? `${Number(value).toFixed(0)} W/m²` : "—";
  }
  if (elAir) {
    const value = d.air_temperature_c ?? d.temp_ambiente ?? null;
    elAir.textContent = value != null ? `${Number(value).toFixed(1)} °C` : "—";
  }
  if (elModule) {
    const value = d.module_temperature_c ?? d.temp_modulo ?? null;
    elModule.textContent = value != null ? `${Number(value).toFixed(1)} °C` : "—";
  }

  // Painel expandido
  const wxWind = document.getElementById("wxWindSpeed");
  if (wxWind) {
    const v = d.wind_speed_ms ?? d.vel_vento ?? null;
    wxWind.textContent = v != null ? `${Number(v).toFixed(1)} m/s` : "—";
  }

  const wxDir = document.getElementById("wxWindDir");
  if (wxDir) {
    const v = d.wind_direction_deg ?? d.wind_direction ?? null;
    wxDir.textContent = v != null ? `${Number(v).toFixed(0)}°` : "—";
  }

  const wxGhi = document.getElementById("wxGhi");
  if (wxGhi) {
    const v = d.irradiance_ghi_wm2 ?? d.GHI_irradiance ?? null;
    wxGhi.textContent = v != null ? `${Number(v).toFixed(0)} W/m²` : "—";
  }

  const wxRain = document.getElementById("wxRain");
  if (wxRain) {
    const hour = d.rainfall_hour_mm ?? d.acumulador_pluv_hour ?? null;
    const month = d.rainfall_month_mm ?? d.acumulador_pluv_month ?? null;
    const hStr = hour != null ? `${Number(hour).toFixed(1)}` : "—";
    const mStr = month != null ? `${Number(month).toFixed(1)}` : "—";
    wxRain.textContent = `${hStr} / ${mStr} mm`;
  }

  const wxBatt = document.getElementById("wxBattery");
  if (wxBatt) {
    const v = d.battery_voltage_v ?? d.volt_battery ?? null;
    wxBatt.textContent = v != null ? `${Number(v).toFixed(2)} V` : "—";
  }

  const wxSensor = document.getElementById("wxRainSensor");
  if (wxSensor) {
    const v = d.rain_sensor ?? d.sensor_chuva ?? null;
    wxSensor.textContent = v != null ? (Number(v) === 1 ? "Chuva" : "Seco") : "—";
  }
}

function setupWeatherExpand() {
  const btn = document.getElementById("weatherExpandBtn");
  const panel = document.getElementById("weatherExpandPanel");
  if (!btn || !panel) return;
  btn.addEventListener("click", () => {
    const open = panel.classList.toggle("is-open");
    btn.classList.toggle("is-open", open);
  });
}

// ======================================================
// RENDER — ALARMES ATIVOS
// ======================================================
function renderAlarms(alarms) {
  const container = document.getElementById("plantActiveAlarms");
  if (!container) return;

  const sublineEl = document.getElementById("plantSubline");
  container.innerHTML = "";
  const filtered = sortPlantAlarmsDesc(
    dedupePlantAlarms(
      (Array.isArray(alarms) ? alarms : []).filter(
        (alarm) => normalizeAlarmState(alarm?.state ?? alarm?.alarm_state ?? alarm?.status) === "ACTIVE" && alarm?.acknowledged !== true
      )
    )
  );

  if (!filtered.length) {
    container.innerHTML = "";
    if (sublineEl) {
      sublineEl.textContent = "Nenhum alarme ativo";
      sublineEl.classList.remove("plant-subline--alarm");
    }
    renderAlarmMenuButton();
    return;
  }

  if (sublineEl) {
    sublineEl.textContent = `${filtered.length} alarme(s) ativo(s)`;
    sublineEl.classList.add("plant-subline--alarm");
  }

  filtered.forEach(a => {
    const row = document.createElement("div");
    row.className = `alarm-row ${normalizeAlarmSeverity(a.severity) || ""}`.trim();
    const deviceType =
      a.device_type ??
      a.device_type_name ??
      a.event_source ??
      "—";
    const when =
      a.started_at ??
      a.timestamp ??
      null;

    row.innerHTML = `
      <span class="alarm-device">${deviceType} • ${a.device_name || "—"}</span>
      <span class="alarm-desc">${a.event_name || (a.event_code != null ? `Evento ${a.event_code}` : "—")}</span>
      <span class="alarm-time">${when ? new Date(when).toLocaleString("pt-BR") : "—"}</span>
    `;
    row.title = "Duplo clique para reconhecer alarme";
    row.addEventListener("dblclick", async () => {
      row.style.opacity = "0.6";
      try {
        await acknowledgePlantAlarm(a);
        ACTIVE_ALARMS = ACTIVE_ALARMS.filter((alarm) => String(alarm?.event_row_id ?? alarm?.alarm_id ?? alarm?.id) !== String(a?.event_row_id ?? a?.alarm_id ?? a?.id));
        renderAlarms(ACTIVE_ALARMS);
        renderAlarmMenuButton();
        if (!ACTIVE_ALARMS.length) {
          setPlantAlarmMenuOpen(false);
        }
      } catch (err) {
        row.style.opacity = "";
        console.error("[alarms][ack] erro", err);
        alert("Não foi possível reconhecer o alarme. Tente novamente.");
      }
    });

    container.appendChild(row);
  });
  renderAlarmMenuButton();
}

// ======================================================
// ✅ RENDER — RELÉ (NOVO SHAPE DO ENDPOINT /relay/realtime)
// item: { is_online, relay_on, last_update, analog:{active_power_kw} }
// ======================================================
function ensureRelayUiScaffold() {
  const relayRow = document.getElementById("relayRow");
  if (!relayRow) return null;

  const nameEl = relayRow.querySelector(".relay-left");
  const dotEl = document.getElementById("relayDot") || relayRow.querySelector(".status-dot");
  const commandBarWrap = document.getElementById("relayCommandBarWrap");
  const legacyRight = relayRow.querySelector(".relay-right");
  const detailsPanel = document.getElementById("relayDetailsPanel");

  // Remove “extras antigos” visualmente (não remove do DOM, só não usa)
  const oldOnline = document.getElementById("relayOnlineText");
  const oldAvail = document.getElementById("relayAvailabilityText");
  const oldLast = document.getElementById("relayLastUpdateText");

  if (oldOnline) oldOnline.textContent = "—";
  if (oldAvail) oldAvail.textContent = "";
  if (oldLast) oldLast.textContent = "";
  if (legacyRight) legacyRight.style.display = "none";

  // cria badge ONLINE/OFFLINE ao lado do nome
  let badgeOnline = relayRow.querySelector("#relayOnlineBadge");
  if (!badgeOnline) {
    badgeOnline = document.createElement("span");
    badgeOnline.id = "relayOnlineBadge";
    badgeOnline.style.display = "inline-flex";
    badgeOnline.style.alignItems = "center";
    badgeOnline.style.justifyContent = "center";
    badgeOnline.style.padding = "6px 10px";
    badgeOnline.style.borderRadius = "999px";
    badgeOnline.style.fontSize = "11px";
    badgeOnline.style.letterSpacing = "0.06em";
    badgeOnline.style.textTransform = "uppercase";
    badgeOnline.style.border = "1px solid rgba(255,255,255,0.10)";
    badgeOnline.style.background = "rgba(255,255,255,0.04)";
    badgeOnline.style.color = "rgba(233,255,243,0.88)";
    badgeOnline.style.marginLeft = "10px";
    badgeOnline.style.whiteSpace = "nowrap";

    if (nameEl) nameEl.appendChild(badgeOnline);
  }

  const legacyStateEl = relayRow.querySelector("#relayStateBadge");
  if (legacyStateEl) legacyStateEl.remove();

  if (commandBarWrap && commandBarWrap.parentElement !== relayRow) {
    relayRow.appendChild(commandBarWrap);
  }

  relayRow.classList.add("relay-row--table");
  relayRow.style.gridTemplateColumns = "14px minmax(250px,1.45fr) minmax(150px,0.95fr) minmax(150px,0.95fr) minmax(150px,0.95fr) minmax(190px,1fr) 88px";

  let expandIcon = relayRow.querySelector("#relayExpandIcon");
  if (!expandIcon) {
    expandIcon = document.createElement("i");
    expandIcon.id = "relayExpandIcon";
    expandIcon.className = "fa-solid fa-chevron-down relay-expand-icon";
    if (nameEl) nameEl.appendChild(expandIcon);
  }

  const ensureMetricCell = (id, gridColumn) => {
    let el = relayRow.querySelector(`#${id}`);
    if (!el) {
      el = document.createElement("div");
      el.id = id;
      el.className = "device-metric-cell";
      relayRow.appendChild(el);
    }
    el.style.gridColumn = String(gridColumn);
    el.style.gridRow = "1";
    return el;
  };

  const activePowerEl = ensureMetricCell("relayActivePowerValue", 3);
  const apparentPowerEl = ensureMetricCell("relayApparentPowerValue", 4);
  const reactivePowerEl = ensureMetricCell("relayReactivePowerValue", 5);
  const tsEl = ensureMetricCell("relayTsText", 6);
  tsEl.classList.add("relay-timestamp-cell");
  activePowerEl.dataset.label = "ACTIVE POWER";
  apparentPowerEl.dataset.label = "APPARENT POWER";
  reactivePowerEl.dataset.label = "REACTIVE POWER";
  tsEl.dataset.label = "ÚLTIMA LEITURA";

  if (commandBarWrap) {
    commandBarWrap.style.gridColumn = "7";
    commandBarWrap.style.gridRow = "1";
    commandBarWrap.style.justifySelf = "center";
    commandBarWrap.dataset.label = _canSendCommand() ? "COMANDOS" : "";
    if (!_canSendCommand()) commandBarWrap.style.visibility = "hidden";
  }

  if (detailsPanel) {
    detailsPanel.style.maxHeight = detailsPanel.classList.contains("open") ? "1200px" : "0px";
  }

  if (!relayRow.dataset.toggleBound) {
    relayRow.dataset.toggleBound = "true";
    relayRow.addEventListener("click", (event) => {
      if (
        event.target.closest("#relayCommandBarWrap") ||
        event.target.closest(".device-command-control") ||
        event.target.closest(".device-command-popover")
      ) return;

      const nextOpen = !detailsPanel?.classList.contains("open");
      relayRow.classList.toggle("open", nextOpen);
      detailsPanel?.classList.toggle("open", nextOpen);
      if (detailsPanel) {
        detailsPanel.style.maxHeight = nextOpen ? "1200px" : "0px";
      }
    });
  }

  return {
    relayRow,
    nameEl,
    dotEl,
    badgeOnline,
    activePowerEl,
    apparentPowerEl,
    reactivePowerEl,
    tsEl,
    detailsPanel
  };
}

function ensureDeviceMiniHeaders() {
  const relayHeader = document.querySelector("#relaySection .device-mini-header");
  const multimeterHeader = document.querySelector("#multimeterSection .device-mini-header");

  const applyHeader = (headerEl) => {
    if (!headerEl) return;
    let spans = headerEl.querySelectorAll("span");

    if (spans.length < 7) {
      headerEl.innerHTML = `
        <span></span>
        <span></span>
        <span>ACTIVE POWER</span>
        <span>APPARENT POWER</span>
        <span>REACTIVE POWER</span>
        <span>ÚLTIMA LEITURA</span>
        <span>${_canSendCommand() ? "COMANDOS" : ""}</span>
      `;
      spans = headerEl.querySelectorAll("span");
    } else {
      spans[0].textContent = "";
      spans[1].textContent = "";
      spans[2].textContent = "ACTIVE POWER";
      spans[3].textContent = "APPARENT POWER";
      spans[4].textContent = "REACTIVE POWER";
      spans[5].textContent = "ÚLTIMA LEITURA";
      spans[6].textContent = _canSendCommand() ? "COMANDOS" : "";
    }
  };

  applyHeader(relayHeader);
  applyHeader(multimeterHeader);
}

function pickDeviceMetricValue(primary, secondary, keys) {
  for (const key of keys) {
    const secondaryValue = secondary?.[key];
    if (secondaryValue !== null && secondaryValue !== undefined && secondaryValue !== "") return secondaryValue;
    const primaryValue = primary?.[key];
    if (primaryValue !== null && primaryValue !== undefined && primaryValue !== "") return primaryValue;
  }
  return null;
}

function formatMetricValue(value, unit, digits = 1) {
  const n = Number(typeof value === "string" ? value.replace(",", ".") : value);
  if (!Number.isFinite(n)) return "—";
  return `${n.toFixed(digits)} ${unit}`;
}

function renderRelayDetailsPanel(relayItem) {
  const panel = document.getElementById("relayDetailsPanel");
  if (!panel) return;

  if (!relayItem) {
    panel.innerHTML = `<div class="relay-details-empty">Sem dados detalhados do relé.</div>`;
    return;
  }

  const analog = relayItem?.analog ?? {};
  const metric = (keys, unit, digits = 1) => formatMetricValue(pickDeviceMetricValue(relayItem, analog, keys), unit, digits);
  const raw = (keys) => {
    const value = pickDeviceMetricValue(relayItem, analog, keys);
    return value === null || value === undefined || value === "" ? "—" : String(value);
  };

  const electricalItems = [
    ["V AB", metric(["voltage_ab_v", "voltage_ab", "line_voltage_ab_v", "line_voltage_ab", "vab"], "V", 1)],
    ["V BC", metric(["voltage_bc_v", "voltage_bc", "line_voltage_bc_v", "line_voltage_bc", "vbc"], "V", 1)],
    ["V CA", metric(["voltage_ca_v", "voltage_ca", "line_voltage_ca_v", "line_voltage_ca", "vca"], "V", 1)],
    ["Ia", metric(["current_a_a", "current_a", "ia"], "A", 1)],
    ["Ib", metric(["current_b_a", "current_b", "ib"], "A", 1)],
    ["Ic", metric(["current_c_a", "current_c", "ic"], "A", 1)],
    ["Status Relay", raw(["status_relay"])]
  ];

  const flagItems = [
    ["46", raw(["flag_46"])], ["50", raw(["flag_50"])], ["51-1", raw(["flag_51_1"])],
    ["50N", raw(["flag_50N"])], ["51GS", raw(["flag_51GS"])], ["51N", raw(["flag_51N"])],
    ["27", raw(["flag_27"])], ["59", raw(["flag_59"])], ["47", raw(["flag_47"])],
    ["81 O", raw(["flag_81_O"])], ["81 U", raw(["flag_81_U"])], ["51-2", raw(["flag_51_2"])]
  ];

  panel.innerHTML = `
    <div class="relay-details-card">
      <div class="relay-details-title">Leituras elétricas</div>
      <div class="relay-details-grid">
        ${electricalItems.map(([label, value]) => `
          <div class="relay-detail-chip">
            <span>${label}</span>
            <strong>${value}</strong>
          </div>
        `).join("")}
      </div>
    </div>
    <div class="relay-details-card">
      <div class="relay-details-title">Proteções</div>
      <div class="relay-flag-grid">
        ${flagItems.map(([label, value]) => `
          <div class="relay-flag-pill ${String(value) === "1" ? "is-on" : "is-off"}">
            <span>${label}</span>
            <strong>${value}</strong>
          </div>
        `).join("")}
      </div>
    </div>
  `;
}

function ensureMultimeterUiScaffold() {
  const row = document.getElementById("multimeterRow");
  if (!row) return null;

  const onlineBadge = document.getElementById("multimeterOnlineBadge");
  const commandBarWrap = document.getElementById("multimeterCommandBarWrap");
  const legacyRight = document.getElementById("multimeterRight");
  const leftBlock =
    row?.querySelector(".relay-left") ||
    row?.querySelector(".multimeter-left") ||
    row?.children?.[1] ||
    null;

  if (legacyRight) legacyRight.style.display = "none";

  if (leftBlock && onlineBadge && onlineBadge.parentElement !== leftBlock) {
    leftBlock.appendChild(onlineBadge);
  }
  if (commandBarWrap && commandBarWrap.parentElement !== row) {
    row.appendChild(commandBarWrap);
  }

  row.classList.add("relay-row--table");
  row.style.gridTemplateColumns = "14px minmax(250px,1.45fr) minmax(150px,0.95fr) minmax(150px,0.95fr) minmax(150px,0.95fr) minmax(190px,1fr) 88px";

  const ensureMetricCell = (id, gridColumn) => {
    let el = row.querySelector(`#${id}`);
    if (!el) {
      el = document.createElement("div");
      el.id = id;
      el.className = "device-metric-cell";
      row.appendChild(el);
    }
    el.style.gridColumn = String(gridColumn);
    el.style.gridRow = "1";
    return el;
  };

  const activePowerEl = ensureMetricCell("multimeterActivePowerValue", 3);
  const apparentPowerEl = ensureMetricCell("multimeterApparentPowerValue", 4);
  const reactivePowerEl = ensureMetricCell("multimeterReactivePowerValue", 5);
  const tsEl = ensureMetricCell("multimeterLastReadingValue", 6);
  tsEl.classList.add("relay-timestamp-cell");
  activePowerEl.dataset.label = "ACTIVE POWER";
  apparentPowerEl.dataset.label = "APPARENT POWER";
  reactivePowerEl.dataset.label = "REACTIVE POWER";
  tsEl.dataset.label = "ÚLTIMA LEITURA";

  if (commandBarWrap) {
    commandBarWrap.style.gridColumn = "7";
    commandBarWrap.style.gridRow = "1";
    commandBarWrap.style.justifySelf = "center";
    commandBarWrap.dataset.label = _canSendCommand() ? "COMANDOS" : "";
    if (!_canSendCommand()) commandBarWrap.style.visibility = "hidden";
  }

  return {
    row,
    onlineBadge,
    activePowerEl,
    apparentPowerEl,
    reactivePowerEl,
    tsEl
  };
}

function renderRelayCommandBar(deviceId, currentState = "off") {
  const wrap = document.getElementById("relayCommandBarWrap");
  if (!wrap) return;
  if (deviceId == null || !/^\d+$/.test(String(deviceId))) {
    wrap.innerHTML = "";
    wrap.style.display = "none";
    return;
  }
  const safeId = String(deviceId);
  const normalizedState = currentState === "on" ? "on" : "off";
  setDevicePersistentState("relay", safeId, normalizedState);
  if (!_canSendCommand()) {
    wrap.innerHTML = "";
    wrap.style.display = "none";
    return;
  }
  wrap.innerHTML = renderDeviceCommandControl("relay", safeId, normalizedState);
  wrap.style.display = "flex";
  wrap.style.alignItems = "center";
  wrap.style.justifyContent = "center";
  wireDeviceCommandButtons(wrap);
  applyDeviceVisualState("relay", safeId, normalizedState);
}

function renderMultimeterCommandBar(deviceId) {
  const wrap = document.getElementById("multimeterCommandBarWrap");
  if (!wrap) return;
  if (deviceId == null || !/^\d+$/.test(String(deviceId))) {
    wrap.innerHTML = "";
    wrap.style.display = "none";
    return;
  }
  const safeId = String(deviceId);
  if (!_canSendCommand()) {
    wrap.innerHTML = "";
    wrap.style.display = "none";
    return;
  }
  wrap.innerHTML = renderDeviceCommandControl("multimeter", safeId, getDevicePersistentState("multimeter", safeId, "off"));
  wrap.style.display = "flex";
  wrap.style.alignItems = "center";
  wrap.style.justifyContent = "center";
  wireDeviceCommandButtons(wrap);
  applyDeviceVisualState("multimeter", safeId, getDevicePersistentState("multimeter", safeId, "off"));
}

function renderRelayCard(relayItem) {
  const ui = ensureRelayUiScaffold();
  if (!ui) return;

  const { relayRow, badgeOnline, activePowerEl, apparentPowerEl, reactivePowerEl, tsEl, detailsPanel } = ui;
  ensureDeviceMiniHeaders();

  // sem dados ainda
  if (!relayItem) {
    relayRow.classList.remove("online", "offline", "open");
    relayRow.classList.add("offline");
    badgeOnline.textContent = "OFFLINE";
    activePowerEl.textContent = "—";
    apparentPowerEl.textContent = "—";
    reactivePowerEl.textContent = "—";
    tsEl.textContent = "—";
    if (detailsPanel) {
      detailsPanel.classList.remove("open");
      detailsPanel.style.maxHeight = "0px";
    }
    renderRelayDetailsPanel(null);
    renderRelayCommandBar(null, "off");
    return;
  }

  const analog = relayItem?.analog ?? {};
  const isOnline = relayOnlineFromPayload(relayItem);
  const relayState = relayStateFromPayload(relayItem);
  const activePower = pickDeviceMetricValue(relayItem, analog, ["active_power_kw", "power_kw", "active_power"]);
  const apparentPower = pickDeviceMetricValue(relayItem, analog, ["apparent_power_kva", "power_apparent_kva", "apparent_power", "apparent_power_va"]);
  const reactivePower = pickDeviceMetricValue(relayItem, analog, ["reactive_power_kvar", "power_reactive_kvar", "reactive_power", "reactive_power_var"]);
  const lastUpdate =
    relayItem?.last_update ??
    relayItem?.timestamp ??
    relayItem?.analog?.timestamp ??
    relayItem?.event?.timestamp ??
    null;
  const deviceId = relayItem?.device_id ?? relayItem?.relay_id ?? null;

  // classes do row (para a bolinha)
  relayRow.classList.remove("online", "offline");
  relayRow.classList.add(isOnline ? "online" : "offline");

  // badge online/offline
  badgeOnline.textContent = isOnline ? "ONLINE" : "OFFLINE";
  badgeOnline.style.borderColor = isOnline ? "rgba(57,229,140,0.26)" : "rgba(255,92,92,0.25)";
  badgeOnline.style.background = isOnline ? "rgba(57,229,140,0.08)" : "rgba(255,92,92,0.08)";
  badgeOnline.style.color = isOnline ? "rgba(233,255,243,0.92)" : "rgba(255,255,255,0.92)";
  badgeOnline.style.marginLeft = "8px";
  badgeOnline.style.whiteSpace = "nowrap";

  activePowerEl.textContent = formatMetricValue(activePower, "kW", 1);
  apparentPowerEl.textContent = formatMetricValue(apparentPower, "kVA", 1);
  reactivePowerEl.textContent = formatMetricValue(reactivePower, "kvar", 1);
  tsEl.textContent = fmtDatePtBR(lastUpdate);

  renderRelayDetailsPanel(relayItem);
  if (detailsPanel?.classList.contains("open")) {
    detailsPanel.style.maxHeight = "1200px";
  }
  renderRelayCommandBar(deviceId, relayState);
}

function renderMultimeterCard(item) {
  const ui = ensureMultimeterUiScaffold();
  if (!ui) return;

  const { row, onlineBadge, activePowerEl, apparentPowerEl, reactivePowerEl, tsEl } = ui;
  const dot = document.getElementById("multimeterDot");
  ensureDeviceMiniHeaders();

  if (!item) {
    row.classList.remove("online", "offline");
    row.classList.add("offline");
    if (onlineBadge) onlineBadge.textContent = "OFFLINE";
    activePowerEl.textContent = "—";
    apparentPowerEl.textContent = "—";
    reactivePowerEl.textContent = "—";
    tsEl.textContent = "—";
    if (dot) dot.style.opacity = "0.65";
    renderMultimeterCommandBar(null);
    return;
  }

  const analog = item?.analog ?? item?.data ?? {};
  const isOnline = multimeterOnlineFromPayload(item);
  const activePower = pickDeviceMetricValue(item, analog, ["active_power_kw", "p_kw", "power_kw", "active_power"]);
  const apparentPower = pickDeviceMetricValue(item, analog, ["apparent_power_kva", "power_apparent_kva", "apparent_power", "apparent_power_va"]);
  const reactivePower = pickDeviceMetricValue(item, analog, ["reactive_power_kvar", "power_reactive_kvar", "reactive_power", "reactive_power_var"]);
  const lastUpdate = item.last_update ?? item.timestamp ?? null;

  renderMultimeterCommandBar(item?.device_id ?? item?.multimeter_id ?? null);

  row.classList.remove("online", "offline");
  row.classList.add(isOnline ? "online" : "offline");

  if (onlineBadge) {
    onlineBadge.textContent = isOnline ? "ONLINE" : "OFFLINE";
    onlineBadge.classList.remove("relay-state--on", "relay-state--off", "relay-state--unknown");
    onlineBadge.classList.add(isOnline ? "relay-state--on" : "relay-state--off");
    onlineBadge.style.marginLeft = "8px";
  }

  activePowerEl.textContent = formatMetricValue(activePower, "kW", 1);
  apparentPowerEl.textContent = formatMetricValue(apparentPower, "kVA", 1);
  reactivePowerEl.textContent = formatMetricValue(reactivePower, "kvar", 1);
  tsEl.textContent = fmtDatePtBR(lastUpdate);
}

// ======================================================
// ✅ RENDER — INVERTERS (KPIs por inversor) ✅
// ======================================================
function fillInverterRowSpans(rowEl, values) {
  const metrics = rowEl.querySelectorAll(".inv-metric");
  if (metrics.length >= 6) {
    setInverterMetricCell(metrics[0], values.power);
    setInverterMetricCell(metrics[1], values.eff);
    setInverterMetricCell(metrics[2], values.temp);
    setInverterMetricCell(metrics[3], values.freq);
    setInverterMetricCell(metrics[4], values.pr);
    metrics[5].textContent = values.last;
    return true;
  }
  // fallback: layout antigo
  const spans = rowEl.querySelectorAll(":scope > span");
  if (!spans || spans.length < 8) return false;
  setInverterMetricCell(spans[2], values.power);
  setInverterMetricCell(spans[3], values.eff);
  setInverterMetricCell(spans[4], values.temp);
  setInverterMetricCell(spans[5], values.freq);
  setInverterMetricCell(spans[6], values.pr);
  spans[7].textContent = values.last;
  return true;
}

function setInverterMetricCell(cellEl, metricText) {
  if (!cellEl) return;

  if (!metricText || metricText === "—") {
    cellEl.textContent = "—";
    return;
  }

  const parts = String(metricText).trim().split(/\s+/);
  const numberPart = parts.shift();
  const unitPart = parts.join(" ");

  if (!numberPart) {
    cellEl.textContent = metricText;
    return;
  }

  const unitHtml = unitPart
    ? `<span class="metric-unit"> ${unitPart}</span>`
    : "";

  cellEl.innerHTML = `<span class="metric-number">${numberPart}</span>${unitHtml}`;
}

function setRowOnlineUi(rowEl, online, inv) {
  const stInfo = inv ? getInverterStatusInfo(inv) : null;
  const statusCls = stInfo ? stInfo.cls : (online ? "inv-st-run" : "inv-st-off");

  rowEl.classList.remove("online", "offline", "inv-st-off", "inv-st-standby", "inv-st-run", "inv-st-fault", "inv-st-unknown");
  rowEl.classList.add(online ? "online" : "offline", statusCls);

  const dot = rowEl.querySelector(".status-dot, [data-role='status-dot']");
  if (dot) {
    dot.classList.remove("online", "offline", "inv-st-off", "inv-st-standby", "inv-st-run", "inv-st-fault", "inv-st-unknown");
    dot.classList.add(online ? "online" : "offline", statusCls);
  }
}

function isOnlineByFreshness(inv) {
  const lastMs = getInvTsMs(inv);
  if (!lastMs) return false;
  const ageMs = Date.now() - lastMs;
  return ageMs <= INVERTER_ONLINE_AFTER_MS;
}

function isZeroSnapshot(inv) {
  const powerKw = asNumber(inv.active_power_kw ?? inv.power_kw ?? inv.power ?? inv.active_power, 0);
  const freqHz = asNumber(inv.frequency_hz ?? inv.freq_hz ?? inv.frequency, 0);
  const tempC = asNumber(inv.temperature_internal_c ?? inv.temperature_c ?? inv.temp_c ?? inv.temperature_current ?? inv.temperature, 0);

  // pacote "morto": 0 kW + 0 Hz + 0°C
  return powerKw === 0 && freqHz === 0 && tempC === 0;
}

function renderInverterRowKpis(rowEl, inv) {
  const powerKw = inv.active_power_kw ?? inv.power_kw ?? inv.power ?? inv.active_power;
  const effPct  = inv.efficiency_pct ?? inv.efficiency ?? inv.eff_pct;
  const tempC   = inv.temperature_internal_c ?? inv.temperature_c ?? inv.temp_c ?? inv.temperature_current ?? inv.temperature;
  const freqHz  = inv.frequency_hz ?? inv.freq_hz ?? inv.frequency;

  const prRaw = inv.performance_ratio ?? inv.pr ?? inv.pr_ratio ?? inv.performance;
  const lastTs =
    inv.last_reading_at ??
    inv.last_reading_ts ??
    inv.last_ts ??
    inv.timestamp ??
    inv.event_ts ??
    null;

  const prPct = normalizePercentMaybe(prRaw);

  const powerText = powerKw != null ? `${numFixedOrDash(powerKw, 0)} kW` : "—";
  const effText   = effPct  != null ? `${numFixedOrDash(effPct, 1)} %` : "—";
  const tempText  = tempC   != null ? `${numFixedOrDash(tempC, 1)} °C` : "—";
  const freqText  = freqHz  != null ? `${numFixedOrDash(freqHz, 2)} Hz` : "—";
  const prText    = prPct   != null ? `${numFixedOrDash(prPct, 2)} %` : "—";
  const lastText  = fmtDatePtBR(lastTs);

  fillInverterRowSpans(rowEl, {
    power: powerText,
    eff: effText,
    temp: tempText,
    freq: freqText,
    pr: prText,
    last: lastText
  });

  const freshOnline = isOnlineByFreshness(inv);
  const online = freshOnline && !isZeroSnapshot(inv);
  setRowOnlineUi(rowEl, online, inv);
}

function renderInvertersRows(inverters) {
  const map = new Map();

  dedupInvertersById(Array.isArray(inverters) ? inverters : []).forEach(inv => {
    const id = getInverterRealId(inv);
    if (id != null) map.set(String(id), inv);
  });

  const rows = document.querySelectorAll(".inverter-toggle[data-inverter-real-id]");
  if (!rows || !rows.length) return;

  rows.forEach(row => {
    const id = row.dataset.inverterRealId;
    const inv = map.get(String(id));

    if (!inv) {
      fillInverterRowSpans(row, {
        power: "—",
        eff: "—",
        temp: "—",
        freq: "—",
        pr: "—",
        last: "—"
      });
      setRowOnlineUi(row, false, null);
      return;
    }

    renderInverterRowKpis(row, inv);
  });
}

// ======================================================
// ✅ MERGE: config(/strings) + realtime(/strings/realtime)
// ======================================================
function mergeStringsPayload(configPayload, realtimePayload, inverterRealId) {
  const maxStrings = configPayload?.max_strings ?? realtimePayload?.max_strings ?? 30;

  const cfgList = configPayload?.strings ?? [];
  const rtList = realtimePayload?.items ?? realtimePayload?.strings ?? [];

  const cfgMap = new Map(cfgList.map(s => [Number(s.string_index), s]));
  const rtMap = new Map(rtList.map(s => [Number(s.string_index), s]));

  const strings = [];
  for (let i = 1; i <= maxStrings; i++) {
    const cfg = cfgMap.get(i);
    const rt = rtMap.get(i);

    const enabled = cfg ? !!cfg.enabled : true;
    const disabledByPref = isDisabledPref(inverterRealId, i);
    const effective_enabled = disabledByPref ? false : !!enabled;
    const has_data = (rt?.has_data ?? cfg?.has_data ?? false) === true;
    const exists_in_config = !!cfg;
    const exists_in_realtime = !!rt;
    const exists_in_api = exists_in_config || exists_in_realtime;

    const monitorable = exists_in_api && effective_enabled;

    strings.push({
      string_index: i,
      exists_in_config,
      exists_in_realtime,
      exists_in_api,
      enabled,
      effective_enabled,
      has_data,
      current_a: rt?.current_a ?? null,
      is_online: rt?.is_online ?? false,
      last_ts: rt?.last_ts ?? null,
      monitorable,
      alarm_active: rt?.alarm_active ?? cfg?.alarm_active ?? null,
      alarm_state: rt?.alarm_state ?? cfg?.alarm_state ?? null,
      alarm_reason: rt?.alarm_reason ?? cfg?.alarm_reason ?? null,
      alarm_code: rt?.alarm_code ?? cfg?.alarm_code ?? null
    });
  }

  return {
    inverter_id: Number(inverterRealId),
    max_strings: maxStrings,
    strings
  };
}

function isStringMonitorable(str) {
  if (!str) return false;
  if (str.effective_enabled === false) return false;
  if (str.exists_in_api !== true) return false;
  return str.monitorable === true;
}

function getInverterOnlineStateById(inverterRealId) {
  const inv = INVERTER_EXTRAS_BY_ID.get(String(inverterRealId));
  if (!inv) return null;
  return isOnlineByFreshness(inv) && !isZeroSnapshot(inv);
}

function isStringInAlarm(str, inverterOnline) {
  if (!isStringMonitorable(str)) return false;
  if (!inverterOnline) return false;

  // futura integração backend: quando alarm_active vier pronto, ele manda na regra local.
  if (str.alarm_active === true) return true;
  if (str.alarm_active === false) return false;

  const noData = str.has_data !== true;
  const nullCurrent = str.current_a === null || str.current_a === undefined || str.current_a === "";

  let stale = false;
  if (str.last_ts) {
    const ts = new Date(str.last_ts);
    if (!Number.isNaN(ts.getTime())) {
      stale = (Date.now() - ts.getTime()) > STRING_STALE_AFTER_MS;
    }
  }

  return noData || nullCurrent || stale;
}

function setInverterStringAlarmBadge(inverterRealId, show) {
  const row = document.querySelector(`.inverter-toggle[data-inverter-real-id="${inverterRealId}"]`);
  if (!row) return;
  row.classList.toggle("has-string-alarm", !!show);

  let badge = row.querySelector(".string-alarm-badge");
  if (show) {
    if (!badge) {
      badge = document.createElement("span");
      badge.className = "string-alarm-badge";
      badge.innerHTML = `<i class="fa-solid fa-triangle-exclamation"></i>`;
      row.appendChild(badge);
    }
  } else if (badge) {
    badge.remove();
  }
}

// ======================================================
// RENDER — STRINGS (COM PERSISTÊNCIA LOCAL + VALOR REAL)
// ======================================================
function renderStringsGrid(gridEl, payload) {
  if (!gridEl) return;

  const strings = Array.isArray(payload?.strings) ? payload.strings : [];
  const inverterRealId = payload?.inverter_id;

  gridEl.innerHTML = "";

  if (!strings.length || inverterRealId == null) {
    if (inverterRealId != null) setInverterStringAlarmBadge(inverterRealId, false);
    gridEl.innerHTML = `<div style="color:#9adbb8; opacity:.7; padding:6px 2px;">Sem dados de strings</div>`;
    return;
  }

  const isEffectiveEnabled = (str) => {
    const disabledByPref = isDisabledPref(inverterRealId, str.string_index);
    return disabledByPref ? false : !!str.enabled;
  };

  const rerender = () => {
    renderStringsGrid(gridEl, {
      ...payload,
      strings
    });
    const panel = gridEl.closest(".inverter-strings");
    if (panel && panel.classList.contains("open")) {
      panel.style.maxHeight = panel.scrollHeight + "px";
    }
  };

  const visibleStrings = strings.filter(isEffectiveEnabled);
  const hiddenStrings = strings.filter(s => !isEffectiveEnabled(s));

  const inverterOnline = getInverterOnlineStateById(inverterRealId);
  let hasAlarmOnAnyMonitorable = false;

  visibleStrings.forEach(str => {
    const el = document.createElement("div");
    el.className = "string-card";
    el.dataset.string = str.string_index;
    const inAlarm = isStringInAlarm(str, inverterOnline);
    if (inAlarm) {
      el.classList.add("string-alarm");
      hasAlarmOnAnyMonitorable = true;
    } else if (!str.has_data) {
      el.classList.add("nodata");
    } else {
      el.classList.add("active");
    }

    const ampText = (str.has_data && inverterOnline !== false) ? fmtAmp(str.current_a) : "—";

    el.innerHTML = `
      S${str.string_index}
      <strong>${ampText}</strong>
    `;

    el.addEventListener("click", async (e) => {
      e.stopPropagation();

      el.classList.add("removing");

      setTimeout(async () => {
        setDisabledPref(inverterRealId, str.string_index, true);
        rerender();

        try {
          await patchInverterString(PLANT_ID, inverterRealId, str.string_index, false);
          str.enabled = false;
        } catch (error) {
          // ✅ FIX Bug 3: não desfaz preferência local — localStorage é fonte da verdade
          console.warn("[strings] PATCH falhou ao desativar string, preferência local mantida:", error?.message || error);
        }
      }, 180);
    });

    gridEl.appendChild(el);
  });

  if (hiddenStrings.length > 0) {
    const addBtn = document.createElement("button");
    addBtn.type = "button";
    addBtn.className = "string-card string-card-add";

    const next = hiddenStrings
      .slice()
      .sort((a, b) => Number(a.string_index) - Number(b.string_index))[0];

    addBtn.title = `Reativar ${next ? `S${next.string_index}` : "string"}`;
    addBtn.innerHTML = `
      <span class="plus">+</span>
      <strong>${next ? `S${next.string_index}` : `${hiddenStrings.length} off`}</strong>
    `;

    addBtn.addEventListener("click", async (e) => {
      e.stopPropagation();

      if (!next) return;

      setDisabledPref(inverterRealId, next.string_index, false);
      next.enabled = true;
      rerender();

      try {
        await patchInverterString(PLANT_ID, inverterRealId, next.string_index, true);
      } catch (error) {
        // ✅ FIX Bug 3: não desfaz preferência local — localStorage é fonte da verdade
        console.warn("[strings] PATCH falhou ao reativar string, preferência local mantida:", error?.message || error);
      }
    });

    gridEl.appendChild(addBtn);
  }

  setInverterStringAlarmBadge(inverterRealId, hasAlarmOnAnyMonitorable);
}

// ======================================================
// ✅ EXTRAS DO INVERSOR (chips agrupados abaixo das strings)
// ======================================================
function ensureInverterExtrasContainer(inverterRealId) {
  const panel = document.getElementById(`strings-${inverterRealId}`);
  if (!panel) return null;
  return panel;
}

function makeChip(label, value) {
  const el = document.createElement("div");
  el.className = "inv-chip";
  el.innerHTML = `
    <span class="inv-chip__label">${label}</span>
    <strong class="inv-chip__value">${value ?? "—"}</strong>
  `;
  return el;
}

function renderInverterExtras(inverterRealId, inv) {
  const wrap = ensureInverterExtrasContainer(inverterRealId);
  if (!wrap) return;

  const rowAc = wrap.querySelector(`.inv-side-row[data-row="ac"]`);
  const rowDc = wrap.querySelector(`.inv-side-row[data-row="dc"]`);
  if (!rowAc || !rowDc) return;

  // ✅ FIX Bug 1: remove status rows anteriores para evitar duplicação a cada refresh
  wrap.querySelectorAll(".inverter-extra-row").forEach(el => el.remove());

  rowAc.innerHTML = "";
  rowDc.innerHTML = "";

  const get = (k) => (inv && typeof inv === "object") ? inv[k] : null;

  // helpers de format (NÃO escondem zero)
  const f = (v, digits, unit) => {
    const n = Number(typeof v === "string" ? v.replace(",", ".") : v);
    if (!Number.isFinite(n)) return "—";
    return `${n.toFixed(digits)} ${unit}`;
  };

  const f0 = (v, unit) => {
    const n = Number(typeof v === "string" ? v.replace(",", ".") : v);
    if (!Number.isFinite(n)) return "—";
    return `${n.toFixed(0)} ${unit}`;
  };

  // ===== AC: Potências / FP + Tensões / Correntes =====
  rowAc.appendChild(makeChip("S aparente", f(get("apparent_power_kva"), 2, "kVA")));
  rowAc.appendChild(makeChip("FP", (() => {
    const v = get("power_factor");
    const n = Number(typeof v === "string" ? v.replace(",", ".") : v);
    if (!Number.isFinite(n)) return "—";
    return n.toFixed(3);
  })()));
  rowAc.appendChild(makeChip("Q reativa", f(get("power_reactive_kvar"), 2, "kvar")));
  rowAc.appendChild(makeChip("Energia dia", f(get("daily_active_energy_kwh"), 1, "kWh")));
  rowAc.appendChild(makeChip("Energia total", f(get("cumulative_active_energy_kwh"), 1, "kWh")));

  // ===== AC: Tensões / Correntes (usar nomes reais do backend + aliases) =====
  const vab = get("line_voltage_ab_v") ?? get("line_voltage_ab");
  const vbc = get("line_voltage_bc_v") ?? get("line_voltage_bc");
  const vca = get("line_voltage_ca_v") ?? get("line_voltage_ca");

  rowAc.appendChild(makeChip("V AB", f0(vab, "V")));
  rowAc.appendChild(makeChip("V BC", f0(vbc, "V")));
  rowAc.appendChild(makeChip("V CA", f0(vca, "V")));

  const ia = get("current_phase_a_a") ?? get("current_phase_a");
  const ib = get("current_phase_b_a") ?? get("current_phase_b");
  const ic = get("current_phase_c_a") ?? get("current_phase_c");

  rowAc.appendChild(makeChip("Ia", (() => {
    const n = Number(typeof ia === "string" ? ia.replace(",", ".") : ia);
    return Number.isFinite(n) ? `${n.toFixed(2)} A` : "—";
  })()));
  rowAc.appendChild(makeChip("Ib", (() => {
    const n = Number(typeof ib === "string" ? ib.replace(",", ".") : ib);
    return Number.isFinite(n) ? `${n.toFixed(2)} A` : "—";
  })()));
  rowAc.appendChild(makeChip("Ic", (() => {
    const n = Number(typeof ic === "string" ? ic.replace(",", ".") : ic);
    return Number.isFinite(n) ? `${n.toFixed(2)} A` : "—";
  })()));

  // ===== DC: Energia / DC / Isolação =====
  rowDc.appendChild(makeChip("P DC", f(get("power_dc_kw"), 2, "kW")));
  rowDc.appendChild(makeChip("V string", f0(get("string_voltage_v"), "V")));
  rowDc.appendChild(makeChip("R isol.", (() => {
    const v = get("resistance_insulation_mohm");
    const n = Number(typeof v === "string" ? v.replace(",", ".") : v);
    return Number.isFinite(n) ? `${n.toFixed(2)} MΩ` : "—";
  })()));

  // ── Inv Status / Work Status chips ──
  const stInfo = getInverterStatusInfo(inv);
  const wkStatus = getWorkingStatus(inv);
  const rowStatus = document.createElement("div");
  rowStatus.className = "inverter-extra-row";

  const invStatusSvg = `<svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="10"/><line x1="12" y1="8" x2="12" y2="12"/><line x1="12" y1="16" x2="12.01" y2="16"/></svg>`;
  const workStatusSvg = `<svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M22 11.08V12a10 10 0 1 1-5.93-9.14"/><polyline points="22 4 12 14.01 9 11.01"/></svg>`;

  const stChip = makeChip(invStatusSvg + " Inv Status", stInfo.label);
  stChip.classList.add(stInfo.cls);
  rowStatus.appendChild(stChip);

  rowStatus.appendChild(makeChip(workStatusSvg + " Work Status", wkStatus));
  wrap.appendChild(rowStatus);
}

// ======================================================
// RENDER — FAIXA OPERACIONAL (se existir no HTML)
// ======================================================
function renderSummaryStrip() {
  const elActive = document.getElementById("summaryActivePower");
  const elRated = document.getElementById("summaryRatedPower");
  const elInv = document.getElementById("summaryInverters");
  const elPR = document.getElementById("summaryPR");

  if (!elActive || !elRated || !elInv || !elPR) return;

  elActive.textContent = `${asNumber(PLANT_STATE.active_power_kw).toFixed(1)} kW`;
  elRated.textContent = `${asNumber(PLANT_STATE.rated_power_kwp).toFixed(1)} kWp`;
  elInv.textContent = `${PLANT_STATE.inverter_online} / ${PLANT_STATE.inverter_total} Online`;
  elPR.textContent = `${asNumber(PLANT_STATE.pr_percent).toFixed(1)} %`;
}

// ======================================================
// CHART INSTANCES
// ======================================================
let dailyChartInstance = null;
let monthlyChartInstance = null;
let LAST_INVERTER_ROWS_SIGNATURE = "";
let DAILY_CHART_ZOOM_WIRED = false;

function resetDailyChartZoom() {
  if (!dailyChartInstance || typeof dailyChartInstance.resetZoom !== "function") return;
  try {
    dailyChartInstance.resetZoom();
  } catch (err) {
    console.warn("[dailyChart] erro ao resetar zoom:", err);
  }
}

function wireDailyChartZoomControlsOnce() {
  if (DAILY_CHART_ZOOM_WIRED) return;
  DAILY_CHART_ZOOM_WIRED = true;

  document.getElementById("dailyZoomInBtn")?.addEventListener("click", () => {
    if (!dailyChartInstance || typeof dailyChartInstance.zoom !== "function") return;
    dailyChartInstance.zoom({ x: 1.2 });
  });

  document.getElementById("dailyZoomOutBtn")?.addEventListener("click", () => {
    if (!dailyChartInstance || typeof dailyChartInstance.zoom !== "function") return;
    dailyChartInstance.zoom({ x: 0.8 });
  });

  document.getElementById("dailyZoomResetBtn")?.addEventListener("click", resetDailyChartZoom);
}

// ======================================================
// GRÁFICO DIÁRIO
// ======================================================
function renderDailyChart() {
  const canvas = document.getElementById("plantMainChart");
  if (!canvas || !DAILY?.labels?.length) return;
  const ratedPower = asNumber(PLANT_STATE.rated_power_kwp, 0);
  const powerAxisMax = ratedPower > 0 ? Math.ceil(ratedPower) : 1250;

  const ctx = canvas.getContext("2d");

  if (dailyChartInstance) {
    dailyChartInstance.destroy();
    dailyChartInstance = null;
  }

  const greenGradient = ctx.createLinearGradient(0, 0, 0, 320);
  greenGradient.addColorStop(0, "rgba(57,229,140,0.36)");
  greenGradient.addColorStop(0.6, "rgba(57,229,140,0.20)");
  greenGradient.addColorStop(1, "rgba(57,229,140,0.03)");

  const yellowGradient = ctx.createLinearGradient(0, 0, 0, 320);
  yellowGradient.addColorStop(0, "rgba(255,216,77,0.24)");
  yellowGradient.addColorStop(1, "rgba(255,216,77,0.02)");

  dailyChartInstance = new Chart(ctx, {
    type: "line",
    data: {
      labels: DAILY.labels,
      datasets: [
        {
          label: "Esperado",
          data: Array.isArray(DAILY.expectedPower) ? DAILY.expectedPower : [],
          borderColor: "rgba(205, 213, 225, 0.70)",
          fill: false,
          tension: 0.28,
          pointRadius: 0,
          borderWidth: 1.5,
          borderDash: [6, 6],
          yAxisID: "yPower",
          spanGaps: true,
          order: 0
        },
        {
          label: "Potência Ativa",
          data: DAILY.activePower,
          borderColor: "#39e58c",
          backgroundColor: greenGradient,
          fill: true,
          tension: 0.4,
          pointRadius: 0,
          borderWidth: 2,
          yAxisID: "yPower",
          spanGaps: true,
          order: 1
        },
        {
          label: "Irradiância POA",
          data: DAILY.irradiance,
          borderColor: "#ffd84d",
          backgroundColor: yellowGradient,
          fill: true,
          tension: 0.3,
          pointRadius: 0,
          borderWidth: 2,
          yAxisID: "yIrr",
          spanGaps: true,
          order: 2
        }
      ]
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      interaction: { mode: "index", intersect: false },
      plugins: {
        legend: { display: false },
        tooltip: {
          backgroundColor: "rgba(6,18,14,0.96)",
          borderColor: "rgba(57,229,140,0.18)",
          borderWidth: 1,
          titleColor: "#dbe7ef",
          bodyColor: "#dbe7ef",
          padding: 10,
          displayColors: true,
          usePointStyle: true,
          callbacks: {
            label: (item) => {
              const label = item?.dataset?.label || "";
              const value = Number(item?.raw ?? 0);

              if (label === "Esperado") {
                return `Esperado: ${formatKwPtBR(value)}`;
              }
              if (label === "Potência Ativa") {
                return `Potência Ativa: ${formatKwPtBR(value)}`;
              }
              if (label === "Irradiância POA") {
                return `Irradiância POA: ${formatWm2PtBR(value)}`;
              }

              return `${label}: ${formatNumberPtBR(value)}`;
            }
          }
        },
        zoom: {
          pan: {
            enabled: true,
            mode: "x"
          },
          zoom: {
            wheel: {
              enabled: true
            },
            pinch: {
              enabled: true
            },
            drag: {
              enabled: false
            },
            mode: "x"
          },
          limits: {
            x: { minRange: 6 }
          }
        }
      },
      scales: {
        x: {
          ticks: { color: "#9adbb8", maxTicksLimit: 12 },
          grid: { color: "rgba(255,255,255,0.04)" }
        },
        yPower: {
          position: "left",
          min: 0,
          suggestedMax: powerAxisMax,
          ticks: { color: "#39e58c", callback: v => `${v} kW` },
          grid: { color: "rgba(255,255,255,0.05)" }
        },
        yIrr: {
          position: "right",
          min: 0,
          max: 1200,
          ticks: { color: "#ffd84d", callback: v => `${v} W/m²` },
          grid: { drawOnChartArea: false }
        }
      }
    }
  });
}

// ======================================================
// ✅ NORMALIZA PAYLOAD MENSAL (unidade + outlier + labels)
// ======================================================
function normalizeMonthlyPayload(payload) {
  if (!payload) return payload;

  const labels = Array.isArray(payload.labels) ? payload.labels.slice() : [];

  const dailyNew = Array.isArray(payload.daily_kwh) ? payload.daily_kwh.slice() : null;
  const mtdNew = Array.isArray(payload.mtd_kwh) ? payload.mtd_kwh.slice() : null;
  const irrDailyNew = Array.isArray(payload.irradiation_daily_kwh_m2)
    ? payload.irradiation_daily_kwh_m2.slice()
    : null;
  const expectedDailyNew = Array.isArray(payload.expected_daily_kwh)
    ? payload.expected_daily_kwh.slice()
    : (Array.isArray(payload.expectedDailyKwh) ? payload.expectedDailyKwh.slice() : null);
  const energyLegacy = Array.isArray(payload.energy_kwh) ? payload.energy_kwh.slice() : null;

  let daily = (dailyNew ?? energyLegacy ?? []).map(v => Number(v) || 0);

  const n = Math.min(labels.length || daily.length, daily.length || labels.length);
  const cutLabels = labels.slice(0, n);
  daily = daily.slice(0, n);

  const allLookDayOnly = cutLabels.length > 0 && cutLabels.every(looksLikeDayOnlyLabel);
  const duplicated = hasDuplicateLabels(cutLabels);

  const finalLabels =
    allLookDayOnly && duplicated ? buildLastNDaysLabels(daily.length) : cutLabels;

  let mtd = [];
  if (mtdNew && mtdNew.length >= daily.length) {
    mtd = mtdNew.slice(0, daily.length).map(v => Number(v) || 0);
  } else {
    let acc = 0;
    mtd = daily.map(v => (acc += (Number(v) || 0)));
  }

  const converted = maybeConvertWhToKwh(daily, mtd);
  daily = converted.daily;
  mtd = converted.mtd;
  const dailyForTotals = daily.slice();
  const mtdForTotals = mtd.slice();

  const capped = capMonthlyOutliers(daily);
  daily = capped.daily;

  let acc = 0;
  mtd = daily.map(v => (acc += (Number(v) || 0)));
  const irradiationDaily = (irrDailyNew ?? []).slice(0, daily.length).map(v => Number(v) || 0);
  while (irradiationDaily.length < daily.length) irradiationDaily.push(0);

  let expectedDaily = (expectedDailyNew ?? []).slice(0, daily.length).map(v => Number(v) || 0);
  while (expectedDaily.length < daily.length) expectedDaily.push(0);

  const expectedMtdFromPayload =
    Array.isArray(payload.expected_mtd_kwh) ? payload.expected_mtd_kwh.slice() :
    Array.isArray(payload.expectedMtdKwh) ? payload.expectedMtdKwh.slice() :
    null;

  let expectedMtd = [];
  if (expectedMtdFromPayload && expectedMtdFromPayload.length >= expectedDaily.length) {
    expectedMtd = expectedMtdFromPayload.slice(0, expectedDaily.length).map(v => Number(v) || 0);
  } else {
    let accExpected = 0;
    expectedMtd = expectedDaily.map(v => (accExpected += (Number(v) || 0)));
  }

  return {
    ...payload,
    labels: finalLabels,
    daily_kwh: daily,
    mtd_kwh: mtd,
    daily_kwh_for_totals: dailyForTotals,
    mtd_kwh_for_totals: mtdForTotals,
    expected_daily_kwh: expectedDaily,
    expected_mtd_kwh: expectedMtd,
    irradiation_daily_kwh_m2: irradiationDaily,
    energy_kwh: daily
  };
}

function getMonthlyCurrentIndex(labels, daily) {
  const list = Array.isArray(labels) ? labels : [];
  if (!list.length) return -1;

  const today = new Date();
  const dd = String(today.getDate()).padStart(2, "0");
  const mm = String(today.getMonth() + 1).padStart(2, "0");
  const yyyy = String(today.getFullYear());
  const todayKeys = new Set([
    String(today.getDate()),
    dd,
    `${dd}/${mm}`,
    `${dd}-${mm}`,
    `${yyyy}-${mm}-${dd}`
  ]);

  const indexFromToday = list.findIndex(label => todayKeys.has(String(label ?? "").trim()));
  if (indexFromToday >= 0) return indexFromToday;

  const real = Array.isArray(daily) ? daily : [];
  for (let i = Math.min(real.length, list.length) - 1; i >= 0; i--) {
    if (Number(real[i]) > 0) return i;
  }

  return list.length - 1;
}

// ======================================================
// GRÁFICO MENSAL — SÓ BARRAS (SEM LINHA)
// ======================================================
function renderMonthlyChart() {
  const canvas = document.getElementById("plantMonthlyChart");
  if (!canvas) return;

  const labels = Array.isArray(MONTHLY?.labels) ? MONTHLY.labels : [];
  const daily = Array.isArray(MONTHLY?.daily_kwh)
    ? MONTHLY.daily_kwh.map(v => Number(v) || 0)
    : (Array.isArray(MONTHLY?.energy_kwh) ? MONTHLY.energy_kwh.map(v => Number(v) || 0) : []);
  const dailyForTotals = Array.isArray(MONTHLY?.daily_kwh_for_totals)
    ? MONTHLY.daily_kwh_for_totals.map(v => Number(v) || 0)
    : daily;
  const realMtd = Array.isArray(MONTHLY?.mtd_kwh_for_totals)
    ? MONTHLY.mtd_kwh_for_totals.map(v => Number(v) || 0)
    : [];
  const expectedDaily = Array.isArray(MONTHLY?.expected_daily_kwh)
    ? MONTHLY.expected_daily_kwh.map(v => Number(v) || 0)
    : [];
  const expectedMtd = Array.isArray(MONTHLY?.expected_mtd_kwh)
    ? MONTHLY.expected_mtd_kwh.map(v => Number(v) || 0)
    : [];

  if (!labels.length || !daily.length) return;

  const expectedPadded = expectedDaily.slice(0, daily.length);
  while (expectedPadded.length < daily.length) expectedPadded.push(0);

  const currentIndex = Math.max(0, Math.min(getMonthlyCurrentIndex(labels, dailyForTotals), daily.length - 1));
  const currentLabel = String(labels[currentIndex] ?? "").trim() || "hoje";
  const dailyToDate = dailyForTotals.slice(0, currentIndex + 1);
  const expectedToDate = expectedPadded.slice(0, currentIndex + 1);

  const totalReal = Number(realMtd[currentIndex]) ||
    dailyToDate.reduce((a, b) => a + (Number(b) || 0), 0);
  const totalExpectedToDate = Number(expectedMtd[currentIndex]) ||
    expectedToDate.reduce((a, b) => a + (Number(b) || 0), 0);
  const totalExpectedMonth = expectedPadded.reduce((a, b) => a + (Number(b) || 0), 0);
  const deviation = totalExpectedToDate > 0 ? ((totalReal - totalExpectedToDate) / totalExpectedToDate) * 100 : 0;
  const elapsedDays = currentIndex + 1;

  const kpiRealEl = document.getElementById("monthlyKpiReal");
  const kpiExpectedMtdEl = document.getElementById("monthlyKpiExpectedMtd");
  const kpiExpEl = document.getElementById("monthlyKpiExp");
  const kpiDevEl = document.getElementById("monthlyKpiDev");
  const progressEl = document.getElementById("monthlyProgressFill");
  const bottomLeftEl = document.getElementById("monthlyBottomLeft");
  const bottomRightEl = document.getElementById("monthlyBottomRight");

  if (kpiRealEl) kpiRealEl.textContent = formatKwhPtBR(totalReal);
  if (kpiExpectedMtdEl) kpiExpectedMtdEl.textContent = formatKwhPtBR(totalExpectedToDate);
  if (kpiExpEl) kpiExpEl.textContent = formatKwhPtBR(totalExpectedMonth);
  if (kpiDevEl) {
    kpiDevEl.textContent = `${deviation >= 0 ? "+" : ""}${deviation.toFixed(1)}%`;
    kpiDevEl.style.color = deviation >= 0 ? "#7FD055" : "#ff6b6b";
  }
  if (progressEl) progressEl.style.width = `${((elapsedDays / daily.length) * 100).toFixed(1)}%`;
  if (bottomLeftEl) bottomLeftEl.textContent = `Ate ${currentLabel}: real produzido x esperado acumulado`;
  if (bottomRightEl) bottomRightEl.textContent = `Expectativa mensal: ${formatKwhPtBR(totalExpectedMonth)}`;

  const ctx = canvas.getContext("2d");

  if (monthlyChartInstance) {
    monthlyChartInstance.destroy();
    monthlyChartInstance = null;
  }

  const maxDaily = Math.max(...daily, 0);
  const suggestedMaxDaily = maxDaily > 0 ? Math.ceil(maxDaily * 1.25) : undefined;
  const realColors = daily.map((v, idx) => {
    const exp = Number(expectedPadded[idx] ?? 0);
    if (v === 0) return "rgba(255,255,255,.06)";
    return v >= exp ? "rgba(127,208,85,.92)" : "rgba(127,208,85,.50)";
  });
  const realBorders = daily.map((v, idx) => {
    const exp = Number(expectedPadded[idx] ?? 0);
    if (v === 0) return "rgba(255,255,255,.06)";
    return v >= exp ? "#7FD055" : "rgba(127,208,85,.70)";
  });

  const isMobile = window.innerWidth <= 768;
  const barThickExp     = isMobile ? 6  : 14;
  const barThickReal    = isMobile ? 4  : 9;
  const maxBarThickExp  = isMobile ? 10 : 20;
  const maxBarThickReal = isMobile ? 8  : 16;
  const catPerc         = isMobile ? 0.88 : 0.78;
  const yTicksLimit     = isMobile ? 5 : 6;
  const xTicksLimit     = isMobile ? 8 : 6;
  const tickFontSize    = isMobile ? 10 : 12;

  monthlyChartInstance = new Chart(ctx, {
    type: "bar",
    data: {
      labels,
      datasets: [
        {
          label: "Esperado",
          data: expectedPadded,
          backgroundColor: "rgba(190,200,210,.28)",
          borderColor: "rgba(190,200,210,.38)",
          borderWidth: 1,
          borderRadius: { topLeft: 5, topRight: 5 },
          borderSkipped: "bottom",
          barThickness: barThickExp,
          maxBarThickness: maxBarThickExp,
          categoryPercentage: catPerc,
          barPercentage: 0.92,
          order: 0,
          hoverBackgroundColor: "rgba(190,200,210,.46)"
        },
        {
          label: "Real",
          data: daily,
          backgroundColor: realColors,
          borderColor: realBorders,
          borderWidth: 1,
          borderRadius: { topLeft: 4, topRight: 4 },
          borderSkipped: "bottom",
          barThickness: barThickReal,
          maxBarThickness: maxBarThickReal,
          categoryPercentage: catPerc,
          barPercentage: 0.70,
          order: 1
        }
      ]
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      interaction: { mode: "index", intersect: false },
      plugins: {
        legend: { display: false },
        tooltip: {
          backgroundColor: "rgba(6,18,14,0.96)",
          borderColor: "rgba(57,229,140,0.18)",
          borderWidth: 1,
          titleColor: "#dbe7ef",
          bodyColor: "#dbe7ef",
          padding: 10,
          displayColors: false,
          callbacks: {
            title: (items) => items?.[0]?.label ? `Dia ${items[0].label}` : "",
            label: (item) => {
              const idx = item?.dataIndex ?? 0;
              const real = Number(daily[idx] ?? 0);
              const expected = Number(expectedPadded[idx] ?? 0);
              if (item?.dataset?.label === "Esperado") return `Esperado: ${formatKwhPtBR(expected)}`;
              if (item?.dataset?.label === "Real") return `Real: ${formatKwhPtBR(real)}`;
              return "";
            },
            afterBody: (items) => {
              const idx = items?.[0]?.dataIndex ?? 0;
              const real = Number(daily[idx] ?? 0);
              const expected = Number(expectedPadded[idx] ?? 0);
              const deviation = expected > 0 ? ((real - expected) / expected) * 100 : 0;
              const sign = deviation > 0 ? "+" : "";
              const expectedAccum = expectedPadded
                .slice(0, idx + 1)
                .reduce((a, b) => a + (Number(b) || 0), 0);
              return [
                `Desvio diario: ${sign}${deviation.toFixed(1)}%`,
                `Esperado acumulado: ${formatKwhPtBR(expectedAccum)}`
              ];
            }
          }
        }
      },
      scales: {
        x: {
          offset: true,
          ticks: {
            color: "#9adbb8",
            maxTicksLimit: xTicksLimit,
            autoSkip: true,
            maxRotation: 0,
            minRotation: 0,
            padding: 8,
            font: { size: tickFontSize }
          },
          grid: { display: false }
        },
        y: {
          beginAtZero: true,
          suggestedMax: suggestedMaxDaily,
          grace: "12%",
          ticks: {
            color: "#9adbb8",
            maxTicksLimit: yTicksLimit,
            padding: 8,
            font: { size: tickFontSize },
            callback: (v) => formatNumberPtBR(v)
          },
          grid: {
            color: "rgba(255,255,255,0.04)",
            drawBorder: false
          }
        }
      }
    }
  });
}

// ======================================================
// TOGGLE — abre/fecha o painel
// ======================================================
function setupInverterToggles() {
  const container = document.querySelector(".inverters-section");
  if (!container) return;

  container.addEventListener("click", async (e) => {
    if (e.target.closest(".string-card")) return;

    const row = e.target.closest(".inverter-toggle");
    if (!row) return;

    const inverterRealId = row.dataset.inverterRealId;
    const panel = document.getElementById(`strings-${inverterRealId}`);
    if (!panel) return;

    const willOpen = !row.classList.contains("open");

    document.querySelectorAll(".inverter-toggle.open").forEach(r => r.classList.remove("open"));
    document.querySelectorAll(".inverter-strings.open").forEach(p => {
      p.classList.remove("open");
      p.style.maxHeight = "0px";
      p.style.opacity = "0";
    });

    if (!willOpen) {
      OPEN_INVERTER_REAL_ID = null;
      return;
    }

    OPEN_INVERTER_REAL_ID = inverterRealId;
    row.classList.add("open");
    panel.classList.add("open");
    panel.style.opacity = "1";

    // Mostra spinner enquanto carrega
    const stringsGrid = panel.querySelector(".strings-grid");
    if (stringsGrid) {
      const loader = document.createElement("div");
      loader.className = "inv-panel-loader";
      loader.innerHTML = `<div class="inv-panel-spinner"></div><span>Carregando dados…</span>`;
      stringsGrid.innerHTML = "";
      stringsGrid.appendChild(loader);
    }

    panel.style.maxHeight = panel.scrollHeight + "px";

    refreshStringsForRealInverter(inverterRealId).finally(() => {
      // ✅ renderiza extras (chips amarelos) abaixo das strings
      const inv = INVERTER_EXTRAS_BY_ID.get(String(inverterRealId));
      renderInverterExtras(inverterRealId, inv);

      const samePanel = document.getElementById(`strings-${inverterRealId}`);
      if (samePanel && samePanel.classList.contains("open")) {
        samePanel.style.maxHeight = samePanel.scrollHeight + "px";
      }
    });
  });
}

async function refreshStringsForRealInverter(inverterRealId) {
  const grid = document.querySelector(`.strings-grid[data-inverter-real-id="${inverterRealId}"]`);
  if (!grid) return;

  const prev = STRINGS_REFRESH_SEQ_MAP.get(String(inverterRealId)) ?? 0;
  const reqSeq = prev + 1;
  STRINGS_REFRESH_SEQ_MAP.set(String(inverterRealId), reqSeq);

  const [cfg, rt] = await Promise.all([
    fetchInverterStrings(PLANT_ID, inverterRealId),
    fetchInverterStringsRealtime(PLANT_ID, inverterRealId)
  ]);

  if (reqSeq !== STRINGS_REFRESH_SEQ_MAP.get(String(inverterRealId))) return;

  const merged = mergeStringsPayload(cfg, rt, inverterRealId);
  renderStringsGrid(grid, merged);

  const panel = document.getElementById(`strings-${inverterRealId}`);
  if (panel && panel.classList.contains("open")) {
    panel.style.maxHeight = panel.scrollHeight + "px";
  }
}

async function refreshOpenStringsPanels() {
  const trackedId = OPEN_INVERTER_REAL_ID;
  if (trackedId != null) {
    await refreshStringsForRealInverter(trackedId);
    // Garante que o painel continue visualmente aberto após eventual DOM rebuild
    const panel = document.getElementById(`strings-${trackedId}`);
    const row = document.querySelector(`.inverter-toggle[data-inverter-real-id="${trackedId}"]`);
    if (panel && row && !panel.classList.contains("open")) {
      row.classList.add("open");
      panel.classList.add("open");
      panel.style.opacity = "1";
      panel.style.maxHeight = panel.scrollHeight + "px";
    }
    return;
  }

  const openRow = document.querySelector(".inverter-toggle.open[data-inverter-real-id]");
  if (!openRow) return;
  OPEN_INVERTER_REAL_ID = openRow.dataset.inverterRealId;
  await refreshStringsForRealInverter(openRow.dataset.inverterRealId);
}

function renderPlantName(realtime) {
  const name =
    realtime?.power_plant_name ??
    realtime?.powerPlantName ??
    realtime?.name ??
    "—";

  PLANT_STATE = { ...PLANT_STATE, name };

  const el = document.getElementById("plantName") || document.querySelector(".plant-name");
  if (el) el.textContent = name;
}

// ======================================================
// ✅ REFRESH (realtime + alarms + inverters rows + strings abertas + relay)
// ======================================================
async function refreshRealtimeEverything() {
  if (IS_REFRESHING_PLANT) return;
  IS_REFRESHING_PLANT = true;

  let realtime = null;
  try {
    const [realtimeRes, alarmsRes, invertersRes, relayRes, multimeterRes, trackersRes] = await Promise.allSettled([
      fetchPlantRealtime(PLANT_ID),
      fetchActiveAlarms(PLANT_ID),
      fetchInvertersRealtime(PLANT_ID),
      safeFetchRelayIfSupported(PLANT_ID),
      safeFetchMultimeterIfSupported(PLANT_ID),
      fetchTrackersRealtime(PLANT_ID)
    ]);

    if (realtimeRes.status === "fulfilled") {
      realtime = realtimeRes.value;
      renderPlantName(realtime);
      if (realtime) {
        const rated = asNumber(
          realtime.rated_power_ac_kw ?? realtime.rated_power_kw ?? realtime.rated_power_kwp,
          PLANT_STATE.rated_power_kwp
        );
        const active = asNumber(
          realtime.active_power_kw ?? realtime.active_power_inverter_kw ?? realtime.active_power_meter_kw,
          PLANT_STATE.active_power_kw
        );
        const prPct = normalizePercentMaybe(
          realtime.performance_ratio ?? realtime.pr_daily_pct ?? realtime.pr_percent
        );
        PLANT_STATE = {
          ...PLANT_STATE,
          rated_power_kwp: rated,
          active_power_kw: active,
          capacity_percent: rated > 0 ? (active / rated) * 100 : PLANT_STATE.capacity_percent,
          pr_percent: prPct != null ? prPct : PLANT_STATE.pr_percent,
          capacity_ac: realtime.capacity_ac != null ? Number(realtime.capacity_ac) : PLANT_STATE.capacity_ac,
        };
      }
    } else {
      console.error("[refreshRealtimeEverything][realtime] erro", realtimeRes.reason);
    }

    if (alarmsRes.status === "fulfilled") {
      ACTIVE_ALARMS = Array.isArray(alarmsRes.value) ? alarmsRes.value : [];
      renderAlarms(ACTIVE_ALARMS);
      renderAlarmMenuButton();
    } else {
      ACTIVE_ALARMS = [];
      renderAlarms(ACTIVE_ALARMS);
      renderAlarmMenuButton();
      console.error("[refreshRealtimeEverything][alarms] erro", alarmsRes.reason);
    }

    if (invertersRes.status === "fulfilled") {
      INVERTERS_REALTIME = invertersRes.value;
      window.INVERTERS_REALTIME = INVERTERS_REALTIME;
      INVERTER_EXTRAS_BY_ID = new Map();
      dedupInvertersById(INVERTERS_REALTIME).forEach(inv => {
        const id = getInverterRealId(inv);
        if (id != null) INVERTER_EXTRAS_BY_ID.set(String(id), inv);
      });

      const dedup = dedupInvertersById(INVERTERS_REALTIME);
      PLANT_CATALOG.inverters = dedup;
      PLANT_STATE = {
        ...PLANT_STATE,
        inverter_total: dedup.length,
        inverter_online: countOnlineInverters(dedup)
      };

      ensureInverterRowsFromRealtime(INVERTERS_REALTIME);
      renderInvertersRows(INVERTERS_REALTIME);
      refreshInverterStatusChips(INVERTERS_REALTIME);
      refreshCabineMapCards(INVERTERS_REALTIME);
    } else {
      console.error("[refreshRealtimeEverything][inverters] erro", invertersRes.reason);
    }

    if (relayRes.status === "fulfilled") {
      const relayItem = relayRes.value;
      RELAY_REALTIME = relayItem;
      window.RELAY_REALTIME = RELAY_REALTIME;
      PLANT_CATALOG.hasRelay = !!relayItem;
      setRelaySectionVisible(RELAY_SUPPORTED !== false);
      if (RELAY_SUPPORTED !== false) renderRelayCard(relayItem);
      updateCabineRelayNode(relayItem);
    } else {
      console.error("[refreshRealtimeEverything][relay] erro", relayRes.reason);
    }

    if (multimeterRes.status === "fulfilled") {
      const multimeterItem = multimeterRes.value;
      MULTIMETER_REALTIME = multimeterItem;
      window.MULTIMETER_REALTIME = MULTIMETER_REALTIME;
      setMultimeterSectionVisible(MULTIMETER_SUPPORTED !== false);
      if (MULTIMETER_SUPPORTED !== false) renderMultimeterCard(multimeterItem);
      updateCabineMeterNode(multimeterItem);
    } else {
      console.error("[refreshRealtimeEverything][multimeter] erro", multimeterRes.reason);
    }

    if (trackersRes.status === "fulfilled") {
      const trackersPayload = trackersRes.value;
      TRACKERS_DATA = Array.isArray(trackersPayload?.items) ? trackersPayload.items : [];
      TRACKERS_PLANT_CENTER = trackersPayload?.plant_center ?? null;
      TRACKERS_PLANT_BOUNDS = trackersPayload?.plant_bounds ?? null;
      const hasTrackers = Array.isArray(TRACKERS_DATA) && TRACKERS_DATA.some(
        (t) => Number.isFinite(Number(t.latitude)) && Number.isFinite(Number(t.longitude))
      );
      if (hasTrackers) {
        TRACKERS_LAST_HAS_DATA = true;
      }

      if (!TRACKERS_USER_OPENED) {
        setTrackersSectionVisible(hasTrackers);
      } else {
        setTrackersSectionVisible(TRACKERS_LAST_HAS_DATA);
      }

      if (TRACKERS_LAST_HAS_DATA) {
        const trackersSection = document.getElementById("trackersSection");
        const trackersVisible =
          trackersSection &&
          !trackersSection.classList.contains("trackers-hidden") &&
          !trackersSection.classList.contains("is-collapsed");
        if (trackersVisible && hasTrackers) renderTrackersPanel();
      }
    } else {
      TRACKERS_DATA = [];
      TRACKERS_PLANT_CENTER = null;
      TRACKERS_PLANT_BOUNDS = null;
      renderTrackersPanel();
      console.error("[refreshRealtimeEverything][trackers] erro", trackersRes.reason);
    }

    renderHeaderSummary();
    renderWeather(realtime?.weather ?? null);
    renderSummaryStrip();

    try {
      await refreshOpenStringsPanels();
      if (OPEN_INVERTER_REAL_ID != null) {
        const inv = INVERTER_EXTRAS_BY_ID.get(String(OPEN_INVERTER_REAL_ID));
        renderInverterExtras(OPEN_INVERTER_REAL_ID, inv);
      }
    } catch (e) {
      console.error("[refreshRealtimeEverything][strings] erro", e);
    }
  } finally {
    IS_REFRESHING_PLANT = false;
  }
}

// ======================================================
// TRACKERS (MOCK LOCAL) — MÓDULO INDEPENDENTE
// ======================================================
let TRACKER_VIEW_MODE = "state";
let TRACKERS_DATA = [];
let TRACKERS_FILTER_TEXT = "";
let TRACKERS_TRANSFORM = { scale: 1, x: 0, y: 0 };
let TRACKERS_PLANT_CENTER = null;
let TRACKERS_PLANT_BOUNDS = null;
let TRACKERS_HAS_FITTED_ONCE = false;
let TRACKERS_USER_OPENED = false;
let TRACKERS_LAST_HAS_DATA = false;
let TRACKERS_MAP = null;
let TRACKERS_MARKERS_LAYER = null;

function createMockTrackers(count = 220) {
  const items = [];
  const cols = 22;
  const spacingX = 65;
  const spacingY = 78;
  const states = [
    "off",
    "manual_daytime",
    "auto_daytime",
    "manual_tracking",
    "auto_tracking",
    "manual_nighttime",
    "auto_nighttime",
    "manual_sleep",
    "auto_sleep"
  ];

  for (let i = 0; i < count; i++) {
    const row = Math.floor(i / cols);
    const col = i % cols;
    const offline = i % 17 === 0;
    const stateCode = offline ? "no_comm" : states[i % states.length];
    const angle = offline ? null : -60 + ((i * 7) % 131);
    const error = offline ? null : Number(((i * 1.7) % 11).toFixed(1));

    items.push({
      id: `TRK-${String(i + 1).padStart(4, "0")}`,
      name: `Tracker ${String(i + 1).padStart(3, "0")}`,
      kind: i % 2 === 0 ? "tcu" : "rsu",
      x: 50 + col * spacingX + (row % 2 ? 12 : 0),
      y: 45 + row * spacingY,
      state_code: stateCode,
      angle_deg: angle,
      error_value: error,
      is_online: !offline
    });
  }
  return items;
}

function getTrackersLegendItems(mode) {
  if (mode === "state") {
    return [
      ["tracker desligado", "#707b86"],
      ["manual + daytime", "#f6bd60"],
      ["automático + daytime", "#f2e85e"],
      ["manual + tracking", "#4f9dff"],
      ["automático + tracking", "#2ad37f"],
      ["manual + nighttime", "#7f8cff"],
      ["automático + nighttime", "#6375ff"],
      ["manual sleep", "#b47dff"],
      ["automático sleep", "#9255ff"],
      ["sem comunicação", "#4a5057"]
    ];
  }

  if (mode === "angle") {
    return [
      ["-60 a -50", "#7b1fa2"],
      ["-50 a -40", "#5c2dd6"],
      ["-40 a -30", "#3949ab"],
      ["-30 a -20", "#1e88e5"],
      ["-20 a -10", "#00acc1"],
      ["-10 a 0", "#26a69a"],
      ["0 a 10", "#43a047"],
      ["10 a 20", "#7cb342"],
      ["20 a 30", "#c0ca33"],
      ["30 a 40", "#fdd835"],
      ["40 a 50", "#ffb300"],
      ["50 a 60", "#fb8c00"],
      ["60 a 70", "#ef6c00"],
      ["sem comunicação", "#4a5057"]
    ];
  }

  return [
    ["erro <= 5", "#2ad37f"],
    ["erro > 5", "#ff8a65"],
    ["offline", "#4a5057"]
  ];
}

function getTrackerColorByMode(item, mode) {
  if (!item?.is_online) return "#4a5057";

  if (mode === "state") {
    const map = {
      off: "#707b86",
      manual_daytime: "#f6bd60",
      auto_daytime: "#f2e85e",
      manual_tracking: "#4f9dff",
      auto_tracking: "#2ad37f",
      manual_nighttime: "#7f8cff",
      auto_nighttime: "#6375ff",
      manual_sleep: "#b47dff",
      auto_sleep: "#9255ff",
      no_comm: "#4a5057"
    };
    return map[item.state_code] || "#8a949d";
  }

  if (mode === "angle") {
    const a = Number(item.angle_deg);
    if (!Number.isFinite(a)) return "#4a5057";
    const ranges = [
      [-60, -50, "#7b1fa2"], [-50, -40, "#5c2dd6"], [-40, -30, "#3949ab"],
      [-30, -20, "#1e88e5"], [-20, -10, "#00acc1"], [-10, 0, "#26a69a"],
      [0, 10, "#43a047"], [10, 20, "#7cb342"], [20, 30, "#c0ca33"],
      [30, 40, "#fdd835"], [40, 50, "#ffb300"], [50, 60, "#fb8c00"], [60, 70, "#ef6c00"]
    ];
    const found = ranges.find(([lo, hi]) => a >= lo && a < hi);
    return found ? found[2] : "#ef6c00";
  }

  const err = Number(item.error_value);
  if (!Number.isFinite(err)) return "#4a5057";
  return err <= 5 ? "#2ad37f" : "#ff8a65";
}

function renderTrackersLegend() {
  const legendEl = document.getElementById("trackersLegend");
  if (!legendEl) return;
  const items = getTrackersLegendItems(TRACKER_VIEW_MODE);
  legendEl.innerHTML = items
    .map(([label, color]) => `
      <div class="trackers-legend-item">
        <span class="trackers-legend-dot" style="background:${color}"></span>
        <span>${label}</span>
      </div>
    `)
    .join("");
}

function applyTrackersTransform() {
  if (!TRACKERS_MAP) return;
  TRACKERS_MAP.invalidateSize();
}

function renderTrackersNodes() {
  if (!TRACKERS_MAP || !TRACKERS_MARKERS_LAYER) return;
  TRACKERS_MARKERS_LAYER.clearLayers();

  const filterText = TRACKERS_FILTER_TEXT.trim().toLowerCase();
  const filtered = TRACKERS_DATA.filter((t) => {
    if (!filterText) return true;
    const hay = `${t.name || ""} ${t.id || ""} ${t.tracker_id || ""} ${t.kind || ""} ${t.tracker_type || ""}`.toLowerCase();
    return hay.includes(filterText);
  });

  const valid = filtered.filter(t =>
    Number.isFinite(Number(t.latitude)) && Number.isFinite(Number(t.longitude))
  );

  const fallback = document.getElementById("trackersMapFallback");
  if (!valid.length) {
    if (fallback) fallback.hidden = false;
    return;
  }
  if (fallback) fallback.hidden = true;

  const bounds = [];
  const markerIcon = (color) => L.divIcon({
    className: "",
    html: `<div class="tracker-map-marker" style="background:${color}"></div>`,
    iconSize: [14, 14],
    iconAnchor: [7, 7]
  });

  valid.forEach((tracker) => {
    const lat = Number(tracker.latitude);
    const lng = Number(tracker.longitude);
    bounds.push([lat, lng]);

    const color = getTrackerColorByMode(tracker, TRACKER_VIEW_MODE);
    const m = L.marker([lat, lng], { icon: markerIcon(color) });
    const displayName = tracker.name || tracker.tracker_code || tracker.tracker_id || "Tracker";
    const displayType = String(tracker.tracker_type || tracker.kind || "—").toUpperCase();
    m.bindPopup(`
      <strong>${displayName}</strong><br>
      Tipo: ${displayType}<br>
      Estado: ${tracker.state_code ?? "—"}<br>
      Ângulo: ${tracker.angle_deg ?? "—"}<br>
      Erro: ${tracker.error_value ?? "—"}<br>
      Status: ${tracker.is_online ? "online" : "offline"}<br>
      Atualização: ${fmtDatePtBR(tracker.last_update)}
    `);
    m.addTo(TRACKERS_MARKERS_LAYER);
  });

  if (!TRACKERS_HAS_FITTED_ONCE) {
    if (TRACKERS_PLANT_BOUNDS &&
        Number.isFinite(Number(TRACKERS_PLANT_BOUNDS.min_lat)) &&
        Number.isFinite(Number(TRACKERS_PLANT_BOUNDS.max_lat)) &&
        Number.isFinite(Number(TRACKERS_PLANT_BOUNDS.min_lng)) &&
        Number.isFinite(Number(TRACKERS_PLANT_BOUNDS.max_lng))) {
      TRACKERS_MAP.fitBounds([
        [Number(TRACKERS_PLANT_BOUNDS.min_lat), Number(TRACKERS_PLANT_BOUNDS.min_lng)],
        [Number(TRACKERS_PLANT_BOUNDS.max_lat), Number(TRACKERS_PLANT_BOUNDS.max_lng)]
      ], { padding: [20, 20] });
    } else if (TRACKERS_PLANT_CENTER &&
        Number.isFinite(Number(TRACKERS_PLANT_CENTER.latitude)) &&
        Number.isFinite(Number(TRACKERS_PLANT_CENTER.longitude))) {
      TRACKERS_MAP.setView([Number(TRACKERS_PLANT_CENTER.latitude), Number(TRACKERS_PLANT_CENTER.longitude)], 18);
    } else if (bounds.length) {
      TRACKERS_MAP.fitBounds(bounds, { padding: [20, 20] });
    }
    TRACKERS_HAS_FITTED_ONCE = true;
  }
}

function renderTrackersPanel() {
  renderTrackersLegend();
  renderTrackersNodes();
}

function setTrackerMode(mode) {
  TRACKER_VIEW_MODE = mode;
  document.getElementById("trackerModeState")?.classList.toggle("is-active", mode === "state");
  document.getElementById("trackerModeAngle")?.classList.toggle("is-active", mode === "angle");
  document.getElementById("trackerModeError")?.classList.toggle("is-active", mode === "error");
  renderTrackersPanel();
}

function filterTrackers(searchText) {
  TRACKERS_FILTER_TEXT = searchText || "";
  renderTrackersNodes();
}

function initTrackersPanel() {
  const sectionEl = document.getElementById("trackersSection");
  const stageWrapEl = document.getElementById("trackersStageWrap");
  const mapEl = document.getElementById("trackersMap");
  if (!sectionEl || !stageWrapEl || !mapEl || typeof L === "undefined") return;
  const tabToggleEl = document.getElementById("trackersTabToggle");
  const menuToggleEl = document.getElementById("trackersMenuToggle");

  if (tabToggleEl) {
    tabToggleEl.addEventListener("click", () => {
      const collapsed = !sectionEl.classList.contains("is-collapsed");
      setTrackersCollapsed(collapsed);
      const expanded = !collapsed;
      if (expanded) applyTrackersTransform();
    });
  }

  if (menuToggleEl) {
    menuToggleEl.addEventListener("click", () => {
      const section = document.getElementById("trackersSection");
      if (!section) return;

      const isHidden = section.classList.contains("trackers-hidden");
      const willShow = isHidden;

      TRACKERS_USER_OPENED = true;

      if (willShow) {
        setTrackersSectionVisible(true);
        setTrackersCollapsed(false);
        TRACKERS_LAST_HAS_DATA = true;
        requestAnimationFrame(() => {
          applyTrackersTransform();
          if (Array.isArray(TRACKERS_DATA) && TRACKERS_DATA.length) {
            renderTrackersPanel();
          }
        });
      } else {
        setTrackersSectionVisible(false);
      }
    });
  }

  TRACKERS_DATA = [];
  TRACKERS_TRANSFORM = { scale: 1, x: 0, y: 0 };
  TRACKERS_HAS_FITTED_ONCE = false;
  TRACKERS_MAP = L.map(mapEl, {
    zoomControl: false,
    attributionControl: false
  }).setView([-14.235, -51.9253], 4);

  L.tileLayer("https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png", {
    subdomains: "abcd",
    maxZoom: 20
  }).addTo(TRACKERS_MAP);
  TRACKERS_MARKERS_LAYER = L.layerGroup().addTo(TRACKERS_MAP);

  document.getElementById("trackerModeState")?.addEventListener("click", () => setTrackerMode("state"));
  document.getElementById("trackerModeAngle")?.addEventListener("click", () => setTrackerMode("angle"));
  document.getElementById("trackerModeError")?.addEventListener("click", () => setTrackerMode("error"));
  document.getElementById("trackersSearchInput")?.addEventListener("input", (e) => filterTrackers(e.target.value));

  document.getElementById("trackersZoomIn")?.addEventListener("click", () => {
    if (TRACKERS_MAP) TRACKERS_MAP.zoomIn();
  });
  document.getElementById("trackersZoomOut")?.addEventListener("click", () => {
    if (TRACKERS_MAP) TRACKERS_MAP.zoomOut();
  });
  document.getElementById("trackersZoomReset")?.addEventListener("click", () => {
    if (!TRACKERS_MAP) return;
    if (TRACKERS_PLANT_BOUNDS &&
      Number.isFinite(Number(TRACKERS_PLANT_BOUNDS.min_lat)) &&
      Number.isFinite(Number(TRACKERS_PLANT_BOUNDS.max_lat)) &&
      Number.isFinite(Number(TRACKERS_PLANT_BOUNDS.min_lng)) &&
      Number.isFinite(Number(TRACKERS_PLANT_BOUNDS.max_lng))) {
      TRACKERS_MAP.fitBounds([
        [Number(TRACKERS_PLANT_BOUNDS.min_lat), Number(TRACKERS_PLANT_BOUNDS.min_lng)],
        [Number(TRACKERS_PLANT_BOUNDS.max_lat), Number(TRACKERS_PLANT_BOUNDS.max_lng)]
      ], { padding: [20, 20] });
    } else if (TRACKERS_PLANT_CENTER &&
      Number.isFinite(Number(TRACKERS_PLANT_CENTER.latitude)) &&
      Number.isFinite(Number(TRACKERS_PLANT_CENTER.longitude))) {
      TRACKERS_MAP.setView([Number(TRACKERS_PLANT_CENTER.latitude), Number(TRACKERS_PLANT_CENTER.longitude)], 18);
    } else {
      TRACKERS_MAP.setView([-14.235, -51.9253], 4);
    }
  });

  renderTrackersPanel();
  setTrackersSectionVisible(false);
  setTrackersCollapsed(true);
}

function scrollPlantSectionTarget(target) {
  if (!target) return;

  if (target === "#sec-trackers") {
    const section = document.getElementById("trackersSection");
    const tab = document.getElementById("trackersTabToggle");
    if (!section) return;

    TRACKERS_USER_OPENED = true;
    setTrackersSectionVisible(true);
    setTrackersCollapsed(false);
    TRACKERS_LAST_HAS_DATA = true;
    tab?.setAttribute("aria-expanded", "true");

    const anchor = document.querySelector(target);
    if (anchor) {
      anchor.scrollIntoView({ behavior: "smooth", block: "start" });
    }

    requestAnimationFrame(() => {
      applyTrackersTransform();
      if (Array.isArray(TRACKERS_DATA) && TRACKERS_DATA.length) {
        renderTrackersPanel();
      }
    });

    return;
  }

  const el = document.querySelector(target);
  if (!el) return;
  el.scrollIntoView({ behavior: "smooth", block: "start" });
}

function setupDeviceNav() {
  const btns = document.querySelectorAll(".device-nav-btn[data-target]");
  if (!btns.length) return;

  btns.forEach((btn) => {
    btn.addEventListener("click", (e) => {
      e.preventDefault();
      scrollPlantSectionTarget(btn.getAttribute("data-target"));
    });
  });

  if (location.hash) {
    const hash = location.hash;
    setTimeout(() => scrollPlantSectionTarget(hash), 0);
  }
}

function buildCommandDeviceOptions() {
  const out = [];
  const seen = new Set();
  const add = (deviceType, deviceId, label) => {
    if (deviceId == null || deviceId === "") return;
    const key = `${deviceType}:${deviceId}`;
    if (seen.has(key)) return;
    seen.add(key);
    out.push({
      deviceType,
      deviceId: String(deviceId),
      label: label || `${String(deviceType).toUpperCase()} ${deviceId}`,
    });
  };

  const inverters = Array.isArray(PLANT_CATALOG.inverters) && PLANT_CATALOG.inverters.length
    ? PLANT_CATALOG.inverters
    : INVERTERS_REALTIME;

  (Array.isArray(inverters) ? inverters : []).forEach((inv, index) => {
    const id = getInverterRealId(inv);
    add("inverter", id, getInverterDisplayName(inv, index));
  });

  const relayId = RELAY_REALTIME?.device_id ?? RELAY_REALTIME?.relay_id ?? null;
  add("relay", relayId, RELAY_REALTIME?.device_name || RELAY_REALTIME?.name || "Relé");

  const meterId = MULTIMETER_REALTIME?.device_id ?? MULTIMETER_REALTIME?.multimeter_id ?? null;
  add("multimeter", meterId, MULTIMETER_REALTIME?.device_name || MULTIMETER_REALTIME?.name || "Multimedidor");

  return out;
}

function ensureCommandDevicePickerModal() {
  if (document.getElementById("cmdDevicePickerOverlay")) return;

  const el = document.createElement("div");
  el.innerHTML = `
    <div id="cmdDevicePickerOverlay" class="cmd-console-overlay hidden" role="dialog" aria-modal="true" aria-label="Selecionar dispositivo para comando">
      <div class="cmd-console cmd-device-picker">
        <div class="cmd-console__header">
          <div class="cmd-console__title-group">
            <div class="cmd-console__icon"><i class="fa-solid fa-terminal"></i></div>
            <div>
              <div class="cmd-console__label">Console de Comandos</div>
              <div class="cmd-console__device-name">Selecione um dispositivo</div>
            </div>
          </div>
          <button class="cmd-console__close" id="cmdDevicePickerClose" aria-label="Fechar seleção">
            <i class="fa-solid fa-xmark"></i>
          </button>
        </div>
        <div id="cmdDevicePickerList" class="cmd-device-picker__list"></div>
      </div>
    </div>
  `;
  document.body.appendChild(el);

  const overlay = document.getElementById("cmdDevicePickerOverlay");
  overlay?.addEventListener("click", (event) => {
    if (event.target === overlay) closeCommandDevicePicker();
  });
  document.getElementById("cmdDevicePickerClose")?.addEventListener("click", closeCommandDevicePicker);
}

function closeCommandDevicePicker() {
  document.getElementById("cmdDevicePickerOverlay")?.classList.add("hidden");
  document.body.style.overflow = "";
}

function openCommandDevicePicker() {
  ensureCommandConsoleModal();
  ensureDeviceCommandModals();
  ensureCommandDevicePickerModal();

  const overlay = document.getElementById("cmdDevicePickerOverlay");
  const list = document.getElementById("cmdDevicePickerList");
  if (!overlay || !list) return;

  const devices = buildCommandDeviceOptions();
  list.innerHTML = "";

  if (!devices.length) {
    list.innerHTML = `
      <div class="cmd-device-picker__empty">
        Nenhum dispositivo com comando real foi encontrado para esta usina.
      </div>
    `;
  } else {
    devices.forEach((device) => {
      const btn = document.createElement("button");
      btn.type = "button";
      btn.className = "cmd-device-picker__item";
      const type = document.createElement("span");
      type.className = "cmd-device-picker__type";
      type.textContent = String(device.deviceType).toUpperCase();
      const label = document.createElement("span");
      label.className = "cmd-device-picker__label";
      label.textContent = device.label;
      const id = document.createElement("span");
      id.className = "cmd-device-picker__id";
      id.textContent = `ID ${device.deviceId}`;
      btn.append(type, label, id);
      btn.addEventListener("click", () => {
        closeCommandDevicePicker();
        openCommandConsole({
          deviceType: device.deviceType,
          deviceId: device.deviceId,
        });
      });
      list.appendChild(btn);
    });
  }

  overlay.classList.remove("hidden");
  document.body.style.overflow = "hidden";
}

function handleInitialPlantAction() {
  const params = new URLSearchParams(window.location.search);
  const action = params.get("action");
  if (action === "command") {
    setTimeout(openCommandDevicePicker, 120);
  }
  // Deep-link to specific device (from robot assistant diagnostics)
  const deviceId = params.get("device_id");
  if (deviceId) {
    setTimeout(() => {
      if (typeof openCabineDetailForInverter === "function") {
        openCabineDetailForInverter(deviceId);
      }
    }, 600);
  }
}

window.PlantActions = {
  ...(window.PlantActions || {}),
  openCommandDevicePicker,
  openCommandConsole,
  scrollToSection: scrollPlantSectionTarget,
};

// ======================================================
// INIT
// ======================================================
/* ================================================================
   ROBOT ASSISTENTE (insights analíticos) — plant.js
   ================================================================ */
const _ROBOT_STATE = {
  issues: [], currentIndex: 0, isTyping: false,
  typingTimer: null, cycleTimer: null, dismissTimer: null,
  reportOpen: false, bubbleVisible: false,
  lastState: null, lastIssueHash: null, userRead: false,
  avatarTimer: null, avatarState: null,
};
const _ROBOT_TYPE_SPEED      = 40;
const _ROBOT_MAX_CHARS       = 110;
const _ROBOT_DISMISS_DELAY   = 12000;
const _ROBOT_AVATAR_GIF_MS   = 10000;
const _ROBOT_AVATAR_MAP = {
  critical: { gif: "img/roboaiotiredgif.gif",  png: "img/roboaiotired.png"  },
  warning:  { gif: "img/roboaiotiidlegif.gif", png: "img/roboaiotiidle.png" },
  gray:     { gif: "img/roboaiotioffgif.gif",  png: "img/roboaiotioff.png"  },
  ok:       { gif: null,                        png: "img/roboaiotiok.png"   },
};

const _ROBOT_NOTIF_PREFS_KEY = "robot_notif_prefs";
const _ROBOT_CAT_OPEN_KEY    = "robot_cat_open";

const _ROBOT_CATEGORY_META = {
  temp_sustained:  { label: "Temperatura elevada",        order: 0 },
  plant_shutdown:  { label: "Usina desligada",            order: 1 },
  pr_declining:    { label: "PR em queda",                order: 2 },
  sub_performance: { label: "Sub-performance",            order: 3 },
  inv_clipping:    { label: "Clipping",                   order: 4 },
  string_zero:     { label: "String zerada",              order: 5 },
  string_low:      { label: "String abaixo da m\u00e9dia", order: 6 },
};

const _ROBOT_CATEGORY_ICONS = {
  temp_sustained:  "fa-temperature-high",
  plant_shutdown:  "fa-power-off",
  pr_declining:    "fa-arrow-trend-down",
  sub_performance: "fa-chart-bar",
  inv_clipping:    "fa-bolt",
  string_zero:     "fa-circle-xmark",
  string_low:      "fa-battery-quarter",
};

function _robotGetOpenCats() {
  try { return JSON.parse(localStorage.getItem(_ROBOT_CAT_OPEN_KEY) || "[]"); }
  catch (_) { return []; }
}

function _robotToggleCat(type) {
  let open = _robotGetOpenCats();
  if (open.includes(type)) open = open.filter(t => t !== type);
  else open.push(type);
  localStorage.setItem(_ROBOT_CAT_OPEN_KEY, JSON.stringify(open));
  return open.includes(type);
}

function _pRobotGetNotifPrefs() {
  try {
    return JSON.parse(localStorage.getItem(_ROBOT_NOTIF_PREFS_KEY) || "{}");
  } catch (_) { return {}; }
}

function _pRobotSaveNotifPrefs(prefs) {
  localStorage.setItem(_ROBOT_NOTIF_PREFS_KEY, JSON.stringify(prefs));
  const user = JSON.parse(localStorage.getItem("user") || "{}");
  if (user.id) {
    const hdrs = { "Content-Type": "application/json" };
    if (user.customer_id) hdrs["X-Customer-Id"] = user.customer_id;
    if (user.is_superuser === true) hdrs["X-Is-Superuser"] = "true";
    if (user.username) hdrs["X-Username"] = user.username;
    fetch(`${API_BASE}/users/notif-prefs`, {
      method: "POST", headers: hdrs,
      body: JSON.stringify({ prefs })
    }).catch(() => {});
  }
}

function _pRobotIsMuted() {
  const prefs = _pRobotGetNotifPrefs();
  return prefs.muted === true;
}

function _pRobotIsTypeEnabled(type) {
  const prefs = _pRobotGetNotifPrefs();
  if (prefs.muted === true) return false;
  if (prefs.disabled_types && prefs.disabled_types.includes(type)) return false;
  return true;
}

function _robotApiFetch(path) {
  const user = JSON.parse(localStorage.getItem("user") || "{}");
  const headers = {};
  if (user.customer_id) headers["X-Customer-Id"] = user.customer_id;
  if (user.is_superuser === true) headers["X-Is-Superuser"] = "true";
  return fetch(`${API_BASE}${path}`, { headers, cache: "no-store" });
}

async function _robotFetchDiag() {
  try {
    const res = await _robotApiFetch("/diagnostics/summary");
    if (!res.ok) return null;
    const data = await res.json();
    if (data && data.body) return typeof data.body === "string" ? JSON.parse(data.body) : data.body;
    return data;
  } catch (e) { console.error("[ROBOT]", e); return null; }
}

function _robotIssuesHash(issues) {
  return issues.map(i => `${i.plant_id}:${i.type}:${i.device_id||i.device_name||""}`).join("|");
}

function _robotFormatTs(ts) {
  if (!ts) return "";
  try {
    const d = new Date(ts);
    if (isNaN(d)) return "";
    return `${String(d.getDate()).padStart(2,"0")}/${String(d.getMonth()+1).padStart(2,"0")} ${String(d.getHours()).padStart(2,"0")}:${String(d.getMinutes()).padStart(2,"0")}`;
  } catch (_) { return ""; }
}

function _robotIssueToText(issue) {
  if (!issue) return "Sem anomalias\ndetectadas \u2713";
  const plant  = issue.plant_name  || "Usina";
  const device = issue.device_name ? ` \u2022 ${issue.device_name}` : "";
  const msg    = issue.message     || "Insight detectado";
  const ts     = _robotFormatTs(issue.ts);
  const prefix = {
    temp_sustained:"\ud83c\udf21", pr_declining:"\ud83d\udcc9", plant_shutdown:"\u26d4",
    inv_clipping:"\u26a1", string_low:"\ud83d\udd0b", string_zero:"\u274c", sub_performance:"\ud83d\udcca"
  }[issue.type] || "\ud83d\udcca";
  return `${prefix} ${plant}${device}\n${msg}${ts ? "\n\ud83d\udd52 " + ts : ""}`;
}

function _robotBuildUrl(issue) {
  if (!issue.plant_id) return null;
  let url = "plant.html?plant_id=" + encodeURIComponent(issue.plant_id);
  if (issue.device_id) url += "&device_id=" + encodeURIComponent(issue.device_id);
  const invTypes = ["temp_sustained","inv_clipping","string_low","string_zero","sub_performance"];
  if (issue.device_type === "inverter" || invTypes.includes(issue.type)) url += "#sec-inverters";
  return url;
}

function _robotGetState(issues) {
  if (!issues || !issues.length) return "ok";
  const nonShutdown = issues.filter(i => i.type !== "plant_shutdown");
  // Só plant_shutdown → usina desligada → robô cinza/off
  if (nonShutdown.length === 0) return "gray";
  // Demais: vermelho (critical) ou amarelo (warning)
  if (nonShutdown.some(i => i.severity === "critical")) return "critical";
  return "warning";
}

function _robotTypewrite(text, onDone) {
  const textEl = document.getElementById("robotBubbleText");
  const cursorEl = document.getElementById("robotBubbleCursor");
  if (!textEl || !cursorEl) return;
  if (_ROBOT_STATE.typingTimer) { clearInterval(_ROBOT_STATE.typingTimer); _ROBOT_STATE.typingTimer = null; }
  _ROBOT_STATE.isTyping = true;
  cursorEl.classList.remove("hidden");
  textEl.textContent = "";
  const txt = text.length > _ROBOT_MAX_CHARS ? text.slice(0, _ROBOT_MAX_CHARS) + "..." : text;
  let ci = 0;
  _ROBOT_STATE.typingTimer = setInterval(() => {
    if (ci < txt.length) { textEl.textContent = txt.slice(0, ++ci); }
    else {
      clearInterval(_ROBOT_STATE.typingTimer); _ROBOT_STATE.typingTimer = null;
      _ROBOT_STATE.isTyping = false; cursorEl.classList.add("hidden");
      if (onDone) onDone();
    }
  }, _ROBOT_TYPE_SPEED);
}

function _robotSetBubbleOk() {
  const textEl = document.getElementById("robotBubbleText");
  const cursorEl = document.getElementById("robotBubbleCursor");
  if (textEl) textEl.textContent = "Ok!";
  if (cursorEl) cursorEl.classList.add("hidden");
  _robotUpdateExpandBtn(false);
  _ROBOT_STATE.userRead = true;
}

function _robotDismissBubble() {
  _robotSetBubbleOk();
  if (_ROBOT_STATE.dismissTimer) { clearTimeout(_ROBOT_STATE.dismissTimer); _ROBOT_STATE.dismissTimer = null; }
}

function _robotScheduleDismiss() {
  if (_ROBOT_STATE.dismissTimer) clearTimeout(_ROBOT_STATE.dismissTimer);
  _ROBOT_STATE.dismissTimer = setTimeout(() => {
    if (!_ROBOT_STATE.reportOpen && _ROBOT_STATE.bubbleVisible) {
      _robotShowBubble(false);
      if (_ROBOT_STATE.dismissTimer) { clearTimeout(_ROBOT_STATE.dismissTimer); _ROBOT_STATE.dismissTimer = null; }
    }
  }, _ROBOT_DISMISS_DELAY);
}

function _robotUpdateAvatar(state, overrideSrc) {
  const img = document.getElementById("robotImg");
  if (!img) return;
  if (_ROBOT_STATE.avatarTimer) { clearTimeout(_ROBOT_STATE.avatarTimer); _ROBOT_STATE.avatarTimer = null; }
  if (overrideSrc) {
    if (!img.src.endsWith(overrideSrc)) img.src = overrideSrc;
    _ROBOT_STATE.avatarState = state + "_override";
    return;
  }
  const map = _ROBOT_AVATAR_MAP[state] || _ROBOT_AVATAR_MAP.ok;
  if (map.gif && _ROBOT_STATE.avatarState !== state) {
    img.src = map.gif;
    _ROBOT_STATE.avatarState = state;
    _ROBOT_STATE.avatarTimer = setTimeout(() => {
      _ROBOT_STATE.avatarTimer = null;
      img.src = map.png;
      img.classList.remove("robot-squish");
      void img.offsetWidth;
      img.classList.add("robot-squish");
      img.addEventListener("animationend", () => img.classList.remove("robot-squish"), { once: true });
    }, _ROBOT_AVATAR_GIF_MS);
  } else if (!map.gif && _ROBOT_STATE.avatarState !== state) {
    img.src = map.png;
    _ROBOT_STATE.avatarState = state;
  }
}

function _robotUpdateBubble(state) {
  const el = document.getElementById("robotBubble");
  if (!el) return;
  el.classList.remove("state-ok","state-warning","state-critical","state-gray");
  el.classList.add("state-" + state);
}

function _robotShowBubble(show) {
  const el = document.getElementById("robotBubble");
  if (el) el.classList.toggle("visible", !!show);
  _ROBOT_STATE.bubbleVisible = !!show;
}

function _robotUpdateBadge(n) {
  const el = document.getElementById("robotBadge");
  if (!el) return;
  if (n > 0) { el.textContent = n > 99 ? "99+" : n; el.classList.remove("hidden"); }
  else el.classList.add("hidden");
}

function _robotUpdateExpandBtn(show) {
  const el = document.getElementById("robotBubbleExpand");
  if (el) el.classList.toggle("visible", !!show);
}

function _robotStartCycle() {
  if (_ROBOT_STATE.cycleTimer) { clearTimeout(_ROBOT_STATE.cycleTimer); _ROBOT_STATE.cycleTimer = null; }
  const issues = _ROBOT_STATE.issues;
  if (!issues.length) return;
  if (_ROBOT_STATE.reportOpen || _ROBOT_STATE.userRead) return;
  _robotTypewrite(_robotIssueToText(issues[0]), () => _robotScheduleDismiss());
}

function _robotRenderReport(issues) {
  const list = document.getElementById("robotReportList");
  if (!list) return;
  if (!issues || !issues.length) {
    list.innerHTML = '<div class="robot-report-empty">Sem anomalias\ndetectadas \u2713</div>';
    _pRobotRenderPrefsFooter();
    return;
  }

  const groups = {};
  issues.forEach(iss => {
    if (!groups[iss.type]) groups[iss.type] = [];
    groups[iss.type].push(iss);
  });

  const types = Object.keys(groups).sort((a, b) => {
    const oa = (_ROBOT_CATEGORY_META[a] || { order: 99 }).order;
    const ob = (_ROBOT_CATEGORY_META[b] || { order: 99 }).order;
    return oa - ob;
  });

  const openCats = _robotGetOpenCats();

  const html = types.map(type => {
    const meta   = _ROBOT_CATEGORY_META[type] || { label: "Outros" };
    const icon   = _ROBOT_CATEGORY_ICONS[type] || "fa-list";
    const items  = groups[type];
    const isOpen = openCats.includes(type);
    const worst  = items.some(i => i.severity === "critical") ? "critical"
                 : items.some(i => i.severity === "warning")  ? "warning" : "info";

    const rows = items.map(iss => {
      const sc = "sev-" + (iss.severity || "info");
      const deviceUrl = _robotBuildUrl(iss);
      let deviceLine = iss.device_name || "";
      if (iss.cabin_name) deviceLine += (deviceLine ? " \u2022 " : "") + iss.cabin_name;
      const tsStr = _robotFormatTs(iss.ts);
      return `<div class="robot-cat-item" data-href="${deviceUrl || ''}">
        <span class="robot-issue-dot ${sc}"></span>
        <div class="robot-cat-item-body">
          <div class="robot-cat-item-plant">${iss.plant_name || "\u2014"}${tsStr ? ` <span class="robot-issue-ts">${tsStr}</span>` : ""}</div>
          <div class="robot-cat-item-msg">${iss.message || "\u2014"}</div>
          ${deviceLine ? `<div class="robot-cat-item-device">${deviceLine}</div>` : ""}
        </div>
        ${deviceUrl ? '<i class="fa-solid fa-arrow-up-right-from-square robot-cat-item-go"></i>' : ''}
      </div>`;
    }).join("");

    return `<div class="robot-cat-block ${isOpen ? "is-open" : ""}" data-cat="${type}">
      <button type="button" class="robot-cat-header" data-cat="${type}">
        <i class="fa-solid ${icon} robot-cat-icon sev-icon-${worst}"></i>
        <span class="robot-cat-label">${meta.label}</span>
        <span class="robot-cat-count">${items.length}</span>
        <i class="fa-solid fa-chevron-down robot-cat-chevron"></i>
      </button>
      <div class="robot-cat-items">${rows}</div>
    </div>`;
  }).join("");

  list.innerHTML = html;

  list.querySelectorAll(".robot-cat-header").forEach(btn => {
    btn.addEventListener("click", () => {
      const type  = btn.dataset.cat;
      const block = list.querySelector(`.robot-cat-block[data-cat="${type}"]`);
      const nowOpen = _robotToggleCat(type);
      if (block) block.classList.toggle("is-open", nowOpen);
    });
  });

  list.querySelectorAll(".robot-cat-item[data-href]").forEach(item => {
    if (item.dataset.href) {
      item.style.cursor = "pointer";
      item.addEventListener("click", () => { window.location.href = item.dataset.href; });
    }
  });

  _pRobotRenderPrefsFooter();
}

function _robotToggleReport(forceOpen) {
  const panel = document.getElementById("robotReport");
  const badge = document.getElementById("robotBadge");
  if (!panel) return;
  const open = forceOpen !== undefined ? forceOpen : !_ROBOT_STATE.reportOpen;
  if (open) {
    _robotRenderReport(_ROBOT_STATE.issues);
    _pRobotRenderPrefsFooter();
    panel.classList.remove("hidden");
    if (badge) badge.classList.add("hidden");
    _ROBOT_STATE.reportOpen = true;
    if (_ROBOT_STATE.cycleTimer) { clearTimeout(_ROBOT_STATE.cycleTimer); _ROBOT_STATE.cycleTimer = null; }
    if (_ROBOT_STATE.dismissTimer) { clearTimeout(_ROBOT_STATE.dismissTimer); _ROBOT_STATE.dismissTimer = null; }
  } else {
    panel.classList.add("hidden");
    panel.classList.remove("ronda-expanded");
    panel.style.width = ""; panel.style.maxHeight = "";
    _rondaSwitchTab("diag");
    if (badge && _ROBOT_STATE.issues.length > 0) badge.classList.remove("hidden");
    _ROBOT_STATE.reportOpen = false;
    _robotDismissBubble();
    _robotUpdateAvatar("ok", "img/roboaiotiok.png");
  }
}

async function _robotRefresh() {
  if (_pRobotIsMuted()) {
    _pRobotApplyMutedState();
    return;
  }
  _pRobotRemoveMutedState();
  const data = await _robotFetchDiag();
  if (!data) return;
  const issues = (Array.isArray(data.issues) ? data.issues : [])
    .filter(i => _pRobotIsTypeEnabled(i.type));
  const newHash = _robotIssuesHash(issues);
  const issuesChanged = newHash !== _ROBOT_STATE.lastIssueHash;
  _ROBOT_STATE.lastIssueHash = newHash;
  _ROBOT_STATE.issues = issues;
  const state = _robotGetState(issues);

  _robotUpdateBubble(state);
  _robotUpdateBadge(issues.length);
  _robotUpdateExpandBtn(issues.length > 1);

  if (!issues.length) {
    _ROBOT_STATE.currentIndex = 0;
    if (!_ROBOT_STATE.bubbleVisible || _ROBOT_STATE.lastState !== "ok") {
      _ROBOT_STATE.userRead = false;
      _robotUpdateAvatar("ok");
      _robotShowBubble(true);
      _robotTypewrite("Sem anomalias\ndetectadas \u2713", () => _robotScheduleDismiss());
    }
    _ROBOT_STATE.lastState = "ok";
    return;
  }

  _ROBOT_STATE.lastState = state;
  _robotShowBubble(true);
  if (_ROBOT_STATE.reportOpen) _robotRenderReport(issues);

  if (issuesChanged) {
    _ROBOT_STATE.userRead = false;
    _ROBOT_STATE.currentIndex = 0;
    _robotUpdateAvatar(state);
    _robotStartCycle();
  }
}

function _pRobotApplyMutedState() {
  _robotShowBubble(false);
  _robotUpdateBadge(0);
  const img = document.getElementById("robotImg");
  if (img) img.src = "img/roboaiotipeek.png";
  const container = document.getElementById("robotAssistant");
  if (container) container.classList.add("robot-muted-peek");
  const panel = document.getElementById("robotReport");
  if (panel) panel.classList.add("hidden");
  _ROBOT_STATE.reportOpen = false;
}

function _pRobotRemoveMutedState() {
  const container = document.getElementById("robotAssistant");
  if (container) container.classList.remove("robot-muted-peek");
}

const _ROBOT_TYPE_LABELS = {
  temp_sustained:  '<i class="fa-solid fa-temperature-high"></i> Temperatura elevada',
  plant_shutdown:  '<i class="fa-solid fa-power-off"></i> Usina desligada',
  pr_declining:    '<i class="fa-solid fa-arrow-trend-down"></i> PR em queda',
  sub_performance: '<i class="fa-solid fa-chart-bar"></i> Sub-performance',
  inv_clipping:    '<i class="fa-solid fa-bolt"></i> Clipping',
  string_zero:     '<i class="fa-solid fa-circle-xmark"></i> String zerada',
  string_low:      '<i class="fa-solid fa-battery-quarter"></i> String abaixo da m\u00e9dia',
};

function _pRobotRenderPrefsFooter() {
  const panel = document.getElementById("robotReport");
  if (!panel) return;
  let footer = panel.querySelector(".robot-prefs-footer");
  if (!footer) {
    footer = document.createElement("div");
    footer.className = "robot-prefs-footer";
    panel.appendChild(footer);
  }
  const prefs = _pRobotGetNotifPrefs();
  const isMuted = prefs.muted === true;
  const disabled = prefs.disabled_types || [];

  footer.innerHTML = `
    <div class="robot-prefs-section">
      <div class="robot-prefs-title">Prefer\u00eancias de notifica\u00e7\u00e3o</div>
      <label class="robot-prefs-toggle">
        <input type="checkbox" id="robotPrefMuteAll" ${isMuted ? "checked" : ""}>
        <span>Silenciar todas as notifica\u00e7\u00f5es</span>
      </label>
      <div class="robot-prefs-types ${isMuted ? "robot-prefs-types--disabled" : ""}" id="robotPrefTypesList">
        ${Object.entries(_ROBOT_TYPE_LABELS).map(([type, label]) => `
          <label class="robot-prefs-toggle robot-prefs-toggle--sub">
            <input type="checkbox" class="robot-pref-type-cb"
                   data-type="${type}"
                   ${disabled.includes(type) ? "" : "checked"}
                   ${isMuted ? "disabled" : ""}>
            <span>${label}</span>
          </label>`).join("")}
      </div>
    </div>`;

  const muteAll = footer.querySelector("#robotPrefMuteAll");
  muteAll?.addEventListener("change", () => {
    const p = _pRobotGetNotifPrefs();
    p.muted = muteAll.checked;
    _pRobotSaveNotifPrefs(p);
    _pRobotRenderPrefsFooter();
    if (p.muted) {
      _pRobotApplyMutedState();
    } else {
      _pRobotRemoveMutedState();
      _robotRefresh();
    }
  });

  footer.querySelectorAll(".robot-pref-type-cb").forEach(cb => {
    cb.addEventListener("change", () => {
      const p = _pRobotGetNotifPrefs();
      let dis = p.disabled_types || [];
      const type = cb.dataset.type;
      if (cb.checked) {
        dis = dis.filter(t => t !== type);
      } else {
        if (!dis.includes(type)) dis.push(type);
      }
      p.disabled_types = dis;
      _pRobotSaveNotifPrefs(p);
    });
  });
}

/* ── Ronda Diária (tab no robô) ── */
let _RONDA_DATA = null;
let _RONDA_LOADING = false;

function _rondaActiveTab() {
  const active = document.querySelector(".robot-tab.active");
  return active ? active.dataset.tab : "diag";
}

function _rondaSwitchTab(tab) {
  document.querySelectorAll(".robot-tab").forEach(b => b.classList.toggle("active", b.dataset.tab === tab));
  const list = document.getElementById("robotReportList");
  const ronda = document.getElementById("robotRondaContent");
  const panel = document.getElementById("robotReport");
  const prefsFooter = panel ? panel.querySelector(".robot-prefs-footer") : null;
  if (tab === "diag") {
    if (list) list.classList.remove("hidden");
    if (ronda) ronda.classList.add("hidden");
    if (prefsFooter) prefsFooter.style.display = "";
    if (panel) { panel.classList.remove("ronda-expanded"); panel.style.width = ""; panel.style.maxHeight = ""; }
  } else {
    if (list) list.classList.add("hidden");
    if (ronda) { ronda.classList.remove("hidden"); ronda.scrollTop = 0; }
    if (prefsFooter) prefsFooter.style.display = "none";
    if (panel) panel.classList.add("ronda-expanded");
    if (!_RONDA_DATA && !_RONDA_LOADING) _rondaFetchAndRender();
  }
}

async function _rondaFetchAndRender(dateStr) {
  const el = document.getElementById("robotRondaContent");
  if (!el || !PLANT_ID) return;
  _RONDA_LOADING = true;
  el.innerHTML = '<div class="ronda-loading"><i class="fa-solid fa-spinner fa-spin"></i><br>Carregando ronda...</div>';
  try {
    let url = `${API_BASE}/plants/${PLANT_ID}/realtime?view=daily-round`;
    if (dateStr) url += `&date=${dateStr}`;
    const user = JSON.parse(localStorage.getItem("user") || "{}");
    const headers = {};
    if (user.customer_id) headers["X-Customer-Id"] = user.customer_id;
    if (user.is_superuser === true) headers["X-Is-Superuser"] = "true";
    const res = await fetch(url, { headers, cache: "no-store" });
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    let data = await res.json();
    if (data && data.body) data = typeof data.body === "string" ? JSON.parse(data.body) : data.body;
    _RONDA_DATA = data;
    _rondaRender(data, el);
  } catch (e) {
    console.error("[RONDA]", e);
    el.innerHTML = `<div class="ronda-error"><i class="fa-solid fa-triangle-exclamation"></i> Erro ao carregar: ${e.message}</div>`;
  } finally {
    _RONDA_LOADING = false;
  }
}

function _rondaFmt(v, dec) {
  if (v == null || v === "") return "—";
  const n = Number(v);
  return isNaN(n) ? String(v) : n.toLocaleString("pt-BR", { minimumFractionDigits: dec || 0, maximumFractionDigits: dec || 0 });
}

function _rondaPerfClass(cls) {
  if (!cls || cls === "sem_dados") return "";
  return `ronda-perf-${cls}`;
}

function _rondaPerfLabel(cls) {
  const map = { acima: "Acima", normal: "Normal", abaixo: "Abaixo", sem_dados: "—" };
  return map[cls] || cls || "—";
}

function _rondaRender(data, el) {
  if (!data) return;
  const ps = data.plant_summary || {};
  const w = data.weather || {};
  const invs = data.inverters || [];
  const sb = data.string_box || [];
  const alarms = data.alarms || [];

  const dateVal = data.date || "";
  const yesterday = new Date();
  yesterday.setDate(yesterday.getDate() - 1);
  const defaultDate = yesterday.toISOString().slice(0, 10);

  let html = "";

  // Date picker + toolbar
  html += `<div class="ronda-toolbar" style="border-top:none; border-bottom:1px solid rgba(255,255,255,0.06);">
    <input type="date" class="ronda-date-picker" id="rondaDatePicker" value="${dateVal || defaultDate}" max="${new Date().toISOString().slice(0,10)}">
    <button class="ronda-btn" id="rondaDownloadCsv"><i class="fa-solid fa-download"></i> CSV</button>
    <button class="ronda-expand-btn" id="rondaExpandBtn"><i class="fa-solid fa-expand"></i> Expandir</button>
  </div>`;

  // Plant summary
  html += `<div class="ronda-section">
    <div class="ronda-section-title"><i class="fa-solid fa-solar-panel"></i> Resumo da Usina</div>
    <div class="ronda-kpi-grid">
      <div class="ronda-kpi"><span class="ronda-kpi-label">Geração</span><span class="ronda-kpi-value">${_rondaFmt(ps.generation_kwh, 1)} kWh</span></div>
      <div class="ronda-kpi"><span class="ronda-kpi-label">PR Diário</span><span class="ronda-kpi-value ${(ps.pr_daily_pct || 0) >= 75 ? "val-good" : (ps.pr_daily_pct || 0) >= 60 ? "val-warn" : "val-bad"}">${_rondaFmt(ps.pr_daily_pct, 1)}%</span></div>
      <div class="ronda-kpi"><span class="ronda-kpi-label">PR Acumulado</span><span class="ronda-kpi-value">${_rondaFmt(ps.pr_accumulated_pct, 1)}%</span></div>
      <div class="ronda-kpi"><span class="ronda-kpi-label">Fator Capac.</span><span class="ronda-kpi-value">${_rondaFmt(ps.capacity_factor_daily_pct, 1)}%</span></div>
      <div class="ronda-kpi"><span class="ronda-kpi-label">Início Geração</span><span class="ronda-kpi-value">${ps.gen_start_time || "—"}</span></div>
      <div class="ronda-kpi"><span class="ronda-kpi-label">Fim Geração</span><span class="ronda-kpi-value">${ps.gen_end_time || "—"}</span></div>
    </div>
  </div>`;

  // Weather
  const irradCls = w.irradiance_classification;
  html += `<div class="ronda-section">
    <div class="ronda-section-title"><i class="fa-solid fa-cloud-sun"></i> Estação Solarimétrica</div>
    <div class="ronda-kpi-grid">
      <div class="ronda-kpi"><span class="ronda-kpi-label">Irrad. Média</span><span class="ronda-kpi-value">${_rondaFmt(w.irradiance_avg_wm2, 1)} W/m²</span></div>
      <div class="ronda-kpi"><span class="ronda-kpi-label">Irrad. Máx</span><span class="ronda-kpi-value">${_rondaFmt(w.irradiance_max_wm2, 1)} W/m²</span></div>
      <div class="ronda-kpi"><span class="ronda-kpi-label">Temp. Média</span><span class="ronda-kpi-value">${_rondaFmt(w.air_temp_avg_c, 1)} °C</span></div>
      <div class="ronda-kpi"><span class="ronda-kpi-label">Temp. Máx</span><span class="ronda-kpi-value">${_rondaFmt(w.air_temp_max_c, 1)} °C</span></div>
      <div class="ronda-kpi"><span class="ronda-kpi-label">Vento Méd</span><span class="ronda-kpi-value">${_rondaFmt(w.wind_speed_avg, 1)} m/s</span></div>
      <div class="ronda-kpi"><span class="ronda-kpi-label">Chuva</span><span class="ronda-kpi-value">${w.rain_detected ? "Sim" : "Não"}</span></div>
    </div>
  </div>`;

  // Inverters table
  if (invs.length) {
    html += `<div class="ronda-section">
      <div class="ronda-section-title"><i class="fa-solid fa-bolt"></i> Inversores</div>
      <div style="overflow-x:auto;">
      <table class="ronda-inv-table">
        <thead><tr>
          <th>Inv</th><th>Pot Méd</th><th>Energia</th><th>PR</th><th>Perf.</th><th>vs Média</th>
        </tr></thead>
        <tbody>`;
    invs.forEach(inv => {
      const perfCls = _rondaPerfClass(inv.power_performance);
      const prCls = _rondaPerfClass(inv.pr_vs_fleet);
      html += `<tr>
        <td>${inv.inverter_name || "Inv" + inv.device_id}</td>
        <td>${_rondaFmt(inv.avg_power_kw, 1)} kW</td>
        <td>${_rondaFmt(inv.energy_daily_kwh, 0)} kWh</td>
        <td>${_rondaFmt(inv.pr_inverter_pct, 1)}%</td>
        <td><span class="ronda-perf-badge ${perfCls}">${_rondaPerfLabel(inv.power_performance)}</span></td>
        <td><span class="ronda-perf-badge ${prCls}">${_rondaPerfLabel(inv.pr_vs_fleet)}</span></td>
      </tr>`;
    });
    html += `</tbody></table></div></div>`;
  }

  // String box health
  if (sb.length) {
    const grouped = {};
    sb.forEach(s => {
      const key = s.device_name || s.inverter_name || ("Inv" + s.device_id);
      if (!grouped[key]) grouped[key] = { items: [], health: s.health_pct };
      grouped[key].items.push(s);
      grouped[key].health = s.health_pct;
    });
    html += `<div class="ronda-section">
      <div class="ronda-section-title"><i class="fa-solid fa-plug-circle-check"></i> Saúde String Box</div>`;
    Object.entries(grouped).forEach(([name, g]) => {
      const hp = g.health != null ? Number(g.health) : 100;
      const hCls = hp >= 80 ? "health-good" : hp >= 50 ? "health-mid" : "health-bad";
      html += `<div class="ronda-string-row">
        <span style="width:70px;font-size:10.5px;color:rgba(255,255,255,0.55);flex-shrink:0;">${name}</span>
        <div class="ronda-string-bar"><div class="ronda-string-fill ${hCls}" style="width:${Math.min(100, hp)}%;"></div></div>
        <span style="width:36px;text-align:right;font-size:10.5px;font-weight:700;color:${hp >= 80 ? '#39e58c' : hp >= 50 ? '#eab308' : '#ef4444'}">${_rondaFmt(hp, 0)}%</span>
      </div>`;
    });
    html += `</div>`;
  }

  // Alarms
  if (alarms.length) {
    html += `<div class="ronda-section">
      <div class="ronda-section-title"><i class="fa-solid fa-bell"></i> Alarmes (${data.alarm_count || alarms.length})</div>`;
    const shown = alarms.slice(0, 20);
    shown.forEach(a => {
      const sevCls = (a.severity === "high" || a.severity === "critical") ? "sev-high" : a.severity === "medium" ? "sev-medium" : "sev-low";
      const ts = a.timestamp ? a.timestamp.replace(/T/, " ").slice(0, 19) : "";
      html += `<div class="ronda-alarm-row">
        <div class="ronda-alarm-dot ${sevCls}"></div>
        <div class="ronda-alarm-body">
          <div class="ronda-alarm-device">${a.device_name || "—"}</div>
          <div class="ronda-alarm-desc">${a.description || a.code || "—"}</div>
          <div class="ronda-alarm-ts">${ts}</div>
        </div>
      </div>`;
    });
    if (alarms.length > 20) {
      html += `<div style="text-align:center;font-size:10px;color:rgba(255,255,255,0.4);padding:6px;">+${alarms.length - 20} alarmes</div>`;
    }
    html += `</div>`;
  }

  el.innerHTML = html;

  // Wire date picker
  const picker = document.getElementById("rondaDatePicker");
  if (picker) {
    picker.addEventListener("change", () => {
      _RONDA_DATA = null;
      _rondaFetchAndRender(picker.value);
    });
  }

  // Wire download CSV
  const dlBtn = document.getElementById("rondaDownloadCsv");
  if (dlBtn) dlBtn.addEventListener("click", () => _rondaDownloadCsv(data));

  // Wire expand to fullpanel
  const expBtn = document.getElementById("rondaExpandBtn");
  if (expBtn) expBtn.addEventListener("click", () => _rondaOpenFullPanel(data));
}

function _rondaOpenFullPanel(data) {
  const panel = document.getElementById("rondaFullPanel");
  if (!panel || !data) return;
  panel.classList.remove("hidden");
  document.body.style.overflow = "hidden";

  const ps = data.plant_summary || {};
  const w = data.weather || {};
  const invs = data.inverters || [];
  const sb = data.string_box || [];
  const alarms = data.alarms || [];

  const nameEl = document.getElementById("rondaFullPlantName");
  if (nameEl) nameEl.textContent = ps.power_plant_name ? `— ${ps.power_plant_name}` : "";

  const datePicker = document.getElementById("rondaFullDatePicker");
  if (datePicker) {
    datePicker.value = data.date || "";
    datePicker.max = new Date().toISOString().slice(0, 10);
    datePicker.onchange = () => {
      _RONDA_DATA = null;
      _rondaFetchFullPanel(datePicker.value);
    };
  }

  const dlBtn = document.getElementById("rondaFullDownloadCsv");
  if (dlBtn) dlBtn.onclick = () => _rondaDownloadCsv(data);

  const closeBtn = document.getElementById("rondaFullClose");
  if (closeBtn) closeBtn.onclick = () => {
    panel.classList.add("hidden");
    document.body.style.overflow = "";
  };

  const svgSolar = '<svg viewBox="0 0 24 24" fill="none" stroke="#facc15" stroke-width="2"><circle cx="12" cy="12" r="4"/><path d="M12 2v2M12 20v2M4.93 4.93l1.41 1.41M17.66 17.66l1.41 1.41M2 12h2M20 12h2M4.93 19.07l1.41-1.41M17.66 6.34l1.41-1.41"/></svg>';
  const svgWeather = '<svg viewBox="0 0 24 24" fill="none" stroke="#60a5fa" stroke-width="2"><path d="M3 15a4 4 0 0 0 4 4h9a5 5 0 0 0 .5-9.97A7 7 0 0 0 3 11.5"/><path d="M9.17 12A5 5 0 0 1 16.5 9.03"/></svg>';
  const svgBolt = '<svg viewBox="0 0 24 24" fill="none" stroke="#39e58c" stroke-width="2"><path d="M13 2L3 14h9l-1 8 10-12h-9l1-8z"/></svg>';
  const svgString = '<svg viewBox="0 0 24 24" fill="none" stroke="#a855f7" stroke-width="2"><rect x="2" y="7" width="20" height="10" rx="2"/><path d="M6 7V5a2 2 0 0 1 2-2h8a2 2 0 0 1 2 2v2M6 17v2a2 2 0 0 0 2 2h8a2 2 0 0 0 2-2v-2"/></svg>';
  const svgAlarm = '<svg viewBox="0 0 24 24" fill="none" stroke="#ef4444" stroke-width="2"><path d="M18 8A6 6 0 0 0 6 8c0 7-3 9-3 9h18s-3-2-3-9"/><path d="M13.73 21a2 2 0 0 1-3.46 0"/></svg>';

  const prCls = (ps.pr_daily_pct || 0) >= 75 ? "val-good" : (ps.pr_daily_pct || 0) >= 60 ? "val-warn" : "val-bad";

  let body = '<div class="ronda-full-grid">';

  // Card: Plant Summary
  body += `<div class="ronda-card">
    <div class="ronda-card-header">
      <div class="ronda-card-icon icon-solar">${svgSolar}</div>
      <div><div class="ronda-card-title">Resumo da Usina</div><div class="ronda-card-subtitle">${data.date || ""}</div></div>
    </div>
    <div class="ronda-card-body">
      <div class="ronda-full-kpi-row">
        <div class="ronda-full-kpi"><div class="ronda-full-kpi-label">Geração</div><div class="ronda-full-kpi-value">${_rondaFmt(ps.generation_kwh, 1)}<span class="ronda-full-kpi-unit">kWh</span></div></div>
        <div class="ronda-full-kpi"><div class="ronda-full-kpi-label">PR Diário</div><div class="ronda-full-kpi-value ${prCls}">${_rondaFmt(ps.pr_daily_pct, 1)}<span class="ronda-full-kpi-unit">%</span></div></div>
        <div class="ronda-full-kpi"><div class="ronda-full-kpi-label">PR Acum.</div><div class="ronda-full-kpi-value">${_rondaFmt(ps.pr_accumulated_pct, 1)}<span class="ronda-full-kpi-unit">%</span></div></div>
      </div>
      <div class="ronda-full-kpi-row" style="margin-top:8px;">
        <div class="ronda-full-kpi"><div class="ronda-full-kpi-label">Fator Capac.</div><div class="ronda-full-kpi-value">${_rondaFmt(ps.capacity_factor_daily_pct, 1)}<span class="ronda-full-kpi-unit">%</span></div></div>
        <div class="ronda-full-kpi"><div class="ronda-full-kpi-label">Início</div><div class="ronda-full-kpi-value" style="font-size:16px;">${ps.gen_start_time || "—"}</div></div>
        <div class="ronda-full-kpi"><div class="ronda-full-kpi-label">Fim</div><div class="ronda-full-kpi-value" style="font-size:16px;">${ps.gen_end_time || "—"}</div></div>
      </div>
    </div>
  </div>`;

  // Card: Weather
  body += `<div class="ronda-card">
    <div class="ronda-card-header">
      <div class="ronda-card-icon icon-weather">${svgWeather}</div>
      <div><div class="ronda-card-title">Estação Solarimétrica</div><div class="ronda-card-subtitle">${w.irradiance_classification ? "Irradiância: " + w.irradiance_classification : ""}</div></div>
    </div>
    <div class="ronda-card-body">
      <div class="ronda-full-kpi-row">
        <div class="ronda-full-kpi"><div class="ronda-full-kpi-label">Irrad. Média</div><div class="ronda-full-kpi-value">${_rondaFmt(w.irradiance_avg_wm2, 0)}<span class="ronda-full-kpi-unit">W/m²</span></div></div>
        <div class="ronda-full-kpi"><div class="ronda-full-kpi-label">Irrad. Máx</div><div class="ronda-full-kpi-value">${_rondaFmt(w.irradiance_max_wm2, 0)}<span class="ronda-full-kpi-unit">W/m²</span></div></div>
        <div class="ronda-full-kpi"><div class="ronda-full-kpi-label">Temp. Média</div><div class="ronda-full-kpi-value">${_rondaFmt(w.air_temp_avg_c, 1)}<span class="ronda-full-kpi-unit">°C</span></div></div>
      </div>
      <div class="ronda-full-kpi-row" style="margin-top:8px;">
        <div class="ronda-full-kpi"><div class="ronda-full-kpi-label">Temp. Máx</div><div class="ronda-full-kpi-value">${_rondaFmt(w.air_temp_max_c, 1)}<span class="ronda-full-kpi-unit">°C</span></div></div>
        <div class="ronda-full-kpi"><div class="ronda-full-kpi-label">Vento</div><div class="ronda-full-kpi-value">${_rondaFmt(w.wind_speed_avg, 1)}<span class="ronda-full-kpi-unit">m/s</span></div></div>
        <div class="ronda-full-kpi"><div class="ronda-full-kpi-label">Chuva</div><div class="ronda-full-kpi-value">${w.rain_detected ? "\u{1F327}️ Sim" : "☀️ Não"}</div></div>
      </div>
    </div>
  </div>`;

  // Card: Inverters (full width)
  if (invs.length) {
    body += `<div class="ronda-card span-full">
      <div class="ronda-card-header">
        <div class="ronda-card-icon icon-bolt">${svgBolt}</div>
        <div><div class="ronda-card-title">Inversores</div><div class="ronda-card-subtitle">${invs.length} unidades</div></div>
      </div>
      <div class="ronda-card-body" style="padding:0;">
        <div style="overflow-x:auto;">
        <table class="ronda-full-inv-table">
          <thead><tr>
            <th>Inversor</th><th>Pot. Média</th><th>Pot. Máx</th><th>Energia</th><th>PR</th><th>Temp. Média</th><th>Performance</th><th>vs Média (Pot)</th><th>vs Média (PR)</th><th>Disponib.</th>
          </tr></thead>
          <tbody>`;
    invs.forEach(inv => {
      const perfCls = inv.power_performance && inv.power_performance !== "sem_dados" ? `ronda-full-perf-${inv.power_performance}` : "";
      const fleetPCls = inv.power_vs_fleet && inv.power_vs_fleet !== "sem_dados" ? `ronda-full-perf-${inv.power_vs_fleet}` : "";
      const fleetPrCls = inv.pr_vs_fleet && inv.pr_vs_fleet !== "sem_dados" ? `ronda-full-perf-${inv.pr_vs_fleet}` : "";
      const perfArrow = inv.power_performance === "acima" ? "▲" : inv.power_performance === "abaixo" ? "▼" : "";
      body += `<tr>
        <td style="font-weight:600;">${inv.inverter_name || "Inv" + inv.device_id}</td>
        <td>${_rondaFmt(inv.avg_power_kw, 1)} kW</td>
        <td>${_rondaFmt(inv.max_power_kw, 1)} kW</td>
        <td>${_rondaFmt(inv.energy_daily_kwh, 0)} kWh</td>
        <td style="font-weight:700;">${_rondaFmt(inv.pr_inverter_pct, 1)}%</td>
        <td>${_rondaFmt(inv.avg_temp_c, 1)} °C</td>
        <td><span class="ronda-full-perf-badge ${perfCls}">${perfArrow} ${_rondaPerfLabel(inv.power_performance)}</span></td>
        <td><span class="ronda-full-perf-badge ${fleetPCls}">${_rondaPerfLabel(inv.power_vs_fleet)}</span></td>
        <td><span class="ronda-full-perf-badge ${fleetPrCls}">${_rondaPerfLabel(inv.pr_vs_fleet)}</span></td>
        <td>${_rondaFmt(inv.running_pct, 1)}%</td>
      </tr>`;
    });
    body += `</tbody></table></div></div></div>`;
  }

  // Card: String Health
  if (sb.length) {
    const grouped = {};
    sb.forEach(s => {
      const key = s.device_name || s.inverter_name || ("Inv" + s.device_id);
      if (!grouped[key]) grouped[key] = { items: [], health: s.health_pct };
      grouped[key].items.push(s);
      grouped[key].health = s.health_pct;
    });
    body += `<div class="ronda-card span-full">
      <div class="ronda-card-header">
        <div class="ronda-card-icon icon-string">${svgString}</div>
        <div><div class="ronda-card-title">Saúde String Box</div><div class="ronda-card-subtitle">${Object.keys(grouped).length} inversores</div></div>
      </div>
      <div class="ronda-card-body">
        <div class="ronda-full-string-grid">`;
    Object.entries(grouped).forEach(([name, g]) => {
      const hp = g.health != null ? Number(g.health) : 100;
      const hCls = hp >= 80 ? "health-good" : hp >= 50 ? "health-mid" : "health-bad";
      const hColor = hp >= 80 ? "#39e58c" : hp >= 50 ? "#eab308" : "#ef4444";
      body += `<div class="ronda-full-string-item">
        <span class="ronda-full-string-name">${name}</span>
        <div class="ronda-full-string-bar"><div class="ronda-full-string-fill ${hCls}" style="width:${Math.min(100, hp)}%;"></div></div>
        <span class="ronda-full-string-pct" style="color:${hColor}">${_rondaFmt(hp, 0)}%</span>
      </div>`;
    });
    body += `</div></div></div>`;
  }

  // Card: Alarms
  if (alarms.length) {
    body += `<div class="ronda-card span-full">
      <div class="ronda-card-header">
        <div class="ronda-card-icon icon-alarm">${svgAlarm}</div>
        <div><div class="ronda-card-title">Alarmes</div><div class="ronda-card-subtitle">${data.alarm_count || alarms.length} eventos</div></div>
      </div>
      <div class="ronda-card-body">`;
    alarms.forEach(a => {
      const sevCls = (a.severity === "high" || a.severity === "critical") ? "sev-high" : a.severity === "medium" ? "sev-medium" : "sev-low";
      const ts = a.timestamp ? a.timestamp.replace(/T/, " ").slice(0, 19) : "";
      const activeLabel = a.is_active ? "Ativo" : "Resolvido";
      const activeCls = a.is_active ? "is-active" : "is-resolved";
      body += `<div class="ronda-full-alarm">
        <div class="ronda-full-alarm-severity ${sevCls}"></div>
        <div class="ronda-full-alarm-body">
          <div class="ronda-full-alarm-device">${a.device_name || "—"} <span style="color:rgba(255,255,255,0.3);font-weight:400;font-size:11px;">${a.device_type || ""}</span></div>
          <div class="ronda-full-alarm-desc">${a.description || a.code || "—"}</div>
          <div class="ronda-full-alarm-ts">${ts}</div>
        </div>
        <span class="ronda-full-alarm-active ${activeCls}">${activeLabel}</span>
      </div>`;
    });
    body += `</div></div>`;
  }

  body += "</div>";

  const bodyEl = document.getElementById("rondaFullBody");
  if (bodyEl) bodyEl.innerHTML = body;

  document.addEventListener("keydown", function _rondaEsc(e) {
    if (e.key === "Escape") {
      panel.classList.add("hidden");
      document.body.style.overflow = "";
      document.removeEventListener("keydown", _rondaEsc);
    }
  });
}

async function _rondaFetchFullPanel(dateStr) {
  const bodyEl = document.getElementById("rondaFullBody");
  if (!bodyEl || !PLANT_ID) return;
  bodyEl.innerHTML = '<div class="ronda-loading"><i class="fa-solid fa-spinner fa-spin"></i><br>Carregando...</div>';
  try {
    let url = `${API_BASE}/plants/${PLANT_ID}/realtime?view=daily-round`;
    if (dateStr) url += `&date=${dateStr}`;
    const user = JSON.parse(localStorage.getItem("user") || "{}");
    const headers = {};
    if (user.customer_id) headers["X-Customer-Id"] = user.customer_id;
    if (user.is_superuser === true) headers["X-Is-Superuser"] = "true";
    const res = await fetch(url, { headers, cache: "no-store" });
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    let data = await res.json();
    if (data && data.body) data = typeof data.body === "string" ? JSON.parse(data.body) : data.body;
    _RONDA_DATA = data;
    _rondaOpenFullPanel(data);
  } catch (e) {
    bodyEl.innerHTML = `<div class="ronda-error"><i class="fa-solid fa-triangle-exclamation"></i> Erro: ${e.message}</div>`;
  }
}

function _rondaDownloadCsv(data) {
  if (!data) return;
  const ps = data.plant_summary || {};
  const w = data.weather || {};
  const invs = data.inverters || [];
  const alarms = data.alarms || [];
  const lines = [];

  lines.push("RONDA DIÁRIA - " + (ps.power_plant_name || "") + " - " + (data.date || ""));
  lines.push("");

  lines.push("RESUMO DA USINA");
  lines.push("Geração kWh," + (ps.generation_kwh || ""));
  lines.push("PR Diário %," + (ps.pr_daily_pct || ""));
  lines.push("PR Acumulado %," + (ps.pr_accumulated_pct || ""));
  lines.push("Fator Capacidade %," + (ps.capacity_factor_daily_pct || ""));
  lines.push("Inicio Geração," + (ps.gen_start_time || ""));
  lines.push("Fim Geração," + (ps.gen_end_time || ""));
  lines.push("");

  lines.push("Estação Solarimétrica");
  lines.push("Irrad Media W/m2," + (w.irradiance_avg_wm2 || ""));
  lines.push("Irrad Max W/m2," + (w.irradiance_max_wm2 || ""));
  lines.push("Temp Media C," + (w.air_temp_avg_c || ""));
  lines.push("Temp Max C," + (w.air_temp_max_c || ""));
  lines.push("Vento Medio m/s," + (w.wind_speed_avg || ""));
  lines.push("Chuva," + (w.rain_detected ? "Sim" : "Nao"));
  lines.push("");

  if (invs.length) {
    lines.push("INVERSORES");
    lines.push("Nome,Pot Media kW,Energia kWh,PR %,Performance,vs Media");
    invs.forEach(inv => {
      lines.push([
        inv.inverter_name || "",
        inv.avg_power_kw || "",
        inv.energy_daily_kwh || "",
        inv.pr_inverter_pct || "",
        inv.power_performance || "",
        inv.pr_vs_fleet || ""
      ].join(","));
    });
    lines.push("");
  }

  if (alarms.length) {
    lines.push("ALARMES");
    lines.push("Timestamp,Dispositivo,Tipo,Descrição,Severidade");
    alarms.forEach(a => {
      lines.push([
        (a.timestamp || "").replace(/,/g, ";"),
        a.device_name || "",
        a.code || "",
        (a.description || "").replace(/,/g, ";"),
        a.severity || ""
      ].join(","));
    });
  }

  const csv = "﻿" + lines.join("\r\n");
  const blob = new Blob([csv], { type: "text/csv;charset=utf-8;" });
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url;
  a.download = `ronda_diaria_${ps.power_plant_name || "usina"}_${data.date || "hoje"}.csv`;
  a.click();
  URL.revokeObjectURL(url);
}

function _wireRobotPlant() {
  if (!document.getElementById("robotAssistant")) return;
  const avatar    = document.getElementById("robotAvatar");
  const expandBtn = document.getElementById("robotBubbleExpand");
  const closeBtn  = document.getElementById("robotReportClose");
  const bubble    = document.getElementById("robotBubble");

  document.querySelectorAll(".robot-tab").forEach(btn => {
    btn.addEventListener("click", e => { e.stopPropagation(); _rondaSwitchTab(btn.dataset.tab); });
  });

  if (avatar) avatar.addEventListener("click", () => _robotToggleReport());
  if (expandBtn) expandBtn.addEventListener("click", e => { e.stopPropagation(); _robotToggleReport(true); });
  if (closeBtn) closeBtn.addEventListener("click", () => _robotToggleReport(false));
  if (bubble) bubble.addEventListener("click", () => {
    if (_ROBOT_STATE.issues.length > 1) _robotToggleReport(true);
    else if (_ROBOT_STATE.issues.length === 1) {
      const url = _robotBuildUrl(_ROBOT_STATE.issues[0]);
      if (url) window.location.href = url;
    } else {
      _robotDismissBubble();
    }
  });
  document.addEventListener("click", e => {
    const el = document.getElementById("robotAssistant");
    if (_ROBOT_STATE.reportOpen && el && !el.contains(e.target)) _robotToggleReport(false);
  });

  // Drag-resize: left edge (width) and top edge (height)
  _wireRobotResize();

  // Initial fetch + periodic refresh
  _robotRefresh().catch(e => console.warn("[ROBOT]", e));
  setInterval(() => _robotRefresh().catch(e => console.warn("[ROBOT]", e)), 60000);
}

function _wireRobotResize() {
  const panel = document.getElementById("robotReport");
  const handleLeft = document.getElementById("robotReportResizeLeft");
  const handleTop = document.getElementById("robotReportResizeTop");
  if (!panel) return;

  let dragging = null;
  let startX = 0, startY = 0, startW = 0, startH = 0;

  function onPointerDown(axis, e) {
    e.preventDefault(); e.stopPropagation();
    dragging = axis;
    startX = e.clientX; startY = e.clientY;
    const rect = panel.getBoundingClientRect();
    startW = rect.width; startH = rect.height;
    panel.classList.add("ronda-resizing");
    document.addEventListener("pointermove", onPointerMove);
    document.addEventListener("pointerup", onPointerUp);
  }

  function onPointerMove(e) {
    if (!dragging) return;
    if (dragging === "x") {
      const dx = startX - e.clientX;
      const newW = Math.max(300, Math.min(window.innerWidth - 40, startW + dx));
      panel.style.width = newW + "px";
    } else {
      const dy = startY - e.clientY;
      const newH = Math.max(200, Math.min(window.innerHeight - 100, startH + dy));
      panel.style.maxHeight = newH + "px";
    }
  }

  function onPointerUp() {
    dragging = null;
    panel.classList.remove("ronda-resizing");
    document.removeEventListener("pointermove", onPointerMove);
    document.removeEventListener("pointerup", onPointerUp);
  }

  if (handleLeft) handleLeft.addEventListener("pointerdown", e => onPointerDown("x", e));
  if (handleTop) handleTop.addEventListener("pointerdown", e => onPointerDown("y", e));
}

document.addEventListener("DOMContentLoaded", async () => {
  document.body.classList.add("plant-enter");
  const expectedMtdPlaceholder = document.getElementById("monthlyKpiExpectedMtd");
  if (expectedMtdPlaceholder) expectedMtdPlaceholder.textContent = "-";
  setTimeout(() => document.body.classList.remove("plant-enter"), 500);
  setupInverterToggles();
  setupWeatherExpand();
  wireDailyChartZoomControlsOnce();
  initTrackersPanel();
  setupDeviceNav();
  initInvViewToggle();
  setupPlantAlarmMenu();
  renderAlarmMenuButton();
  renderRelayCommandBar(null);
  renderMultimeterCommandBar(null);
  wireDeviceCommandButtons(document);
  _wireRobotPlant();
  document.addEventListener("click", () => closeAllDeviceCommandMenus());
  document.addEventListener("keydown", (e) => {
    if (e.key === "Escape") closeAllDeviceCommandMenus();
  });

  if (!PLANT_ID) {
    console.warn("[plant] plant_id ausente na URL; mantendo tela sem dados de fallback.");
    renderHeaderSummary();
    renderSummaryStrip();
    return;
  }

  try {
    void fetchPlantCapabilities(PLANT_ID);
    const refreshPromise = refreshRealtimeEverything();

    const [dailyRaw, monthlyRaw] = await Promise.all([
      fetchDailyEnergy(PLANT_ID),
      fetchMonthlyEnergy(PLANT_ID)
    ]);

    if (dailyRaw) {
      DAILY = normalizeDailyPayload(dailyRaw);
      renderDailyChart();
    }

    if (monthlyRaw) {
      MONTHLY = normalizeMonthlyPayload(monthlyRaw);
      renderMonthlyChart();
    }

    await refreshPromise;
    handleInitialPlantAction();

    setInterval(() => {
      void refreshRealtimeEverything();
    }, PLANT_REFRESH_INTERVAL_MS);

    document.addEventListener("visibilitychange", async () => {
      if (!document.hidden) {
        await refreshRealtimeEverything();
      }
    });

    window.addEventListener("focus", async () => {
      await refreshRealtimeEverything();
    });
  } catch (e) {
    console.error(e);
    renderHeaderSummary();
    renderSummaryStrip();
  }
});
