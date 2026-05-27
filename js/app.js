// =============================================================================
// 🔒 AUTH GUARD (ESTÁVEL E PROFISSIONAL)
// =============================================================================
(function authGuard() {
  const user = localStorage.getItem("user");
  if (!user) {
    window.location.href = "index.html";
    return;
  }
})();

/**
 * Limpa a sessão e redireciona para o login.
 */
function logout() {
  localStorage.removeItem("user");
  localStorage.removeItem("currentView");
  window.location.href = "index.html";
}

// =============================================================================
// API FETCH COM CONTEXTO DO USUÁRIO LOGADO
// =============================================================================
const API_BASE = "https://jgeg9i0js1.execute-api.us-east-1.amazonaws.com";
const INVERTER_NO_COMM_AFTER_MS = 15 * 60 * 1000; // legado (chips usam status do mart)
const DASHBOARD_REFRESH_INTERVAL_MS = 10000;
const DS_SERIES_PALETTE = [
  "#4da3ff", "#39e58c", "#ffd84d", "#ff8a65",
  "#b39ddb", "#80cbc4", "#f06292", "#aed581",
  "#ffb74d", "#4dd0e1", "#ce93d8", "#a5d6a7"
];
const EVENTS_REFRESH_INTERVAL_MS = 10000;

function apiFetch(path, options = {}) {
  const user = JSON.parse(localStorage.getItem("user") || "{}");

  const headers = {
    ...(options.headers || {})
  };

  if (user.customer_id) headers["X-Customer-Id"] = user.customer_id;
  if (user.is_superuser === true) headers["X-Is-Superuser"] = "true";

  return fetch(`${API_BASE}${path}`, {
    ...options,
    headers,
    cache: "no-store"
  });
}

// =============================================================================
// HELPERS DE PERMISSÃO (lê role_key do localStorage — salvo no login)
// =============================================================================
function _getUser() {
  try { return JSON.parse(localStorage.getItem("user") || "{}"); } catch { return {}; }
}
function _canEditPlantUI() {
  const u = _getUser();
  return u.is_superuser === true || u.role_key === "admin_customer";
}
function _canAckAlarmUI() {
  const u = _getUser();
  return u.is_superuser === true || u.role_key === "admin_customer" || u.role_key === "operator";
}

// =============================================================================
// CONFIGURAÇÃO GLOBAL E ESTADO
// =============================================================================
let lastValidPlants = [];
let lastAlarmSeverityByPlant = new Map();
let CURRENT_PLANT_ID = null; // planta selecionada no dashboard

function loadSelectedPlantId() {
  const v = localStorage.getItem("selectedPlantId");
  return v && /^\d+$/.test(v) ? Number(v) : null;
}

function saveSelectedPlantId(id) {
  if (id == null) return;
  localStorage.setItem("selectedPlantId", String(id));
}

// EVENTS
let EVENTS_STATE = {
  page: 1,
  page_size: 30,
  total: 0,
  total_pages: 0,
  wired: false,

  // ✅ anti dupla chamada + auto refresh
  loading: false,
  autoTimer: null
};

// DATA STUDIO
let DATASTUDIO_STATE = {
  wired: false,
  loadingTags: false,
  loadingSeries: false,
  savingSelection: false,

  startDate: "",
  endDate: "",
  selectedPlantId: null,
  selectedPlantIds: [],        // multi-plant: array de IDs selecionados

  selectedDataKind: "all", // all | analog | discrete
  selectedSource: "all", // all | historico | consolidado
  selectedContext: "all", // all | PLANT | inverter | relay | meter etc
  selectedCategory: "all",
  searchText: "",

  catalogTags: [],
  availableTags: [],
  selectedTags: [],

  selectionId: null,
  selectionIdsByPlant: {},     // multi-plant: { plantId: selectionId }
  seriesByPlant: {},           // multi-plant: { plantId: seriesPayload }

  aggregationMode: "historico", // historico | consolidado
  aggregationType: "avg", // avg | integral | median | max | mode | propagation | sum | none
  consolidationPeriod: "5min", // 5min | daily | weekly | monthly | yearly | hdaily etc

  chartData: null,
  forceHeroState: false,
  catalogOpen: false,
  catalogConfirmed: false
};

let DATASTUDIO_CHART = {};  // multi-plant: { plantId: ChartInstance }
const DS_PLANT_PALETTE = ["#4da3ff", "#39e58c", "#ffd84d", "#ff8a65", "#b39ddb"];
let DATASTUDIO_TAGS_ABORT_CONTROLLER = null;

// Abort controller pra evitar race condition
let eventsAbortController = null;
let ALARMS_RENDER_SEQ = 0;
let LAST_ACTIVE_ALARMS_RENDER_KEY = "";
let LAST_RECOGNIZED_ALARMS_RENDER_KEY = "";
let LAST_EVENTS_RENDER_KEY = "";
let LOCAL_ACKED_ALARMS = [];
let CURRENT_ALARMS_TAB_MODE = null; // "active" | "recognized"

// ✅ MODO PADRÃO DO EVENTS
let EVENTS_VIEW_MODE = "normal";

// ✅ quantas “rodadas/seqüências” você quer ver (T1..T5)
let EVENTS_ROUNDS = 5;

// =============================================================================
// FUNÇÕES DE UTILIDADE
// =============================================================================
function valueOrDash(v) {
  return v === null || v === undefined || v === "" ? "—" : v;
}

function severityColor(sev) {
  const s = String(sev || "").toLowerCase();
  if (s === "high") return "#f44336";
  if (s === "medium") return "#ff9800";
  if (s === "low") return "#4caf50";
  return "#ccc";
}

function normalizeAlarmSeverity(sev) {
  if (!sev) return null;
  const normalized = String(sev).toLowerCase();
  if (normalized === "high" || normalized === "medium") return normalized;
  return null;
}

function buildAlarmRenderKey(list, isRecognized) {
  const base = Array.isArray(list) ? list : [];
  const compact = base.map((a) => ({
    id: a?.event_row_id ?? a?.id ?? null,
    state: String(a?.alarm_state ?? a?.state ?? "").toUpperCase(),
    acknowledged: a?.acknowledged === true,
    acknowledged_at: a?.acknowledged_at ?? null,
    acknowledged_by: a?.acknowledged_by ?? null,
    acknowledgment_note: a?.acknowledgment_note ?? null,
    started_at: a?.started_at ?? a?.timestamp ?? a?.last_event_ts ?? null,
    event_name: a?.event_name ?? null
  }));
  return JSON.stringify({ mode: isRecognized ? "recognized" : "active", compact });
}

function buildEventsRenderKey(list, page, filters) {
  const compact = (Array.isArray(list) ? list : []).map((ev) => ({
    id: ev?.event_row_id ?? ev?.id ?? null,
    ts: ev?.event_ts ?? ev?.timestamp ?? null,
    state: ev?.state ?? ev?.event_status ?? null,
    severity: ev?.severity ?? null,
    acknowledged_by: ev?.acknowledged_by ?? null,
    acknowledgment_note: ev?.acknowledgment_note ?? null
  }));
  return JSON.stringify({ page, f: filters, compact });
}

function getHigherSeverity(a, b) {
  const rank = { high: 2, medium: 1 };
  if (!a) return b;
  if (!b) return a;
  return (rank[b] || 0) > (rank[a] || 0) ? b : a;
}

function buildPlantAlarmSeverityMap(alarms) {
  const map = new Map();
  const validAlarms = Array.isArray(alarms) ? alarms : [];

  validAlarms.forEach(alarm => {
    const severity = normalizeAlarmSeverity(
      alarm.severity || alarm.alarm_severity || alarm.level || alarm.alarm_level
    );
    if (!severity) return;

    const plantId = alarm.power_plant_id || alarm.plant_id || alarm.plantId;
    const plantName = alarm.power_plant_name || alarm.plant_name || alarm.plantName;

    if (plantId != null) map.set(plantId, getHigherSeverity(map.get(plantId), severity));
    if (plantName) map.set(plantName, getHigherSeverity(map.get(plantName), severity));
  });

  return map;
}

function getAlarmDescription(eventCode) {
  const map = {
    17: "Falha geral",
    59: "Proteção acionada",
    7: "Subtensão",
    9: "Sobretensão"
  };
  return map[eventCode] || `Evento ${eventCode}`;
}


function dedupeAlarms(list) {
  const items = Array.isArray(list) ? list : [];
  const seen = new Set();
  const out = [];

  items.forEach(a => {
    const key =
      a.event_row_id ??
      a.alarm_id ??
      a.id ??
      [
        a.power_plant_id ?? a.power_plant_name ?? "",
        a.device_id ?? a.device_name ?? "",
        a.event_code ?? a.event_name ?? "",
        a.alarm_state ?? a.state ?? "",
        a.started_at ?? a.last_event_ts ?? a.ack_at ?? a.cleared_at ?? ""
      ].join("|");

    const normalized = String(key || "").trim();
    if (!normalized || seen.has(normalized)) return;
    seen.add(normalized);
    out.push(a);
  });

  return out;
}

// =============================================================================
// ✅ TOP CHIPS (GEN / NO COMM / OFF) — TELEMETRIA POR USINA
// ----------------------------------------------------------------------------
// Regras por status (mart_inverter_realtime, sem depender do clock do browser):
// Gen     = RUNNING (code 2)
// No comm = OFFLINE (code 0)
// Off     = STANDBY + FAULT (codes 1 + 3)
// =============================================================================
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

function normalizeInvStatus(inv) {
  const s =
    inv.status ??
    inv.inverter_status ??
    inv.inverterStatus ??
    null;

  if (s) return String(s).trim().toUpperCase();

  const code =
    inv.inverter_status_code ??
    inv.status_code ??
    inv.inverterStatusCode ??
    null;

  if (code === 0) return "OFFLINE";
  if (code === 1) return "STANDBY";
  if (code === 2) return "RUNNING";
  if (code === 3) return "FAULT";
  return "UNKNOWN";
}

function computeInverterChipsByTelemetry(invertersRaw) {
  const inverters = dedupInvertersById(invertersRaw);

  let noComm = 0;
  let gen = 0;
  let off = 0;

  for (const inv of inverters) {
    const st = normalizeInvStatus(inv);

    if (st === "OFFLINE") noComm++;
    else if (st === "RUNNING") gen++;
    else if (st === "STANDBY" || st === "FAULT") off++;
    else {
      // se vier UNKNOWN, joga em off pra não sumir
      off++;
    }
  }

  const total = inverters.length;
  return { total, gen, off, noComm };
}


function computeGlobalChipsFromPlants(plantsRaw) {
  const plants = Array.isArray(plantsRaw) ? plantsRaw : [];

  let total = 0;
  let gen = 0;
  let noComm = 0;
  let off = 0;

  for (const p of plants) {
    total += Number(p.inverter_total ?? 0) || 0;
    gen += Number(p.inverter_generating ?? 0) || 0;
    noComm += Number(p.inverter_no_comm ?? 0) || 0;
    off += Number(p.inverter_off ?? 0) || 0;
  }

  total = Math.max(0, total);
  gen = Math.max(0, gen);
  noComm = Math.max(0, noComm);
  off = Math.max(0, off);

  const sum = gen + noComm + off;
  if (total === 0 && sum > 0) total = sum;

  return { total, gen, noComm, off };
}

function refreshTopChipsGlobalFromPlants(plants) {
  const r = computeGlobalChipsFromPlants(plants);

  setChipCount("countGen", r.gen, `Gerando (global): ${r.gen} de ${r.total}`);
  setChipCount("countNoComm", r.noComm, `Sem comunicação (global): ${r.noComm} de ${r.total}`);
  setChipCount("countOff", r.off, `Off (global): ${r.off} de ${r.total}`);

  console.log("[INV CHIPS - GLOBAL]", r);
}

function setChipCount(id, value, title = "") {
  const el = document.getElementById(id);
  if (!el) return;
  el.textContent = String(value);
  if (title) el.title = title;
  if (id === 'countNoComm' || id === 'countOff') {
    const statusDiv = el.closest('.psf-status');
    if (statusDiv) statusDiv.classList.toggle('psf-status--alarm', Number(value) > 0);
  }
}

async function fetchInvertersRealtimeByPlant(plantId) {
  const res = await apiFetch(`/plants/${plantId}/inverters/realtime`);
  if (!res.ok) {
    console.warn(`[INV CHIPS] HTTP ${res.status} em /plants/${plantId}/inverters/realtime`);
    return [];
  }

  const data = await res.json();
  const normalized = (data && data.body)
    ? (typeof data.body === "string" ? JSON.parse(data.body) : data.body)
    : data;

  return (Array.isArray(normalized?.inverters) ? normalized.inverters : null) ||
         (Array.isArray(normalized?.items) ? normalized.items : null) ||
         (Array.isArray(normalized) ? normalized : []);
}

async function refreshInverterStatusChipsForPlant(plantId) {
  if (plantId == null) {
    setChipCount("countGen", 0);
    setChipCount("countNoComm", 0);
    setChipCount("countOff", 0);
    return;
  }

  try {
    const inverters = await fetchInvertersRealtimeByPlant(plantId);
    const r = computeInverterChipsByTelemetry(inverters);

    setChipCount("countGen", r.gen, `Gerando: ${r.gen} de ${r.total}`);
    setChipCount("countNoComm", r.noComm, `Sem comunicação: ${r.noComm} de ${r.total}`);
    setChipCount("countOff", r.off, `Desligados: ${r.off} de ${r.total}`);

    console.log("[INV CHIPS - PLANT]", { plantId, ...r });
  } catch (e) {
    console.warn("[INV CHIPS] falha:", e?.message || e);
    setChipCount("countGen", 0);
    setChipCount("countNoComm", 0);
    setChipCount("countOff", 0);
  }
}

// =============================================================================
// HELPERS DE DATA (EVENTS)
// =============================================================================
function safeTrim(v) {
  if (v == null) return "";
  return String(v).trim();
}

function todayYYYYMMDD() {
  const now = new Date();
  const yyyy = now.getFullYear();
  const mm = String(now.getMonth() + 1).padStart(2, "0");
  const dd = String(now.getDate()).padStart(2, "0");
  return `${yyyy}-${mm}-${dd}`;
}

function isoFromDateAndTime(dateYYYYMMDD, timeHHMM, isEnd = false) {
  if (!dateYYYYMMDD) return null;

  const [yyyy, mm, dd] = dateYYYYMMDD.split("-").map(Number);
  if (!yyyy || !mm || !dd) return null;

  let HH = 0, MI = 0, SS = 0;
  if (timeHHMM) {
    const [h, m] = String(timeHHMM).split(":").map(Number);
    HH = Number.isFinite(h) ? h : 0;
    MI = Number.isFinite(m) ? m : 0;
    SS = isEnd ? 59 : 0;
  } else if (isEnd) {
    HH = 23;
    MI = 59;
    SS = 59;
  }

  const d = new Date(yyyy, mm - 1, dd, HH, MI, SS);
  return Number.isNaN(d.getTime()) ? null : d.toISOString();
}

function clampEventRange(startISO, endISO) {
  if (!startISO || !endISO) return { startISO, endISO };

  const s = new Date(startISO).getTime();
  const e = new Date(endISO).getTime();

  if (!Number.isFinite(s) || !Number.isFinite(e)) return { startISO, endISO };

  if (e < s) {
    return { startISO: endISO, endISO: startISO };
  }

  return { startISO, endISO };
}

function parseEquipmentFilter(input) {
  const raw = safeTrim(input);
  if (!raw) return { source: null, device_id: null };

  const compact = raw.replace(/\s+/g, "").replace(/[^\w]/g, "");
  const lower = compact.toLowerCase();

  const invMatch =
    lower.match(/^inversor(\d+)$/) ||
    lower.match(/^inverter(\d+)$/) ||
    lower.match(/^inv(\d+)$/);
  if (invMatch) return { source: "inverter", device_id: parseInt(invMatch[1], 10) };

  const relayMatch =
    lower.match(/^relay(\d+)$/) ||
    lower.match(/^rele(\d+)$/) ||
    lower.match(/^rel(\d+)$/);
  if (relayMatch) return { source: "relay", device_id: parseInt(relayMatch[1], 10) };

  if (lower === "weather" || lower === "clima") return { source: "weather", device_id: null };

  return { source: null, device_id: null };
}

// =============================================================================
// CONTROLE DE TEMA E RELÓGIO
// =============================================================================
const themeToggleBtn = document.getElementById("themeToggleBtn");
const themeIcon = document.getElementById("themeIcon");
const body = document.body;

const savedTheme = localStorage.getItem("theme") || "dark";
body.classList.add(`theme-${savedTheme}`);
if (themeIcon) themeIcon.className = savedTheme === "dark" ? "fa-solid fa-sun" : "fa-solid fa-moon";

themeToggleBtn?.addEventListener("click", () => {
  const isDark = body.classList.contains("theme-dark");
  const newTheme = isDark ? "light" : "dark";
  body.classList.remove("theme-light", "theme-dark");
  body.classList.add(`theme-${newTheme}`);
  localStorage.setItem("theme", newTheme);
  if (themeIcon) themeIcon.className = newTheme === "dark" ? "fa-solid fa-sun" : "fa-solid fa-moon";
});

function updateClock() {
  const el = document.getElementById("clock");
  if (!el) return;
  const now = new Date();
  el.textContent =
    now.toLocaleDateString("pt-BR") +
    " • " +
    now.toLocaleTimeString("pt-BR", { hour: "2-digit", minute: "2-digit" });
}
setInterval(updateClock, 1000);
updateClock();

// =============================================================================
// CONSUMO DE API
// =============================================================================
async function fetchPlants() {
  const res = await apiFetch("/plants");
  if (!res.ok) throw new Error("Erro ao buscar plantas");
  const data = await res.json();

  if (data && data.body) {
    const parsed = typeof data.body === "string" ? JSON.parse(data.body) : data.body;
    return Array.isArray(parsed) ? parsed : [];
  }
  return Array.isArray(data) ? data : [];
}

async function fetchPlantDeviceOptions(plantId) {
  if (plantId == null || !String(plantId).match(/^\d+$/)) return [];

  const res = await apiFetch(`/plants/${plantId}/devices/options`);
  if (!res.ok) throw new Error("Erro ao buscar equipamentos da usina");

  const data = await res.json();
  if (data && data.body) {
    const parsed = typeof data.body === "string" ? JSON.parse(data.body) : data.body;
    return Array.isArray(parsed?.items) ? parsed.items : (Array.isArray(parsed) ? parsed : []);
  }
  return Array.isArray(data?.items) ? data.items : (Array.isArray(data) ? data : []);
}


async function fetchPlantsSummary() {
  const res = await apiFetch("/plants/summary");
  if (!res.ok) throw new Error("Erro ao buscar summary global");

  const data = await res.json();
  if (data && data.body) {
    return typeof data.body === "string" ? JSON.parse(data.body) : data.body;
  }
  return data; // esperado: {gen,no_comm,off,total}
}

function refreshTopChipsGlobalFromSummary(summary) {
  const gen = Number(summary?.gen ?? 0) || 0;
  const noComm = Number(summary?.no_comm ?? summary?.noComm ?? 0) || 0;
  const off = Number(summary?.off ?? 0) || 0;
  const total = Number(summary?.total ?? (gen + noComm + off) ?? 0) || 0;

  setChipCount("countGen", gen, `Gerando (global): ${gen} de ${total}`);
  setChipCount("countNoComm", noComm, `Sem comunicação (global): ${noComm} de ${total}`);
  setChipCount("countOff", off, `Off (global): ${off} de ${total}`);

  console.log("[INV CHIPS - GLOBAL SUMMARY]", { total, gen, noComm, off });
}

// ✅ ALARMES: NÃO MEXI
async function fetchActiveAlarms() {
  const res = await apiFetch("/alarms/active");
  if (!res.ok) throw new Error("Erro ao buscar alarmes ativos");
  const data = await res.json();

  if (data && data.body) {
    const parsed = typeof data.body === "string" ? JSON.parse(data.body) : data.body;
    return Array.isArray(parsed) ? parsed : [];
  }
  return Array.isArray(data) ? data : [];
}

async function fetchAcknowledgedAlarms() {
  const res = await apiFetch("/alarms/history");
  if (!res.ok) throw new Error("Erro ao buscar alarmes reconhecidos");
  const data = await res.json();
  const parsed = (data && data.body)
    ? (typeof data.body === "string" ? JSON.parse(data.body) : data.body)
    : data;
  return Array.isArray(parsed) ? parsed : [];
}

async function acknowledgeAlarm(alarm, acknowledgmentNote = "") {
  const user = JSON.parse(localStorage.getItem("user") || "{}");

  const alarmId = alarm?.id || alarm?.event_row_id;
  if (!alarmId) {
    throw new Error("Alarme sem id/event_row_id");
  }

  const res = await apiFetch(`/alarms/${alarmId}/ack`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      event_row_id: alarm.event_row_id || alarm.id,
      power_plant_id: alarm.power_plant_id,
      acknowledged_by: user?.username || user?.name || user?.email || "operador",
      acknowledgment_note: acknowledgmentNote && String(acknowledgmentNote).trim()
        ? String(acknowledgmentNote).trim()
        : null
    })
  });

  if (!res.ok) {
    const txt = await res.text().catch(() => "");
    throw new Error(`Falha ao reconhecer alarme (${res.status}) ${txt}`);
  }

  const data = await res.json().catch(() => ({}));
  return data && data.body
    ? (typeof data.body === "string" ? JSON.parse(data.body) : data.body)
    : data;
}

/**
 * ✅ Busca eventos (corrigido)
 */
async function fetchEventsSafeBackend({
  start_time,
  end_time,
  page = 1,
  page_size = 30,
  severity,
  event_type,
  status,
  q,
  source,
  device_id,
  plant_id,
  mode = "normal",
  rounds = 5,
  include_total = true,
  _retry = 0
} = {}) {
  if (!start_time || !end_time) {
    const end = new Date();
    const start = new Date();
    start.setDate(end.getDate() - 7);
    start_time = start.toISOString();
    end_time = end.toISOString();
  }

  const fixed = clampEventRange(start_time, end_time);
  start_time = fixed.startISO;
  end_time = fixed.endISO;

  const params = new URLSearchParams({
    start_time,
    end_time,
    page: String(page),
    page_size: String(page_size)
  });

  const md = String(mode || "").toLowerCase();
  if (md) params.append("mode", md);
  if (md === "round_robin") params.append("rounds", String(rounds || 5));
  if (include_total) params.append("include_total", "1");

  if (plant_id != null && String(plant_id).match(/^\d+$/)) params.append("plant_id", String(plant_id));

  const sev = String(severity || "").toLowerCase();
  if (sev && sev !== "all") params.append("severity", sev);

  const et = String(event_type || "").toLowerCase();
  const allowedEventTypes = new Set(["all", "alarm", "event", "status"]);
  if (allowedEventTypes.has(et) && et !== "all") params.append("event_type", et);

  const st = String(status || "").toLowerCase();
  const allowedStatus = new Set(["all", "active", "inactive"]);
  if (allowedStatus.has(st) && st !== "all") params.append("status", st);

  const src = String(source || "").toLowerCase();
  const allowedSources = new Set(["inverter", "relay", "weather"]);
  if (allowedSources.has(src)) params.append("source", src);

  if (device_id != null && String(device_id).match(/^\d+$/)) params.append("device_id", String(device_id));

  const qv = safeTrim(q);
  if (qv) params.append("q", qv);

  if (eventsAbortController) eventsAbortController.abort();
  eventsAbortController = new AbortController();

  const url = `/events?${params.toString()}`;
  console.log("[EVENTS] GET", url);

  let res;
  try {
    res = await apiFetch(url, { signal: eventsAbortController.signal });
  } catch (e) {
    if (String(e?.name) === "AbortError") throw e;
    throw e;
  }

  if (!res.ok && res.status >= 500 && _retry < 1) {
    const waitMs = 600;
    console.warn(`[EVENTS] server ${res.status}. retry em ${waitMs}ms...`);
    await new Promise(r => setTimeout(r, waitMs));
    return fetchEventsSafeBackend({
      start_time, end_time, page, page_size, severity, event_type, status, q, source, device_id, plant_id, mode, rounds, include_total,
      _retry: _retry + 1
    });
  }

  if (!res.ok) {
    let bodyText = "";
    try { bodyText = await res.text(); } catch {}
    const err = new Error(`Erro ao buscar eventos (HTTP ${res.status})`);
    err.status = res.status;
    err.body = bodyText;
    err.url = url;
    throw err;
  }

  const data = await res.json();
  if (data && data.body) {
    const parsed = typeof data.body === "string" ? JSON.parse(data.body) : data.body;
    return parsed;
  }
  return data;
}

// =============================================================================
// EVENTS UI: elementos do filtro (IDs FIXOS)
// =============================================================================
function findButtonByText(text) {
  const t = String(text || "").toLowerCase();
  const btns = Array.from(document.querySelectorAll("button"));
  return btns.find(b => String(b.textContent || "").trim().toLowerCase() === t) || null;
}

function getEventsUIElements() {
  const startDateTime = document.getElementById("eventsStartDateTimeInput");
  const endDateTime = document.getElementById("eventsEndDateTimeInput");
  const severitySelect = document.getElementById("eventsSeveritySelect");
  const typeSelect = document.getElementById("eventsTypeSelect");
  const statusSelect = document.getElementById("eventsStatusSelect");
  const plantSelect = document.getElementById("eventsPlantSelect");
  const equipmentSelect = document.getElementById("eventsEquipmentSelect");
  const desc = document.getElementById("eventsDescriptionInput");

  const applyBtn = document.getElementById("eventsApplyBtn") || findButtonByText("apply");
  const clearBtn = document.getElementById("eventsClearBtn") || findButtonByText("clear");

  const prevBtn = document.getElementById("eventsPrevBtn");
  const nextBtn = document.getElementById("eventsNextBtn");
  const pageLabel = document.getElementById("eventsPageLabel");

  return {
    startDateTime, endDateTime,
    severitySelect, typeSelect, statusSelect,
    plantSelect, equipmentSelect, desc,
    applyBtn, clearBtn, prevBtn, nextBtn, pageLabel
  };
}

function ensureSeveritySelectOptions() {
  const ui = getEventsUIElements();
  const sel = ui.severitySelect;
  if (!sel || sel.tagName !== "SELECT") return;

  sel.innerHTML = "";
  [
    { value: "all", text: "All" },
    { value: "high", text: "High" },
    { value: "medium", text: "Medium" },
    { value: "low", text: "Low" }
  ].forEach(o => {
    const opt = document.createElement("option");
    opt.value = o.value;
    opt.textContent = o.text;
    sel.appendChild(opt);
  });

  if (!sel.value) sel.value = "all";
}

function ensureTypeSelectOptions() {
  const ui = getEventsUIElements();
  const sel = ui.typeSelect;
  if (!sel || sel.tagName !== "SELECT") return;

  const previous = String(sel.value || "all");
  sel.innerHTML = "";
  [
    { value: "all", text: "All" },
    { value: "alarm", text: "Alarm" },
    { value: "event", text: "Event" },
    { value: "status", text: "Status" }
  ].forEach(o => {
    const opt = document.createElement("option");
    opt.value = o.value;
    opt.textContent = o.text;
    sel.appendChild(opt);
  });

  sel.value = [...sel.options].some(o => o.value === previous) ? previous : "all";
}

function ensureStatusSelectOptions() {
  const ui = getEventsUIElements();
  const sel = ui.statusSelect;
  if (!sel || sel.tagName !== "SELECT") return;

  const previous = String(sel.value || "all");
  sel.innerHTML = "";
  [
    { value: "all", text: "All" },
    { value: "active", text: "Active" },
    { value: "inactive", text: "Inactive" }
  ].forEach(o => {
    const opt = document.createElement("option");
    opt.value = o.value;
    opt.textContent = o.text;
    sel.appendChild(opt);
  });

  sel.value = [...sel.options].some(o => o.value === previous) ? previous : "all";
}

function datetimeLocalToISO(value) {
  const raw = safeTrim(value);
  if (!raw) return null;
  const d = new Date(raw);
  return Number.isNaN(d.getTime()) ? null : d.toISOString();
}

function toDateTimeLocalInputValue(d) {
  const dt = d instanceof Date ? d : new Date(d);
  if (Number.isNaN(dt.getTime())) return "";
  const yyyy = dt.getFullYear();
  const mm = String(dt.getMonth() + 1).padStart(2, "0");
  const dd = String(dt.getDate()).padStart(2, "0");
  const hh = String(dt.getHours()).padStart(2, "0");
  const mi = String(dt.getMinutes()).padStart(2, "0");
  return `${yyyy}-${mm}-${dd}T${hh}:${mi}`;
}

function ensureDefaultEventsDateTimes() {
  const ui = getEventsUIElements();
  if (!ui.startDateTime || !ui.endDateTime) return;

  if (!safeTrim(ui.endDateTime.value)) {
    ui.endDateTime.value = toDateTimeLocalInputValue(new Date());
  }

  if (!safeTrim(ui.startDateTime.value)) {
    const start = new Date();
    start.setHours(start.getHours() - 1);
    ui.startDateTime.value = toDateTimeLocalInputValue(start);
  }
}

function populateEventsPlantSelect(plants) {
  const ui = getEventsUIElements();
  const sel = ui.plantSelect;
  if (!sel || sel.tagName !== "SELECT") return;

  const previous = String(sel.value || "all");
  sel.innerHTML = "";

  const allOpt = document.createElement("option");
  allOpt.value = "all";
  allOpt.textContent = "Todas";
  sel.appendChild(allOpt);

  (Array.isArray(plants) ? plants : []).forEach((p) => {
    const plantId = p.power_plant_id ?? p.plant_id ?? p.id;
    const plantName = p.power_plant_name ?? p.plant_name ?? p.name ?? `Usina ${plantId}`;
    if (plantId == null) return;
    const opt = document.createElement("option");
    opt.value = String(plantId);
    opt.textContent = String(plantName);
    sel.appendChild(opt);
  });

  sel.value = [...sel.options].some(o => o.value === previous) ? previous : "all";
}

function populateEventsEquipmentSelect(devices) {
  const ui = getEventsUIElements();
  const sel = ui.equipmentSelect;
  if (!sel || sel.tagName !== "SELECT") return;

  const previous = String(sel.value || "all");
  sel.innerHTML = "";

  const allOpt = document.createElement("option");
  allOpt.value = "all";
  allOpt.textContent = "Todos";
  sel.appendChild(allOpt);

  (Array.isArray(devices) ? devices : []).forEach((d) => {
    const deviceId = d.device_id ?? d.id;
    if (deviceId == null) return;
    const label = d.label || [d.device_type, d.device_name].filter(Boolean).join(" • ") || `Device ${deviceId}`;
    const opt = document.createElement("option");
    opt.value = String(deviceId);
    opt.textContent = String(label);
    sel.appendChild(opt);
  });

  sel.value = [...sel.options].some(o => o.value === previous) ? previous : "all";
}

async function refreshEventsEquipmentOptionsForPlant(plantId) {
  if (plantId == null || !String(plantId).match(/^\d+$/)) {
    populateEventsEquipmentSelect([]);
    return;
  }

  try {
    const devices = await fetchPlantDeviceOptions(plantId);
    populateEventsEquipmentSelect(devices);
  } catch (e) {
    console.warn("[EVENTS] erro ao carregar equipamentos:", e?.message || e);
    populateEventsEquipmentSelect([]);
  }
}

// =============================================================================
// filtros: Events legado (datetime-local + selects)
// =============================================================================
function getEventsFiltersFromUI() {
  const ui = getEventsUIElements();

  let start_time = datetimeLocalToISO(ui.startDateTime?.value);
  let end_time = datetimeLocalToISO(ui.endDateTime?.value);

  const fixed = clampEventRange(start_time, end_time);
  start_time = fixed.startISO;
  end_time = fixed.endISO;

  let severity = "all";
  if (ui.severitySelect) severity = String(ui.severitySelect.value || "all").trim().toLowerCase() || "all";

  let event_type = "all";
  if (ui.typeSelect) event_type = String(ui.typeSelect.value || "all").trim().toLowerCase() || "all";

  let status = "all";
  if (ui.statusSelect) status = String(ui.statusSelect.value || "all").trim().toLowerCase() || "all";

  const q = safeTrim(ui.desc?.value);

  const plant_id = (ui.plantSelect && ui.plantSelect.value !== "all" && String(ui.plantSelect.value).match(/^\d+$/))
    ? Number(ui.plantSelect.value)
    : null;

  const device_id = (ui.equipmentSelect && ui.equipmentSelect.value !== "all" && String(ui.equipmentSelect.value).match(/^\d+$/))
    ? Number(ui.equipmentSelect.value)
    : null;

  return { start_time, end_time, plant_id, severity, event_type, status, q, source: null, device_id };
}

function updateEventsPaginationUI(pagination) {
  EVENTS_STATE.page = pagination?.page || EVENTS_STATE.page;
  EVENTS_STATE.page_size = pagination?.page_size || EVENTS_STATE.page_size;

  const total = pagination?.total;
  const total_pages = pagination?.total_pages;

  if (total != null) EVENTS_STATE.total = Number.isFinite(Number(total)) ? Number(total) : EVENTS_STATE.total;
  if (total_pages != null) EVENTS_STATE.total_pages = Number.isFinite(Number(total_pages)) ? Number(total_pages) : EVENTS_STATE.total_pages;

  const { prevBtn, nextBtn, pageLabel } = getEventsUIElements();

  if (pageLabel) {
    const tp = EVENTS_STATE.total_pages ? Math.max(1, EVENTS_STATE.total_pages) : "?";
    const tt = (EVENTS_STATE.total != null && Number.isFinite(Number(EVENTS_STATE.total))) ? Number(EVENTS_STATE.total) : "—";
    pageLabel.textContent = `Página ${EVENTS_STATE.page} / ${tp} • Total ${tt}`;
  }

  if (prevBtn) prevBtn.disabled = EVENTS_STATE.page <= 1;
  if (nextBtn) nextBtn.disabled = EVENTS_STATE.total_pages > 0 ? EVENTS_STATE.page >= EVENTS_STATE.total_pages : false;
}

function wireEventsFiltersOnce() {
  if (EVENTS_STATE.wired) return;
  EVENTS_STATE.wired = true;

  ensureSeveritySelectOptions();
  ensureTypeSelectOptions();
  ensureStatusSelectOptions();
  populateEventsPlantSelect(lastValidPlants);
  populateEventsEquipmentSelect([]);

  const ui = getEventsUIElements();

  if (ui.severitySelect) {
    ui.severitySelect.addEventListener("change", () => {
      EVENTS_STATE.page = 1;
      loadEvents(1);
    });
  }

  if (ui.typeSelect) {
    ui.typeSelect.addEventListener("change", () => {
      EVENTS_STATE.page = 1;
      loadEvents(1);
    });
  }

  if (ui.statusSelect) {
    ui.statusSelect.addEventListener("change", () => {
      EVENTS_STATE.page = 1;
      loadEvents(1);
    });
  }

  if (ui.plantSelect) {
    ui.plantSelect.addEventListener("change", async () => {
      await refreshEventsEquipmentOptionsForPlant(ui.plantSelect.value);
      EVENTS_STATE.page = 1;
      loadEvents(1);
    });
  }

  if (ui.equipmentSelect) {
    ui.equipmentSelect.addEventListener("change", () => {
      EVENTS_STATE.page = 1;
      loadEvents(1);
    });
  }

  const textInputs = [ui.desc].filter(Boolean);
  textInputs.forEach(el => {
    el.addEventListener("keydown", (e) => {
      if (e.key === "Enter") {
        EVENTS_STATE.page = 1;
        loadEvents(1);
      }
    });
  });

  if (ui.applyBtn) {
    ui.applyBtn.addEventListener("click", () => {
      EVENTS_STATE.page = 1;
      loadEvents(1);
    });
  }

  if (ui.clearBtn) {
    ui.clearBtn.addEventListener("click", () => {
      const ui2 = getEventsUIElements();

      if (ui2.desc) ui2.desc.value = "";
      if (ui2.typeSelect) ui2.typeSelect.value = "all";
      if (ui2.statusSelect) ui2.statusSelect.value = "all";
      if (ui2.severitySelect) ui2.severitySelect.value = "all";
      if (ui2.plantSelect) ui2.plantSelect.value = "all";
      populateEventsEquipmentSelect([]);
      if (ui2.startDateTime) ui2.startDateTime.value = "";
      if (ui2.endDateTime) ui2.endDateTime.value = "";
      ensureDefaultEventsDateTimes();

      EVENTS_STATE.page = 1;
      loadEvents(1);
    });
  }

  if (ui.prevBtn) ui.prevBtn.addEventListener("click", () => { if (EVENTS_STATE.page > 1) loadEvents(EVENTS_STATE.page - 1); });
  if (ui.nextBtn) ui.nextBtn.addEventListener("click", () => { loadEvents(EVENTS_STATE.page + 1); });
}

// =============================================================================
// AUTO-REFRESH DO EVENTS (sem F5)
// =============================================================================
function startEventsAutoRefresh() {
  stopEventsAutoRefresh();
  EVENTS_STATE.autoTimer = setInterval(() => {
    const evView = document.getElementById("eventsView");
    const isVisible = evView && !evView.classList.contains("hidden");
    if (isVisible) loadEvents(EVENTS_STATE.page || 1, { silent: true });
  }, EVENTS_REFRESH_INTERVAL_MS);
}

function stopEventsAutoRefresh() {
  if (EVENTS_STATE.autoTimer) clearInterval(EVENTS_STATE.autoTimer);
  EVENTS_STATE.autoTimer = null;
}

// =============================================================================
// RENDERIZAÇÃO DA INTERFACE (ALARMS) — NÃO MEXI
// =============================================================================
let ACK_MODAL_READY = false;

function ensureAckModal() {
  if (ACK_MODAL_READY) return;

  const style = document.createElement("style");
  style.id = "ack-modal-styles";
  style.textContent = `
    .ack-modal-overlay{
      position: fixed;
      inset: 0;
      background: rgba(0,0,0,.72);
      backdrop-filter: blur(6px);
      display: flex;
      align-items: center;
      justify-content: center;
      z-index: 99999;
      padding: 20px;
    }
    .ack-modal{
      width: min(520px, 100%);
      border-radius: 18px;
      background:
        radial-gradient(600px 160px at 12% 0%, rgba(57,229,140,.10), transparent 60%),
        linear-gradient(180deg, rgba(10,18,15,.98), rgba(4,9,7,.98));
      border: 1px solid rgba(57,229,140,.18);
      box-shadow:
        0 24px 60px rgba(0,0,0,.55),
        0 0 30px rgba(57,229,140,.08),
        inset 0 1px 0 rgba(255,255,255,.04);
      overflow: hidden;
      animation: ackModalEnter .18s ease;
    }
    @keyframes ackModalEnter{
      from{ opacity:0; transform: translateY(8px) scale(.985); }
      to{ opacity:1; transform: translateY(0) scale(1); }
    }
    .ack-modal__header{
      padding: 18px 20px 12px;
      border-bottom: 1px solid rgba(255,255,255,.06);
    }
    .ack-modal__title{
      margin: 0;
      color: #eafff3;
      font-size: 18px;
      font-weight: 700;
      letter-spacing: .02em;
    }
    .ack-modal__subtitle{
      margin-top: 6px;
      color: rgba(185,235,208,.72);
      font-size: 13px;
      line-height: 1.45;
    }
    .ack-modal__body{
      padding: 18px 20px 10px;
    }
    .ack-modal__alarm{
      margin-bottom: 14px;
      padding: 12px 14px;
      border-radius: 12px;
      background: rgba(255,255,255,.03);
      border: 1px solid rgba(57,229,140,.10);
      color: rgba(233,255,243,.92);
      line-height: 1.45;
      font-size: 13px;
    }
    .ack-modal__label{
      display: block;
      margin-bottom: 8px;
      color: #9adbb8;
      font-size: 12px;
      font-weight: 700;
      letter-spacing: .05em;
      text-transform: uppercase;
    }
    .ack-modal__textarea{
      width: 100%;
      min-height: 120px;
      resize: vertical;
      border-radius: 14px;
      border: 1px solid rgba(57,229,140,.18);
      background: rgba(6,12,10,.92);
      color: #e9fff3;
      padding: 14px 14px;
      outline: none;
      font-size: 14px;
      line-height: 1.5;
      box-shadow: inset 0 1px 0 rgba(255,255,255,.03);
    }
    .ack-modal__textarea:focus{
      border-color: rgba(57,229,140,.42);
      box-shadow:
        0 0 0 3px rgba(57,229,140,.08),
        0 0 16px rgba(57,229,140,.12);
    }
    .ack-modal__hint{
      margin-top: 8px;
      font-size: 12px;
      color: rgba(185,235,208,.62);
    }
    .ack-modal__footer{
      display: flex;
      justify-content: flex-end;
      gap: 10px;
      padding: 16px 20px 20px;
    }
    .ack-btn{
      height: 42px;
      border-radius: 12px;
      border: 1px solid rgba(255,255,255,.08);
      padding: 0 16px;
      font-size: 13px;
      font-weight: 700;
      cursor: pointer;
      transition: all .16s ease;
    }
    .ack-btn--ghost{
      background: rgba(255,255,255,.04);
      color: rgba(233,255,243,.86);
    }
    .ack-btn--ghost:hover{
      background: rgba(255,255,255,.07);
    }
    .ack-btn--confirm{
      background: rgba(57,229,140,.10);
      border-color: rgba(57,229,140,.28);
      color: #cffff0;
      box-shadow: 0 0 18px rgba(57,229,140,.08);
    }
    .ack-btn--confirm:hover{
      background: rgba(57,229,140,.16);
      border-color: rgba(57,229,140,.42);
      box-shadow: 0 0 24px rgba(57,229,140,.14);
    }
    .ack-modal__confirm-box{
      margin-top: 14px;
      padding: 12px 14px;
      border-radius: 12px;
      border: 1px solid rgba(255,186,65,.18);
      background: rgba(255,186,65,.06);
      color: rgba(255,241,214,.92);
      font-size: 13px;
      line-height: 1.45;
    }
    .ack-modal__confirm-actions{
      display: flex;
      justify-content: flex-end;
      gap: 10px;
      margin-top: 12px;
    }
    .ack-modal .hidden{ display: none; }
  `;
  document.head.appendChild(style);
  ACK_MODAL_READY = true;
}

function openAckModal(alarm) {
  ensureAckModal();
  return new Promise((resolve) => {
    const plantLabel = alarm?.power_plant_name || "—";
    const deviceTypeLabel = alarm?.device_type_name || alarm?.device_type || "—";
    const deviceLabel = alarm?.device_name ? `${deviceTypeLabel} • ${alarm.device_name}` : (alarm?.device_id || "—");
    const eventName =
      alarm?.event_name && String(alarm.event_name).trim()
        ? String(alarm.event_name).trim()
        : getAlarmDescription(alarm?.event_code);

    const overlay = document.createElement("div");
    overlay.className = "ack-modal-overlay";
    overlay.innerHTML = `
      <div class="ack-modal" role="dialog" aria-modal="true" aria-labelledby="ackModalTitle">
        <div class="ack-modal__header">
          <h3 class="ack-modal__title" id="ackModalTitle">Reconhecer alerta</h3>
          <div class="ack-modal__subtitle">
            Confirme o reconhecimento do alerta e registre uma observação para histórico.
          </div>
        </div>
        <div class="ack-modal__body">
          <div class="ack-modal__alarm">
            <strong>${plantLabel}</strong><br>
            ${deviceLabel}<br>
            ${eventName}
          </div>
          <label class="ack-modal__label" for="ackModalTextarea">Observação do reconhecimento</label>
          <textarea id="ackModalTextarea" class="ack-modal__textarea" placeholder="Ex.: equipe acionada, verificação em campo, evento validado..."></textarea>
          <div class="ack-modal__hint">
            Essa descrição será salva no banco em <strong>acknowledgment_note</strong>.
          </div>
          <div class="ack-modal__confirm-box hidden" id="ackModalConfirmBox">
            Tem certeza que deseja reconhecer este alerta com essa observação?
            <div class="ack-modal__confirm-actions">
              <button type="button" class="ack-btn ack-btn--ghost" id="ackModalBackBtn">Voltar</button>
              <button type="button" class="ack-btn ack-btn--confirm" id="ackModalFinalConfirmBtn">Confirmar envio</button>
            </div>
          </div>
        </div>
        <div class="ack-modal__footer">
          <button type="button" class="ack-btn ack-btn--ghost" id="ackModalCancelBtn">Cancelar</button>
          <button type="button" class="ack-btn ack-btn--confirm" id="ackModalContinueBtn">Continuar</button>
        </div>
      </div>
    `;
    document.body.appendChild(overlay);

    const textarea = overlay.querySelector("#ackModalTextarea");
    const cancelBtn = overlay.querySelector("#ackModalCancelBtn");
    const continueBtn = overlay.querySelector("#ackModalContinueBtn");
    const confirmBox = overlay.querySelector("#ackModalConfirmBox");
    const backBtn = overlay.querySelector("#ackModalBackBtn");
    const finalConfirmBtn = overlay.querySelector("#ackModalFinalConfirmBtn");

    const close = (result) => {
      overlay.remove();
      resolve(result);
    };

    requestAnimationFrame(() => textarea?.focus());
    cancelBtn?.addEventListener("click", () => close(null));
    overlay.addEventListener("click", (e) => {
      if (e.target === overlay) close(null);
    });
    document.addEventListener("keydown", function escHandler(e) {
      if (e.key === "Escape") {
        document.removeEventListener("keydown", escHandler);
        close(null);
      }
    }, { once: true });
    continueBtn?.addEventListener("click", () => confirmBox?.classList.remove("hidden"));
    backBtn?.addEventListener("click", () => confirmBox?.classList.add("hidden"));
    finalConfirmBtn?.addEventListener("click", () => close(textarea?.value ?? ""));
  });
}

function openAckDetailsModal(alarm) {
  ensureAckModal();
  const overlay = document.createElement("div");
  overlay.className = "ack-modal-overlay";

  const plantLabel = alarm?.power_plant_name || "—";
  const deviceTypeLabel = alarm?.device_type_name || alarm?.device_type || "—";
  const deviceLabel = alarm?.device_name ? `${deviceTypeLabel} • ${alarm.device_name}` : (alarm?.device_id || "—");
  const baseDesc =
    alarm?.event_name && String(alarm.event_name).trim()
      ? String(alarm.event_name).trim()
      : getAlarmDescription(alarm?.event_code);
  const ackBy = alarm?.acknowledged_by ? String(alarm.acknowledged_by).trim() : "—";
  const ackAt = alarm?.acknowledged_at ? new Date(alarm.acknowledged_at).toLocaleString("pt-BR") : "—";
  const ackNote = alarm?.acknowledgment_note ? String(alarm.acknowledgment_note).trim() : "—";

  overlay.innerHTML = `
    <div class="ack-modal" role="dialog" aria-modal="true" aria-labelledby="ackDetailsTitle">
      <div class="ack-modal__header">
        <h3 class="ack-modal__title" id="ackDetailsTitle">Detalhes do reconhecimento</h3>
        <div class="ack-modal__subtitle">Acknowledge note</div>
      </div>
      <div class="ack-modal__body">
        <div class="ack-modal__alarm">
          <strong>${plantLabel}</strong><br>
          ${deviceLabel}<br>
          ${baseDesc}
        </div>
        <div class="ack-modal__hint"><strong>Ack by:</strong> ${ackBy}</div>
        <div class="ack-modal__hint"><strong>Acknowledged at:</strong> ${ackAt}</div>
        <label class="ack-modal__label">Acknowledge note</label>
        <div class="ack-modal__alarm" style="white-space:pre-wrap;">${ackNote}</div>
      </div>
      <div class="ack-modal__footer">
        <button type="button" class="ack-btn ack-btn--confirm" id="ackDetailsCloseBtn">Fechar</button>
      </div>
    </div>
  `;

  document.body.appendChild(overlay);
  const close = () => overlay.remove();
  overlay.querySelector("#ackDetailsCloseBtn")?.addEventListener("click", close);
  overlay.addEventListener("click", (e) => {
    if (e.target === overlay) close();
  });
}

function sortRecognizedAlarms(list) {
  const arr = Array.isArray(list) ? list.slice() : [];
  const ts = (a) => Date.parse(a?.acknowledged_at || "") || 0;
  return arr.sort((a, b) => ts(b) - ts(a));
}

function sortActiveAlarms(list) {
  const arr = Array.isArray(list) ? list.slice() : [];
  const ts = (a) => Date.parse(a?.started_at || a?.timestamp || "") || 0;
  return arr.sort((a, b) => ts(b) - ts(a));
}

function ensureAlarmsHeader(isRecognized) {
  const tr = document.querySelector(".alarms-table thead tr");
  if (!tr) return;
  tr.innerHTML = isRecognized
    ? "<th>Pathname</th><th>Description</th><th>Ack By</th><th>Ack Note</th><th>State</th><th>Timestamp</th>"
    : "<th>Pathname</th><th>Description</th><th>State</th><th>Timestamp</th>";
}

async function renderAlarmsTable(isRecognized = false, { force = false } = {}) {
  const tbody = document.getElementById("alarmsTbody");
  if (!tbody) return;

  const renderSeq = ++ALARMS_RENDER_SEQ;
  ensureAlarmsHeader(isRecognized);

  let alarms = [];
  try {
    if (isRecognized) {
      const fetched = await fetchAcknowledgedAlarms();
      alarms = [...LOCAL_ACKED_ALARMS, ...fetched].filter(a => {
        return a?.acknowledged === true || a?.acknowledged === "true";
      });
      LOCAL_ACKED_ALARMS = sortRecognizedAlarms(dedupeAlarms(alarms)).slice(0, 500);
      alarms = LOCAL_ACKED_ALARMS.slice();
    } else {
      alarms = (await fetchActiveAlarms()).filter(a => {
        const state = String(a.alarm_state || a.state || "").toUpperCase();
        const id = String(a?.event_row_id ?? a?.id ?? "");
        const locallyAcked = LOCAL_ACKED_ALARMS.some((x) => String(x?.event_row_id ?? x?.id ?? "") === id);
        return state === "ACTIVE" && !locallyAcked;
      });
      alarms = sortActiveAlarms(alarms);
    }
  } catch (err) {
    console.error("Erro ao buscar alarmes:", err);
  }

  if (renderSeq !== ALARMS_RENDER_SEQ) return;

  alarms = dedupeAlarms(alarms);
  alarms = isRecognized ? sortRecognizedAlarms(alarms) : sortActiveAlarms(alarms);

  const renderKey = buildAlarmRenderKey(alarms, isRecognized);
  const nextMode = isRecognized ? "recognized" : "active";
  const modeChanged = CURRENT_ALARMS_TAB_MODE !== nextMode;

  if (!force && !modeChanged) {
    if (isRecognized) {
      if (renderKey === LAST_RECOGNIZED_ALARMS_RENDER_KEY) return;
    } else {
      if (renderKey === LAST_ACTIVE_ALARMS_RENDER_KEY) return;
    }
  }

  if (isRecognized) {
    LAST_RECOGNIZED_ALARMS_RENDER_KEY = renderKey;
  } else {
    LAST_ACTIVE_ALARMS_RENDER_KEY = renderKey;
  }
  CURRENT_ALARMS_TAB_MODE = nextMode;

  tbody.innerHTML = "";

  if (!alarms || alarms.length === 0) {
    tbody.innerHTML = `<tr><td colspan="${isRecognized ? 6 : 4}" style="text-align:center; opacity:0.6; padding:40px;">${isRecognized ? "Nenhum alerta reconhecido" : "Nenhum alerta ativo"}</td></tr>`;
    return;
  }

  alarms.forEach(alarm => {
    if (renderSeq !== ALARMS_RENDER_SEQ) return;
    const tr = document.createElement("tr");
    const sev = normalizeAlarmSeverity(
      alarm.severity || alarm.alarm_severity || alarm.level || alarm.alarm_level
    ) || "low";

    const timestamp = isRecognized
      ? (
          alarm.acknowledged_at ||
          alarm.cleared_at ||
          alarm.timestamp ||
          alarm.started_at ||
          "—"
        )
      : (
          alarm.started_at ||
          alarm.timestamp ||
          alarm.last_event_ts ||
          "—"
        );

    const tsFormatted = timestamp !== "—" ? new Date(timestamp).toLocaleString("pt-BR") : "—";

    const rawState = String(alarm.alarm_state || alarm.state || "—").toUpperCase();
    const state = isRecognized ? "ACK" : rawState;
    const stateColor =
      state === "ACTIVE" ? "#f44336" :
      state === "ACK" ? "#ff9800" :
      state === "CLEARED" ? "#4caf50" :
      "#ccc";

    const plantLabel = alarm.power_plant_name ? alarm.power_plant_name : "—";
    const deviceTypeLabel =
      alarm.device_type_name ||
      alarm.device_type ||
      "—";

    const deviceLabel = alarm.device_name
      ? `${deviceTypeLabel} • ${alarm.device_name}`
      : (alarm.device_id || "—");

    const baseDesc =
      alarm.event_name && String(alarm.event_name).trim() !== ""
        ? alarm.event_name
        : getAlarmDescription(alarm.event_code);
    const ackBy = alarm.acknowledged_by ? String(alarm.acknowledged_by).trim() : "";
    const ackNote = alarm.acknowledgment_note ? String(alarm.acknowledgment_note).trim() : "";
    if (isRecognized) {
      tr.innerHTML = `
        <td>${plantLabel} • ${deviceLabel}</td>
        <td>${baseDesc}</td>
        <td>${valueOrDash(ackBy)}</td>
        <td>${ackNote ? `<button type="button" class="ack-note-link">Ver note</button>` : "—"}</td>
        <td class="alarm-state-pill" style="font-weight:bold; color:${stateColor};">${state}</td>
        <td>${tsFormatted}</td>
      `;
    } else {
      tr.innerHTML = `
        <td>${plantLabel} • ${deviceLabel}</td>
        <td>${baseDesc}</td>
        <td class="alarm-state-pill" style="font-weight:bold; color:${stateColor};">${state}</td>
        <td>${tsFormatted}</td>
      `;
    }

    if (!isRecognized) {
      tr.classList.add("alarm-row-attention", `alarm-row-attention--${sev}`);
      tr.style.cursor = "pointer";
      tr.title = "Clique duplo para reconhecer";
      tr.addEventListener("dblclick", async () => {
        try {
          if (!alarm?.event_row_id && !alarm?.id) return;
          const note = await openAckModal(alarm);
          if (note === null) return;
          const ackPayload = await acknowledgeAlarm(alarm, note);

          const user = JSON.parse(localStorage.getItem("user") || "{}");
          const ackByLocal = user?.username || user?.name || user?.email || "operador";
          const ackNowIso = new Date().toISOString();
          const recognizedAlarm = {
            ...alarm,
            ...(ackPayload && typeof ackPayload === "object" ? ackPayload : {}),
            acknowledged: true,
            acknowledged_by: (ackPayload?.acknowledged_by ?? ackByLocal),
            acknowledgment_note: (ackPayload?.acknowledgment_note ?? (note || null)),
            acknowledged_at: (ackPayload?.acknowledged_at ?? ackNowIso),
            alarm_state: (ackPayload?.alarm_state ?? "ACK"),
            state: (ackPayload?.state ?? "ACK")
          };

          const recognizedTab = document.querySelectorAll(".tab-btn")[1];
          if (recognizedTab) {
            document.querySelectorAll(".tab-btn").forEach(b => b.classList.remove("active"));
            recognizedTab.classList.add("active");
          }

          LAST_ACTIVE_ALARMS_RENDER_KEY = "";
          LAST_RECOGNIZED_ALARMS_RENDER_KEY = "";
          LOCAL_ACKED_ALARMS = sortRecognizedAlarms(dedupeAlarms([recognizedAlarm, ...LOCAL_ACKED_ALARMS])).slice(0, 500);

          await renderAlarmsTable(true);
        } catch (err) {
          console.error("Erro ao reconhecer alarme:", err);
          alert(err?.message || "Não foi possível reconhecer o alarme.");
        }
      });
    }

    if (isRecognized && ackNote) {
      requestAnimationFrame(() => {
        tr.querySelector(".ack-note-link")?.addEventListener("click", (e) => {
          e.stopPropagation();
          openAckDetailsModal(alarm);
        });
      });
    }

    tbody.appendChild(tr);
  });
}

// =============================================================================
// EVENTS: render + load
// =============================================================================
function ensureEventsHeaderHasSeverity(tbody) {
  const eventsTable = tbody?.closest("table");
  const thead = eventsTable?.querySelector("thead");
  const tr = thead?.querySelector("tr");
  if (!tr) return;

  tr.innerHTML = `<th>TIMESTAMP</th><th>USINA</th><th>EQUIPMENT</th><th>DESCRIPTION</th><th>TYPE</th><th>STATUS</th><th>SEVERITY</th>`;
}

async function loadEvents(page = 1, { silent = false } = {}) {
  const tbody = document.getElementById("eventsTbody");
  if (!tbody) return;

  if (EVENTS_STATE.loading) return;
  EVENTS_STATE.loading = true;

  try {
    wireEventsFiltersOnce();
    ensureDefaultEventsDateTimes();
    ensureSeveritySelectOptions();
    ensureTypeSelectOptions();
    ensureStatusSelectOptions();
    ensureEventsHeaderHasSeverity(tbody);

    if (!silent) {
      tbody.innerHTML = `
        <tr><td colspan="7" style="text-align:center; opacity:0.7; padding:40px;">Carregando...</td></tr>
      `;
    }

    const filters = getEventsFiltersFromUI();

    if (!filters.start_time || !filters.end_time) {
      const end = new Date();
      const start = new Date();
      start.setDate(end.getDate() - 7);
      filters.start_time = start.toISOString();
      filters.end_time = end.toISOString();
    }

    const response = await fetchEventsSafeBackend({
      start_time: filters.start_time,
      end_time: filters.end_time,
      page,
      page_size: EVENTS_STATE.page_size,
      severity: filters.severity,
      event_type: filters.event_type,
      status: filters.status,
      source: filters.source,
      device_id: filters.device_id,
      q: filters.q,
      plant_id: filters.plant_id,
      mode: EVENTS_VIEW_MODE,
      rounds: EVENTS_ROUNDS,
      include_total: true
    });

    const events = response?.items || [];
    const eventsRenderKey = buildEventsRenderKey(events, page, {
      start_time: filters.start_time,
      end_time: filters.end_time,
      severity: filters.severity,
      event_type: filters.event_type,
      status: filters.status,
      q: filters.q,
      plant_id: filters.plant_id,
      device_id: filters.device_id
    });

    updateEventsPaginationUI({
      page,
      page_size: EVENTS_STATE.page_size,
      total: response?.pagination?.total ?? null,
      total_pages: response?.pagination?.total_pages ?? null
    });

    if (!events.length) {
      LAST_EVENTS_RENDER_KEY = eventsRenderKey;
      tbody.innerHTML = `
        <tr><td colspan="7" style="text-align:center; opacity:0.6; padding:40px;">
          Nenhum evento registrado
        </td></tr>
      `;
      return;
    }

    if (eventsRenderKey === LAST_EVENTS_RENDER_KEY) {
      EVENTS_STATE.page = page;
      return;
    }
    LAST_EVENTS_RENDER_KEY = eventsRenderKey;

    tbody.innerHTML = "";

    events.forEach(ev => {
      const tr = document.createElement("tr");

      const ts = ev.event_ts ? new Date(ev.event_ts).toLocaleString("pt-BR") : "—";
      const plant = valueOrDash(ev.power_plant_name ?? ev.plant_name ?? ev.power_plant_id ?? ev.plant_id);

      const deviceLabel =
        ev.device_type && ev.device_name
          ? `${ev.device_type} • ${ev.device_name}`
          : (ev.device_name || ev.device_id || "—");

      const baseDesc = valueOrDash(ev.event_name ?? ev.description ?? ev.point_name ?? ev.event_code ?? ev.raw_key ?? "—");
      const ackBy = ev.acknowledged_by ? String(ev.acknowledged_by).trim() : "";
      const ackNote = ev.acknowledgment_note ? String(ev.acknowledgment_note).trim() : "";
      const desc = `
        <div>${baseDesc}</div>
        ${ackBy ? `<div class="ack-inline-meta">Ack by: ${ackBy}</div>` : ""}
        ${ackNote ? `<button type="button" class="ack-note-link">Ver note</button>` : ""}
      `;
      const type = valueOrDash(ev.event_type);
      const status = valueOrDash(ev.status ?? ev.event_status ?? ev.state);
      const sev = valueOrDash(ev.severity);

      tr.innerHTML = `
        <td>${ts}</td>
        <td>${plant}</td>
        <td>${deviceLabel}</td>
        <td>${desc}</td>
        <td>${type}</td>
        <td>${status}</td>
        <td style="font-weight:bold; color:${severityColor(sev)};">
          ${sev}
        </td>
      `;

      tbody.appendChild(tr);
      if (ackNote) {
        tr.querySelector(".ack-note-link")?.addEventListener("click", (e) => {
          e.stopPropagation();
          openAckDetailsModal(ev);
        });
      }
    });

    EVENTS_STATE.page = page;
  } catch (err) {
    if (String(err?.name) === "AbortError") return;

    console.error("Erro ao buscar eventos:", err?.message, err?.url, err?.body);
    tbody.innerHTML = `
      <tr><td colspan="7" style="text-align:center; color:#f44336; padding:40px;">
        Erro ao carregar eventos
      </td></tr>
    `;
  } finally {
    EVENTS_STATE.loading = false;
  }
}

// =============================================================================
// SUMMARY + PORTFOLIO
// =============================================================================
function updateSummaryUI(plants) {
  const validPlants = sortPortfolioPlants(plants);

  let totalActivePower = 0;
  let totalRatedPower = 0;
  let totalCapacityAc = 0;

  validPlants.forEach(p => {
    totalActivePower += Number(p?.active_power_kw ?? 0) || 0;
    totalRatedPower += Number(p?.capacity_dc ?? 0) || 0;
    totalCapacityAc += Number(p?.capacity_ac ?? 0) || 0;
  });

  const loadPct = totalRatedPower > 0 ? (totalActivePower / totalRatedPower) * 100 : 0;

  const elActive = document.querySelector("#activePower");
  const elRated = document.querySelector("#ratedPower");
  const elPercent = document.querySelector("#progressPercent");

  if (elActive) elActive.innerText = totalActivePower.toFixed(1) + " kW";
  if (elRated) elRated.innerText = totalRatedPower.toFixed(1) + " kWp";
  if (elPercent) elPercent.innerText = loadPct.toFixed(1) + "%";

  const elPsfActive = document.getElementById("psfActivePower");
  const elPsfRated = document.getElementById("psfRatedPower");
  const elPsfRatedAc = document.getElementById("psfRatedAc");
  const elPsfPercent = document.getElementById("psfCapacityPct");

  if (elPsfActive) elPsfActive.textContent = totalActivePower.toFixed(1) + " kW";
  if (elPsfRated) elPsfRated.textContent = totalRatedPower.toFixed(1) + " kWp";
  if (elPsfRatedAc) elPsfRatedAc.textContent = totalCapacityAc > 0 ? totalCapacityAc.toFixed(1) + " kWp" : "—";
  if (elPsfPercent) elPsfPercent.textContent = loadPct.toFixed(1) + "%";

  // Update SVG progress ring
  const ringFill = document.getElementById("psfRingFill");
  if (ringFill) {
    const circumference = 2 * Math.PI * 30;
    const pct = Math.min(100, Math.max(0, loadPct));
    const filled = (pct / 100) * circumference;
    ringFill.setAttribute("stroke-dasharray", filled.toFixed(1) + " " + circumference.toFixed(1));
  }
}

function getPortfolioPlantVisualState(plant) {
  const activePower = Number(plant?.active_power_kw ?? 0);
  const energyToday = Number(plant?.energy_today_kwh ?? plant?.daily_energy_kwh ?? 0);
  const irradiance = Number(plant?.irradiance_wm2 ?? 0);
  const statusColor = plant?.plant_status_color || plant?.plant_status || '';
  const hasAnyData = activePower > 0 || energyToday > 0 || irradiance > 0;

  if (activePower > 0) {
    return { priority: 0, kind: "generating", activePower, energyToday, irradiance, isOffline: false };
  }

  // Standby: inversores em espera (status_color amarelo) → card amarelo
  if (statusColor === 'yellow') {
    return { priority: 1, kind: "standby", activePower, energyToday, irradiance, isOffline: false };
  }

  // Desligada/sem dados: gray, red sem potência, ou sem dados
  if (!hasAnyData || statusColor === 'gray' || statusColor === 'red') {
    return { priority: 2, kind: "offline", activePower, energyToday, irradiance, isOffline: true };
  }

  return { priority: 1, kind: "standby", activePower, energyToday, irradiance, isOffline: false };
}

function sortPortfolioPlants(plants) {
  const validPlants = Array.isArray(plants) ? [...plants] : [];

  validPlants.sort((a, b) => {
    const stateA = getPortfolioPlantVisualState(a);
    const stateB = getPortfolioPlantVisualState(b);

    if (stateA.priority !== stateB.priority) {
      return stateA.priority - stateB.priority;
    }

    if (stateA.kind === "generating" && stateB.kind === "generating" && stateA.activePower !== stateB.activePower) {
      return stateB.activePower - stateA.activePower;
    }

    if (stateA.kind === "standby" && stateB.kind === "standby" && stateA.energyToday !== stateB.energyToday) {
      return stateB.energyToday - stateA.energyToday;
    }

    const nameA = String(a?.power_plant_name ?? a?.plant_name ?? a?.name ?? "");
    const nameB = String(b?.power_plant_name ?? b?.plant_name ?? b?.name ?? "");
    return nameA.localeCompare(nameB, "pt-BR", { sensitivity: "base" });
  });

  return validPlants;
}

function getPlantCardStatus(plant) {
  // "Sem comunicação" = somente quando NÃO recebemos dados recentes (comunicação real perdida)
  if (plant.comm_status === 'offline') {
    return { colorClass: 'plant-card--offline', badge: 'Sem comunicação', badgeClass: 'badge--offline' };
  }
  const lastUpdate = plant.updated_at || plant.last_update;
  if (lastUpdate) {
    const age = (Date.now() - new Date(lastUpdate).getTime()) / 60000;
    if (age > 30) {
      return { colorClass: 'plant-card--offline', badge: 'Sem comunicação', badgeClass: 'badge--offline' };
    }
  }
  if (plant.comm_status === 'partial') {
    const rpt = Number(plant.inverter_reporting ?? 0);
    const stale = Number(plant.inverter_stale ?? 0);
    const txt = `Comunicação parcial (${rpt}/${rpt + stale})`;
    return { colorClass: 'plant-card--warning', badge: txt, badgeClass: 'badge--partial' };
  }
  // plant_status_color 'red'/'gray'/'yellow' com dados frescos = NÃO é perda de comunicação
  // red = inversores em falha mas comunicando; gray = desligada; yellow = standby
  return { colorClass: '', badge: null, badgeClass: null };
}

function renderPortfolioTable(plants) {
  const tbody = document.getElementById("portfolioTbody");
  if (!tbody) return;

  const validPlants = sortPortfolioPlants(plants);
  if (validPlants.length === 0) return;

  tbody.innerHTML = "";

  validPlants.forEach(plant => {
    const plantId = plant.power_plant_id ?? plant.plant_id ?? plant.id;
    const plantName = plant.power_plant_name ?? plant.plant_name ?? plant.name;
    const openPlantPage = () => {
      if (plantId == null) return;
      window.location.href = `plant.html?plant_id=${encodeURIComponent(plantId)}`;
    };

    const tr = document.createElement("tr");
    tr.classList.add("portfolio-row-linkable");
    tr.setAttribute("role", "link");
    tr.setAttribute("tabindex", "0");

    // Linha cinza se planta sem dados ou status 28
    const plantState = getPortfolioPlantVisualState(plant);
    const commStatus = getPlantCardStatus(plant);
    const isCommOffline = commStatus.colorClass === 'plant-card--offline';
    if (plantState.isOffline || isCommOffline) tr.classList.add("portfolio-row--offline");

    const alarmSeverity =
      normalizeAlarmSeverity(lastAlarmSeverityByPlant.get(plantId)) ||
      normalizeAlarmSeverity(lastAlarmSeverityByPlant.get(plantName)) ||
      null;
    const plantIconClass = alarmSeverity
      ? `plant-icon plant-icon--${alarmSeverity}`
      : (plantState.kind === "standby" ? "plant-icon plant-icon--standby" : "plant-icon plant-icon--ok");

    const commBadgeHtml = commStatus.badge
      ? `<span class="${commStatus.badgeClass}" style="margin-left:8px">${commStatus.badge}</span>`
      : '';
    const activePowerTd = isCommOffline
      ? '<td class="metric-active metric-zero">—</td>'
      : `<td class="metric-active${Number(plant.active_power_kw ?? 0) === 0 ? ' metric-zero' : ''}">${Number(plant.active_power_kw ?? 0).toFixed(1)} kW</td>`;

    tr.innerHTML = `
      <td>
        <button class="plant-cell-btn" title="Abrir usina ${valueOrDash(plantName)}">
          <span class="plant-cell">
            <span class="${plantIconClass}" title="${alarmSeverity || "ok"}">
              <i class="fa-solid fa-seedling"></i>
            </span>
            <span class="plant-name-text">${valueOrDash(plantName)}</span>
            ${commBadgeHtml}
          </span>
        </button>
      </td>
      <td class="metric-neutral">${Number(plant.rated_power_kw ?? 0).toFixed(1)} kWp</td>
      ${activePowerTd}
      <td class="metric-active">${Number(plant.energy_today_kwh ?? 0).toFixed(1)} kWh</td>
      <td>${plant.irradiance_wm2 != null ? Number(plant.irradiance_wm2).toFixed(0) + " W/m²" : "—"}</td>
      <td>${plant.inverter_availability_pct != null ? Number(plant.inverter_availability_pct).toFixed(1) + "%" : "—"}</td>
      <td>${plant.relay_availability_pct != null ? Number(plant.relay_availability_pct).toFixed(1) + "%" : "—"}</td>
      <td>${plant.pr_daily_pct != null ? Number(plant.pr_daily_pct).toFixed(1) + "%" : "—"}</td>
      <td>${plant.pr_accumulated_pct != null ? Number(plant.pr_accumulated_pct).toFixed(1) + "%" : "—"}</td>
      <td style="text-align:center;">
        <button class="plant-link-btn" title="Abrir usina" data-plant-id="${plantId}">
          <i class="fa-solid fa-arrow-up-right-from-square"></i>
        </button>
      </td>
    `;

    tr.querySelector(".plant-link-btn").addEventListener("click", (e) => {
      e.stopPropagation();
      openPlantPage();
    });

    tr.querySelector(".plant-cell-btn").addEventListener("click", (e) => {
      e.stopPropagation();
      openPlantPage();
    });

    tr.addEventListener("click", (e) => {
      if (e.target.closest("button")) return;
      openPlantPage();
    });

    tr.addEventListener("keydown", (e) => {
      if (e.key !== "Enter" && e.key !== " ") return;
      e.preventDefault();
      openPlantPage();
    });

    const _u2 = JSON.parse(localStorage.getItem("user") || "{}");
    if (_u2.is_superuser === true || _u2.role_key === "admin_customer") {
      const _editTd = document.createElement("td");
      _editTd.style.textAlign = "center";
      _editTd.innerHTML = `
        <button class="plant-action-btn plant-list-edit-btn" title="Editar usina" style="cursor:pointer;">
          <svg viewBox="0 0 20 20" fill="currentColor" style="width:14px;height:14px;color:#39e58c;"><path d="M13.586 3.586a2 2 0 112.828 2.828l-.793.793-2.828-2.828.793-.793zM11.379 5.793L3 14.172V17h2.828l8.38-8.379-2.83-2.828z"/></svg>
        </button>`;
      _editTd.querySelector("button").addEventListener("click", (e) => {
        e.stopPropagation();
        openPlantEditModal(plantId, plantName, Number(plant.rated_power_kw ?? 0));
      });
      tr.appendChild(_editTd);
    }

    tbody.appendChild(tr);
  });
}

// =============================================================================
// DATA STUDIO
// =============================================================================

function dsSafeTrim(v) {
  if (v == null) return "";
  return String(v).trim();
}

function dsAbbrevContext(ctx) {
  if (!ctx) return ctx;
  return ctx
    .replace(/Eletrocentro\s*/gi, 'Eletr.')
    .replace(/Cabine\s*/gi,       'Cab.')
    .replace(/Inversor\s*/gi,     'Inv.')
    .replace(/Inverter\s*/gi,     'Inv.');
}

function dsIsoStartOfDay(dateYYYYMMDD) {
  if (!dateYYYYMMDD) return null;
  const [y, m, d] = String(dateYYYYMMDD).split("-").map(Number);
  if (!y || !m || !d) return null;
  const dt = new Date(y, m - 1, d, 0, 0, 0, 0);
  return Number.isNaN(dt.getTime()) ? null : dt.toISOString();
}

function dsIsoEndOfDay(dateYYYYMMDD) {
  if (!dateYYYYMMDD) return null;
  const [y, m, d] = String(dateYYYYMMDD).split("-").map(Number);
  if (!y || !m || !d) return null;
  const dt = new Date(y, m - 1, d, 23, 59, 59, 999);
  return Number.isNaN(dt.getTime()) ? null : dt.toISOString();
}

function dsClampRange(startISO, endISO) {
  if (!startISO || !endISO) return { startISO, endISO };
  const s = new Date(startISO).getTime();
  const e = new Date(endISO).getTime();
  if (!Number.isFinite(s) || !Number.isFinite(e)) return { startISO, endISO };
  if (e < s) return { startISO: endISO, endISO: startISO };
  return { startISO, endISO };
}

function dsNormalizeApiBody(data) {
  if (data && Object.prototype.hasOwnProperty.call(data, "body")) {
    return typeof data.body === "string" ? JSON.parse(data.body) : data.body;
  }
  return data;
}

function dsNormalizeContextText(value) {
  return dsSafeTrim(value)
    .toLowerCase()
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .replace(/[^a-z0-9]+/g, " ")
    .trim();
}

function dsContextMatches(tagContext, selectedContext) {
  if (!selectedContext || selectedContext === "all") return true;

  const tagNorm = dsNormalizeContextText(tagContext);
  const selectedNorm = dsNormalizeContextText(selectedContext);
  if (!tagNorm || !selectedNorm) return false;

  return tagNorm === selectedNorm ||
         tagNorm.includes(selectedNorm) ||
         selectedNorm.includes(tagNorm);
}

function dsNormalizeSearchText(value) {
  return dsSafeTrim(value)
    .toLowerCase()
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .replace(/[^a-z0-9]+/g, " ")
    .trim();
}

function dsTagMatchesSearch(tag, query) {
  const needle = dsNormalizeSearchText(query);
  if (!needle) return true;

  const haystack = [
    tag?.description,
    tag?.point_name,
    tag?.pathname,
    tag?.path_name,
    tag?.context,
    tag?.device_type,
    tag?.unit
  ].map(dsNormalizeSearchText).join(" ");

  return haystack.includes(needle);
}

function dsTagGroup(tag) {
  const pathname = String(tag?.pathname || tag?.path_name || "");
  const deviceType = String(tag?.device_type || "").toLowerCase();

  if (String(tag?.data_kind || "").toLowerCase() === "discrete") return "alarm";
  if (pathname.startsWith("PLANT.")) return "plant";
  if (pathname.startsWith("WEATHER_") || deviceType.includes("weather")) return "weather";
  if (pathname.startsWith("METER_") || deviceType.includes("meter")) return "meter";
  if (pathname.startsWith("RELAY_") || deviceType.includes("relay")) return "relay";
  if (pathname.startsWith("INV_") || deviceType.includes("inverter")) return "inverter";
  return "outro";
}

function dsCategoryMatches(tag, category) {
  const cat = dsSafeTrim(category || "all");
  if (!cat || cat === "all") return true;

  const group = dsTagGroup(tag);
  const map = {
    planta: "plant",
    inversor: "inverter",
    weather: "weather",
    relay: "relay",
    meter: "meter",
    alarm: "alarm"
  };

  return group === (map[cat] || cat);
}

// =============================================================================
// DATA STUDIO — PER-PLANT STATE & FUNCTIONS
// =============================================================================
let DATASTUDIO_PLANTS = {};
// Key: String(plantId), Value: per-plant state object

function _initPlantState(plantId) {
  const plantName = dsGetPlantNameById(plantId) || `Usina ${plantId}`;
  return {
    plantId: Number(plantId),
    plantName,
    catalogTags: [],
    availableTags: [],
    selectedTags: [],
    selectedContext: "all",
    selectedCategory: "all",
    searchText: "",
    selectionId: null,
    chartData: null,
    catalogOpen: true,
    catalogConfirmed: false,
    chartInstance: null,
    tagsRequestSeq: 0,
    tagsAbortController: null
  };
}

function dsGetPlantNameById(plantId) {
  const sel = document.getElementById("dsPlantSelect");
  if (!sel) return null;
  const opt = sel.querySelector(`option[value="${plantId}"]`);
  return opt ? opt.textContent : null;
}

function getPlantUIElements(plantId) {
  return {
    catalogSection: document.getElementById(`dsCatalogSection_${plantId}`),
    searchInput: document.getElementById(`dsSearchInput_${plantId}`),
    contextSelect: document.getElementById(`dsContextSelect_${plantId}`),
    dataKindSelect: document.getElementById(`dsDataKindSelect_${plantId}`),
    sourceSelect: document.getElementById(`dsSourceSelect_${plantId}`),
    tagsTableBody: document.getElementById(`dsTagsTbody_${plantId}`),
    selectedCount: document.getElementById(`dsSelectedCount_${plantId}`),
    selectedTagsList: document.getElementById(`dsSelectedTagsList_${plantId}`),
    emptyState: document.getElementById(`dsEmptyState_${plantId}`),
    workspace: document.getElementById(`dsWorkspace_${plantId}`),
    chartWrap: document.getElementById(`dsChartWrap_${plantId}`),
    chartCanvas: document.getElementById(`dsChart_${plantId}`),
    foundCount: document.getElementById(`dsFoundCount_${plantId}`),
    contextInfo: document.getElementById(`dsContextInfo_${plantId}`)
  };
}

function getDataStudioActiveCategory() {
  return "all"; // now per-plant, kept for compat
}

// =============================================================================
// DATA STUDIO — PER-PLANT FUNCTIONS (replaces old singleton functions)
// =============================================================================

/**
 * Generates the full 3-column HTML block for a single plant.
 */
function _plantBlockHTML(plantId, plantName, plantColor, plantIdx) {
  const pid = plantId;
  return `
  <div class="ds-plant-block" data-plant="${pid}" id="dsPlantBlock_${pid}">
    <div class="ds-plant-block__header">
      <span class="ds-plant-block__dot" style="background:${plantColor}"></span>
      <strong>${plantName}</strong>
      <span class="ds-plant-block__tag-count" id="dsSelectedCount_${pid}">0 medidas</span>
      <button class="ds-plant-block__fs-exit" data-action="exitFullscreen" data-plant="${pid}" type="button" title="Sair da tela cheia">
        <i class="fa-solid fa-compress"></i> Sair
      </button>
    </div>

    <div class="ds-v2-body">
      <!-- LEFT: catalog -->
      <aside class="ds-v2-catalog" id="dsCatalogSection_${pid}">
        <div class="ds-v2-catalog-header">
          <h3>Catálogo de Medidas</h3>
          <button class="ds-v2-catalog-collapse" data-action="toggleCatalog" data-plant="${pid}" type="button">−</button>
        </div>

        <div class="ds-v2-catalog-filters">
          <div class="ds-v2-pills" id="dsCategoryPills_${pid}">
            <button class="ds-v2-pill active" data-cat="all" data-plant="${pid}" type="button">Todas</button>
            <button class="ds-v2-pill" data-cat="planta" data-plant="${pid}" type="button">Planta</button>
            <button class="ds-v2-pill" data-cat="inversor" data-plant="${pid}" type="button">Inversores</button>
            <button class="ds-v2-pill" data-cat="weather" data-plant="${pid}" type="button">Clima</button>
            <button class="ds-v2-pill" data-cat="meter" data-plant="${pid}" type="button">Medidores</button>
            <button class="ds-v2-pill" data-cat="relay" data-plant="${pid}" type="button">Relés</button>
            <button class="ds-v2-pill" data-cat="alarm" data-plant="${pid}" type="button">Alarmes</button>
          </div>
          <input type="text" class="ds-v2-search" id="dsSearchInput_${pid}" placeholder="Buscar medida..." data-plant="${pid}">
          <select class="ds-v2-filter-select" id="dsContextSelect_${pid}" data-plant="${pid}">
            <option value="all">Todos contextos</option>
          </select>
        </div>

        <div class="ds-v2-found-count" id="dsFoundCount_${pid}">0 medidas encontradas</div>
        <div class="ds-v2-context-info hidden" id="dsContextInfo_${pid}"></div>

        <div class="ds-v2-catalog-table-wrap">
          <table class="ds-table" id="dsTagsTable_${pid}">
            <thead>
              <tr>
                <th style="width:30px"></th>
                <th>Contexto</th>
                <th>Descrição</th>
                <th style="display:none">Fonte</th>
                <th style="display:none">Tipo</th>
                <th>Unidade</th>
                <th style="display:none">Usina</th>
                <th style="display:none">Pathname</th>
              </tr>
            </thead>
            <tbody id="dsTagsTbody_${pid}">
              <tr><td colspan="8" style="text-align:center;opacity:.5">Selecione filtros para carregar medidas</td></tr>
            </tbody>
          </table>
        </div>

        <div class="ds-v2-catalog-actions">
          <button class="ds-v2-catalog-confirm" data-action="confirmCatalog" data-plant="${pid}" type="button">
            Confirmar seleção
          </button>
        </div>
      </aside>

      <!-- CENTER: chart area -->
      <div class="ds-v2-chart-col">
        <div class="ds-empty-state" id="dsEmptyState_${pid}">
          <div class="ds-empty-icon"><i class="fa-solid fa-chart-line"></i></div>
          <h3>Selecione medidas no catálogo</h3>
          <p>Escolha medidas à esquerda e confirme para visualizar o gráfico.</p>
        </div>

        <div class="ds-workspace hidden" id="dsWorkspace_${pid}">
          <div class="ds-chart-wrap" id="dsChartWrap_${pid}">
            <canvas id="dsChart_${pid}" class="ds-plant-canvas"></canvas>
          </div>
          <div class="ds-chart-toolbar">
            <button class="ds-chart-toolbar__btn" data-action="zoomIn" data-plant="${pid}" type="button" title="Zoom +"><i class="fa-solid fa-magnifying-glass-plus"></i></button>
            <button class="ds-chart-toolbar__btn" data-action="zoomOut" data-plant="${pid}" type="button" title="Zoom −"><i class="fa-solid fa-magnifying-glass-minus"></i></button>
            <button class="ds-chart-toolbar__btn" data-action="zoomReset" data-plant="${pid}" type="button" title="Resetar zoom"><i class="fa-solid fa-arrows-rotate"></i></button>
            <button class="ds-chart-toolbar__btn" data-action="fullscreen" data-plant="${pid}" type="button" title="Tela cheia"><i class="fa-solid fa-expand"></i></button>
          </div>

          <div class="ds-v2-stats-section">
            <div class="ds-v2-stats-left">
              <h4>Resumo estatístico</h4>
              <div class="ds-v2-stats-table-wrap">
                <table class="ds-v2-stats-table">
                  <thead><tr><th>Série</th><th>Unidade</th><th>Mínimo</th><th>Máximo</th><th>Média</th><th>Último Valor</th></tr></thead>
                  <tbody id="dsStatsTbody_${pid}">
                    <tr><td colspan="6" style="opacity:.5;text-align:center">Sem dados</td></tr>
                  </tbody>
                </table>
              </div>
            </div>
            <div class="ds-v2-insights">
              <h4>Insights</h4>
              <div id="dsInsightsList_${pid}" class="ds-v2-insights-list"></div>
            </div>
          </div>
        </div>
      </div>

      <!-- RIGHT: selected series panel -->
      <aside class="ds-v2-series-panel">
        <div class="ds-v2-series-header">
          <h3>Séries selecionadas</h3>
        </div>
        <div class="ds-v2-series-list" id="dsSelectedTagsList_${pid}"></div>
      </aside>
    </div>
  </div>`;
}

/**
 * Renders plant blocks for all currently selected plants.
 */
function renderPlantBlocks() {
  const container = document.getElementById("dsPlantBlocks");
  if (!container) return;

  const ids = DATASTUDIO_STATE.selectedPlantIds;
  const noPlantState = document.getElementById("dsNoPlantState");

  if (!ids.length) {
    // Destroy charts, clear container, show empty state
    Object.keys(DATASTUDIO_PLANTS).forEach(pid => {
      const ps = DATASTUDIO_PLANTS[pid];
      if (ps.chartInstance) { try { ps.chartInstance.destroy(); } catch(e){} }
    });
    DATASTUDIO_PLANTS = {};
    DATASTUDIO_CHART = {};
    container.innerHTML = "";
    if (noPlantState) container.appendChild(noPlantState);
    if (noPlantState) noPlantState.style.display = "";
    return;
  }

  // Remove blocks for plants no longer selected
  Object.keys(DATASTUDIO_PLANTS).forEach(pid => {
    if (!ids.includes(Number(pid))) {
      const ps = DATASTUDIO_PLANTS[pid];
      if (ps.chartInstance) { try { ps.chartInstance.destroy(); } catch(e){} }
      delete DATASTUDIO_PLANTS[pid];
      delete DATASTUDIO_CHART[pid];
      const el = document.getElementById(`dsPlantBlock_${pid}`);
      if (el) el.remove();
    }
  });

  // Hide empty state
  if (noPlantState) noPlantState.style.display = "none";

  // Add blocks for newly selected plants
  ids.forEach((pid, idx) => {
    const pidStr = String(pid);
    if (DATASTUDIO_PLANTS[pidStr]) return; // already exists

    DATASTUDIO_PLANTS[pidStr] = _initPlantState(pid);
    const plantName = dsGetPlantNameById(pid) || `Usina ${pid}`;
    const plantColor = DS_PLANT_PALETTE[idx % DS_PLANT_PALETTE.length];
    const html = _plantBlockHTML(pidStr, plantName, plantColor, idx);
    container.insertAdjacentHTML("beforeend", html);
  });
}

/**
 * Called when the multi-select plant checkboxes change.
 */
function onSelectedPlantsChanged() {
  renderPlantBlocks();

  const ids = DATASTUDIO_STATE.selectedPlantIds;

  // Fetch tags for each newly added plant
  ids.forEach(pid => {
    const ps = DATASTUDIO_PLANTS[String(pid)];
    if (ps && !ps.catalogTags.length) {
      fetchDataStudioTagsForPlant(String(pid));
    }
  });

  _syncMultiselectLabel();
  if (typeof window._dsUpdateInfobar === "function") window._dsUpdateInfobar();
}

// --- Per-plant filter / UI functions ---

function setActiveCategoryForPlant(plantId, category) {
  const ps = DATASTUDIO_PLANTS[String(plantId)];
  if (!ps) return;
  const cat = dsSafeTrim(category || "all") || "all";
  ps.selectedCategory = cat;

  const pills = document.getElementById(`dsCategoryPills_${plantId}`);
  if (pills) {
    pills.querySelectorAll(".ds-v2-pill").forEach(p => {
      p.classList.toggle("active", (p.dataset.cat || "all") === cat);
    });
  }
}

function updateFoundCountForPlant(plantId, count, text) {
  const el = document.getElementById(`dsFoundCount_${plantId}`);
  if (!el) return;
  if (text) { el.textContent = text; return; }
  const n = Number(count) || 0;
  const plural = n === 1 ? "" : "s";
  el.textContent = `${n} medida${plural} encontrada${plural}`;
}

function updateContextInfoForPlant(plantId) {
  const el = document.getElementById(`dsContextInfo_${plantId}`);
  if (!el) return;
  const ps = DATASTUDIO_PLANTS[String(plantId)];
  const ctx = dsSafeTrim(ps?.selectedContext) || "all";
  el.textContent = ctx !== "all"
    ? `Exibindo medidas de: ${ctx}`
    : "Exibindo medidas de: todos contextos";
  el.classList.remove("hidden");
}

function applyTagFiltersForPlant(plantId) {
  const ps = DATASTUDIO_PLANTS[String(plantId)];
  if (!ps) return [];

  const catalog = Array.isArray(ps.catalogTags) ? ps.catalogTags : [];
  const selectedContext = dsSafeTrim(ps.selectedContext) || "all";
  const selectedCategory = dsSafeTrim(ps.selectedCategory) || "all";
  const searchText = dsSafeTrim(ps.searchText);

  const filtered = catalog.filter(tag =>
    dsContextMatches(tag?.context, selectedContext) &&
    dsCategoryMatches(tag, selectedCategory) &&
    dsTagMatchesSearch(tag, searchText)
  );

  ps.availableTags = filtered;
  updateContextInfoForPlant(plantId);
  updateFoundCountForPlant(plantId, filtered.length);
  renderDataStudioTagsTableForPlant(plantId, filtered);
  return filtered;
}

function populateContextSelectForPlant(plantId, tags) {
  const sel = document.getElementById(`dsContextSelect_${plantId}`);
  if (!sel) return;
  const ps = DATASTUDIO_PLANTS[String(plantId)];
  const prev = dsSafeTrim(sel.value || ps?.selectedContext) || "all";
  const contexts = Array.from(new Set(
    (Array.isArray(tags) ? tags : []).map(t => dsSafeTrim(t?.context)).filter(Boolean)
  ));

  sel.innerHTML = "";
  const allOpt = document.createElement("option");
  allOpt.value = "all";
  allOpt.textContent = "Todos contextos";
  sel.appendChild(allOpt);

  contexts.forEach(ctx => {
    const opt = document.createElement("option");
    opt.value = ctx;
    opt.textContent = ctx;
    sel.appendChild(opt);
  });

  sel.value = contexts.includes(prev) ? prev : "all";
  if (ps) ps.selectedContext = sel.value || "all";
}

// --- Per-plant tag selection ---

function isTagSelectedForPlant(plantId, tagOrPath) {
  const ps = DATASTUDIO_PLANTS[String(plantId)];
  if (!ps) return false;
  const key = selectedTagKey(tagOrPath);
  if (!key || key.endsWith(":")) return false;
  return ps.selectedTags.some(t => selectedTagKey(t) === key);
}

function addSelectedTagForPlant(plantId, tag) {
  const ps = DATASTUDIO_PLANTS[String(plantId)];
  if (!ps || !tag || !dsSafeTrim(tag.pathname)) return false;
  if (isTagSelectedForPlant(plantId, tag)) return true;
  if (ps.selectedTags.length >= 50) {
    window.alert("Você pode selecionar no máximo 50 medidas por usina.");
    return false;
  }
  ps.selectedTags.push(tag);
  updateStageUIForPlant(plantId);
  return true;
}

function removeSelectedTagForPlant(plantId, tagOrPath) {
  const ps = DATASTUDIO_PLANTS[String(plantId)];
  if (!ps) return;
  const key = selectedTagKey(tagOrPath);
  ps.selectedTags = ps.selectedTags.filter(t => selectedTagKey(t) !== key);
  updateStageUIForPlant(plantId);
}

function updateSelectedTagsCounterForPlant(plantId) {
  const ps = DATASTUDIO_PLANTS[String(plantId)];
  const count = ps ? ps.selectedTags.length : 0;
  const el = document.getElementById(`dsSelectedCount_${plantId}`);
  if (el) el.textContent = `${count} medida${count === 1 ? "" : "s"}`;
}

function renderSelectedTagsListForPlant(plantId) {
  const list = document.getElementById(`dsSelectedTagsList_${plantId}`);
  if (!list) return;
  const ps = DATASTUDIO_PLANTS[String(plantId)];
  if (!ps) return;

  list.innerHTML = "";
  const tags = Array.isArray(ps.selectedTags) ? ps.selectedTags : [];

  if (!tags.length) {
    list.classList.add("hidden");
    return;
  }

  list.classList.remove("hidden");
  tags.forEach((tag, idx) => {
    const chip = document.createElement("div");
    chip.className = "ds-selected-tag-chip";

    const chipColor = DS_SERIES_PALETTE[idx % DS_SERIES_PALETTE.length];
    chip.style.setProperty("--ds-chip-color", chipColor);

    const labelFull = `${valueOrDash(tag?.context)} • ${valueOrDash(tag?.point_name || tag?.description || tag?.pathname)}`;
    const labelShort = `${valueOrDash(dsAbbrevContext(tag?.context))} • ${valueOrDash(tag?.point_name || tag?.description || tag?.pathname)}`;
    chip.innerHTML = `
      <span class="ds-selected-tag-chip__dot"></span>
      <span class="ds-selected-tag-chip__text" title="${labelFull}">${labelShort}</span>
      <button type="button" class="ds-selected-tag-chip__remove" data-action="removeTag" data-plant="${plantId}" data-path="${dsSafeTrim(tag?.pathname).replaceAll('"','&quot;')}" aria-label="Remover medida">×</button>
    `;
    list.appendChild(chip);
  });
}

function updateStageUIForPlant(plantId) {
  const ps = DATASTUDIO_PLANTS[String(plantId)];
  if (!ps) return;

  const emptyState = document.getElementById(`dsEmptyState_${plantId}`);
  const workspace = document.getElementById(`dsWorkspace_${plantId}`);
  const catalogSection = document.getElementById(`dsCatalogSection_${plantId}`);

  const hasSelection = ps.selectedTags.length > 0;
  const showWorkspace = hasSelection && ps.catalogConfirmed;

  if (emptyState) emptyState.classList.toggle("hidden", showWorkspace);
  if (workspace) workspace.classList.toggle("hidden", !showWorkspace);
  if (catalogSection) catalogSection.classList.toggle("ds-v2-catalog--collapsed", ps.catalogOpen === false);

  renderSelectedTagsListForPlant(plantId);
  updateSelectedTagsCounterForPlant(plantId);
}

// --- Per-plant tags table ---

function renderDataStudioTagsTableForPlant(plantId, tags) {
  const tbody = document.getElementById(`dsTagsTbody_${plantId}`);
  if (!tbody) return;
  const ps = DATASTUDIO_PLANTS[String(plantId)];
  if (!ps) return;

  const rows = Array.isArray(tags) ? tags : [];
  tbody.innerHTML = "";
  updateFoundCountForPlant(plantId, rows.length);

  if (!rows.length) {
    const tr = document.createElement("tr");
    tr.innerHTML = '<td colspan="8" style="text-align:center;opacity:.8;">Nenhuma medida encontrada</td>';
    tbody.appendChild(tr);
    updateSelectedTagsCounterForPlant(plantId);
    return;
  }

  rows.forEach(tag => {
    const pathname = dsSafeTrim(tag?.pathname || tag?.path_name || tag?.tag);
    if (!pathname) return;

    const tr = document.createElement("tr");
    tr.classList.add("ds-table-row-clickable");
    const checked = isTagSelectedForPlant(plantId, tag) ? "checked" : "";

    const isAlarm = (tag?.data_kind === "discrete") || (tag?.description || "").startsWith("\u26a0");
    const isPlant = (tag?.context || "").toUpperCase().startsWith("PLANT") || (tag?.pathname || "").startsWith("PLANT.");
    const isWeather = (tag?.device_type || "").toLowerCase().includes("weather") || (tag?.pathname || "").toLowerCase().includes("weather");
    const isMeter = (tag?.device_type || "").toLowerCase().includes("meter") || (tag?.context || "").toLowerCase().includes("medidor") || (tag?.context || "").toLowerCase().includes("meter");
    const isRelay = (tag?.device_type || "").toLowerCase().includes("relay") || (tag?.pathname || "").startsWith("RELAY_");

    const svgInverter = '<svg width="12" height="12" viewBox="0 0 12 12" fill="none"><rect x="1" y="3" width="10" height="7" rx="1.5" stroke="rgba(57,229,140,.8)" stroke-width="1.2"/><path d="M4 3V2a2 2 0 014 0v1" stroke="rgba(57,229,140,.8)" stroke-width="1.2"/><line x1="4" y1="6.5" x2="8" y2="6.5" stroke="rgba(57,229,140,.6)" stroke-width="1"/></svg>';
    const svgPlant = '<svg width="12" height="12" viewBox="0 0 12 12" fill="none"><path d="M6 10V5M3 7l3-3 3 3" stroke="rgba(55,138,221,.9)" stroke-width="1.2" stroke-linecap="round"/><rect x="1" y="1" width="4" height="3" rx="0.8" stroke="rgba(55,138,221,.7)" stroke-width="1"/><rect x="7" y="1" width="4" height="3" rx="0.8" stroke="rgba(55,138,221,.7)" stroke-width="1"/></svg>';
    const svgWeather = '<svg width="12" height="12" viewBox="0 0 12 12" fill="none"><circle cx="6" cy="5" r="2.5" stroke="rgba(239,159,39,.8)" stroke-width="1.2"/><line x1="6" y1="1" x2="6" y2="2" stroke="rgba(239,159,39,.7)" stroke-width="1" stroke-linecap="round"/><line x1="6" y1="8" x2="6" y2="9" stroke="rgba(239,159,39,.7)" stroke-width="1" stroke-linecap="round"/><line x1="2" y1="5" x2="3" y2="5" stroke="rgba(239,159,39,.7)" stroke-width="1" stroke-linecap="round"/><line x1="9" y1="5" x2="10" y2="5" stroke="rgba(239,159,39,.7)" stroke-width="1" stroke-linecap="round"/></svg>';
    const svgMeter = '<svg width="12" height="12" viewBox="0 0 12 12" fill="none"><rect x="1" y="2" width="10" height="8" rx="1.5" stroke="rgba(212,83,126,.8)" stroke-width="1.2"/><path d="M3.5 7.5 C3.5 5.5 8.5 5.5 8.5 7.5" stroke="rgba(212,83,126,.7)" stroke-width="1" fill="none"/><line x1="6" y1="7.5" x2="5" y2="5.5" stroke="rgba(212,83,126,.9)" stroke-width="1" stroke-linecap="round"/></svg>';
    const svgRelay = '<svg width="12" height="12" viewBox="0 0 12 12" fill="none"><rect x="1" y="2.5" width="10" height="7" rx="1.3" stroke="rgba(57,229,140,.8)" stroke-width="1.1"/><line x1="3" y1="8" x2="8.5" y2="8" stroke="rgba(57,229,140,.75)" stroke-width="1.1" stroke-linecap="round"/><line x1="3" y1="8" x2="5" y2="5" stroke="rgba(57,229,140,.85)" stroke-width="1.1" stroke-linecap="round"/><line x1="3" y1="5" x2="8.5" y2="5" stroke="rgba(57,229,140,.35)" stroke-width=".9" stroke-dasharray="2 1.3" stroke-linecap="round"/></svg>';
    const svgAlarm = '<svg width="12" height="12" viewBox="0 0 12 12" fill="none"><path d="M6 1.5L1.5 9.5h9L6 1.5z" stroke="rgba(239,159,39,.9)" stroke-width="1.2" stroke-linejoin="round"/><line x1="6" y1="5" x2="6" y2="7.5" stroke="rgba(239,159,39,.9)" stroke-width="1" stroke-linecap="round"/><circle cx="6" cy="9" r=".6" fill="rgba(239,159,39,.9)"/></svg>';

    let iconSvg = svgInverter, iconClass = "analog";
    if (isAlarm) { iconSvg = svgAlarm; iconClass = "discrete"; }
    else if (isPlant) { iconSvg = svgPlant; iconClass = "plant"; }
    else if (isWeather) { iconSvg = svgWeather; }
    else if (isMeter) { iconSvg = svgMeter; }
    else if (isRelay) { iconSvg = svgRelay; }

    const unitHtml = tag?.unit ? `<span class="ds-unit-badge">${tag.unit}</span>` : '<span style="opacity:.35">\u2014</span>';

    tr.innerHTML = `
      <td><input type="checkbox" data-ds-pathname="${pathname.replaceAll('"','&quot;')}" data-plant="${plantId}" ${checked}></td>
      <td style="min-width:90px;max-width:130px;white-space:normal;word-break:break-word;line-height:1.3;font-size:11px;">
        <div class="ds-tag-context-cell">
          <span class="ds-tag-icon ds-tag-icon--${iconClass}">${iconSvg}</span>
          <span title="${(tag?.context||'').replace(/"/g,'&quot;')}">${valueOrDash(dsAbbrevContext(tag?.context))}</span>
        </div>
      </td>
      <td style="white-space:normal;line-height:1.35;font-size:11px;">${valueOrDash(tag?.description)}</td>
      <td style="display:none;">${valueOrDash(tag?.source)}</td>
      <td style="display:none;">${valueOrDash(tag?.data_kind)}</td>
      <td>${unitHtml}</td>
      <td style="display:none;">${valueOrDash(tag?.power_plant_name || tag?.power_plant_id)}</td>
      <td class="ds-pathname-cell" title="${pathname.replaceAll('"','&quot;')}" style="display:none;">${valueOrDash(pathname)}</td>
    `;

    const checkbox = tr.querySelector("input[type='checkbox']");
    const syncRowState = () => tr.classList.toggle("is-selected", Boolean(checkbox?.checked));

    const applySelection = (checkedState) => {
      if (checkedState) {
        const ok = addSelectedTagForPlant(plantId, {
          id: tag?.id ?? null, tag_id: tag?.id ?? null,
          device_type: tag?.device_type ?? null, device_id: tag?.device_id ?? null,
          point_name: tag?.point_name ?? null,
          power_plant_id: tag?.power_plant_id ?? null,
          power_plant_name: tag?.power_plant_name ?? null,
          context: dsSafeTrim(tag?.context) || "PLANT",
          pathname, source: dsSafeTrim(tag?.source) || "historico",
          data_kind: dsSafeTrim(tag?.data_kind) || "analog",
          unit: tag?.unit ?? null, description: tag?.description ?? null
        });
        if (!ok && checkbox) checkbox.checked = false;
      } else {
        removeSelectedTagForPlant(plantId, tag);
      }
      syncRowState();
    };

    checkbox?.addEventListener("change", ev => applySelection(Boolean(ev.target.checked)));
    tr.addEventListener("click", ev => {
      if (ev.target?.closest("input, button, a")) return;
      if (!checkbox) return;
      checkbox.checked = !checkbox.checked;
      applySelection(Boolean(checkbox.checked));
    });

    syncRowState();
    tbody.appendChild(tr);
  });

  updateSelectedTagsCounterForPlant(plantId);
  updateStageUIForPlant(plantId);
}

// --- Per-plant catalog actions ---

function confirmCatalogSelectionForPlant(plantId) {
  const ps = DATASTUDIO_PLANTS[String(plantId)];
  if (!ps) return;
  if (!ps.selectedTags.length) {
    window.alert("Selecione ao menos uma medida antes de confirmar.");
    return;
  }
  ps.catalogConfirmed = true;
  ps.catalogOpen = false;
  updateStageUIForPlant(plantId);
}

function toggleCatalogForPlant(plantId) {
  const ps = DATASTUDIO_PLANTS[String(plantId)];
  if (!ps) return;
  if (!ps.catalogOpen) {
    ps.catalogOpen = true;
    updateStageUIForPlant(plantId);
    if (!ps.catalogTags.length) fetchDataStudioTagsForPlant(plantId);
    return;
  }
  ps.catalogOpen = false;
  updateStageUIForPlant(plantId);
}

// --- Per-plant API: fetch tags ---

async function fetchDataStudioTagsForPlant(plantId) {
  const ps = DATASTUDIO_PLANTS[String(plantId)];
  if (!ps) return;

  const requestSeq = (ps.tagsRequestSeq || 0) + 1;
  ps.tagsRequestSeq = requestSeq;

  if (ps.tagsAbortController) {
    try { ps.tagsAbortController.abort(); } catch(e){}
  }
  ps.tagsAbortController = typeof AbortController !== "undefined" ? new AbortController() : null;

  updateFoundCountForPlant(plantId, null, "Buscando medidas...");

  try {
    const fetchOptions = ps.tagsAbortController ? { signal: ps.tagsAbortController.signal } : {};
    const params = new URLSearchParams();
    params.set("plant_id", String(plantId));
    params.set("limit", "5000");

    const res = await apiFetch(`/datastudio/tags?${params.toString()}`, fetchOptions);
    if (!res.ok) throw new Error(`Falha ao buscar medidas (${res.status})`);
    const data = await res.json();
    const parsed = dsNormalizeApiBody(data);
    if (requestSeq !== ps.tagsRequestSeq) return;

    const normalizeTagsResponse = (p) => {
      if (Array.isArray(p)) return p;
      if (Array.isArray(p?.items)) return p.items;
      if (Array.isArray(p?.data)) return p.data;
      return [];
    };

    const tags = normalizeTagsResponse(parsed);
    const plantName = dsGetPlantNameById(plantId);
    tags.forEach(t => {
      if (!t.power_plant_name && plantName) t.power_plant_name = plantName;
      if (!t.power_plant_id) t.power_plant_id = Number(plantId);
    });

    const sortedTags = dsSortTags(tags);
    ps.catalogTags = sortedTags;
    populateContextSelectForPlant(plantId, sortedTags);

    const ctxSel = document.getElementById(`dsContextSelect_${plantId}`);
    ps.selectedContext = dsSafeTrim(ctxSel?.value || ps.selectedContext || "all");
    applyTagFiltersForPlant(plantId);

    console.log(`[DataStudio] Plant ${plantId}: ${sortedTags.length} tags loaded`);
  } catch (err) {
    if (err?.name === "AbortError") return;
    if (requestSeq !== ps.tagsRequestSeq) return;
    console.error(`[DataStudio] erro ao buscar tags plant ${plantId}:`, err);
    ps.catalogTags = [];
    ps.availableTags = [];
    updateContextInfoForPlant(plantId);
    renderDataStudioTagsTableForPlant(plantId, []);
  } finally {
    if (requestSeq === ps.tagsRequestSeq) {
      ps.tagsAbortController = null;
    }
  }
}

// --- Per-plant chart rendering ---

function renderChartForPlant(plantId, seriesPayload) {
  const ps = DATASTUDIO_PLANTS[String(plantId)];
  if (!ps) return;

  const canvas = document.getElementById(`dsChart_${plantId}`);
  if (!canvas || typeof Chart === "undefined") return;

  // Destroy old chart for this plant
  if (ps.chartInstance) {
    try { ps.chartInstance.destroy(); } catch(e){}
    ps.chartInstance = null;
  }
  delete DATASTUDIO_CHART[plantId];

  const chart = _renderSinglePlantChart(canvas, seriesPayload, 0);
  if (chart) {
    ps.chartInstance = chart;
    DATASTUDIO_CHART[plantId] = chart;
  }

  _fillDsStatsForPlant(seriesPayload, plantId);
}


// --- Shared utility functions (kept from original) ---

function selectedTagKey(tagOrPath) {
  if (typeof tagOrPath === "string") return `path:${dsSafeTrim(tagOrPath)}`;
  const id = tagOrPath?.id ?? tagOrPath?.tag_id;
  if (id !== undefined && id !== null && String(id) !== "") return `id:${id}`;
  return `path:${dsSafeTrim(tagOrPath?.pathname)}`;
}

function dsSortTags(tags) {
  const order = {
    plant: 0, weather: 1, meter: 2, relay: 3, inverter: 4, alarm: 5, outro: 9
  };
  return [...(Array.isArray(tags) ? tags : [])].sort((a, b) => {
    const ga = dsTagGroup(a);
    const gb = dsTagGroup(b);
    if ((order[ga] ?? 9) !== (order[gb] ?? 9)) return (order[ga] ?? 9) - (order[gb] ?? 9);
    const ca = String(a?.context || "");
    const cb = String(b?.context || "");
    const c = ca.localeCompare(cb, "pt-BR", { sensitivity: "base", numeric: true });
    if (c !== 0) return c;
    const da = String(a?.description || a?.point_name || a?.pathname || "");
    const db = String(b?.description || b?.point_name || b?.pathname || "");
    return da.localeCompare(db, "pt-BR", { sensitivity: "base", numeric: true });
  });
}

// Compat stubs for old functions that may be called from inline scripts
function getDataStudioUIElements() {
  return {
    startDateInput: document.getElementById("dsStartDateInput"),
    endDateInput: document.getElementById("dsEndDateInput"),
    plantSelect: document.getElementById("dsPlantSelect"),
    exportBtn: document.getElementById("dsExportBtn") || document.getElementById("dsExportBtnBottom"),
    modeSelect: document.getElementById("dsModeSelect"),
    aggregationSelect: document.getElementById("dsAggregationSelect"),
    consolidationSelect: document.getElementById("dsConsolidationSelect"),
    saveSelectionBtn: document.getElementById("dsSaveSelectionBtn"),
    loadSeriesBtn: document.getElementById("dsLoadSeriesBtn"),
    plantMultiselect: document.getElementById("dsPlantMultiselect"),
    plantMultiselectBtn: document.getElementById("dsPlantMultiselectBtn"),
    plantMultiselectLabel: document.getElementById("dsPlantMultiselectLabel"),
    plantMultiselectDropdown: document.getElementById("dsPlantMultiselectDropdown"),
    backToHeroBtn: document.getElementById("dsBackToHeroBtn"),
    zoomInBtn: document.getElementById("dsZoomInBtn"),
    zoomOutBtn: document.getElementById("dsZoomOutBtn"),
    zoomResetBtn: document.getElementById("dsZoomResetBtn"),
    fullscreenBtn: document.getElementById("dsFullscreenBtn"),
    fullscreenCloseBtn: document.getElementById("dsFullscreenCloseBtn")
  };
}

// Compat: old function called in inline script
function setDataStudioActiveCategory(category) {
  // no-op in per-plant mode
}
function updateDataStudioFoundCount() {}
function applyDataStudioTagFilters() {}
function renderSelectedTagsList() {}
function populateDataStudioContextSelect() {}
function updateDataStudioStageUI() {
  Object.keys(DATASTUDIO_PLANTS).forEach(pid => updateStageUIForPlant(pid));
}
function isTagSelected(tagOrPath) { return false; }
function addSelectedTag(tag) { return false; }
function removeSelectedTag(tagOrPath) {}
function updateSelectedTagsCounter() {
  Object.keys(DATASTUDIO_PLANTS).forEach(pid => updateSelectedTagsCounterForPlant(pid));
}
function renderDataStudioTagsTable(tags) {}
function updateDataStudioContextInfo() {}

function setDataStudioLoadingTags(isLoading) {
  DATASTUDIO_STATE.loadingTags = Boolean(isLoading);
}

function setDataStudioSavingSelection(isLoading) {
  DATASTUDIO_STATE.savingSelection = Boolean(isLoading);
  const btn = document.getElementById("dsSaveSelectionBtn");
  if (btn) {
    btn.disabled = DATASTUDIO_STATE.savingSelection;
    btn.textContent = DATASTUDIO_STATE.savingSelection ? "Salvando..." : "Salvar seleção";
  }
}

function setDataStudioLoadingSeries(isLoading) {
  DATASTUDIO_STATE.loadingSeries = Boolean(isLoading);
  const btn = document.getElementById("dsLoadSeriesBtn");
  if (btn) {
    btn.disabled = DATASTUDIO_STATE.loadingSeries;
    btn.textContent = DATASTUDIO_STATE.loadingSeries ? "Carregando..." : "Carregar seleção";
  }
}

function getDataStudioMainFilters() {
  const startDateInput = document.getElementById("dsStartDateInput");
  const endDateInput = document.getElementById("dsEndDateInput");

  const rawStart = dsSafeTrim(startDateInput?.value || DATASTUDIO_STATE.startDate);
  const rawEnd = dsSafeTrim(endDateInput?.value || DATASTUDIO_STATE.endDate);

  const start_ts_raw = dsIsoStartOfDay(rawStart);
  const end_ts_raw = dsIsoEndOfDay(rawEnd);
  const { startISO, endISO } = dsClampRange(start_ts_raw, end_ts_raw);

  const ids = Array.isArray(DATASTUDIO_STATE.selectedPlantIds) ? DATASTUDIO_STATE.selectedPlantIds : [];
  const firstId = ids.length ? ids[0] : null;

  return {
    power_plant_id: firstId ? Number(firstId) : null,
    power_plant_ids: ids.map(Number),
    start_ts: startISO,
    end_ts: endISO
  };
}

function buildPayloadForPlant(plantId, tags) {
  const filters = getDataStudioMainFilters();
  if (!filters.start_ts || !filters.end_ts) {
    throw new Error("Preencha um período válido (data inicial e final).");
  }

  const allowedAgg = new Set(["none", "avg", "integral", "median", "max", "sum"]);
  const allowedPeriod = new Set(["5min", "daily", "weekly", "monthly", "yearly", "hdaily", "hweekly", "hmonthly", "hyearly"]);

  const aggregationType = allowedAgg.has(DATASTUDIO_STATE.aggregationType) ? DATASTUDIO_STATE.aggregationType : "avg";
  const consolidationPeriod = allowedPeriod.has(DATASTUDIO_STATE.consolidationPeriod) ? DATASTUDIO_STATE.consolidationPeriod : "5min";

  const items = tags.map((t, idx) => ({
    tag_id: t?.id ?? t?.tag_id ?? null,
    pathname: dsSafeTrim(t.pathname),
    display_type: "line",
    series_order: idx + 1,
    source: dsSafeTrim(t.source) || "historico",
    data_kind: dsSafeTrim(t.data_kind) || "analog",
    unit: t.unit ?? null,
    label: dsSafeTrim(t.point_name || t.description || t.pathname) || null
  }));

  return {
    selection_name: "Seleção Data Studio",
    power_plant_id: Number(plantId),
    start_ts: filters.start_ts,
    end_ts: filters.end_ts,
    timezone: "America/Fortaleza",
    historico_aggregation_default: DATASTUDIO_STATE.aggregationMode === "historico" ? aggregationType : "avg",
    consolidado_period_default: DATASTUDIO_STATE.aggregationMode === "consolidado" ? consolidationPeriod : "5min",
    items
  };
}

function updateDataStudioExportButton() {
  const exportBtn = document.getElementById("dsExportBtn") || document.getElementById("dsExportBtnBottom");
  if (!exportBtn) return;
  const hasSelections = Object.keys(DATASTUDIO_STATE.selectionIdsByPlant).length > 0 || DATASTUDIO_STATE.selectionId;
  exportBtn.disabled = !hasSelections;
}

async function exportDataStudioSelection() {
  const selectionEntries = Object.entries(DATASTUDIO_STATE.selectionIdsByPlant);
  if (!selectionEntries.length && DATASTUDIO_STATE.selectionId) {
    selectionEntries.push(["single", DATASTUDIO_STATE.selectionId]);
  }
  if (!selectionEntries.length) {
    window.alert("Salve uma seleção antes de exportar.");
    return;
  }

  const exportBtn = document.getElementById("dsExportBtn") || document.getElementById("dsExportBtnBottom");
  const oldHtml = exportBtn ? exportBtn.innerHTML : "";

  try {
    if (exportBtn) {
      exportBtn.disabled = true;
      exportBtn.innerHTML = '<i class="fa-solid fa-spinner fa-spin"></i>';
    }

    const user = JSON.parse(localStorage.getItem("user") || "{}");
    const headers = {};
    if (user.customer_id) headers["X-Customer-Id"] = user.customer_id;
    if (user.is_superuser === true) headers["X-Is-Superuser"] = "true";

    // Fetch CSV from each plant and merge into a single file
    const csvParts = [];
    let globalHeader = null;

    for (const [plantId, selId] of selectionEntries) {
      const url = `${API_BASE}/datastudio/export?selection_id=${encodeURIComponent(selId)}`;
      const res = await fetch(url, { headers });

      if (!res.ok) {
        const txt = await res.text().catch(() => "");
        throw new Error(`Falha ao exportar (${res.status}) ${txt}`);
      }

      const csvText = await res.text();
      const lines = csvText.split(/\r?\n/).filter(l => l.trim() !== "");

      if (!lines.length) continue;

      if (globalHeader === null) {
        // First plant: keep header + add "Usina" column
        globalHeader = "Usina," + lines[0];
        csvParts.push(globalHeader);
        const plantName = dsGetPlantNameById(plantId) || `Usina ${plantId}`;
        const safeName = plantName.replace(/"/g, '""');
        for (let i = 1; i < lines.length; i++) {
          csvParts.push(`"${safeName}",${lines[i]}`);
        }
      } else {
        // Subsequent plants: skip header, add data rows with plant name
        const plantName = dsGetPlantNameById(plantId) || `Usina ${plantId}`;
        const safeName = plantName.replace(/"/g, '""');
        for (let i = 1; i < lines.length; i++) {
          csvParts.push(`"${safeName}",${lines[i]}`);
        }
      }
    }

    if (!csvParts.length) {
      window.alert("Nenhum dado para exportar.");
      return;
    }

    const mergedCsv = csvParts.join("\n");
    const blob = new Blob([mergedCsv], { type: "text/csv;charset=utf-8" });
    const blobUrl = window.URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = blobUrl;
    a.download = `datastudio_export_${new Date().toISOString().slice(0,10)}.csv`;
    document.body.appendChild(a);
    a.click();
    a.remove();
    window.URL.revokeObjectURL(blobUrl);
  } catch (err) {
    console.error("[DataStudio] erro ao exportar CSV:", err);
    window.alert(`Não foi possível exportar o CSV: ${err.message || err}`);
  } finally {
    if (exportBtn) {
      exportBtn.innerHTML = oldHtml || '<i class="fa-solid fa-file-csv"></i>';
      updateDataStudioExportButton();
    }
  }
}

function zoomDataStudioChart(factor = 1.2) {
  const charts = DATASTUDIO_CHART && typeof DATASTUDIO_CHART === "object" ? Object.values(DATASTUDIO_CHART) : [];
  charts.forEach(chart => {
    try { if (typeof chart.zoom === "function") chart.zoom({ x: factor, y: factor }); }
    catch (err) { console.warn("[DataStudio] erro ao aplicar zoom:", err); }
  });
}

function resetDataStudioChartZoom() {
  const charts = DATASTUDIO_CHART && typeof DATASTUDIO_CHART === "object" ? Object.values(DATASTUDIO_CHART) : [];
  charts.forEach(chart => {
    try { if (typeof chart.resetZoom === "function") chart.resetZoom(); }
    catch (err) { console.warn("[DataStudio] erro ao resetar zoom:", err); }
  });
}

function resizeDataStudioChartSoon() {
  const charts = DATASTUDIO_CHART && typeof DATASTUDIO_CHART === "object" ? Object.values(DATASTUDIO_CHART) : [];
  if (!charts.length) return;
  requestAnimationFrame(() => {
    charts.forEach(chart => {
      try { if (typeof chart.resize === "function") chart.resize(); } catch (err) {}
    });
  });
}

function isMobileViewport() {
  return window.innerWidth <= 768;
}

function clearDataStudioChartActiveState() {
  const charts = DATASTUDIO_CHART && typeof DATASTUDIO_CHART === "object" ? Object.values(DATASTUDIO_CHART) : [];
  charts.forEach(chart => {
    try {
      chart.setActiveElements([]);
      if (chart.tooltip && typeof chart.tooltip.setActiveElements === "function") {
        chart.tooltip.setActiveElements([], { x: 0, y: 0 });
      }
      chart.update("none");
    } catch (err) {}
  });
}

function wireDataStudioChartFullscreenKeysOnce() {
  if (window.__dsChartFullscreenKeysWired) return;
  window.__dsChartFullscreenKeysWired = true;
  document.addEventListener("keydown", event => {
    if (event.key === "Escape") {
      _exitAllFullscreen();
    }
  });
}

function wireDataStudioChartOutsideTapOnce() {
  if (window.__dsOutsideTapWired) return;
  window.__dsOutsideTapWired = true;
  const clearIfOutside = (event) => {
    const container = document.getElementById("dsPlantBlocks");
    if (container && !container.contains(event.target)) {
      clearDataStudioChartActiveState();
    }
  };
  document.addEventListener("touchstart", clearIfOutside, { passive: true });
  document.addEventListener("click", clearIfOutside);
}

function _renderSinglePlantChart(canvas, seriesPayload, plantIdx) {
  const labels = [];
  const datasets = [];

  const seriesList = _extractSeriesList(seriesPayload);

  const scales = {
    x: { ticks: { color: "#9fb0bf" }, grid: { color: "rgba(255,255,255,.08)" } },
    y: { type: "linear", position: "left", ticks: { color: "#9fb0bf" }, grid: { color: "rgba(255,255,255,.08)" } }
  };
  const axisByUnit = new Map([["_default_", "y"]]);

  if (seriesList.length) {
    const palette = DS_SERIES_PALETTE;
    const allTsSet = new Set();
    seriesList.forEach(serie => {
      const points = Array.isArray(serie?.points) ? serie.points : (Array.isArray(serie?.data) ? serie.data : []);
      points.forEach(pt => {
        const ts = pt?.ts || pt?.timestamp || pt?.x;
        if (ts) allTsSet.add(String(ts));
      });
    });
    const allTsSorted = Array.from(allTsSet).sort();
    allTsSorted.forEach(ts => labels.push(new Date(ts).toLocaleString("pt-BR")));

    seriesList.forEach((serie, idx) => {
      const points = Array.isArray(serie?.points) ? serie.points : (Array.isArray(serie?.data) ? serie.data : []);
      const valueByTs = new Map();
      points.forEach(pt => {
        const ts = pt?.ts || pt?.timestamp || pt?.x;
        if (ts != null) {
          let v = pt?.value ?? pt?.y ?? null;
          try { v = v != null ? Number(v) : null; } catch { v = null; }
          valueByTs.set(String(ts), v);
        }
      });

      const unitKey = dsSafeTrim(serie?.unit || "") || "_default_";
      if (!axisByUnit.has(unitKey)) {
        const axisIdx = axisByUnit.size;
        const axisId = axisIdx === 1 ? "y1" : `y${axisIdx}`;
        axisByUnit.set(unitKey, axisId);
        scales[axisId] = {
          type: "linear",
          position: axisIdx % 2 === 0 ? "left" : "right",
          grid: { drawOnChartArea: false, color: "rgba(255,255,255,.08)" },
          ticks: { color: "#9fb0bf" }
        };
      }

      datasets.push({
        label: serie?.label || serie?.pathname || `Série ${idx + 1}`,
        data: allTsSorted.map(ts => valueByTs.has(ts) ? valueByTs.get(ts) : null),
        borderColor: palette[idx % palette.length],
        backgroundColor: palette[idx % palette.length],
        borderWidth: 2, tension: 0.25,
        pointRadius: 0, pointHoverRadius: 6, pointHitRadius: 16,
        fill: false, yAxisID: axisByUnit.get(unitKey) || "y",
        spanGaps: true
      });
    });
  } else {
    labels.push("Sem dados");
    datasets.push({
      label: "Data Studio", data: [0],
      borderColor: "#4da3ff", backgroundColor: "#4da3ff",
      borderWidth: 2, tension: 0.25,
      pointRadius: 0, pointHoverRadius: 6, pointHitRadius: 16,
      fill: false, yAxisID: "y"
    });
  }

  const mobile = isMobileViewport();
  return new Chart(canvas.getContext("2d"), {
    type: "line",
    data: { labels, datasets },
    options: {
      responsive: true, maintainAspectRatio: false,
      interaction: { mode: mobile ? "nearest" : "index", intersect: false, axis: "x" },
      hover: { mode: mobile ? "nearest" : "index", intersect: false },
      elements: {
        point: { radius: 0, hoverRadius: mobile ? 8 : 6, hitRadius: mobile ? 24 : 16 },
        line: { borderWidth: 2 }
      },
      plugins: {
        legend: { labels: { color: "#dbe7ef", boxWidth: 12, usePointStyle: true, pointStyle: "line" } },
        tooltip: {
          enabled: true, mode: mobile ? "nearest" : "index", intersect: false,
          displayColors: true, backgroundColor: "rgba(6, 18, 14, 0.96)",
          borderColor: "rgba(127,208,85,.22)", borderWidth: 1,
          titleColor: "#dbe7ef", bodyColor: "#dbe7ef", padding: 10, caretSize: 6
        },
        zoom: {
          limits: { x: { minRange: 5 }, y: { minRange: 1 } },
          pan: { enabled: true, mode: "xy", threshold: 2 },
          zoom: { wheel: { enabled: true, speed: 0.08 }, pinch: { enabled: true }, drag: { enabled: false }, mode: "xy" }
        }
      },
      scales
    }
  });
}

function _extractSeriesList(payload) {
  if (Array.isArray(payload?.series)) return payload.series;
  if (Array.isArray(payload?.items)) return payload.items;
  if (Array.isArray(payload?.data)) return payload.data;
  if (Array.isArray(payload)) return payload;
  return [];
}

function _fillDsStatsForPlant(payload, plantId) {
  const tbody = document.getElementById(`dsStatsTbody_${plantId}`);
  if (!tbody) return;
  const series = _extractSeriesList(payload);
  if (!series.length) {
    tbody.innerHTML = '<tr><td colspan="6" style="opacity:.5;text-align:center">Sem dados</td></tr>';
    return;
  }
  const colors = DS_SERIES_PALETTE;
  tbody.innerHTML = series.map((s, i) => {
    const pts = (s.points || s.data || []).map(p => Number(p.value ?? p.y ?? null)).filter(v => v != null && isFinite(v));
    const min = pts.length ? Math.min(...pts).toFixed(2) : '\u2014';
    const max = pts.length ? Math.max(...pts).toFixed(2) : '\u2014';
    const avg = pts.length ? (pts.reduce((a,b) => a+b,0)/pts.length).toFixed(2) : '\u2014';
    const last = pts.length ? pts[pts.length-1].toFixed(2) : '\u2014';
    const color = colors[i % colors.length];
    return '<tr><td><span style="display:inline-block;width:10px;height:10px;border-radius:50%;background:'+color+';margin-right:6px;"></span>'+(s.label || s.pathname || 'Série '+(i+1))+'</td><td>'+(s.unit || '\u2014')+'</td><td>'+min+'</td><td>'+max+'</td><td>'+avg+'</td><td>'+last+'</td></tr>';
  }).join('');

  const il = document.getElementById(`dsInsightsList_${plantId}`);
  if (il && series.length) {
    const first = series[0];
    const pts = (first.points || first.data || []).map(p => Number(p.value ?? p.y ?? null)).filter(v => v!=null&&isFinite(v));
    const peak = pts.length ? Math.max(...pts).toFixed(1) : '\u2014';
    il.innerHTML =
      '<div class="ds-v2-insight-item"><i class="fa-solid fa-circle-check" style="color:#39e58c"></i><span>Pico de <strong>'+(first.label||'Série 1')+'</strong>: <strong>'+peak+' '+(first.unit||'')+'</strong></span></div>' +
      '<div class="ds-v2-insight-item"><i class="fa-solid fa-circle-check" style="color:#39e58c"></i><span>Total de séries: <strong>'+series.length+'</strong></span></div>';
  }
}

// Compatibility wrapper
function renderDataStudioChart(seriesPayload) {
  if (seriesPayload === null || seriesPayload === undefined) {
    Object.keys(DATASTUDIO_PLANTS).forEach(pid => {
      const ps = DATASTUDIO_PLANTS[pid];
      if (ps.chartInstance) { try { ps.chartInstance.destroy(); } catch(e){} ps.chartInstance = null; }
      delete DATASTUDIO_CHART[pid];
    });
    return;
  }
  const plantId = DATASTUDIO_STATE.selectedPlantIds.length ? String(DATASTUDIO_STATE.selectedPlantIds[0]) : "single";
  renderChartForPlant(plantId, seriesPayload);
}

// --- Multi-plant save & fetch ---

async function saveDataStudioSelection() {
  setDataStudioSavingSelection(true);
  try {
    const plantIds = Object.keys(DATASTUDIO_PLANTS);
    if (!plantIds.length) throw new Error("Selecione ao menos uma usina para continuar.");

    const selectionIdsByPlant = {};

    for (const pid of plantIds) {
      const ps = DATASTUDIO_PLANTS[pid];
      if (!ps.selectedTags.length) continue;

      const payload = buildPayloadForPlant(pid, ps.selectedTags);
      const res = await apiFetch("/datastudio/selection", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload)
      });

      const data = await res.json().catch(() => ({}));
      const parsed = dsNormalizeApiBody(data);

      if (!res.ok) {
        const msg = parsed?.message || `Falha ao salvar seleção (${res.status})`;
        throw new Error(msg);
      }

      const selectionId = parsed?.selection_id ?? parsed?.id ?? parsed?.selectionId ?? null;
      if (selectionId) {
        selectionIdsByPlant[pid] = selectionId;
        ps.selectionId = selectionId;
      }
    }

    DATASTUDIO_STATE.selectionIdsByPlant = selectionIdsByPlant;
    const firstSelId = Object.values(selectionIdsByPlant)[0] || null;
    DATASTUDIO_STATE.selectionId = firstSelId;

    const loadSeriesBtn = document.getElementById("dsLoadSeriesBtn");
    if (loadSeriesBtn) loadSeriesBtn.disabled = false;
    updateDataStudioExportButton();

    const plantCount = Object.keys(selectionIdsByPlant).length;
    if (!plantCount) {
      window.alert("Nenhuma usina tem medidas selecionadas.");
      return;
    }
    window.alert(plantCount > 1
      ? `Seleções salvas para ${plantCount} usinas!`
      : (firstSelId ? `Seleção salva! ID ${firstSelId}` : "Seleção salva com sucesso."));

    await fetchDataStudioSeriesBySelection();
  } catch (err) {
    console.error("[DataStudio] erro ao salvar seleção:", err);
    window.alert(`Não foi possível salvar a seleção: ${err.message || err}`);
  } finally {
    setDataStudioSavingSelection(false);
  }
}

async function fetchDataStudioSeriesBySelection() {
  const selEntries = Object.entries(DATASTUDIO_STATE.selectionIdsByPlant);
  if (!selEntries.length && DATASTUDIO_STATE.selectionId) {
    selEntries.push([String(DATASTUDIO_STATE.selectedPlantIds[0] || "single"), DATASTUDIO_STATE.selectionId]);
  }
  if (!selEntries.length) {
    window.alert("Salve uma seleção antes de carregar séries.");
    return;
  }

  setDataStudioLoadingSeries(true);
  try {
    DATASTUDIO_STATE.seriesByPlant = {};

    for (const [plantId, selId] of selEntries) {
      const params = new URLSearchParams({ selection_id: String(selId) });
      const res = await apiFetch(`/datastudio/series?${params.toString()}`);
      if (!res.ok) throw new Error(`Falha ao carregar séries (${res.status})`);

      const data = await res.json();
      const parsed = dsNormalizeApiBody(data);
      DATASTUDIO_STATE.seriesByPlant[plantId] = parsed;
    }

    // Render chart for each plant
    for (const [plantId, seriesPayload] of Object.entries(DATASTUDIO_STATE.seriesByPlant)) {
      const ps = DATASTUDIO_PLANTS[plantId];
      if (ps) {
        ps.catalogConfirmed = true;
        // keep catalog open — don't force close
        updateStageUIForPlant(plantId);
      }
      renderChartForPlant(plantId, seriesPayload);
    }
  } catch (err) {
    console.error("[DataStudio] erro ao carregar séries:", err);
    window.alert(`Não foi possível carregar séries: ${err.message || err}`);
  } finally {
    setDataStudioLoadingSeries(false);
  }
}

// --- Apply all plants (called from topbar Aplicar button) ---

window.applyAllPlants = function() {
  const plantIds = Object.keys(DATASTUDIO_PLANTS);
  if (!plantIds.length) {
    window.alert("Selecione ao menos uma usina.");
    return;
  }

  let anyTags = false;
  plantIds.forEach(pid => {
    const ps = DATASTUDIO_PLANTS[pid];
    if (ps.selectedTags.length) {
      ps.catalogConfirmed = true;
      // keep catalog open — user wants to keep seeing it
      updateStageUIForPlant(pid);
      anyTags = true;
    }
  });

  if (!anyTags) {
    window.alert("Selecione medidas em pelo menos uma usina antes de aplicar.");
    return;
  }

  saveDataStudioSelection();
};

function markDataStudioSeriesDirty() {
  DATASTUDIO_STATE.selectionId = null;
  DATASTUDIO_STATE.selectionIdsByPlant = {};
  DATASTUDIO_STATE.seriesByPlant = {};
  DATASTUDIO_STATE.chartData = null;

  const loadSeriesBtn = document.getElementById("dsLoadSeriesBtn");
  const saveSelectionBtn = document.getElementById("dsSaveSelectionBtn");
  if (loadSeriesBtn) { loadSeriesBtn.disabled = true; loadSeriesBtn.textContent = "Carregar séries"; }
  if (saveSelectionBtn) saveSelectionBtn.disabled = false;

  renderDataStudioChart(null);
  updateDataStudioExportButton();
}

// --- Plant select ---

function populateDataStudioPlantSelect(plants) {
  const plantSelect = document.getElementById("dsPlantSelect");
  const plantMultiselectDropdown = document.getElementById("dsPlantMultiselectDropdown");
  const list = Array.isArray(plants) ? plants : [];

  if (plantSelect) {
    const currentValue = dsSafeTrim(plantSelect.value) || dsSafeTrim(DATASTUDIO_STATE.selectedPlantId) || "";
    const nextOptions = [
      { value: "", text: "Selecione uma usina" },
      ...list.map(p => {
        const id = p.power_plant_id ?? p.plant_id ?? p.id;
        const name = p.power_plant_name ?? p.name ?? `Usina ${id}`;
        return id == null ? null : { value: String(id), text: String(name) };
      }).filter(Boolean)
    ];

    const currentSerialized = [...plantSelect.options].map(o => `${o.value}|${o.textContent}`).join("||");
    const nextSerialized = nextOptions.map(o => `${o.value}|${o.text}`).join("||");

    if (currentSerialized !== nextSerialized) {
      plantSelect.innerHTML = "";
      nextOptions.forEach(({ value, text }) => {
        const option = document.createElement("option");
        option.value = value;
        option.textContent = text;
        plantSelect.appendChild(option);
      });
    }

    if (currentValue && nextOptions.some(o => o.value === currentValue)) plantSelect.value = currentValue;
    else if (!plantSelect.value) plantSelect.value = "";
    DATASTUDIO_STATE.selectedPlantId = dsSafeTrim(plantSelect.value) || null;
  }

  if (plantMultiselectDropdown) {
    const prevSelected = new Set(DATASTUDIO_STATE.selectedPlantIds.map(String));
    plantMultiselectDropdown.innerHTML = "";

    list.forEach(p => {
      const id = p.power_plant_id ?? p.plant_id ?? p.id;
      const name = p.power_plant_name ?? p.name ?? `Usina ${id}`;
      if (id == null) return;

      const label = document.createElement("label");
      label.className = "ds-plant-option";
      const checked = prevSelected.has(String(id)) ? "checked" : "";
      label.innerHTML = `<input type="checkbox" value="${id}" data-name="${(name||'').replace(/"/g,'&quot;')}" ${checked}> ${name}`;
      plantMultiselectDropdown.appendChild(label);

      label.querySelector("input").addEventListener("change", () => {
        _syncMultiselectState();
      });
    });

    _syncMultiselectLabel();
  }
}

function _syncMultiselectState() {
  const dropdown = document.getElementById("dsPlantMultiselectDropdown");
  if (!dropdown) return;

  const checked = dropdown.querySelectorAll("input[type='checkbox']:checked");
  const newIds = Array.from(checked).map(cb => Number(cb.value));
  const oldIds = DATASTUDIO_STATE.selectedPlantIds;

  const changed = newIds.length !== oldIds.length || newIds.some((id, i) => id !== oldIds[i]);
  if (!changed) return;

  DATASTUDIO_STATE.selectedPlantIds = newIds;
  DATASTUDIO_STATE.selectedPlantId = newIds.length ? String(newIds[0]) : null;

  const plantSelect = document.getElementById("dsPlantSelect");
  if (plantSelect && newIds.length) plantSelect.value = String(newIds[0]);

  // Clear shared state
  DATASTUDIO_STATE.selectionId = null;
  DATASTUDIO_STATE.selectionIdsByPlant = {};
  DATASTUDIO_STATE.seriesByPlant = {};
  DATASTUDIO_STATE.chartData = null;

  updateDataStudioExportButton();
  _syncMultiselectLabel();

  // Drive per-plant blocks
  onSelectedPlantsChanged();
}

function _syncMultiselectLabel() {
  const dropdown = document.getElementById("dsPlantMultiselectDropdown");
  const label = document.getElementById("dsPlantMultiselectLabel");
  if (!dropdown || !label) return;

  const checked = dropdown.querySelectorAll("input[type='checkbox']:checked");
  if (!checked.length) { label.textContent = "Selecione usinas"; return; }
  const names = Array.from(checked).map(cb => cb.dataset.name || `Usina ${cb.value}`);
  label.textContent = names.join(", ");
  label.title = names.join(", ");
}

function syncDataStudioAggregationUI() {
  const modeSelect = document.getElementById("dsModeSelect");
  const aggregationSelect = document.getElementById("dsAggregationSelect");
  const consolidationSelect = document.getElementById("dsConsolidationSelect");
  if (modeSelect) modeSelect.value = DATASTUDIO_STATE.aggregationMode;
  if (aggregationSelect) {
    aggregationSelect.value = DATASTUDIO_STATE.aggregationType;
    aggregationSelect.disabled = DATASTUDIO_STATE.aggregationMode !== "historico";
  }
  if (consolidationSelect) {
    consolidationSelect.value = DATASTUDIO_STATE.consolidationPeriod;
    consolidationSelect.disabled = DATASTUDIO_STATE.aggregationMode !== "consolidado";
  }
}

function formatDateInputValue(dateObj) {
  const y = dateObj.getFullYear();
  const m = String(dateObj.getMonth() + 1).padStart(2, "0");
  const d = String(dateObj.getDate()).padStart(2, "0");
  return `${y}-${m}-${d}`;
}

function autoAdjustDataStudioDateRangeByMode() {
  const startDateInput = document.getElementById("dsStartDateInput");
  const endDateInput = document.getElementById("dsEndDateInput");
  if (!startDateInput || !endDateInput) return;

  const now = new Date();
  const start = new Date(now);
  const mode = dsSafeTrim(DATASTUDIO_STATE.aggregationMode || "historico");
  const period = dsSafeTrim(DATASTUDIO_STATE.consolidationPeriod || "5min");

  if (mode === "historico") {
    start.setDate(now.getDate() - 7);
  } else {
    switch (period) {
      case "5min": start.setDate(now.getDate() - 1); break;
      case "daily": case "hdaily": start.setDate(now.getDate() - 30); break;
      case "weekly": case "hweekly": start.setDate(now.getDate() - 90); break;
      case "monthly": case "hmonthly": start.setMonth(now.getMonth() - 12); break;
      case "yearly": case "hyearly": start.setFullYear(now.getFullYear() - 5); break;
      default: start.setDate(now.getDate() - 30); break;
    }
  }

  const startStr = formatDateInputValue(start);
  const endStr = formatDateInputValue(now);
  startDateInput.value = startStr;
  endDateInput.value = endStr;
  DATASTUDIO_STATE.startDate = startStr;
  DATASTUDIO_STATE.endDate = endStr;
}

// --- Fullscreen per-plant ---
// Multiple charts can be fullscreen at the same time.
// "Fullscreen" = expand in-place: hide catalog + series panel, tall canvas.
// Multiple blocks can be expanded at the same time — they stay in normal flow.

function _setPlantChartFullscreen(plantId, expanded) {
  const block = document.getElementById(`dsPlantBlock_${plantId}`);
  if (!block) return;
  block.classList.toggle("ds-plant-block--expanded", Boolean(expanded));
  resizeDataStudioChartSoon();
  window.setTimeout(resizeDataStudioChartSoon, 200);
}

function _togglePlantChartFullscreen(plantId) {
  const block = document.getElementById(`dsPlantBlock_${plantId}`);
  _setPlantChartFullscreen(plantId, !block?.classList.contains("ds-plant-block--expanded"));
}

function _exitAllFullscreen() {
  document.querySelectorAll(".ds-plant-block--expanded").forEach(el => {
    el.classList.remove("ds-plant-block--expanded");
  });
  resizeDataStudioChartSoon();
}

// --- Pan cursor wiring (delegated) ---

function _wireChartPanCursor() {
  const container = document.getElementById("dsPlantBlocks");
  if (!container || container.dataset.dsPanWired === "true") return;
  container.dataset.dsPanWired = "true";

  const stopPanning = () => {
    document.querySelectorAll(".ds-chart-wrap.is-panning").forEach(el => el.classList.remove("is-panning"));
  };

  container.addEventListener("pointerdown", ev => {
    const canvas = ev.target.closest("canvas");
    if (!canvas) return;
    const wrap = canvas.closest(".ds-chart-wrap");
    if (!wrap || (ev.button != null && ev.button !== 0)) return;
    wrap.classList.add("is-panning");
    try { ev.target.setPointerCapture(ev.pointerId); } catch(e){}
  });

  ["pointerup", "pointercancel", "pointerleave", "lostpointercapture"].forEach(evName => {
    container.addEventListener(evName, ev => {
      if (ev.target.closest("canvas")) stopPanning();
    });
  });

  window.addEventListener("pointerup", stopPanning);
}

// --- Event delegation on #dsPlantBlocks ---

function _wireDataStudioPlantBlocksDelegation() {
  const container = document.getElementById("dsPlantBlocks");
  if (!container || container.dataset.dsDelegated === "true") return;
  container.dataset.dsDelegated = "true";

  // Click delegation
  container.addEventListener("click", ev => {
    const target = ev.target.closest("[data-action]");
    if (!target) return;
    const action = target.dataset.action;
    const plantId = target.dataset.plant;
    if (!plantId) return;

    switch (action) {
      case "toggleCatalog":
        toggleCatalogForPlant(plantId);
        break;
      case "confirmCatalog":
        confirmCatalogSelectionForPlant(plantId);
        break;
      case "removeTag": {
        const path = target.dataset.path;
        if (path) {
          removeSelectedTagForPlant(plantId, path);
          renderDataStudioTagsTableForPlant(plantId, DATASTUDIO_PLANTS[plantId]?.availableTags || []);
        }
        break;
      }
      case "fullscreen":
        _togglePlantChartFullscreen(plantId);
        break;
      case "exitFullscreen":
        _setPlantChartFullscreen(plantId, false);
        break;
      case "zoomIn": {
        const ch = DATASTUDIO_CHART[plantId];
        if (ch && typeof ch.zoom === "function") ch.zoom({ x: 1.2, y: 1.2 });
        break;
      }
      case "zoomOut": {
        const ch2 = DATASTUDIO_CHART[plantId];
        if (ch2 && typeof ch2.zoom === "function") ch2.zoom({ x: 0.8, y: 0.8 });
        break;
      }
      case "zoomReset": {
        const ch3 = DATASTUDIO_CHART[plantId];
        if (ch3 && typeof ch3.resetZoom === "function") ch3.resetZoom();
        break;
      }
    }
  });

  // Category pill clicks
  container.addEventListener("click", ev => {
    const pill = ev.target.closest(".ds-v2-pill[data-plant]");
    if (!pill) return;
    const plantId = pill.dataset.plant;
    const cat = pill.dataset.cat || "all";
    setActiveCategoryForPlant(plantId, cat);
    applyTagFiltersForPlant(plantId);
  });

  // Search input
  container.addEventListener("input", ev => {
    const input = ev.target.closest(".ds-v2-search[data-plant]");
    if (!input) return;
    const plantId = input.dataset.plant;
    const ps = DATASTUDIO_PLANTS[String(plantId)];
    if (!ps) return;
    ps.searchText = dsSafeTrim(input.value);
    if (ps.catalogTags.length) {
      applyTagFiltersForPlant(plantId);
    }
  });

  // Context select change
  container.addEventListener("change", ev => {
    const sel = ev.target.closest("select[data-plant]");
    if (!sel) return;
    const plantId = sel.dataset.plant;
    const ps = DATASTUDIO_PLANTS[String(plantId)];
    if (!ps) return;

    if (sel.id && sel.id.startsWith("dsContextSelect_")) {
      ps.selectedContext = dsSafeTrim(sel.value) || "all";
      if (ps.catalogTags.length) applyTagFiltersForPlant(plantId);
    }
  });
}

// --- wireDataStudioOnce (rewritten for per-plant architecture) ---

function wireDataStudioOnce() {
  if (DATASTUDIO_STATE.wired) return;

  const startDateInput = document.getElementById("dsStartDateInput");
  const endDateInput = document.getElementById("dsEndDateInput");
  if (!startDateInput && !endDateInput) return;

  DATASTUDIO_STATE.wired = true;

  const now = new Date();
  const week = new Date();
  week.setDate(now.getDate() - 7);
  const asDate = d => `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, "0")}-${String(d.getDate()).padStart(2, "0")}`;

  if (startDateInput && !startDateInput.value) startDateInput.value = asDate(week);
  if (endDateInput && !endDateInput.value) endDateInput.value = asDate(now);
  DATASTUDIO_STATE.startDate = dsSafeTrim(startDateInput?.value);
  DATASTUDIO_STATE.endDate = dsSafeTrim(endDateInput?.value);

  autoAdjustDataStudioDateRangeByMode();

  // Date change listeners
  startDateInput?.addEventListener("change", e => {
    DATASTUDIO_STATE.startDate = dsSafeTrim(e.target.value);
    markDataStudioSeriesDirty();
  });
  endDateInput?.addEventListener("change", e => {
    DATASTUDIO_STATE.endDate = dsSafeTrim(e.target.value);
    markDataStudioSeriesDirty();
  });

  // Multi-select dropdown toggle
  const plantMultiselectBtn = document.getElementById("dsPlantMultiselectBtn");
  plantMultiselectBtn?.addEventListener("click", e => {
    e.stopPropagation();
    const wrapper = document.getElementById("dsPlantMultiselect");
    if (wrapper) wrapper.classList.toggle("is-open");
  });

  document.addEventListener("click", e => {
    const wrapper = document.getElementById("dsPlantMultiselect");
    if (wrapper && !wrapper.contains(e.target)) wrapper.classList.remove("is-open");
  });

  // Aggregation controls
  const modeSelect = document.getElementById("dsModeSelect");
  const aggregationSelect = document.getElementById("dsAggregationSelect");
  const consolidationSelect = document.getElementById("dsConsolidationSelect");

  modeSelect?.addEventListener("change", e => {
    DATASTUDIO_STATE.aggregationMode = dsSafeTrim(e.target.value) || "historico";
    syncDataStudioAggregationUI();
    autoAdjustDataStudioDateRangeByMode();
    markDataStudioSeriesDirty();
  });

  aggregationSelect?.addEventListener("change", e => {
    DATASTUDIO_STATE.aggregationType = dsSafeTrim(e.target.value) || "avg";
    markDataStudioSeriesDirty();
  });

  consolidationSelect?.addEventListener("change", e => {
    DATASTUDIO_STATE.consolidationPeriod = dsSafeTrim(e.target.value) || "5min";
    autoAdjustDataStudioDateRangeByMode();
    markDataStudioSeriesDirty();
  });

  // Bottombar buttons
  const loadSeriesBtn = document.getElementById("dsLoadSeriesBtn");
  const saveSelectionBtn = document.getElementById("dsSaveSelectionBtn");
  const exportBtn = document.getElementById("dsExportBtn") || document.getElementById("dsExportBtnBottom");
  const clearAllBtn = document.getElementById("dsClearAllBtn");

  loadSeriesBtn?.addEventListener("click", () => saveDataStudioSelection());
  saveSelectionBtn?.addEventListener("click", saveDataStudioSelection);
  exportBtn?.addEventListener("click", exportDataStudioSelection);

  clearAllBtn?.addEventListener("click", () => {
    DATASTUDIO_STATE.selectionId = null;
    DATASTUDIO_STATE.selectionIdsByPlant = {};
    DATASTUDIO_STATE.seriesByPlant = {};
    DATASTUDIO_STATE.chartData = null;
    DATASTUDIO_STATE.selectedPlantIds = [];
    DATASTUDIO_STATE.selectedPlantId = null;

    // Uncheck all multi-select checkboxes
    const dropdown = document.getElementById("dsPlantMultiselectDropdown");
    if (dropdown) dropdown.querySelectorAll("input[type='checkbox']").forEach(cb => { cb.checked = false; });
    _syncMultiselectLabel();

    // Destroy all plant states and charts
    Object.keys(DATASTUDIO_PLANTS).forEach(pid => {
      const ps = DATASTUDIO_PLANTS[pid];
      if (ps.chartInstance) { try { ps.chartInstance.destroy(); } catch(e){} }
    });
    DATASTUDIO_PLANTS = {};
    DATASTUDIO_CHART = {};

    renderPlantBlocks();
    updateDataStudioExportButton();
  });

  // Wire event delegation on plant blocks container
  _wireDataStudioPlantBlocksDelegation();
  _wireChartPanCursor();

  wireDataStudioChartOutsideTapOnce();
  wireDataStudioChartFullscreenKeysOnce();
  syncDataStudioAggregationUI();
  updateDataStudioExportButton();
}


// =============================================================================
// NAVEGAÇÃO E INICIALIZAÇÃO
// =============================================================================

function animateViewEntrance(viewEl) {
  if (!viewEl) return;
  viewEl.classList.remove("view-enter");
  // força reflow para reiniciar animação sem mexer em API/state
  void viewEl.offsetWidth;
  viewEl.classList.add("view-enter");

  const done = () => viewEl.classList.remove("view-enter");
  viewEl.addEventListener("animationend", done, { once: true });
}

const views = {
  overview: document.getElementById("overviewView"),
  alarms: document.getElementById("alarmsView"),
  events: document.getElementById("eventsView"),
  diagram: document.getElementById("diagramView"),
  datastudio: document.getElementById("dataStudioView")
};

function syncTopSummaryLayout() {
  const rootStyle = document.documentElement.style;
  const topbar = document.querySelector(".topbar");
  const topSummary = document.getElementById("topSummary");
  const topbarHeight = topbar ? Math.ceil(topbar.getBoundingClientRect().height) : 54;

  rootStyle.setProperty("--topbar-height", `${topbarHeight}px`);
  if (!topSummary) return;

  const isOverviewVisible = !!views.overview && !views.overview.classList.contains("hidden");
  topSummary.classList.toggle("hidden", !isOverviewVisible);

  requestAnimationFrame(() => {
    const summaryHeight = isOverviewVisible ? Math.ceil(topSummary.getBoundingClientRect().height) : 0;
    rootStyle.setProperty("--top-summary-height", `${summaryHeight}px`);
    rootStyle.setProperty("--top-summary-stack-height", `${topbarHeight + summaryHeight}px`);
  });
}

function showView(viewName) {
  localStorage.setItem("currentView", viewName);
  Object.values(views).forEach(v => { if (v) v.classList.add("hidden"); });
  if (views[viewName]) {
    views[viewName].classList.remove("hidden");
    animateViewEntrance(views[viewName]);
  }

  document.querySelectorAll(".sidebar-btn").forEach(b => b.classList.remove("active"));
  const btnMap = {
    overview: "btnOverview",
    alarms: "btnAlarms",
    events: "btnEvents",
    datastudio: "btnDataStudio"
  };
  const activeBtn = document.getElementById(btnMap[viewName]);
  if (activeBtn) activeBtn.classList.add("active");

  syncTopSummaryLayout();

  if (viewName === "events") {
    EVENTS_STATE.page = 1;
    loadEvents(1);
    startEventsAutoRefresh();
  } else {
    stopEventsAutoRefresh();
  }

  if (viewName === "datastudio") {
    wireDataStudioOnce();
    populateDataStudioPlantSelect(lastValidPlants);
    syncDataStudioAggregationUI();
  }
}

document.getElementById("btnOverview")?.addEventListener("click", () => showView("overview"));

document.getElementById("btnAlarms")?.addEventListener("click", async () => {
  showView("alarms");
  document.querySelectorAll(".tab-btn").forEach(b => b.classList.remove("active"));
  const firstTab = document.querySelector(".tab-btn");
  if (firstTab) firstTab.classList.add("active");
  CURRENT_ALARMS_TAB_MODE = null;
  await renderAlarmsTable(false, { force: true });
});

document.getElementById("btnEvents")?.addEventListener("click", () => showView("events"));
document.getElementById("btnDataStudio")?.addEventListener("click", () => showView("datastudio"));
// Botão OS desabilitado temporariamente — não disponível para clientes ainda
// document.getElementById("btnOS")?.addEventListener("click", () => {
//   window.location.href = "os.html";
// });

document.querySelectorAll(".tab-btn").forEach(btn => {
  btn.addEventListener("click", async () => {
    document.querySelectorAll(".tab-btn").forEach(b => b.classList.remove("active"));
    btn.classList.add("active");
    const isRecognized = btn.textContent.toUpperCase().includes("RECONHECIDOS");
    await renderAlarmsTable(isRecognized, { force: true });

    const alarmsView = document.getElementById("alarmsView");
    animateViewEntrance(alarmsView);
  });
});

function isAlarmsRecognizedTabActive() {
  const activeTab = document.querySelector(".alarms-tabs .tab-btn.active");
  return Boolean(activeTab?.textContent?.toUpperCase().includes("RECONHECID"));
}

async function refreshVisibleViewData() {
  const alarmsView = document.getElementById("alarmsView");
  const eventsView = document.getElementById("eventsView");

  if (alarmsView && !alarmsView.classList.contains("hidden")) {
    await renderAlarmsTable(isAlarmsRecognizedTabActive());
  }

  if (eventsView && !eventsView.classList.contains("hidden")) {
    await loadEvents(EVENTS_STATE.page || 1, { silent: true });
  }
}


async function refreshDashboard() {
  let plants = [];
  let alarms = [];

  try {
    plants = await fetchPlants();
    if (Array.isArray(plants) && plants.length > 0) {
      lastValidPlants = plants;
    }
  } catch (err) {
    console.error("Erro ao buscar plantas:", err);
    plants = lastValidPlants;
  }

  const dsViewEl = document.getElementById("dataStudioView");
  const dsViewVisible = dsViewEl && !dsViewEl.classList.contains("hidden");

  const dsPlantSelect = document.getElementById("dsPlantSelect");
  const dsNeedPopulate =
    !dsPlantSelect ||
    dsPlantSelect.options.length <= 1;

  if (!dsViewVisible || dsNeedPopulate) {
    populateDataStudioPlantSelect(lastValidPlants);
  }
  populateEventsPlantSelect(lastValidPlants);

  try {
    alarms = await fetchActiveAlarms();
  } catch (err) {
    console.error("Erro ao buscar alarmes ativos:", err);
    alarms = [];
  }

  lastAlarmSeverityByPlant = buildPlantAlarmSeverityMap(alarms);

  // Ícone de alarme na sidebar pisca vermelho se houver qualquer alarme ativo
  const alarmBtn = document.getElementById("btnAlarms");
  if (alarmBtn) {
    if (lastAlarmSeverityByPlant.size > 0) {
      alarmBtn.classList.add("sidebar-btn--alarm-active");
    } else {
      alarmBtn.classList.remove("sidebar-btn--alarm-active");
    }
  }

  if (typeof _portfolioCurrentView !== "undefined" && _portfolioCurrentView === "card") {
    updatePortfolioCardAlarms();
  }

  try {
    const summary = await fetchPlantsSummary();
    refreshTopChipsGlobalFromSummary(summary);
  } catch (e) {
    console.warn("[SUMMARY] falhou, fallback via /plants:", e?.message || e);
    refreshTopChipsGlobalFromPlants(lastValidPlants);
  }
  // topo sempre global: soma de todas as usinas visíveis para o usuário
  updateSummaryUI(lastValidPlants);

  renderPortfolioTable(lastValidPlants);

  if (typeof _portfolioCurrentView !== "undefined" && _portfolioCurrentView === "card") {
    const _cardGrid = document.getElementById("portfolioCardView");
    if (!_cardGrid || _cardGrid.children.length === 0) {
      renderPortfolioCards(typeof portfolioFilterPlants === "function"
        ? portfolioFilterPlants(lastValidPlants) : lastValidPlants);
    } else {
      updatePortfolioCardData(typeof portfolioFilterPlants === "function"
        ? portfolioFilterPlants(lastValidPlants) : lastValidPlants);
      _refreshMiniChartsIfStale();
    }
  }

  await refreshVisibleViewData();
  syncTopSummaryLayout();
  try { await robotRefresh(); } catch(e) { console.warn("[ROBOT]", e); }
}

document.addEventListener("DOMContentLoaded", async () => {
  wireDataStudioOnce();
  wireRobotAssistant();

  // Preenche nome do usuário logado
  try {
    const _u = JSON.parse(localStorage.getItem("user") || "{}");
    const _name = _u.username || _u.name || _u.email || "Operador";
    const _initials = _name.split(/[\s._@]+/).slice(0,2).map(p => p[0]?.toUpperCase() || "").join("") || "?";
    const _nameEl = document.getElementById("userDisplayName");
    const _avatarEl = document.getElementById("userAvatar");
    if (_nameEl) _nameEl.textContent = _name;
    if (_avatarEl) _avatarEl.textContent = _initials;
  } catch(_e) {}

  const savedView = localStorage.getItem("currentView") || "overview";
  showView(savedView);
  syncTopSummaryLayout();

  wirePortfolioControls();
  await refreshDashboard();
  setInterval(refreshDashboard, DASHBOARD_REFRESH_INTERVAL_MS);

  document.addEventListener("visibilitychange", async () => {
    if (!document.hidden) {
      await refreshDashboard();
    }
  });

  window.addEventListener("focus", async () => {
    await refreshDashboard();
  });

  window.addEventListener("resize", () => {
    syncTopSummaryLayout();
  });

  document.querySelector(".logout-icon")?.addEventListener("click", logout);
  document.querySelector(".sidebar-logout")?.addEventListener("click", logout);
});

// =============================================================================
// PORTFOLIO VIEW TOGGLE + SEARCH + CARD VIEW
// =============================================================================

let _portfolioCurrentView = localStorage.getItem("portfolioView") || "card";
let _portfolioMiniCharts = new Map();
let _portfolioRenderGen = 0;
const _miniChartDataCache = new Map(); // plantId → { ts, body }
const _MINI_CHART_CACHE_TTL_MS = 5 * 60 * 1000;

function portfolioSetView(view) {
  _portfolioCurrentView = view;
  localStorage.setItem("portfolioView", view);
  const listView = document.getElementById("portfolioListView");
  const cardView = document.getElementById("portfolioCardView");
  const btnList = document.getElementById("btnViewList");
  const btnCard = document.getElementById("btnViewCard");
  if (listView) listView.classList.toggle("hidden", view !== "list");
  if (cardView) cardView.classList.toggle("hidden", view !== "card");
  if (btnList) btnList.classList.toggle("active", view === "list");
  if (btnCard) btnCard.classList.toggle("active", view === "card");
  if (view === "card" && lastValidPlants.length > 0) {
    const _grid = document.getElementById("portfolioCardView");
    if (!_grid || _grid.children.length === 0) {
      renderPortfolioCards(lastValidPlants);
    }
  }
}

function portfolioGetSearchFilter() {
  const input = document.getElementById("portfolioSearchInput");
  return (input?.value || "").trim().toLowerCase();
}

function portfolioFilterPlants(plants) {
  const q = portfolioGetSearchFilter();
  if (!q) return plants;
  return plants.filter(p => {
    const name = (p.power_plant_name || p.plant_name || p.name || "").toLowerCase();
    return name.includes(q);
  });
}

function wirePortfolioControls() {
  const btnList = document.getElementById("btnViewList");
  const btnCard = document.getElementById("btnViewCard");
  const searchInput = document.getElementById("portfolioSearchInput");

  btnList?.addEventListener("click", () => portfolioSetView("list"));
  btnCard?.addEventListener("click", () => portfolioSetView("card"));

  searchInput?.addEventListener("input", () => {
    const filtered = portfolioFilterPlants(lastValidPlants);
    if (_portfolioCurrentView === "list") {
      renderPortfolioTable(filtered);
    } else {
      renderPortfolioCards(filtered);
    }
  });

  portfolioSetView(_portfolioCurrentView);
}

function updatePortfolioCardData(plants) {
  const grid = document.getElementById("portfolioCardView");
  if (!grid) return;

  // Build lookup by plantId
  const plantMap = new Map();
  (Array.isArray(plants) ? plants : []).forEach(p => {
    const id = String(p.power_plant_id ?? p.plant_id ?? p.id);
    plantMap.set(id, p);
  });

  grid.querySelectorAll(".plant-card[data-plant-id]").forEach(card => {
    const pid = card.dataset.plantId;
    const plant = plantMap.get(String(pid));
    if (!plant) return;

    const plantState = getPortfolioPlantVisualState(plant);
    const commStatus = getPlantCardStatus(plant);
    const isCommOffline = commStatus.colorClass === 'plant-card--offline';
    const activePower = plantState.activePower;
    const ratedPower = Number(plant.rated_power_kw ?? plant.rated_power_kwp ?? 0);
    const energyToday = plantState.energyToday;
    const pr = plant.pr_daily_pct != null ? Number(plant.pr_daily_pct).toFixed(1) + "%" : "\u2014";
    const irr = plant.irradiance_wm2 != null ? Number(plant.irradiance_wm2).toFixed(0) + " W/m\u00B2" : "\u2014";
    const invAvail = plant.inverter_availability_pct != null ? Number(plant.inverter_availability_pct).toFixed(1) + "%" : "\u2014";
    const activePowerDisplay = isCommOffline ? '\u2014' : activePower.toFixed(1) + ' kW';

    // Update stat values in-place
    const statValues = card.querySelectorAll(".plant-card__stat-value");
    if (statValues[0]) statValues[0].textContent = activePowerDisplay;
    if (statValues[1]) statValues[1].textContent = ratedPower.toFixed(1) + " kWp";
    if (statValues[2]) statValues[2].textContent = energyToday.toFixed(1) + " kWh";
    if (statValues[3]) statValues[3].textContent = pr;
    if (statValues[4]) statValues[4].textContent = irr;
    if (statValues[5]) statValues[5].textContent = invAvail;

    // Update status dot + text
    const isOffline = plantState.isOffline || isCommOffline;
    let statusDotClass = "plant-card__status-dot";
    let statusText;
    if (isCommOffline) { statusDotClass += " offline"; statusText = "Sem comunica\u00E7\u00E3o"; }
    else if (plantState.kind === "offline") { statusDotClass += " offline"; statusText = "Desligada"; }
    else if (plantState.kind === "generating") { statusDotClass += " generating"; statusText = "Em gera\u00E7\u00E3o"; }
    else { statusDotClass += " standby"; statusText = "Aguardando"; }

    const dot = card.querySelector(".plant-card__status-dot");
    const txt = card.querySelector(".plant-card__status-text");
    if (dot) dot.className = statusDotClass;
    if (txt) txt.textContent = statusText;

    // Update card class (comm > alarm > offline)
    const pname = card.dataset.plantName || "";
    const alarmSeverity = normalizeAlarmSeverity(lastAlarmSeverityByPlant.get(pid))
      || normalizeAlarmSeverity(lastAlarmSeverityByPlant.get(pname))
      || normalizeAlarmSeverity(lastAlarmSeverityByPlant.get(Number(pid)))
      || null;
    const commSt = plant.comm_status || '';
    card.dataset.commStatus = commSt;

    let newCardClass;
    if (commStatus.badge) {
      newCardClass = `plant-card ${commStatus.colorClass}`;
    } else {
      const offCls = isOffline ? " plant-card--offline" : "";
      const almCls = alarmSeverity ? ` alarm-${alarmSeverity}` : "";
      const stdCls = (!isOffline && !alarmSeverity && plantState.kind === "standby") ? " standby-card" : "";
      newCardClass = `plant-card${offCls}${almCls}${stdCls}`;
    }
    card.className = newCardClass;

    // Update icon
    const icon = card.querySelector(".plant-card__icon");
    if (icon) {
      if (commStatus.badge) icon.className = "plant-card__icon";
      else if (alarmSeverity) icon.className = `plant-card__icon alarm-${alarmSeverity}`;
      else if (plantState.kind === "standby") icon.className = "plant-card__icon standby-icon";
      else icon.className = "plant-card__icon";
    }

    // Update comm badge
    const existingBadge = card.querySelector(".badge--offline, .badge--partial");
    if (commStatus.badge) {
      if (existingBadge) {
        existingBadge.className = commStatus.badgeClass;
        existingBadge.textContent = commStatus.badge;
      } else {
        const top = card.querySelector(".plant-card__top");
        if (top) {
          const span = document.createElement("span");
          span.className = commStatus.badgeClass;
          span.textContent = commStatus.badge;
          top.appendChild(span);
        }
      }
    } else if (existingBadge) {
      existingBadge.remove();
    }
  });
}

function updatePortfolioCardAlarms() {
  const grid = document.getElementById("portfolioCardView");
  if (!grid) return;
  grid.querySelectorAll(".plant-card[data-plant-id]").forEach(card => {
    const pid = card.dataset.plantId;
    const pname = card.dataset.plantName || "";
    const sev = normalizeAlarmSeverity(lastAlarmSeverityByPlant.get(pid))
      || normalizeAlarmSeverity(lastAlarmSeverityByPlant.get(pname))
      || normalizeAlarmSeverity(lastAlarmSeverityByPlant.get(Number(pid)))
      || null;
    // comm_status takes visual priority — skip alarm override for comm offline/partial
    const commSt = card.dataset.commStatus;
    if (commSt === 'offline' || commSt === 'partial') return;
    const isOff = card.classList.contains("plant-card--offline");
    card.className = `plant-card${isOff ? " plant-card--offline" : ""}${sev ? ` alarm-${sev}` : ""}`;
    const icon = card.querySelector(".plant-card__icon");
    if (icon) icon.className = sev ? `plant-card__icon alarm-${sev}` : "plant-card__icon";
  });
}

function renderPortfolioCards(plants) {
  const grid = document.getElementById("portfolioCardView");
  if (!grid) return;

  const renderGen = ++_portfolioRenderGen;
  const validPlants = sortPortfolioPlants(plants);
  grid.innerHTML = "";

  _portfolioMiniCharts.forEach(chart => { try { chart.destroy(); } catch(e) {} });
  _portfolioMiniCharts.clear();

  // Coleta (canvasId, plantId) para render de charts depois
  const chartTargets = [];

  validPlants.forEach(plant => {
    const plantId = plant.power_plant_id ?? plant.plant_id ?? plant.id;
    const plantName = plant.power_plant_name ?? plant.plant_name ?? plant.name ?? "\u2014";

    const alarmSeverity = normalizeAlarmSeverity(lastAlarmSeverityByPlant.get(plantId))
      || normalizeAlarmSeverity(lastAlarmSeverityByPlant.get(plantName)) || null;

    const plantState = getPortfolioPlantVisualState(plant);
    const commStatus = getPlantCardStatus(plant);
    const isCommOffline = commStatus.colorClass === 'plant-card--offline';
    const activePower = plantState.activePower;
    const ratedPower = Number(plant.rated_power_kw ?? plant.rated_power_kwp ?? 0);
    const energyToday = plantState.energyToday;
    const pr = plant.pr_daily_pct != null ? Number(plant.pr_daily_pct).toFixed(1) + "%" : "\u2014";
    const irr = plant.irradiance_wm2 != null ? Number(plant.irradiance_wm2).toFixed(0) + " W/m\u00B2" : "\u2014";
    const invAvail = plant.inverter_availability_pct != null ? Number(plant.inverter_availability_pct).toFixed(1) + "%" : "\u2014";

    const isOffline = plantState.isOffline || isCommOffline;
    const isGenerating = plantState.kind === "generating";
    let statusDotClass = "plant-card__status-dot";
    let statusText;
    if (isCommOffline) {
      statusDotClass += " offline";
      statusText = "Sem comunicação";
    }
    else if (plantState.kind === "offline") {
      statusDotClass += " offline";
      statusText = "Desligada";
    }
    else if (isGenerating){ statusDotClass += " generating"; statusText = "Em gera\u00E7\u00E3o"; }
    else                  { statusDotClass += " standby";   statusText = "Aguardando"; }

    // comm_status takes visual priority over alarm severity
    let cardClass, iconClass;
    if (commStatus.badge) {
      cardClass = `plant-card ${commStatus.colorClass}`;
      iconClass = "plant-card__icon";
    } else {
      const offlineClass = isOffline ? " plant-card--offline" : "";
      const alarmSuffix  = alarmSeverity ? ` alarm-${alarmSeverity}` : "";
      const standbySuffix = (!isOffline && !alarmSeverity && plantState.kind === "standby") ? " standby-card" : "";
      iconClass = alarmSeverity ? `plant-card__icon alarm-${alarmSeverity}` :
                  (plantState.kind === "standby" ? "plant-card__icon standby-icon" : "plant-card__icon");
      cardClass = `plant-card${offlineClass}${alarmSuffix}${standbySuffix}`;
    }
    const canvasId = "mini-chart-" + plantId;

    const commBadgeHtml = commStatus.badge
      ? `<span class="${commStatus.badgeClass}">${commStatus.badge}</span>`
      : '';
    const activePowerDisplay = isCommOffline ? '—' : activePower.toFixed(1) + ' kW';

    const card = document.createElement("div");
    card.className = cardClass;
    card.setAttribute("role", "link");
    card.setAttribute("tabindex", "0");
    card.dataset.plantId = plantId;
    card.dataset.plantName = plantName;
    card.dataset.commStatus = plant.comm_status || '';

    card.innerHTML = `
      <div class="plant-card__top">
        <div class="${iconClass}"><i class="fa-solid fa-seedling"></i></div>
        <div class="plant-card__name">${plantName}</div>
        ${commBadgeHtml}
      </div>
      <div class="plant-card__stats">
        <div class="plant-card__stat">
          <div class="plant-card__stat-label"><i class="fa-solid fa-bolt"></i> Active Power</div>
          <div class="plant-card__stat-value active">${activePowerDisplay}</div>
        </div>
        <div class="plant-card__stat">
          <div class="plant-card__stat-label"><i class="fa-solid fa-layer-group"></i> Rated</div>
          <div class="plant-card__stat-value muted">${ratedPower.toFixed(1)} kWp</div>
        </div>
        <div class="plant-card__stat">
          <div class="plant-card__stat-label"><i class="fa-solid fa-calendar-day"></i> Hoje</div>
          <div class="plant-card__stat-value">${energyToday.toFixed(1)} kWh</div>
        </div>
        <div class="plant-card__stat">
          <div class="plant-card__stat-label"><i class="fa-solid fa-gauge-high"></i> PR Di\u00E1rio</div>
          <div class="plant-card__stat-value">${pr}</div>
        </div>
        <div class="plant-card__stat">
          <div class="plant-card__stat-label"><i class="fa-solid fa-sun"></i> Irradi\u00E2ncia</div>
          <div class="plant-card__stat-value">${irr}</div>
        </div>
        <div class="plant-card__stat">
          <div class="plant-card__stat-label"><i class="fa-solid fa-microchip"></i> Inv. Disp.</div>
          <div class="plant-card__stat-value">${invAvail}</div>
        </div>
      </div>
      <div class="plant-card__chart-area">
        <div class="plant-card__chart-wrap">
          <canvas id="${canvasId}"></canvas>
        </div>
        <div class="plant-card__chart-legend">
          <span class="pcc-leg pcc-leg--power">Active Power</span>
          <span class="pcc-leg pcc-leg--irr">Irrad. POA</span>
          <span class="pcc-leg pcc-leg--pr">PR</span>
        </div>
      </div>
      <div class="plant-card__status">
        <div class="${statusDotClass}"></div>
        <span class="plant-card__status-text">${statusText}</span>
      </div>
      <button
        class="plant-card__edit-btn"
        title="Editar usina"
        data-edit-plant-id="${plantId}"
        aria-label="Editar usina"
        style="display:none;"
      >
        <svg viewBox="0 0 20 20" fill="currentColor"><path d="M13.586 3.586a2 2 0 112.828 2.828l-.793.793-2.828-2.828.793-.793zM11.379 5.793L3 14.172V17h2.828l8.38-8.379-2.83-2.828z"/></svg>
      </button>
    `;

    const openPlant = () => {
      if (plantId != null) window.location.href = `plant.html?plant_id=${encodeURIComponent(plantId)}`;
    };
    card.addEventListener("click", openPlant);
    card.addEventListener("keydown", e => {
      if (e.key === "Enter" || e.key === " ") { e.preventDefault(); openPlant(); }
    });

    const _u = JSON.parse(localStorage.getItem("user") || "{}");
    const _canEdit = _u.is_superuser === true || _u.role_key === "admin_customer";
    const _editBtn = card.querySelector(".plant-card__edit-btn");
    if (_editBtn && _canEdit) {
      _editBtn.style.display = "";
      _editBtn.addEventListener("click", (e) => {
        e.stopPropagation();
        openPlantEditModal(plantId, plantName, ratedPower);
      });
    }

    grid.appendChild(card);
    chartTargets.push({ canvasId, plantId });
  });

  // Inicia fetch de todos os mini-charts APOS todo o DOM estar montado,
  // com concorrencia controlada (3 por vez) e retry automatico.
  if (chartTargets.length > 0) {
    _startMiniChartBatch(chartTargets, renderGen);
  }
}

/**
 * Busca dados e renderiza mini-charts em lotes de CONCURRENCY,
 * com 1 retry automatico por falha. Usa cache (TTL 5 min).
 * Verifica renderGen antes de cada chart para abortar se houve re-render.
 */
async function _startMiniChartBatch(targets, renderGen) {
  const CONCURRENCY = 3;
  const queue = [...targets];
  const failed = [];

  async function processOne(target) {
    if (renderGen !== _portfolioRenderGen) return;
    try {
      await _fetchAndRenderOneMiniChart(target.canvasId, target.plantId, renderGen);
    } catch (e) {
      console.warn("[mini-chart] falhou, sera retentado:", target.plantId, e?.message || e);
      failed.push(target);
    }
  }

  // Pool de concorrencia: no maximo CONCURRENCY simultaneos
  async function drainQueue() {
    while (queue.length > 0) {
      if (renderGen !== _portfolioRenderGen) return;
      const batch = queue.splice(0, CONCURRENCY);
      await Promise.allSettled(batch.map(t => processOne(t)));
    }
  }

  // Aguarda 2 frames de layout do browser antes de criar os Chart.js
  await new Promise(r => requestAnimationFrame(() => requestAnimationFrame(r)));
  if (renderGen !== _portfolioRenderGen) return;

  await drainQueue();

  // Retry das que falharam (1 tentativa extra, sequencial)
  if (failed.length > 0 && renderGen === _portfolioRenderGen) {
    console.log("[mini-chart] retentando", failed.length, "charts que falharam");
    await new Promise(r => setTimeout(r, 1500));
    for (const target of failed) {
      if (renderGen !== _portfolioRenderGen) return;
      try {
        await _fetchAndRenderOneMiniChart(target.canvasId, target.plantId, renderGen);
      } catch (e) {
        console.warn("[mini-chart] retry falhou:", target.plantId, e?.message || e);
      }
    }
  }
}

/**
 * Re-fetches and re-renders mini-charts whose cache has expired (>5 min).
 * Called on each refresh cycle so charts update without full re-render.
 */
function _refreshMiniChartsIfStale() {
  const grid = document.getElementById("portfolioCardView");
  if (!grid) return;
  const staleTargets = [];
  grid.querySelectorAll("canvas[id^='mini-chart-']").forEach(canvas => {
    const plantId = canvas.id.replace("mini-chart-", "");
    const cached = _miniChartDataCache.get(plantId) || _miniChartDataCache.get(Number(plantId));
    if (!cached || (Date.now() - cached.ts) >= _MINI_CHART_CACHE_TTL_MS) {
      staleTargets.push({ canvasId: canvas.id, plantId });
    }
  });
  if (staleTargets.length > 0) {
    _startMiniChartBatch(staleTargets, _portfolioRenderGen);
  }
}

async function _fetchAndRenderOneMiniChart(canvasId, plantId, renderGen) {
  // 1) Busca dados (cache ou fetch)
  let body;
  const cached = _miniChartDataCache.get(plantId);
  if (cached && (Date.now() - cached.ts) < _MINI_CHART_CACHE_TTL_MS) {
    body = cached.body;
  } else {
    const res = await apiFetch(`/plants/${plantId}/energy/daily`);
    if (!res.ok) {
      throw new Error(`HTTP ${res.status} para plant ${plantId}`);
    }
    const raw = await res.json();
    body = (raw && raw.body)
      ? (typeof raw.body === "string" ? JSON.parse(raw.body) : raw.body)
      : raw;
    _miniChartDataCache.set(plantId, { ts: Date.now(), body });
  }

  // 2) Verifica se ainda estamos no mesmo render
  if (renderGen !== _portfolioRenderGen) return;

  // 3) Re-busca canvas no DOM (pode ter mudado)
  const canvas = document.getElementById(canvasId);
  if (!canvas) return;

  // 4) Monta chart
  _renderMiniChartOnCanvas(canvas, plantId, body);
}

function _renderMiniChartOnCanvas(canvas, plantId, body) {
  const labels   = body?.labels || [];
  const powerRaw = body?.activePower || body?.active_power_kw || body?.power_kw || [];
  const irrRaw   = body?.irradiance  || body?.irradiance_wm2  || [];
  const prRaw    = body?.pr          || body?.pr_pct          || body?.performance_ratio || [];

  if (!labels.length || (!powerRaw.length && !irrRaw.length)) return;

  const toNums = arr => arr.map(v => (v == null ? null : Number(v)));
  const seriesMax = arr => Math.max(...arr.filter(v => v != null && isFinite(v)), 0.001);
  const fmtTick = (v, unit) => {
    if (v === 0) return "0";
    if (unit === "kW"  && v >= 1000) return (v/1000).toFixed(0) + "M";
    if (unit === "W/m\u00B2"&& v >= 1000) return (v/1000).toFixed(1) + "k";
    return v % 1 === 0 ? v.toFixed(0) : v.toFixed(1);
  };

  const pNums  = toNums(powerRaw);
  const iNums  = toNums(irrRaw);
  const prNums = toNums(prRaw);
  const maxP   = powerRaw.length ? seriesMax(pNums)  : 0;
  const maxI   = irrRaw.length   ? seriesMax(iNums)  : 0;

  const datasets = [];

  if (powerRaw.length) {
    datasets.push({
      label: "Active Power", _raw: powerRaw, _unit: "kW",
      data: pNums, yAxisID: "y",
      borderColor: "rgba(127,208,85,0.9)",
      backgroundColor: "rgba(127,208,85,0.07)",
      borderWidth: 1.5, pointRadius: 0, pointHoverRadius: 4,
      pointHoverBackgroundColor: "#7fd055",
      tension: 0.4, fill: true,
    });
  }

  if (irrRaw.length) {
    datasets.push({
      label: "Irrad. POA", _raw: irrRaw, _unit: "W/m\u00B2",
      data: iNums, yAxisID: "y1",
      borderColor: "rgba(255,200,50,0.85)",
      backgroundColor: "transparent",
      borderWidth: 1.5, pointRadius: 0, pointHoverRadius: 4,
      pointHoverBackgroundColor: "#ffc832",
      tension: 0.4, fill: false,
    });
  }

  if (prRaw.length) {
    datasets.push({
      label: "PR", _raw: prRaw, _unit: "%",
      data: prNums, yAxisID: "y2",
      borderColor: "rgba(80,200,255,0.75)",
      backgroundColor: "transparent",
      borderWidth: 1.5, borderDash: [4, 3],
      pointRadius: 0, pointHoverRadius: 4,
      pointHoverBackgroundColor: "#50c8ff",
      tension: 0.4, fill: false,
    });
  }

  if (!datasets.length) return;

  const tickStyle = (color) => ({
    display: true,
    maxTicksLimit: 3,
    color,
    font: { family: "'JetBrains Mono', monospace", size: 8 },
    padding: 2,
  });

  try {
    const existing = Chart.getChart(canvas);
    if (existing) existing.destroy();
  } catch (_) {}

  const ctx = canvas.getContext("2d");
  const chart = new Chart(ctx, {
    type: "line",
    data: { labels, datasets },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      animation: false,
      interaction: { mode: "index", intersect: false },
      plugins: {
        legend: { display: false },
        tooltip: {
          enabled: true,
          backgroundColor: "rgba(4,12,8,0.94)",
          borderColor: "rgba(57,229,140,0.20)",
          borderWidth: 1,
          padding: { top: 7, bottom: 7, left: 10, right: 10 },
          titleColor: "rgba(154,219,184,0.55)",
          titleFont: { family: "'JetBrains Mono', monospace", size: 10 },
          bodyColor: "#ddeee4",
          bodyFont: { family: "'JetBrains Mono', monospace", size: 11 },
          callbacks: {
            title: items => items[0]?.label || "",
            label: item => {
              const raw = item.dataset._raw?.[item.dataIndex];
              if (raw == null) return null;
              return ` ${item.dataset.label}: ${Number(raw).toFixed(1)} ${item.dataset._unit}`;
            },
            labelColor: item => ({
              borderColor: item.dataset.borderColor,
              backgroundColor: item.dataset.borderColor,
              borderRadius: 2,
            }),
          },
        },
      },
      scales: {
        x: {
          display: true,
          grid: { display: false },
          ticks: { display: false },
          border: { display: false },
        },
        y: {
          type: "linear", position: "left",
          display: powerRaw.length > 0,
          min: 0, max: maxP * 1.12,
          grid: { display: false },
          ticks: { ...tickStyle("rgba(57,229,140,0.55)"), callback: v => fmtTick(v, "kW") },
          border: { display: false },
        },
        y1: {
          type: "linear", position: "right",
          display: irrRaw.length > 0,
          min: 0, max: maxI * 1.12,
          grid: { drawOnChartArea: false, drawTicks: false },
          ticks: { ...tickStyle("rgba(255,200,50,0.55)"), callback: v => fmtTick(v, "W/m\u00B2") },
          border: { display: false },
        },
        y2: {
          type: "linear", position: "right",
          display: false,
          min: 0, max: 100,
          grid: { drawOnChartArea: false },
        },
      },
      layout: { padding: { top: 4, bottom: 2, left: 0, right: 0 } },
    },
  });

  _portfolioMiniCharts.set(plantId, chart);
}

// =============================================================================
// MODAL EDITAR USINA
// =============================================================================
let _PLANT_EDIT_ID = null;

// ─── Gerenciar Usina Modal ───────────────────────────────────────────────────

function switchPlantEditTab(tab) {
  document.querySelectorAll(".pem-tab").forEach(b => b.classList.toggle("active", b.dataset.tab === tab));
  document.querySelectorAll(".pem-tab-panel").forEach(p => p.classList.add("hidden"));
  const panel = document.getElementById(
    tab === "info" ? "pemTabInfo" : tab === "devices" ? "pemTabDevices" : "pemTabCabins"
  );
  if (panel) panel.classList.remove("hidden");
  if (tab === "devices" && _PLANT_EDIT_ID) _pemLoadDevices(_PLANT_EDIT_ID);
  if (tab === "cabins"  && _PLANT_EDIT_ID) _pemLoadCabins(_PLANT_EDIT_ID);
}

function openPlantEditModal(plantId, plantName, ratedPower) {
  _PLANT_EDIT_ID = plantId;
  const nameInput  = document.getElementById("plantEditNameInput");
  const ratedInput = document.getElementById("plantEditRatedInput");
  if (nameInput)  nameInput.value  = plantName  || "";
  if (ratedInput) ratedInput.value = ratedPower != null ? Number(ratedPower).toFixed(1) : "";
  ["plantEditNameFeedback","plantEditRatedFeedback","pemAddDeviceFeedback","pemAddCabinFeedback"].forEach(id => {
    const el = document.getElementById(id);
    if (el) { el.textContent = ""; el.className = "plant-edit-feedback"; }
  });
  switchPlantEditTab("info");
  const modal = document.getElementById("plantEditModal");
  if (modal) { modal.classList.remove("hidden"); modal.setAttribute("aria-hidden","false"); document.body.classList.add("plant-edit-modal-open"); }
}

function closePlantEditModal() {
  const modal = document.getElementById("plantEditModal");
  if (modal) { modal.classList.add("hidden"); modal.setAttribute("aria-hidden","true"); document.body.classList.remove("plant-edit-modal-open"); }
  _PLANT_EDIT_ID = null;
}

async function _pemLoadDevices(plantId) {
  const list = document.getElementById("pemDevicesList");
  if (!list) return;
  list.innerHTML = `<div class="plant-edit-empty">Carregando…</div>`;
  try {
    // Carregar device types para o select (uma vez)
    const selEl = document.getElementById("pemDeviceTypeSelect");
    if (selEl && selEl.options.length <= 1) {
      try {
        const dtRes  = await apiFetch(`/plants/${plantId}/device-types`);
        const dtData = await dtRes.json();
        (dtData?.items || []).forEach(dt => {
          const opt = document.createElement("option");
          opt.value       = dt.id;
          opt.textContent = dt.name;
          selEl.appendChild(opt);
        });
      } catch(_) {}
    }
    // Carregar dispositivos
    const res   = await apiFetch(`/plants/${plantId}/devices/options`);
    const data  = await res.json();
    const items = data?.items || [];
    if (!items.length) { list.innerHTML = `<div class="plant-edit-empty">Nenhum dispositivo.</div>`; return; }
    const canCmd = _canSendCommand();
    list.innerHTML = items.map(d => `
      <div class="plant-edit-device-row" data-device-id="${d.device_id}">
        <div class="plant-edit-device-info">
          <span class="plant-edit-device-type">${d.device_type || "—"}</span>
          <span class="plant-edit-device-meta">#${d.device_id}</span>
        </div>
        <div class="plant-edit-inline">
          <input class="plant-edit-device-name" type="text"
            value="${(d.device_name || "").replace(/"/g,'&quot;')}"
            placeholder="Nome do dispositivo"/>
          <button class="plant-edit-device-save" type="button" title="Salvar nome">
            <i class="fa-solid fa-check"></i>
          </button>
          ${canCmd ? _pemRenderCommandControl(d.device_type || "", d.device_id) : ""}
          <button class="pem-del-device-btn" type="button" title="Excluir dispositivo"
            data-device-id="${d.device_id}">
            <i class="fa-solid fa-trash"></i>
          </button>
        </div>
        <div class="plant-edit-device-feedback" id="pem-devfb-${d.device_id}"></div>
      </div>`).join("");
    _pemWireCommandButtons(list);
    list.querySelectorAll(".plant-edit-device-row").forEach(row => {
      const did     = row.dataset.deviceId;
      const inp     = row.querySelector(".plant-edit-device-name");
      const saveBtn = row.querySelector(".plant-edit-device-save");
      const delBtn  = row.querySelector(".pem-del-device-btn");
      const fb      = row.querySelector(".plant-edit-device-feedback");
      saveBtn?.addEventListener("click", async () => {
        const n = inp.value.trim();
        if (!n) { fb.textContent = "Nome vazio."; fb.className = "plant-edit-device-feedback err"; return; }
        saveBtn.disabled = true; fb.textContent = "Salvando…"; fb.className = "plant-edit-device-feedback";
        try {
          const r = await apiFetch(`/plants/${_PLANT_EDIT_ID}/devices/${did}/name`, {
            method: "PATCH", headers: {"Content-Type":"application/json"},
            body: JSON.stringify({display_name: n})
          });
          if (!r.ok) throw new Error((await r.json())?.error || `HTTP ${r.status}`);
          fb.textContent = "✓ Salvo!"; fb.className = "plant-edit-device-feedback ok";
        } catch(e) { fb.textContent = e.message || "Erro."; fb.className = "plant-edit-device-feedback err"; }
        finally    { saveBtn.disabled = false; }
      });
      delBtn?.addEventListener("click", async () => {
        if (!confirm(`Excluir dispositivo #${did}? Esta ação desativa o dispositivo.`)) return;
        delBtn.disabled = true; fb.textContent = "Excluindo…"; fb.className = "plant-edit-device-feedback";
        try {
          const r = await apiFetch(`/plants/${_PLANT_EDIT_ID}/devices/${did}`, { method: "DELETE" });
          if (!r.ok) throw new Error((await r.json())?.error || `HTTP ${r.status}`);
          fb.textContent = "✓ Removido!"; fb.className = "plant-edit-device-feedback ok";
          setTimeout(() => row.remove(), 900);
        } catch(e) { fb.textContent = e.message || "Erro."; fb.className = "plant-edit-device-feedback err"; delBtn.disabled = false; }
      });
    });
  } catch(e) {
    console.error("[_pemLoadDevices]", e);
    list.innerHTML = `<div class="plant-edit-empty plant-edit-empty--error">Erro ao carregar.</div>`;
  }
}

async function _pemLoadCabins(plantId) {
  const list = document.getElementById("pemCabinsList");
  if (!list) return;
  list.innerHTML = `<div class="plant-edit-empty">Carregando…</div>`;
  try {
    const res  = await apiFetch(`/plants/${plantId}/cabin-groups`);
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    const data = await res.json();
    const items = data?.items || [];
    if (!items.length) { list.innerHTML = `<div class="plant-edit-empty">Nenhuma cabine.</div>`; return; }
    list.innerHTML = items.map(c => `
      <div class="plant-edit-device-row">
        <div class="plant-edit-device-info">
          <span class="plant-edit-device-type">${c.name}</span>
          <span class="plant-edit-device-meta">${c.inverter_count} inv.</span>
        </div>
      </div>`).join("");
  } catch(e) {
    console.error("[_pemLoadCabins]", e);
    list.innerHTML = `<div class="plant-edit-empty plant-edit-empty--error">Erro ao carregar cabines.</div>`;
  }
}

async function pemAddDevice() {
  if (!_PLANT_EDIT_ID) return;
  const selEl  = document.getElementById("pemDeviceTypeSelect");
  const nameEl = document.getElementById("pemDeviceNameInput");
  const fb     = document.getElementById("pemAddDeviceFeedback");
  const dtId   = selEl?.value;
  const name   = nameEl?.value?.trim();
  if (!dtId) { fb.textContent = "Selecione o tipo."; fb.className = "plant-edit-feedback err"; return; }
  if (!name) { fb.textContent = "Nome obrigatório."; fb.className = "plant-edit-feedback err"; return; }
  fb.textContent = "Adicionando…"; fb.className = "plant-edit-feedback";
  try {
    const r = await apiFetch(`/plants/${_PLANT_EDIT_ID}/devices`, {
      method: "POST", headers: {"Content-Type":"application/json"},
      body: JSON.stringify({device_type_id: parseInt(dtId), name, display_name: name})
    });
    const d = await r.json();
    if (!r.ok) throw new Error(d?.error || `HTTP ${r.status}`);
    fb.textContent = `✓ Dispositivo #${d.device_id} adicionado.`; fb.className = "plant-edit-feedback ok";
    if (nameEl) nameEl.value = "";
    _pemLoadDevices(_PLANT_EDIT_ID);
  } catch(e) { fb.textContent = e.message || "Erro."; fb.className = "plant-edit-feedback err"; }
}

async function pemAddCabin() {
  if (!_PLANT_EDIT_ID) return;
  const nameEl = document.getElementById("pemCabinNameInput");
  const fb     = document.getElementById("pemAddCabinFeedback");
  const name   = nameEl?.value?.trim();
  if (!name) { fb.textContent = "Nome obrigatório."; fb.className = "plant-edit-feedback err"; return; }
  fb.textContent = "Adicionando…"; fb.className = "plant-edit-feedback";
  try {
    const r = await apiFetch(`/plants/${_PLANT_EDIT_ID}/cabin-groups`, {
      method: "POST", headers: {"Content-Type":"application/json"},
      body: JSON.stringify({name, code: name})
    });
    const d = await r.json();
    if (!r.ok) throw new Error(d?.error || `HTTP ${r.status}`);
    fb.textContent = `✓ Cabine "${d.name}" criada.`; fb.className = "plant-edit-feedback ok";
    if (nameEl) nameEl.value = "";
    _pemLoadCabins(_PLANT_EDIT_ID);
  } catch(e) { fb.textContent = e.message || "Erro."; fb.className = "plant-edit-feedback err"; }
}

function _applyPlantEditLocally(plantId, patch) {
  const pid = String(plantId);
  for (const p of lastValidPlants) {
    const id = String(p.power_plant_id ?? p.plant_id ?? p.id);
    if (id === pid) { Object.assign(p, patch); break; }
  }
  // Re-render table + cards with updated local data
  renderPortfolioTable(lastValidPlants);
  if (typeof _portfolioCurrentView !== "undefined" && _portfolioCurrentView === "card") {
    const filtered = typeof portfolioFilterPlants === "function"
      ? portfolioFilterPlants(lastValidPlants) : lastValidPlants;
    updatePortfolioCardData(filtered);
    // Also update card title if name changed
    if (patch.power_plant_name) {
      const grid = document.getElementById("portfolioCardView");
      if (grid) grid.querySelectorAll(`.plant-card[data-plant-id="${pid}"]`).forEach(card => {
        card.dataset.plantName = patch.power_plant_name;
        const nameEl = card.querySelector(".plant-card__name");
        if (nameEl) nameEl.textContent = patch.power_plant_name;
      });
    }
  }
  updateSummaryUI(lastValidPlants);
}

async function savePlantName() {
  if (!_PLANT_EDIT_ID) return;
  const inp = document.getElementById("plantEditNameInput");
  const fb  = document.getElementById("plantEditNameFeedback");
  const n   = inp?.value?.trim();
  if (!n) { fb.textContent = "Nome vazio."; fb.className = "plant-edit-feedback err"; return; }
  fb.textContent = "Salvando…"; fb.className = "plant-edit-feedback";
  try {
    const _u = JSON.parse(localStorage.getItem("user") || "{}");
    const r = await apiFetch(`/plants/${_PLANT_EDIT_ID}/name`, {
      method: "PATCH",
      headers: {"Content-Type":"application/json", "X-Username": _u.username || ""},
      body: JSON.stringify({plant_name: n})
    });
    const resp = await r.json();
    if (!r.ok) throw new Error(resp?.error || `HTTP ${r.status}`);
    // resp comes from RETURNING — confirmed written to DB
    const savedName = resp?.body?.power_plant_name ?? resp?.power_plant_name ?? n;
    fb.textContent = "✓ Salvo!"; fb.className = "plant-edit-feedback ok";
    _applyPlantEditLocally(_PLANT_EDIT_ID, { power_plant_name: savedName, plant_name: savedName, name: savedName, display_name: savedName });
  } catch(e) { fb.textContent = e.message || "Erro."; fb.className = "plant-edit-feedback err"; }
}

async function savePlantRatedPower() {
  if (!_PLANT_EDIT_ID) return;
  const inp = document.getElementById("plantEditRatedInput");
  const fb  = document.getElementById("plantEditRatedFeedback");
  const val = parseFloat(inp?.value);
  if (isNaN(val) || val < 0) { fb.textContent = "Valor inválido."; fb.className = "plant-edit-feedback err"; return; }
  fb.textContent = "Salvando…"; fb.className = "plant-edit-feedback";
  try {
    const _u = JSON.parse(localStorage.getItem("user") || "{}");
    const r = await apiFetch(`/plants/${_PLANT_EDIT_ID}/name`, {
      method: "PATCH",
      headers: {"Content-Type":"application/json", "X-Username": _u.username || ""},
      body: JSON.stringify({capacity_dc: val})
    });
    const resp = await r.json();
    if (!r.ok) throw new Error(resp?.error || `HTTP ${r.status}`);
    // resp comes from RETURNING — confirmed written to DB
    const savedDc = resp?.body?.capacity_dc ?? resp?.capacity_dc ?? val;
    fb.textContent = "✓ Salvo!"; fb.className = "plant-edit-feedback ok";
    _applyPlantEditLocally(_PLANT_EDIT_ID, { rated_power_kw: savedDc, rated_power_kwp: savedDc, capacity_dc: savedDc });
  } catch(e) { fb.textContent = e.message || "Erro."; fb.className = "plant-edit-feedback err"; }
}

// =============================================================================
// DEVICE COMMAND INFRASTRUCTURE — PEM context (resumo.html)
// (Versão adaptada de plant.js; usa _PLANT_EDIT_ID em vez de PLANT_ID)
// =============================================================================

const _PEM_CMD_STATE = new Map();

function _pemCmdKey(deviceType, deviceId) {
  return `${String(deviceType || "")}:${String(deviceId ?? "")}`;
}
function _pemCmdGetState(deviceType, deviceId, fallback = "off") {
  return _PEM_CMD_STATE.get(_pemCmdKey(deviceType, deviceId)) || fallback;
}
function _pemCmdSetState(deviceType, deviceId, state) {
  if (state !== "on" && state !== "off") return;
  _PEM_CMD_STATE.set(_pemCmdKey(deviceType, deviceId), state);
}
function _pemCmdApplyVisual(deviceType, deviceId, state) {
  const key = _pemCmdKey(deviceType, deviceId);
  document.querySelectorAll(`.device-command-control[data-device-key="${key}"]`).forEach(el => {
    el.classList.remove("is-on", "is-off", "is-reset-flash");
    el.classList.add(state === "on" ? "is-on" : "is-off");
  });
}

function _pemRenderCommandControl(deviceType, deviceId) {
  const safeType = String(deviceType || "");
  const safeId   = String(deviceId ?? "");
  const state    = _pemCmdGetState(safeType, safeId, "off");
  const key      = _pemCmdKey(safeType, safeId);
  return `
    <div class="device-command-control ${state === "on" ? "is-on" : "is-off"}"
         data-device-key="${key}" data-device-type="${safeType}" data-device-id="${safeId}">
      <button type="button" class="device-command-trigger"
              data-device-key="${key}" data-device-type="${safeType}" data-device-id="${safeId}"
              aria-label="Comandos do dispositivo">
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
    </div>`;
}

function _pemWireCommandButtons(rootEl) {
  if (!rootEl) return;
  rootEl.querySelectorAll(".device-command-trigger").forEach(btn => {
    if (btn.dataset.wiredPemCmd === "true") return;
    btn.dataset.wiredPemCmd = "true";
    btn.addEventListener("click", e => {
      e.preventDefault();
      e.stopPropagation();
      _pemOpenConsole({ deviceType: btn.dataset.deviceType || "", deviceId: btn.dataset.deviceId || "" });
    });
  });
}

function _pemEnsureAuthModal() {
  if (document.getElementById("deviceCommandAuthModal")) return;
  const wrap = document.createElement("div");
  wrap.innerHTML = `
    <div id="deviceCommandAuthModal" class="device-command-modal hidden">
      <div class="device-command-modal-card">
        <h3>Autenticação</h3>
        <p id="deviceCommandAuthLabel">Confirme usuário e senha</p>
        <input id="deviceCommandUser" type="text" placeholder="Usuário" autocomplete="username" />
        <input id="deviceCommandPass" type="password" placeholder="Senha" autocomplete="current-password" />
        <div class="device-command-modal-actions">
          <button id="deviceCommandCancelBtn" type="button">Cancelar</button>
          <button id="deviceCommandConfirmBtn" type="button">Confirmar</button>
        </div>
      </div>
    </div>`;
  document.body.appendChild(wrap);
}

function _pemEnsureConsole() {
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
        <div class="cmd-console__power-section">
          <div class="cmd-console__power-label"><i class="fa-solid fa-sliders"></i> Setar Potência Ativa</div>
          <div class="cmd-console__power-row">
            <input id="cmdConsolePowerInput" class="cmd-console__power-input" type="number" min="0" step="0.1" placeholder="kW" aria-label="Potência em kW" />
            <span class="cmd-console__power-unit">kW</span>
            <button class="cmd-console__power-btn" id="cmdConsoleBtnSetPower">
              <i class="fa-solid fa-paper-plane"></i> Setar
            </button>
          </div>
        </div>
        <div class="cmd-console__feedback hidden" id="cmdConsoleFeedback">
          <div class="cmd-console__feedback-inner">
            <span class="cmd-console__feedback-icon" id="cmdConsoleFeedbackIcon"></span>
            <span class="cmd-console__feedback-text" id="cmdConsoleFeedbackText"></span>
          </div>
        </div>
        <div class="cmd-console__footer">
          <i class="fa-solid fa-lock"></i>
          Todos os comandos requerem autenticação antes de serem executados.
        </div>
      </div>
    </div>`;
  document.body.appendChild(el);
  document.getElementById("cmdConsoleOverlay").addEventListener("click", e => {
    if (e.target === e.currentTarget) _pemCloseConsole();
  });
  document.getElementById("cmdConsoleClose").addEventListener("click", _pemCloseConsole);
}

function _pemOpenConsole({ deviceType, deviceId }) {
  _pemEnsureConsole();
  _pemEnsureAuthModal();
  const overlay    = document.getElementById("cmdConsoleOverlay");
  const nameEl     = document.getElementById("cmdConsoleDeviceName");
  const dotEl      = document.getElementById("cmdConsoleStateDot");
  const textEl     = document.getElementById("cmdConsoleStateText");
  const feedbackEl = document.getElementById("cmdConsoleFeedback");
  if (!overlay) return;

  nameEl.textContent = `${String(deviceType || "").toUpperCase()} — ID ${deviceId}`;
  const state = _pemCmdGetState(deviceType, deviceId, "off");
  dotEl.className = "cmd-console__state-dot " + (state === "on" ? "is-on" : "is-off");
  textEl.textContent = state === "on" ? "ESTADO ATUAL: LIGADO" : "ESTADO ATUAL: DESLIGADO";
  if (feedbackEl) feedbackEl.classList.add("hidden");

  // Reclona botões para limpar handlers anteriores
  ["cmdConsoleBtnOn","cmdConsoleBtnOff","cmdConsoleBtnReset","cmdConsoleBtnSetPower"].forEach(id => {
    const btn = document.getElementById(id);
    if (btn) { const c = btn.cloneNode(true); btn.parentNode.replaceChild(c, btn); }
  });
  const pwrInput = document.getElementById("cmdConsolePowerInput");
  if (pwrInput) pwrInput.value = "";

  const dispatch = (action, value) => {
    _pemCloseConsole();
    _pemAuthFlow({ deviceType, deviceId, action, value });
  };
  document.getElementById("cmdConsoleBtnOn").addEventListener("click",    () => dispatch("on"));
  document.getElementById("cmdConsoleBtnOff").addEventListener("click",   () => dispatch("off"));
  document.getElementById("cmdConsoleBtnReset").addEventListener("click", () => dispatch("reset"));
  document.getElementById("cmdConsoleBtnSetPower").addEventListener("click", () => {
    const input = document.getElementById("cmdConsolePowerInput");
    const val = input ? parseFloat(input.value) : NaN;
    if (isNaN(val) || val < 0) { if (input) input.focus(); return; }
    dispatch("set_power", val);
  });

  overlay.classList.remove("hidden");
  document.body.style.overflow = "hidden";
}

function _pemCloseConsole() {
  const overlay = document.getElementById("cmdConsoleOverlay");
  if (overlay) overlay.classList.add("hidden");
  document.body.style.overflow = "";
}

function _pemAuthFlow({ deviceType, deviceId, action, value }) {
  _pemEnsureAuthModal();
  const auth       = document.getElementById("deviceCommandAuthModal");
  const authLabel  = document.getElementById("deviceCommandAuthLabel");
  const cancelBtn  = document.getElementById("deviceCommandCancelBtn");
  const confirmBtn = document.getElementById("deviceCommandConfirmBtn");
  const userInput  = document.getElementById("deviceCommandUser");
  const passInput  = document.getElementById("deviceCommandPass");
  if (!auth || !confirmBtn || !cancelBtn) return;

  userInput.value = ""; passInput.value = "";
  authLabel.textContent = `${String(deviceType).toUpperCase()} ${deviceId} • ${action === "set_power" ? `SET POWER → ${value} kW` : action.toUpperCase()}`;
  auth.classList.remove("hidden");

  const closeAuth = () => auth.classList.add("hidden");
  cancelBtn.onclick = closeAuth;

  function showFeedback({ success, message }) {
    _pemEnsureConsole();
    const overlay2   = document.getElementById("cmdConsoleOverlay");
    const feedbackEl = document.getElementById("cmdConsoleFeedback");
    const iconEl     = document.getElementById("cmdConsoleFeedbackIcon");
    const textEl2    = document.getElementById("cmdConsoleFeedbackText");
    const dotEl      = document.getElementById("cmdConsoleStateDot");
    const stateTextEl= document.getElementById("cmdConsoleStateText");
    const nameEl2    = document.getElementById("cmdConsoleDeviceName");

    if (overlay2) { overlay2.classList.remove("hidden"); document.body.style.overflow = "hidden"; }
    if (nameEl2) nameEl2.textContent = `${String(deviceType).toUpperCase()} — ID ${deviceId}`;
    if (success && (action === "on" || action === "off")) {
      _pemCmdSetState(deviceType, deviceId, action);
      _pemCmdApplyVisual(deviceType, deviceId, action);
      if (dotEl) dotEl.className = "cmd-console__state-dot " + (action === "on" ? "is-on" : "is-off");
      if (stateTextEl) stateTextEl.textContent = `ESTADO ATUAL: ${action === "on" ? "LIGADO" : "DESLIGADO"}`;
    }
    if (feedbackEl && iconEl && textEl2) {
      feedbackEl.classList.remove("hidden", "is-success", "is-error");
      feedbackEl.classList.add(success ? "is-success" : "is-error");
      iconEl.innerHTML = success ? `<i class="fa-solid fa-circle-check"></i>` : `<i class="fa-solid fa-circle-xmark"></i>`;
      textEl2.textContent = message;
    }
  }

  confirmBtn.onclick = async () => {
    const username = userInput.value.trim();
    const password = passInput.value;
    if (!username || !password) { authLabel.textContent = "Preencha usuário e senha."; return; }
    confirmBtn.disabled = true; confirmBtn.textContent = "Aguarde...";
    try {
      if (!_PLANT_EDIT_ID) throw new Error("plant_id não encontrado");
      const r = await apiFetch(`/plants/${_PLANT_EDIT_ID}/devices/${deviceId}/command`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          action, username, password, requested_by: username,
          ...(action === "set_power" && value != null ? { value } : {})
        })
      });
      let data = {};
      try { data = await r.json(); } catch (_) {}
      closeAuth();
      if (r.ok && data.ok) {
        showFeedback({ success: true, message: action === "set_power" ? `Potência setada para ${value} kW com sucesso.` : `Comando ${action.toUpperCase()} enviado com sucesso.` });
      } else {
        showFeedback({ success: false, message: data.error || (r.status === 401 ? "Credenciais inválidas." : `Falha. (${r.status})`) });
      }
    } catch (err) {
      closeAuth();
      showFeedback({ success: false, message: `Erro: ${err.message}` });
    } finally {
      confirmBtn.disabled = false; confirmBtn.textContent = "Confirmar";
    }
  };
}

document.addEventListener("DOMContentLoaded",()=>{
  document.querySelector(".plant-edit-modal__backdrop")?.addEventListener("click", closePlantEditModal);

  // Botão "+" — só aparece para superuser / admin_customer
  const _btnAdd = document.getElementById("btnAddPlant");
  if (_btnAdd) {
    const _usr = JSON.parse(localStorage.getItem("user") || "{}");
    if (_usr.is_superuser === true || _usr.role_key === "admin_customer") {
      _btnAdd.classList.remove("hidden");
      _btnAdd.addEventListener("click", openPlantCreateModal);
    }
    // Fechar modal de criação com backdrop
    document.getElementById("plantCreateModal")
      ?.querySelector(".plant-edit-modal__backdrop")
      ?.addEventListener("click", closePlantCreateModal);
  }
});

// ─────────────────────────────────────────────────
// CRIAR USINA
// ─────────────────────────────────────────────────
function openPlantCreateModal() {
  const modal = document.getElementById("plantCreateModal");
  if (!modal) return;
  ["plantCreateNameInput","plantCreateDcInput","plantCreateAcInput",
   "plantCreateLocationInput","plantCreateCustomerInput"].forEach(id => {
    const el = document.getElementById(id);
    if (el) el.value = "";
  });
  const fb = document.getElementById("plantCreateFeedback");
  if (fb) { fb.textContent = ""; fb.className = "plant-edit-feedback"; }

  const _u = JSON.parse(localStorage.getItem("user") || "{}");
  const customerSection = document.getElementById("plantCreateCustomerSection");
  if (customerSection) customerSection.style.display = _u.is_superuser ? "" : "none";

  modal.classList.remove("hidden");
  modal.setAttribute("aria-hidden", "false");
  document.body.classList.add("plant-edit-modal-open");
  document.getElementById("plantCreateNameInput")?.focus();
}

function closePlantCreateModal() {
  const modal = document.getElementById("plantCreateModal");
  if (modal) { modal.classList.add("hidden"); modal.setAttribute("aria-hidden","true"); }
  document.body.classList.remove("plant-edit-modal-open");
}

async function createNewPlant() {
  const nameEl   = document.getElementById("plantCreateNameInput");
  const dcEl     = document.getElementById("plantCreateDcInput");
  const acEl     = document.getElementById("plantCreateAcInput");
  const locEl    = document.getElementById("plantCreateLocationInput");
  const custEl   = document.getElementById("plantCreateCustomerInput");
  const fb       = document.getElementById("plantCreateFeedback");
  const saveBtn  = document.getElementById("plantCreateSaveBtn");

  const name = nameEl?.value?.trim();
  if (!name) {
    if (fb) { fb.textContent = "Nome da usina é obrigatório."; fb.className = "plant-edit-feedback err"; }
    nameEl?.focus();
    return;
  }

  const payload = { plant_name: name };
  if (dcEl?.value)   payload.capacity_dc = parseFloat(dcEl.value);
  if (acEl?.value)   payload.capacity_ac = parseFloat(acEl.value);
  if (locEl?.value)  payload.location    = locEl.value.trim();
  if (custEl?.value) payload.customer_id = parseInt(custEl.value, 10);

  if (fb)      { fb.textContent = "Criando…"; fb.className = "plant-edit-feedback"; }
  if (saveBtn) saveBtn.disabled = true;

  try {
    const res = await apiFetch("/plants", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    });
    const data = await res.json();
    if (!res.ok) throw new Error(data?.error || `HTTP ${res.status}`);

    if (fb) {
      fb.textContent = `✓ Usina "${data.plant_name}" criada (ID ${data.plant_id}).`;
      fb.className = "plant-edit-feedback ok";
    }
    setTimeout(async () => {
      closePlantCreateModal();
      try { await loadPortfolio(); } catch(_) {}
    }, 1200);
  } catch(e) {
    if (fb) { fb.textContent = e.message || "Erro ao criar usina."; fb.className = "plant-edit-feedback err"; }
  } finally {
    if (saveBtn) saveBtn.disabled = false;
  }
}

// wirePortfolioControls is now called synchronously inside the main DOMContentLoaded listener (above).

// =============================================================================
// ROBÔ ASSISTENTE DE DIAGNÓSTICO
// =============================================================================
const ROBOT_STATE = {
  issues: [], currentIndex: 0, isTyping: false,
  typingTimer: null, cycleTimer: null, dismissTimer: null,
  reportOpen: false, bubbleVisible: false,
  lastState: null, lastIssueHash: null, userRead: false,
  avatarTimer: null, avatarState: null,
};
const ROBOT_TYPE_SPEED      = 40;
const ROBOT_CYCLE_PAUSE     = 5000;
const ROBOT_MAX_CHARS       = 110;
const ROBOT_DISMISS_DELAY   = 12000;
const ROBOT_AVATAR_GIF_MS   = 10000; // GIF plays for 10s then switches to static

const ROBOT_AVATAR_MAP = {
  critical: { gif: "img/roboaiotiredgif.gif",  png: "img/roboaiotired.png"  },
  warning:  { gif: "img/roboaiotiidlegif.gif", png: "img/roboaiotiidle.png" },
  gray:     { gif: "img/roboaiotioffgif.gif",  png: "img/roboaiotioff.png"  },
  ok:       { gif: null,                        png: "img/roboaiotiok.png"   },
};

const ROBOT_NOTIF_PREFS_KEY = "robot_notif_prefs";
const ROBOT_CAT_OPEN_KEY    = "robot_cat_open";

const ROBOT_CATEGORY_META = {
  temp_sustained:  { label: "Temperatura elevada",   order: 0 },
  plant_shutdown:  { label: "Usina desligada",        order: 1 },
  pr_declining:    { label: "PR em queda",            order: 2 },
  sub_performance: { label: "Sub-performance",        order: 3 },
  inv_clipping:    { label: "Clipping",               order: 4 },
  string_zero:     { label: "String zerada",          order: 5 },
  string_low:      { label: "String abaixo da m\u00e9dia", order: 6 },
};

const ROBOT_CATEGORY_ICONS = {
  temp_sustained:  "fa-temperature-high",
  plant_shutdown:  "fa-power-off",
  pr_declining:    "fa-arrow-trend-down",
  sub_performance: "fa-chart-bar",
  inv_clipping:    "fa-bolt",
  string_zero:     "fa-circle-xmark",
  string_low:      "fa-battery-quarter",
};

function _robotGetNotifPrefs() {
  try { return JSON.parse(localStorage.getItem(ROBOT_NOTIF_PREFS_KEY) || "{}"); }
  catch (_) { return {}; }
}

function _robotSaveNotifPrefs(prefs) {
  localStorage.setItem(ROBOT_NOTIF_PREFS_KEY, JSON.stringify(prefs));
  const user = JSON.parse(localStorage.getItem("user") || "{}");
  if (user.id || user.username) {
    apiFetch("/users/notif-prefs", {
      method: "POST",
      headers: { "Content-Type": "application/json", "X-Username": user.username || "" },
      body: JSON.stringify({ prefs })
    }).catch(() => {});
  }
}

function _robotIsMuted() {
  return _robotGetNotifPrefs().muted === true;
}

function _robotIsTypeEnabled(type) {
  const prefs = _robotGetNotifPrefs();
  if (prefs.muted === true) return false;
  if (Array.isArray(prefs.disabled_types) && prefs.disabled_types.includes(type)) return false;
  return true;
}

function _robotGetOpenCats() {
  try { return JSON.parse(localStorage.getItem(ROBOT_CAT_OPEN_KEY) || "[]"); }
  catch (_) { return []; }
}

function _robotToggleCat(type) {
  let open = _robotGetOpenCats();
  if (open.includes(type)) open = open.filter(t => t !== type);
  else open.push(type);
  localStorage.setItem(ROBOT_CAT_OPEN_KEY, JSON.stringify(open));
  return open.includes(type);
}

async function fetchDiagnosticsSummary() {
  try {
    const res = await apiFetch("/diagnostics/summary");
    if (!res.ok) { console.warn("[ROBOT] HTTP", res.status); return null; }
    const data = await res.json();
    if (data && data.body) return typeof data.body === "string" ? JSON.parse(data.body) : data.body;
    return data;
  } catch (e) { console.error("[ROBOT]", e); return null; }
}

function robotIssuesHash(issues) {
  return issues.map(i => `${i.plant_id}:${i.type}:${i.device_id||i.device_name||""}`).join("|");
}

function robotFormatTs(ts) {
  if (!ts) return "";
  try {
    const d = new Date(ts);
    if (isNaN(d)) return "";
    const hh = String(d.getHours()).padStart(2, "0");
    const mm = String(d.getMinutes()).padStart(2, "0");
    const dd = String(d.getDate()).padStart(2, "0");
    const mo = String(d.getMonth() + 1).padStart(2, "0");
    return `${dd}/${mo} ${hh}:${mm}`;
  } catch (_) { return ""; }
}

function robotIssueToText(issue) {
  if (!issue) return "Sem anomalias\ndetectadas \u2713";
  const plant  = issue.plant_name  || "Usina";
  const device = issue.device_name ? ` \u2022 ${issue.device_name}` : "";
  const msg    = issue.message     || "Insight detectado";
  const ts     = robotFormatTs(issue.ts);
  const prefix = {
    temp_sustained: "\ud83c\udf21",
    pr_declining:   "\ud83d\udcc9",
    plant_shutdown: "\u26d4",
    inv_clipping:   "\u26a1",
    string_low:     "\ud83d\udd0b",
    string_zero:      "\u274c",
    sub_performance:  "\ud83d\udcca"
  }[issue.type] || "\ud83d\udcca";
  return `${prefix} ${plant}${device}\n${msg}${ts ? "\n\ud83d\udd52 " + ts : ""}`;
}

function robotBuildDeviceUrl(issue) {
  if (!issue.plant_id) return null;
  let url = "plant.html?plant_id=" + encodeURIComponent(issue.plant_id);
  if (issue.device_id) url += "&device_id=" + encodeURIComponent(issue.device_id);
  const invTypes = ["temp_sustained","inv_clipping","string_low","string_zero","sub_performance"];
  if (issue.device_type === "inverter" || invTypes.includes(issue.type)) url += "#sec-inverters";
  return url;
}

function robotTypewrite(text, onDone) {
  const textEl   = document.getElementById("robotBubbleText");
  const cursorEl = document.getElementById("robotBubbleCursor");
  if (!textEl || !cursorEl) return;
  if (ROBOT_STATE.typingTimer) { clearInterval(ROBOT_STATE.typingTimer); ROBOT_STATE.typingTimer = null; }
  ROBOT_STATE.isTyping = true;
  cursorEl.classList.remove("hidden");
  textEl.textContent = "";
  const txt = text.length > ROBOT_MAX_CHARS ? text.slice(0, ROBOT_MAX_CHARS) + "..." : text;
  let ci = 0;
  ROBOT_STATE.typingTimer = setInterval(() => {
    if (ci < txt.length) { textEl.textContent = txt.slice(0, ++ci); }
    else {
      clearInterval(ROBOT_STATE.typingTimer); ROBOT_STATE.typingTimer = null;
      ROBOT_STATE.isTyping = false; cursorEl.classList.add("hidden");
      if (onDone) onDone();
    }
  }, ROBOT_TYPE_SPEED);
}

function robotSetBubbleOk() {
  const textEl   = document.getElementById("robotBubbleText");
  const cursorEl = document.getElementById("robotBubbleCursor");
  if (textEl) textEl.textContent = "Ok!";
  if (cursorEl) cursorEl.classList.add("hidden");
  robotUpdateExpandBtn(false);
  ROBOT_STATE.userRead = true;
}

function robotDismissBubble() {
  robotSetBubbleOk();
  if (ROBOT_STATE.dismissTimer) { clearTimeout(ROBOT_STATE.dismissTimer); ROBOT_STATE.dismissTimer = null; }
}

function robotScheduleDismiss() {
  if (ROBOT_STATE.dismissTimer) clearTimeout(ROBOT_STATE.dismissTimer);
  ROBOT_STATE.dismissTimer = setTimeout(() => {
    if (!ROBOT_STATE.reportOpen && ROBOT_STATE.bubbleVisible) {
      // Auto-dismiss: just hide bubble, do NOT mark as "ok" or change avatar
      robotShowBubble(false);
      if (ROBOT_STATE.dismissTimer) { clearTimeout(ROBOT_STATE.dismissTimer); ROBOT_STATE.dismissTimer = null; }
    }
  }, ROBOT_DISMISS_DELAY);
}

function robotGetGlobalState(issues) {
  if (!issues || !issues.length) return "ok";
  const nonShutdown = issues.filter(i => i.type !== "plant_shutdown");
  // Só plant_shutdown → usina desligada → robô cinza/off
  if (nonShutdown.length === 0) return "gray";
  // Demais: vermelho (critical) ou amarelo (warning)
  if (nonShutdown.some(i => i.severity === "critical")) return "critical";
  return "warning";
}

function robotUpdateAvatar(state, overrideSrc) {
  const img = document.getElementById("robotImg");
  if (!img) return;

  // Clear pending GIF→static timer
  if (ROBOT_STATE.avatarTimer) {
    clearTimeout(ROBOT_STATE.avatarTimer);
    ROBOT_STATE.avatarTimer = null;
  }

  // No border/circle around the robot — image speaks for itself

  // Direct override (e.g., roboaiotiok.png for thumbs-up after user reads)
  if (overrideSrc) {
    if (!img.src.endsWith(overrideSrc)) img.src = overrideSrc;
    ROBOT_STATE.avatarState = state + "_override";
    return;
  }

  const map = ROBOT_AVATAR_MAP[state] || ROBOT_AVATAR_MAP.ok;

  // Play GIF if state changed and a GIF exists
  if (map.gif && ROBOT_STATE.avatarState !== state) {
    img.src = map.gif;
    ROBOT_STATE.avatarState = state;

    // After 10s → switch to static PNG + squish animation
    ROBOT_STATE.avatarTimer = setTimeout(() => {
      ROBOT_STATE.avatarTimer = null;
      img.src = map.png;
      img.classList.remove("robot-squish");
      void img.offsetWidth; // force reflow to restart animation
      img.classList.add("robot-squish");
      img.addEventListener("animationend", () => img.classList.remove("robot-squish"), { once: true });
    }, ROBOT_AVATAR_GIF_MS);
  } else if (!map.gif && ROBOT_STATE.avatarState !== state) {
    // No GIF for this state (e.g. "ok") — just set static
    img.src = map.png;
    ROBOT_STATE.avatarState = state;
  }
  // Same state → keep current image (GIF still playing or already static)
}

function robotUpdateBubble(state) {
  const el = document.getElementById("robotBubble");
  if (!el) return;
  el.classList.remove("state-ok","state-warning","state-critical","state-gray");
  el.classList.add("state-" + state);
}

function robotShowBubble(show) {
  const el = document.getElementById("robotBubble");
  if (el) el.classList.toggle("visible", !!show);
  ROBOT_STATE.bubbleVisible = !!show;
}

function robotUpdateBadge(n) {
  const el = document.getElementById("robotBadge");
  if (!el) return;
  if (n > 0) { el.textContent = n > 99 ? "99+" : n; el.classList.remove("hidden"); }
  else el.classList.add("hidden");
}

function robotUpdateExpandBtn(show) {
  const el = document.getElementById("robotBubbleExpand");
  if (el) el.classList.toggle("visible", !!show);
}

function robotStartCycle() {
  if (ROBOT_STATE.cycleTimer) { clearTimeout(ROBOT_STATE.cycleTimer); ROBOT_STATE.cycleTimer = null; }
  const issues = ROBOT_STATE.issues;
  if (!issues.length) return;
  if (ROBOT_STATE.reportOpen || ROBOT_STATE.userRead) return;
  // Show only the most recent/important issue in the bubble (first in the list)
  robotTypewrite(robotIssueToText(issues[0]), () => robotScheduleDismiss());
}

function robotRenderReport(issues) {
  const list = document.getElementById("robotReportList");
  if (!list) return;
  if (!issues || !issues.length) {
    list.innerHTML = '<div class="robot-report-empty">Sem anomalias\ndetectadas \u2713</div>';
    _robotRenderPrefsFooter();
    return;
  }

  // Agrupar por categoria na ordem definida
  const groups = {};
  issues.forEach(iss => {
    if (!groups[iss.type]) groups[iss.type] = [];
    groups[iss.type].push(iss);
  });

  const types = Object.keys(groups).sort((a, b) => {
    const oa = (ROBOT_CATEGORY_META[a] || { order: 99 }).order;
    const ob = (ROBOT_CATEGORY_META[b] || { order: 99 }).order;
    return oa - ob;
  });

  const openCats = _robotGetOpenCats();

  const html = types.map(type => {
    const meta  = ROBOT_CATEGORY_META[type] || { label: "Outros" };
    const icon  = ROBOT_CATEGORY_ICONS[type] || "fa-list";
    const items = groups[type];
    const isOpen = openCats.includes(type);
    const worst = items.some(i => i.severity === "critical") ? "critical"
                : items.some(i => i.severity === "warning")  ? "warning" : "info";

    const rows = items.map(iss => {
      const sc = "sev-" + (iss.severity || "info");
      const deviceUrl = robotBuildDeviceUrl(iss);
      let deviceLine = iss.device_name || "";
      if (iss.cabin_name) deviceLine += (deviceLine ? " \u2022 " : "") + iss.cabin_name;
      const tsStr = robotFormatTs(iss.ts);
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

  _robotRenderPrefsFooter();
}

function robotToggleReport(forceOpen) {
  const panel = document.getElementById("robotReport");
  const badge = document.getElementById("robotBadge");
  if (!panel) return;
  const open = forceOpen !== undefined ? forceOpen : !ROBOT_STATE.reportOpen;
  if (open) {
    robotRenderReport(ROBOT_STATE.issues);
    panel.classList.remove("hidden");
    if (badge) badge.classList.add("hidden");
    ROBOT_STATE.reportOpen = true;
    if (ROBOT_STATE.cycleTimer) { clearTimeout(ROBOT_STATE.cycleTimer); ROBOT_STATE.cycleTimer = null; }
    if (ROBOT_STATE.dismissTimer) { clearTimeout(ROBOT_STATE.dismissTimer); ROBOT_STATE.dismissTimer = null; }
  } else {
    panel.classList.add("hidden");
    if (badge && ROBOT_STATE.issues.length > 0) badge.classList.remove("hidden");
    ROBOT_STATE.reportOpen = false;
    // After closing report, user has read — show "Ok!" + thumbs-up
    robotDismissBubble();
    robotUpdateAvatar("ok", "img/roboaiotiok.png");
  }
}

async function robotRefresh() {
  if (_robotIsMuted()) {
    _robotApplyMutedState();
    return;
  }
  _robotRemoveMutedState();
  const data = await fetchDiagnosticsSummary();
  if (!data) return;
  const issues = (Array.isArray(data.issues) ? data.issues : [])
    .filter(i => _robotIsTypeEnabled(i.type));
  const newHash = robotIssuesHash(issues);
  const issuesChanged = newHash !== ROBOT_STATE.lastIssueHash;
  ROBOT_STATE.lastIssueHash = newHash;
  ROBOT_STATE.issues = issues;
  const state = robotGetGlobalState(issues);

  // Always update bubble border color, badge, expand button
  robotUpdateBubble(state);
  robotUpdateBadge(issues.length);
  robotUpdateExpandBtn(issues.length > 1);

  if (!issues.length) {
    ROBOT_STATE.currentIndex = 0;
    if (!ROBOT_STATE.bubbleVisible || ROBOT_STATE.lastState !== "ok") {
      ROBOT_STATE.userRead = false;
      robotUpdateAvatar("ok");
      robotShowBubble(true);
      robotTypewrite("Sem anomalias\ndetectadas \u2713", () => robotScheduleDismiss());
    }
    ROBOT_STATE.lastState = "ok";
    return;
  }

  ROBOT_STATE.lastState = state;
  robotShowBubble(true);
  if (ROBOT_STATE.reportOpen) robotRenderReport(issues);

  // Only re-type and update avatar when issues actually changed
  if (issuesChanged) {
    ROBOT_STATE.userRead = false;
    ROBOT_STATE.currentIndex = 0;
    robotUpdateAvatar(state);
    robotStartCycle();
  }
}

function _robotApplyMutedState() {
  robotShowBubble(false);
  robotUpdateBadge(0);
  const container = document.getElementById("robotAssistant");
  if (container) container.classList.add("robot-muted-peek");
  const img = document.getElementById("robotImg");
  if (img) {
    const peek = new Image();
    peek.onload = () => { img.src = "img/roboaiotipeek.png"; };
    peek.onerror = () => {}; // imagem não existe ainda → mantém atual
    peek.src = "img/roboaiotipeek.png";
  }
  const panel = document.getElementById("robotReport");
  if (panel) panel.classList.add("hidden");
  ROBOT_STATE.reportOpen = false;
}

function _robotRemoveMutedState() {
  const container = document.getElementById("robotAssistant");
  if (container) container.classList.remove("robot-muted-peek");
  const img = document.getElementById("robotImg");
  if (img && img.src.includes("roboaiotipeek")) img.src = "img/roboaiotiidle.png";
}

function _robotRenderPrefsFooter() {
  const panel = document.getElementById("robotReport");
  if (!panel) return;
  let footer = panel.querySelector(".robot-prefs-footer");
  if (!footer) {
    footer = document.createElement("div");
    footer.className = "robot-prefs-footer";
    panel.appendChild(footer);
  }
  const prefs    = _robotGetNotifPrefs();
  const isMuted  = prefs.muted === true;
  const disabled = Array.isArray(prefs.disabled_types) ? prefs.disabled_types : [];

  footer.innerHTML = `
    <button type="button" class="robot-prefs-collapse" id="robotPrefsCollapse">
      <i class="fa-solid fa-gear"></i> Prefer\u00eancias de notifica\u00e7\u00e3o
      <i class="fa-solid fa-chevron-down robot-prefs-chevron"></i>
    </button>
    <div class="robot-prefs-body" id="robotPrefsBody">
      <label class="robot-prefs-toggle">
        <input type="checkbox" id="robotPrefMuteAll" ${isMuted ? "checked" : ""}>
        <span>Silenciar todas as notifica\u00e7\u00f5es</span>
      </label>
      <div class="robot-prefs-types ${isMuted ? "robot-prefs-types--disabled" : ""}">
        ${Object.entries(ROBOT_CATEGORY_META).map(([type, m]) => {
          const ico = ROBOT_CATEGORY_ICONS[type] || "fa-list";
          return `<label class="robot-prefs-toggle robot-prefs-toggle--sub">
            <input type="checkbox" class="robot-pref-type-cb" data-type="${type}"
                   ${disabled.includes(type) ? "" : "checked"} ${isMuted ? "disabled" : ""}>
            <span><i class="fa-solid ${ico}"></i> ${m.label}</span>
          </label>`;
        }).join("")}
      </div>
    </div>`;

  const collapseBtn = footer.querySelector("#robotPrefsCollapse");
  collapseBtn?.addEventListener("click", () => footer.classList.toggle("is-open"));

  footer.querySelector("#robotPrefMuteAll")?.addEventListener("change", (e) => {
    const p = _robotGetNotifPrefs();
    p.muted = e.target.checked;
    _robotSaveNotifPrefs(p);
    _robotRenderPrefsFooter();
    footer.classList.add("is-open");
    if (p.muted) { _robotApplyMutedState(); }
    else { _robotRemoveMutedState(); robotRefresh(); }
  });

  footer.querySelectorAll(".robot-pref-type-cb").forEach(cb => {
    cb.addEventListener("change", () => {
      const p = _robotGetNotifPrefs();
      let dis = Array.isArray(p.disabled_types) ? p.disabled_types : [];
      const type = cb.dataset.type;
      if (cb.checked) dis = dis.filter(t => t !== type);
      else if (!dis.includes(type)) dis.push(type);
      p.disabled_types = dis;
      _robotSaveNotifPrefs(p);
      robotRefresh();
    });
  });
}

function wireRobotAssistant() {
  // Sincronizar preferências salvas no backend
  (async () => {
    try {
      const user = JSON.parse(localStorage.getItem("user") || "{}");
      if (!user.username) return;
      const r = await apiFetch("/users/notif-prefs", {
        headers: { "X-Username": user.username }
      });
      if (r.ok) {
        const d = await r.json();
        if (d && d.prefs && typeof d.prefs === "object") {
          localStorage.setItem(ROBOT_NOTIF_PREFS_KEY, JSON.stringify(d.prefs));
        }
      }
    } catch (_) {}
  })();

  const avatar    = document.getElementById("robotAvatar");
  const expandBtn = document.getElementById("robotBubbleExpand");
  const closeBtn  = document.getElementById("robotReportClose");
  const bubble    = document.getElementById("robotBubble");

  if (avatar) avatar.addEventListener("click", () => robotToggleReport());
  if (expandBtn) expandBtn.addEventListener("click", e => { e.stopPropagation(); robotToggleReport(true); });
  if (closeBtn) closeBtn.addEventListener("click", () => robotToggleReport(false));
  if (bubble) bubble.addEventListener("click", () => {
    if (ROBOT_STATE.issues.length > 1) robotToggleReport(true);
    else if (ROBOT_STATE.issues.length === 1) {
      const url = robotBuildDeviceUrl(ROBOT_STATE.issues[0]);
      if (url) window.location.href = url;
    } else {
      // No issues — dismiss bubble on click
      robotDismissBubble();
    }
  });
  document.addEventListener("click", e => {
    const el = document.getElementById("robotAssistant");
    if (ROBOT_STATE.reportOpen && el && !el.contains(e.target)) robotToggleReport(false);
  });
}
