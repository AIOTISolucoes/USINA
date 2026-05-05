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
    headers
  });
}

function normalizeApiPayload(data) {
  if (data && Object.prototype.hasOwnProperty.call(data, "body")) {
    return typeof data.body === "string" ? JSON.parse(data.body) : data.body;
  }
  return data;
}

function escapeHtml(value) {
  return String(value ?? "").replace(/[&<>"']/g, char => ({
    "&": "&amp;",
    "<": "&lt;",
    ">": "&gt;",
    '"': "&quot;",
    "'": "&#39;"
  }[char]));
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

  selectedDataKind: "all", // all | analog | discrete
  selectedSource: "all", // all | historico | consolidado
  selectedContext: "all", // all | PLANT | inverter | relay | meter etc
  searchText: "",

  availableTags: [],
  selectedTags: [],

  selectionId: null,

  aggregationMode: "historico", // historico | consolidado
  aggregationType: "avg", // avg | integral | median | max | mode | propagation | sum | none
  consolidationPeriod: "5min", // 5min | daily | weekly | monthly | yearly | hdaily etc

  chartData: null,
  forceHeroState: false,
  catalogOpen: false,
  catalogConfirmed: false
};

let DATASTUDIO_CHART = null;

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
  const parsed = normalizeApiPayload(data);
  return Array.isArray(parsed?.items) ? parsed.items : (Array.isArray(parsed) ? parsed : []);
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

  validPlants.forEach(p => {
    totalActivePower += Number(p?.active_power_kw ?? 0) || 0;
    totalRatedPower += Number(p?.rated_power_kw ?? p?.rated_power_kwp ?? 0) || 0;
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
  const elPsfPercent = document.getElementById("psfCapacityPct");

  if (elPsfActive) elPsfActive.textContent = totalActivePower.toFixed(1) + " kW";
  if (elPsfRated) elPsfRated.textContent = totalRatedPower.toFixed(1) + " kWp";
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
  const statusNum = Number(plant?.plant_status);
  const hasAnyData = activePower > 0 || energyToday > 0 || irradiance > 0;

  if (activePower > 0) {
    return { priority: 0, kind: "generating", activePower, energyToday, irradiance, isOffline: false };
  }

  if (statusNum === 28 || !hasAnyData) {
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
    if (plantState.isOffline) tr.classList.add("portfolio-row--offline");

    const alarmSeverity =
      normalizeAlarmSeverity(lastAlarmSeverityByPlant.get(plantId)) ||
      normalizeAlarmSeverity(lastAlarmSeverityByPlant.get(plantName)) ||
      null;
    const plantIconClass = alarmSeverity
      ? `plant-icon plant-icon--${alarmSeverity}`
      : (plantState.kind === "standby" ? "plant-icon plant-icon--standby" : "plant-icon plant-icon--ok");

    tr.innerHTML = `
      <td>
        <button class="plant-cell-btn" title="Abrir usina ${valueOrDash(plantName)}">
          <span class="plant-cell">
            <span class="${plantIconClass}" title="${alarmSeverity || "ok"}">
              <i class="fa-solid fa-seedling"></i>
            </span>
            <span class="plant-name-text">${valueOrDash(plantName)}</span>
          </span>
        </button>
      </td>
      <td class="metric-neutral">${Number(plant.rated_power_kw ?? 0).toFixed(1)} kWp</td>
      <td class="metric-active${Number(plant.active_power_kw ?? 0) === 0 ? ' metric-zero' : ''}">${Number(plant.active_power_kw ?? 0).toFixed(1)} kW</td>
      <td class="metric-active">${Number(plant.energy_today_kwh ?? 0).toFixed(1)} kWh</td>
      <td>${plant.irradiance_wm2 != null ? Number(plant.irradiance_wm2).toFixed(0) + " W/m²" : "—"}</td>
      <td>${plant.inverter_availability_pct != null ? Number(plant.inverter_availability_pct).toFixed(1) + "%" : "—"}</td>
      <td>${plant.relay_availability_pct != null ? Number(plant.relay_availability_pct).toFixed(1) + "%" : "—"}</td>
      <td>${plant.pr_daily_pct != null ? Number(plant.pr_daily_pct).toFixed(1) + "%" : "—"}</td>
      <td>${plant.pr_accumulated_pct != null ? Number(plant.pr_accumulated_pct).toFixed(1) + "%" : "—"}</td>
      <td style="text-align:center; white-space:nowrap;">
        <button class="plant-action-btn" title="Ações da usina" aria-label="Ações da usina" aria-haspopup="menu" aria-expanded="false" data-plant-id="${plantId}" style="margin-right:6px;">
          <svg viewBox="0 0 18 18" fill="none" xmlns="http://www.w3.org/2000/svg" aria-hidden="true">
            <path d="M10.5 3.5 L2.5 11.5 L2 15.5 L6 15 L14 7 Z" stroke="currentColor" stroke-width="1.3" stroke-linejoin="round" stroke-linecap="round" fill="currentColor" fill-opacity="0.12"/>
            <path d="M8.5 5.5 L12 9" stroke="currentColor" stroke-width="1.3" stroke-linecap="round"/>
            <path d="M13 2.5 L15 4.5 L14 7 L10.5 3.5 Z" stroke="currentColor" stroke-width="1.3" stroke-linejoin="round" fill="currentColor" fill-opacity="0.3"/>
          </svg>
        </button>
        <button class="plant-link-btn" title="Abrir usina" data-plant-id="${plantId}">
          <i class="fa-solid fa-arrow-up-right-from-square"></i>
        </button>
      </td>
    `;

    tr.querySelector(".plant-action-btn").addEventListener("click", (e) => {
      e.stopPropagation();
      openPortfolioPlantActionMenu(e, plantId, plantName);
    });

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
  if (!tagNorm) return false;

  const tagNum = tagNorm.match(/\d+/)?.[0] || null;
  const selectedNum = selectedNorm.match(/\d+/)?.[0] || null;
  if (tagNum && selectedNum && tagNum === selectedNum) return true;

  return tagNorm.includes(selectedNorm) || selectedNorm.includes(tagNorm);
}

function getDataStudioUIElements() {
  return {
    startDateInput: document.getElementById("dsStartDateInput"),
    endDateInput: document.getElementById("dsEndDateInput"),
    plantSelect: document.getElementById("dsPlantSelect"),
    openTagsBtn: document.getElementById("dsOpenTagsBtn"),
    backToHeroBtn: document.getElementById("dsBackToHeroBtn"),
    exportBtn: document.getElementById("dsExportBtn"),
    zoomInBtn: document.getElementById("dsZoomInBtn"),
    zoomOutBtn: document.getElementById("dsZoomOutBtn"),
    zoomResetBtn: document.getElementById("dsZoomResetBtn"),

    catalogSection: document.getElementById("dsCatalogSection"),
    contextInfo: document.getElementById("dsContextInfo"),

    dataKindSelect: document.getElementById("dsDataKindSelect"),
    sourceSelect: document.getElementById("dsSourceSelect"),
    contextSelect: document.getElementById("dsContextSelect"),
    searchInput: document.getElementById("dsSearchInput"),
    tagsApplyBtn: document.getElementById("dsTagsApplyBtn"),
    tagsClearBtn: document.getElementById("dsTagsClearBtn"),
    confirmSelectionBtn: document.getElementById("dsConfirmSelectionBtn"),
    tagsTableBody: document.getElementById("dsTagsTbody"),
    selectedCount: document.getElementById("dsSelectedCount"),
    selectedTagsList: document.getElementById("dsSelectedTagsList"),
    emptyState: document.getElementById("dsEmptyState"),
    workspace: document.getElementById("dsWorkspace"),

    bulkPanel: document.getElementById("dsBulkPanel"),
    modeSelect: document.getElementById("dsModeSelect"),
    aggregationSelect: document.getElementById("dsAggregationSelect"),
    consolidationSelect: document.getElementById("dsConsolidationSelect"),
    saveSelectionBtn: document.getElementById("dsSaveSelectionBtn"),
    loadSeriesBtn: document.getElementById("dsLoadSeriesBtn"),

    chartCanvas: document.getElementById("dsChart")
  };
}


function selectedTagKey(tagOrPath) {
  if (typeof tagOrPath === "string") return `path:${dsSafeTrim(tagOrPath)}`;
  const id = tagOrPath?.id ?? tagOrPath?.tag_id;
  if (id !== undefined && id !== null && String(id) !== "") return `id:${id}`;
  return `path:${dsSafeTrim(tagOrPath?.pathname)}`;
}

function renderSelectedTagsList() {
  const { selectedTagsList } = getDataStudioUIElements();
  if (!selectedTagsList) return;

  selectedTagsList.innerHTML = "";
  const tags = Array.isArray(DATASTUDIO_STATE.selectedTags) ? DATASTUDIO_STATE.selectedTags : [];

  if (!tags.length) {
    selectedTagsList.classList.add("hidden");
    return;
  }

  selectedTagsList.classList.remove("hidden");
  tags.forEach((tag) => {
    const chip = document.createElement("div");
    chip.className = "ds-selected-tag-chip";

    const CHIP_COLORS = [
      '#1d9e75','#378add','#d4537e','#ef9f27',
      '#7f77dd','#5dcaa5','#e24b4a','#639922',
      '#ba7517','#185fa5','#993556','#0f6e56'
    ];
    const colorIdx = DATASTUDIO_STATE.selectedTags.indexOf(tag) % CHIP_COLORS.length;
    const chipColor = CHIP_COLORS[Math.max(0, colorIdx)];
    chip.style.setProperty('--ds-chip-color', chipColor);
    const label = `${valueOrDash(tag?.context)} • ${valueOrDash(tag?.point_name || tag?.description || tag?.pathname)}`;
    chip.innerHTML = `
      <span class="ds-selected-tag-chip__dot"></span>
      <span class="ds-selected-tag-chip__text" title="${label}">${label}</span>
      <button type="button" class="ds-selected-tag-chip__remove" aria-label="Remover medida">×</button>
    `;

    chip.querySelector(".ds-selected-tag-chip__remove")?.addEventListener("click", () => {
      removeSelectedTag(tag);
      renderDataStudioTagsTable(DATASTUDIO_STATE.availableTags);
      updateSelectedTagsCounter();
    });

    selectedTagsList.appendChild(chip);
  });
}

function populateDataStudioContextSelect(tags) {
  const { contextSelect } = getDataStudioUIElements();
  if (!contextSelect) return;

  const prev = dsSafeTrim(contextSelect.value || DATASTUDIO_STATE.selectedContext) || "all";
  const contexts = Array.from(new Set((Array.isArray(tags) ? tags : [])
    .map((t) => dsSafeTrim(t?.context))
    .filter(Boolean))).sort((a, b) => a.localeCompare(b, "pt-BR"));

  contextSelect.innerHTML = "";
  const allOpt = document.createElement("option");
  allOpt.value = "all";
  allOpt.textContent = "Todos contextos";
  contextSelect.appendChild(allOpt);

  contexts.forEach((ctx) => {
    const opt = document.createElement("option");
    opt.value = ctx;
    opt.textContent = ctx;
    contextSelect.appendChild(opt);
  });

  if (contexts.includes(prev)) contextSelect.value = prev;
  else contextSelect.value = "all";

  DATASTUDIO_STATE.selectedContext = contextSelect.value || "all";
}


function updateDataStudioStageUI() {
  const { emptyState, workspace, catalogSection, openTagsBtn } = getDataStudioUIElements();
  if (!emptyState || !workspace) return;

  const hasSelection = Array.isArray(DATASTUDIO_STATE.selectedTags) && DATASTUDIO_STATE.selectedTags.length > 0;
  const showWorkspace = hasSelection && DATASTUDIO_STATE.catalogConfirmed && !DATASTUDIO_STATE.forceHeroState;

  if (showWorkspace) {
    emptyState.classList.add("hidden");
    workspace.classList.remove("hidden");
  } else {
    emptyState.classList.remove("hidden");
    workspace.classList.add("hidden");
  }

  if (catalogSection) {
    catalogSection.classList.toggle("is-open", Boolean(DATASTUDIO_STATE.catalogOpen));
  }

  if (openTagsBtn && !DATASTUDIO_STATE.loadingTags) {
    openTagsBtn.textContent = DATASTUDIO_STATE.catalogOpen ? "−" : "+";
  }

  renderSelectedTagsList();
}

function isTagSelected(tagOrPath) {
  const key = selectedTagKey(tagOrPath);
  if (!key || key.endsWith(":")) return false;
  return DATASTUDIO_STATE.selectedTags.some((t) => selectedTagKey(t) === key);
}

function addSelectedTag(tag) {
  if (!tag || !dsSafeTrim(tag.pathname)) return false;
  if (isTagSelected(tag)) return true;
  DATASTUDIO_STATE.selectedTags.push(tag);
  DATASTUDIO_STATE.forceHeroState = false;
  updateDataStudioStageUI();
  return true;
}

function removeSelectedTag(tagOrPath) {
  const key = selectedTagKey(tagOrPath);
  DATASTUDIO_STATE.selectedTags = DATASTUDIO_STATE.selectedTags.filter(
    (t) => selectedTagKey(t) !== key
  );
  updateDataStudioStageUI();
}

function updateSelectedTagsCounter() {
  const count = String(DATASTUDIO_STATE.selectedTags.length);
  ["dsSelectedCount", "dsSelectedCountTop", "dsSelectedCountBottom"].forEach((id) => {
    const el = document.getElementById(id);
    if (el) el.textContent = count;
  });
}

function renderDataStudioTagsTable(tags) {
  const { tagsTableBody } = getDataStudioUIElements();
  if (!tagsTableBody) return;

  const rows = Array.isArray(tags) ? tags : [];
  tagsTableBody.innerHTML = "";

  if (!rows.length) {
    const tr = document.createElement("tr");
    tr.innerHTML = '<td colspan="8" style="text-align:center;opacity:.8;">Nenhuma medida encontrada</td>';
    tagsTableBody.appendChild(tr);
    updateSelectedTagsCounter();
    return;
  }

  rows.forEach((tag) => {
    const pathname = dsSafeTrim(tag?.pathname || tag?.path_name || tag?.tag);
    if (!pathname) return;

    const tr = document.createElement("tr");
    tr.classList.add("ds-table-row-clickable");
    const checked = isTagSelected(tag) ? "checked" : "";

    const isAlarm = (tag?.data_kind === 'discrete') || (tag?.description || '').startsWith('⚠');
    const isPlant = (tag?.context || '').toUpperCase().startsWith('PLANT') ||
                    (tag?.pathname || '').startsWith('PLANT.');
    const isWeather = (tag?.device_type || '').toLowerCase().includes('weather') ||
                      (tag?.pathname || '').toLowerCase().includes('weather');
    const isMeter   = (tag?.device_type || '').toLowerCase().includes('meter') ||
                      (tag?.context || '').toLowerCase().includes('medidor') ||
                      (tag?.context || '').toLowerCase().includes('meter');

    const svgInverter = `<svg width="12" height="12" viewBox="0 0 12 12" fill="none"><rect x="1" y="3" width="10" height="7" rx="1.5" stroke="rgba(57,229,140,.8)" stroke-width="1.2"/><path d="M4 3V2a2 2 0 014 0v1" stroke="rgba(57,229,140,.8)" stroke-width="1.2"/><line x1="4" y1="6.5" x2="8" y2="6.5" stroke="rgba(57,229,140,.6)" stroke-width="1"/></svg>`;
    const svgPlant   = `<svg width="12" height="12" viewBox="0 0 12 12" fill="none"><path d="M6 10V5M3 7l3-3 3 3" stroke="rgba(55,138,221,.9)" stroke-width="1.2" stroke-linecap="round"/><rect x="1" y="1" width="4" height="3" rx="0.8" stroke="rgba(55,138,221,.7)" stroke-width="1"/><rect x="7" y="1" width="4" height="3" rx="0.8" stroke="rgba(55,138,221,.7)" stroke-width="1"/></svg>`;
    const svgWeather = `<svg width="12" height="12" viewBox="0 0 12 12" fill="none"><circle cx="6" cy="5" r="2.5" stroke="rgba(239,159,39,.8)" stroke-width="1.2"/><line x1="6" y1="1" x2="6" y2="2" stroke="rgba(239,159,39,.7)" stroke-width="1" stroke-linecap="round"/><line x1="6" y1="8" x2="6" y2="9" stroke="rgba(239,159,39,.7)" stroke-width="1" stroke-linecap="round"/><line x1="2" y1="5" x2="3" y2="5" stroke="rgba(239,159,39,.7)" stroke-width="1" stroke-linecap="round"/><line x1="9" y1="5" x2="10" y2="5" stroke="rgba(239,159,39,.7)" stroke-width="1" stroke-linecap="round"/></svg>`;
    const svgMeter   = `<svg width="12" height="12" viewBox="0 0 12 12" fill="none"><rect x="1" y="2" width="10" height="8" rx="1.5" stroke="rgba(212,83,126,.8)" stroke-width="1.2"/><path d="M3.5 7.5 C3.5 5.5 8.5 5.5 8.5 7.5" stroke="rgba(212,83,126,.7)" stroke-width="1" fill="none"/><line x1="6" y1="7.5" x2="5" y2="5.5" stroke="rgba(212,83,126,.9)" stroke-width="1" stroke-linecap="round"/></svg>`;
    const svgAlarm   = `<svg width="12" height="12" viewBox="0 0 12 12" fill="none"><path d="M6 1.5L1.5 9.5h9L6 1.5z" stroke="rgba(239,159,39,.9)" stroke-width="1.2" stroke-linejoin="round"/><line x1="6" y1="5" x2="6" y2="7.5" stroke="rgba(239,159,39,.9)" stroke-width="1" stroke-linecap="round"/><circle cx="6" cy="9" r=".6" fill="rgba(239,159,39,.9)"/></svg>`;

    let iconSvg = svgInverter;
    let iconClass = 'analog';
    if (isAlarm)        { iconSvg = svgAlarm;   iconClass = 'discrete'; }
    else if (isPlant)   { iconSvg = svgPlant;   iconClass = 'plant'; }
    else if (isWeather) { iconSvg = svgWeather; iconClass = 'analog'; }
    else if (isMeter)   { iconSvg = svgMeter;   iconClass = 'analog'; }

    const unitHtml = tag?.unit ? `<span class="ds-unit-badge">${tag.unit}</span>` : '<span style="opacity:.35">—</span>';
    const descHtml = valueOrDash(tag?.description);

    tr.innerHTML = `
      <td><input type="checkbox" data-ds-pathname="${pathname.replaceAll('"','&quot;')}" ${checked}></td>
      <td>
        <div class="ds-tag-context-cell">
          <span class="ds-tag-icon ds-tag-icon--${iconClass}">${iconSvg}</span>
          ${valueOrDash(tag?.context)}
        </div>
      </td>
      <td style="white-space:normal;line-height:1.35;font-size:11px;">${descHtml}</td>
      <td style="display:none;">${valueOrDash(tag?.source)}</td>
      <td style="display:none;">${valueOrDash(tag?.data_kind)}</td>
      <td>${unitHtml}</td>
      <td style="display:none;">${valueOrDash(tag?.power_plant_id)}</td>
      <td class="ds-pathname-cell" title="${pathname.replaceAll('"','&quot;')}" style="display:none;">${valueOrDash(pathname)}</td>
    `;

    const checkbox = tr.querySelector("input[type='checkbox']");
    const syncRowSelectionState = () => {
      tr.classList.toggle("is-selected", Boolean(checkbox?.checked));
    };

    const applySelection = (checkedState) => {
      if (checkedState) {
        const ok = addSelectedTag({
          id: tag?.id ?? null,
          tag_id: tag?.id ?? null,
          device_type: tag?.device_type ?? null,
          device_id: tag?.device_id ?? null,
          point_name: tag?.point_name ?? null,
          power_plant_id: tag?.power_plant_id ?? null,
          context: dsSafeTrim(tag?.context) || "PLANT",
          pathname,
          source: dsSafeTrim(tag?.source) || "historico",
          data_kind: dsSafeTrim(tag?.data_kind) || "analog",
          unit: tag?.unit ?? null,
          description: tag?.description ?? null
        });
        if (!ok && checkbox) checkbox.checked = false;
      } else {
        removeSelectedTag(tag);
      }
      updateSelectedTagsCounter();
      syncRowSelectionState();
    };

    checkbox?.addEventListener("change", (ev) => {
      applySelection(Boolean(ev.target.checked));
    });

    tr.addEventListener("click", (ev) => {
      if (ev.target?.closest("input, button, a")) return;
      if (!checkbox) return;
      checkbox.checked = !checkbox.checked;
      applySelection(Boolean(checkbox.checked));
    });

    syncRowSelectionState();
    tagsTableBody.appendChild(tr);
  });

  updateSelectedTagsCounter();
  updateDataStudioStageUI();
}


function setDataStudioLoadingTags(isLoading) {
  DATASTUDIO_STATE.loadingTags = Boolean(isLoading);
  const { openTagsBtn, tagsApplyBtn, tagsClearBtn } = getDataStudioUIElements();
  if (openTagsBtn) {
    openTagsBtn.disabled = DATASTUDIO_STATE.loadingTags;
    openTagsBtn.textContent = DATASTUDIO_STATE.loadingTags
      ? "Carregando..."
      : (DATASTUDIO_STATE.catalogOpen ? "−" : "+");
  }
  if (tagsApplyBtn) tagsApplyBtn.disabled = DATASTUDIO_STATE.loadingTags;
  if (tagsClearBtn) tagsClearBtn.disabled = DATASTUDIO_STATE.loadingTags;
}

function setDataStudioSavingSelection(isLoading) {
  DATASTUDIO_STATE.savingSelection = Boolean(isLoading);
  const { saveSelectionBtn } = getDataStudioUIElements();
  if (!saveSelectionBtn) return;
  saveSelectionBtn.disabled = DATASTUDIO_STATE.savingSelection;
  saveSelectionBtn.textContent = DATASTUDIO_STATE.savingSelection ? "Salvando..." : "Salvar seleção";
}

function setDataStudioLoadingSeries(isLoading) {
  DATASTUDIO_STATE.loadingSeries = Boolean(isLoading);
  const { loadSeriesBtn } = getDataStudioUIElements();
  if (!loadSeriesBtn) return;
  loadSeriesBtn.disabled = DATASTUDIO_STATE.loadingSeries;
  loadSeriesBtn.textContent = DATASTUDIO_STATE.loadingSeries ? "Carregando..." : "Carregar séries";
}

function getDataStudioMainFilters() {
  const { startDateInput, endDateInput, plantSelect } = getDataStudioUIElements();

  const rawStart = dsSafeTrim(startDateInput?.value || DATASTUDIO_STATE.startDate);
  const rawEnd = dsSafeTrim(endDateInput?.value || DATASTUDIO_STATE.endDate);
  const plantRaw = dsSafeTrim(plantSelect?.value || DATASTUDIO_STATE.selectedPlantId);

  const start_ts_raw = dsIsoStartOfDay(rawStart);
  const end_ts_raw = dsIsoEndOfDay(rawEnd);
  const { startISO, endISO } = dsClampRange(start_ts_raw, end_ts_raw);

  return {
    power_plant_id: plantRaw ? Number(plantRaw) : null,
    start_ts: startISO,
    end_ts: endISO
  };
}

function buildDataStudioSelectionPayload() {
  const filters = getDataStudioMainFilters();

  if (!filters.power_plant_id) {
    throw new Error("Selecione uma usina para continuar.");
  }
  if (!filters.start_ts || !filters.end_ts) {
    throw new Error("Preencha um período válido (data inicial e final).");
  }
  if (!Array.isArray(DATASTUDIO_STATE.selectedTags) || !DATASTUDIO_STATE.selectedTags.length) {
    throw new Error("Selecione ao menos uma medida.");
  }

  const allowedAgg = new Set(["none", "avg", "integral", "median", "max", "sum"]);
  const allowedPeriod = new Set(["5min", "daily", "weekly", "monthly", "yearly", "hdaily", "hweekly", "hmonthly", "hyearly"]);

  const aggregationType = allowedAgg.has(DATASTUDIO_STATE.aggregationType)
    ? DATASTUDIO_STATE.aggregationType
    : "avg";
  const consolidationPeriod = allowedPeriod.has(DATASTUDIO_STATE.consolidationPeriod)
    ? DATASTUDIO_STATE.consolidationPeriod
    : "5min";

  const invalidTag = DATASTUDIO_STATE.selectedTags.find(
    (t) => Number(t?.power_plant_id) !== Number(filters.power_plant_id)
  );

  if (invalidTag) {
    throw new Error("Há medidas selecionadas de outra usina. Limpe a seleção e selecione novamente.");
  }

  const items = DATASTUDIO_STATE.selectedTags.map((t, idx) => ({
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
    power_plant_id: filters.power_plant_id,
    start_ts: filters.start_ts,
    end_ts: filters.end_ts,
    timezone: "America/Fortaleza",
    historico_aggregation_default:
      DATASTUDIO_STATE.aggregationMode === "historico" ? aggregationType : "avg",
    consolidado_period_default:
      DATASTUDIO_STATE.aggregationMode === "consolidado" ? consolidationPeriod : "5min",
    items
  };
}

function updateDataStudioContextInfo() {
  const { contextInfo } = getDataStudioUIElements();
  if (!contextInfo) return;

  const selectedContext = dsSafeTrim(DATASTUDIO_STATE.selectedContext) || "all";
  if (selectedContext && selectedContext !== "all") {
    contextInfo.textContent = `Exibindo medidas de: ${selectedContext}`;
    contextInfo.classList.remove("hidden");
  } else {
    contextInfo.textContent = "Exibindo medidas de: todos contextos";
    contextInfo.classList.remove("hidden");
  }
}

function updateDataStudioExportButton() {
  const { exportBtn } = getDataStudioUIElements();
  if (!exportBtn) return;
  exportBtn.disabled = !DATASTUDIO_STATE.selectionId;
}

async function exportDataStudioSelection() {
  if (!DATASTUDIO_STATE.selectionId) {
    window.alert("Salve uma seleção antes de exportar.");
    return;
  }

  const { exportBtn } = getDataStudioUIElements();
  const oldHtml = exportBtn ? exportBtn.innerHTML : "";

  try {
    if (exportBtn) {
      exportBtn.disabled = true;
      exportBtn.innerHTML = '<i class="fa-solid fa-spinner fa-spin"></i>';
    }

    const url = `${API_BASE}/datastudio/export?selection_id=${encodeURIComponent(DATASTUDIO_STATE.selectionId)}`;

    const user = JSON.parse(localStorage.getItem("user") || "{}");
    const headers = {};
    if (user.customer_id) headers["X-Customer-Id"] = user.customer_id;
    if (user.is_superuser === true) headers["X-Is-Superuser"] = "true";

    const res = await fetch(url, { headers });

    if (!res.ok) {
      const txt = await res.text().catch(() => "");
      throw new Error(`Falha ao exportar (${res.status}) ${txt}`);
    }

    const blob = await res.blob();

    let filename = `datastudio_export_${DATASTUDIO_STATE.selectionId}.csv`;
    const contentDisposition = res.headers.get("Content-Disposition");
    const match = contentDisposition && contentDisposition.match(/filename="([^"]+)"/i);
    if (match && match[1]) {
      filename = match[1];
    }

    const blobUrl = window.URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = blobUrl;
    a.download = filename;
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
  if (!DATASTUDIO_CHART || typeof DATASTUDIO_CHART.zoom !== "function") return;

  try {
    DATASTUDIO_CHART.zoom({ x: factor, y: factor });
  } catch (err) {
    console.warn("[DataStudio] erro ao aplicar zoom:", err);
  }
}

function resetDataStudioChartZoom() {
  if (!DATASTUDIO_CHART || typeof DATASTUDIO_CHART.resetZoom !== "function") return;

  try {
    DATASTUDIO_CHART.resetZoom();
  } catch (err) {
    console.warn("[DataStudio] erro ao resetar zoom:", err);
  }
}

function isMobileViewport() {
  return window.innerWidth <= 768;
}

function clearDataStudioChartActiveState() {
  if (!DATASTUDIO_CHART) return;

  try {
    DATASTUDIO_CHART.setActiveElements([]);
    if (DATASTUDIO_CHART.tooltip && typeof DATASTUDIO_CHART.tooltip.setActiveElements === "function") {
      DATASTUDIO_CHART.tooltip.setActiveElements([], { x: 0, y: 0 });
    }
    DATASTUDIO_CHART.update("none");
  } catch (err) {
    console.warn("[DataStudio] erro ao limpar seleção do gráfico:", err);
  }
}

function wireDataStudioChartOutsideTapOnce() {
  if (window.__dsOutsideTapWired) return;
  window.__dsOutsideTapWired = true;

  const clearIfOutside = (event) => {
    if (!DATASTUDIO_CHART) return;
    const { chartCanvas } = getDataStudioUIElements();
    if (!chartCanvas) return;
    if (!chartCanvas.contains(event.target)) {
      clearDataStudioChartActiveState();
    }
  };

  document.addEventListener("touchstart", clearIfOutside, { passive: true });
  document.addEventListener("click", clearIfOutside);
}

function openDataStudioCatalogInline({ resetFilters = false } = {}) {
  const { plantSelect, startDateInput, endDateInput } = getDataStudioUIElements();

  DATASTUDIO_STATE.startDate = dsSafeTrim(startDateInput?.value);
  DATASTUDIO_STATE.endDate = dsSafeTrim(endDateInput?.value);
  DATASTUDIO_STATE.selectedPlantId = dsSafeTrim(plantSelect?.value) || null;

  const filters = getDataStudioMainFilters();
  if (!filters.power_plant_id || !filters.start_ts || !filters.end_ts) {
    window.alert("Selecione usina e período válido antes de abrir as medidas.");
    return;
  }

  if (resetFilters) {
    DATASTUDIO_STATE.selectedDataKind = "all";
    DATASTUDIO_STATE.selectedSource = "all";
    DATASTUDIO_STATE.selectedContext = "all";
    DATASTUDIO_STATE.searchText = "";

    const { dataKindSelect, sourceSelect, contextSelect, searchInput } = getDataStudioUIElements();
    if (dataKindSelect) dataKindSelect.value = "all";
    if (sourceSelect) sourceSelect.value = "all";
    if (contextSelect) contextSelect.value = "all";
    if (searchInput) searchInput.value = "";
  }

  DATASTUDIO_STATE.catalogOpen = true;
  DATASTUDIO_STATE.forceHeroState = false;
  updateDataStudioStageUI();
  updateDataStudioContextInfo();
  fetchDataStudioTags();
}

function toggleDataStudioCatalogInline() {
  if (!DATASTUDIO_STATE.catalogOpen) {
    openDataStudioCatalogInline({ resetFilters: !DATASTUDIO_STATE.availableTags.length });
    return;
  }
  DATASTUDIO_STATE.catalogOpen = false;
  updateDataStudioStageUI();
}

function confirmDataStudioCatalogSelection() {
  if (!Array.isArray(DATASTUDIO_STATE.selectedTags) || !DATASTUDIO_STATE.selectedTags.length) {
    window.alert("Selecione ao menos uma medida antes de confirmar.");
    return;
  }

  DATASTUDIO_STATE.catalogConfirmed = true;
  DATASTUDIO_STATE.forceHeroState = false;
  DATASTUDIO_STATE.catalogOpen = false;
  updateDataStudioStageUI();

  setTimeout(() => {
    const { bulkPanel } = getDataStudioUIElements();
    bulkPanel?.scrollIntoView({ behavior: "smooth", block: "start" });
  }, 120);
}

async function fetchDataStudioTags() {
  const { plantSelect, dataKindSelect, sourceSelect, contextSelect, searchInput } = getDataStudioUIElements();

  const plantId = dsSafeTrim(plantSelect?.value || DATASTUDIO_STATE.selectedPlantId);
  const dataKind = dsSafeTrim(dataKindSelect?.value || DATASTUDIO_STATE.selectedDataKind);
  const source = dsSafeTrim(sourceSelect?.value || DATASTUDIO_STATE.selectedSource);
  const context = dsSafeTrim(contextSelect?.value || DATASTUDIO_STATE.selectedContext);
  const q = dsSafeTrim(searchInput?.value || DATASTUDIO_STATE.searchText);

  DATASTUDIO_STATE.selectedPlantId = plantId || null;
  DATASTUDIO_STATE.selectedDataKind = dataKind || "all";
  DATASTUDIO_STATE.selectedSource = source || "all";
  DATASTUDIO_STATE.selectedContext = context || "all";
  DATASTUDIO_STATE.searchText = q;

  const params = new URLSearchParams();
  if (plantId) params.set("plant_id", plantId);
  if (dataKind && dataKind !== "all") params.set("data_kind", dataKind);
  if (source && source !== "all") params.set("source", source);
  if (q) params.set("q", q);
  params.set("limit", "3000");

  const normalizeTagsResponse = (parsed) => {
    if (Array.isArray(parsed)) return parsed;
    if (Array.isArray(parsed?.items)) return parsed.items;
    if (Array.isArray(parsed?.data)) return parsed.data;
    return [];
  };

  setDataStudioLoadingTags(true);

  try {
    const qs = params.toString();
    const res = await apiFetch(`/datastudio/tags${qs ? `?${qs}` : ""}`);
    if (!res.ok) throw new Error(`Falha ao buscar medidas (${res.status})`);
    const data = await res.json();
    const parsed = dsNormalizeApiBody(data);

    const allTags = normalizeTagsResponse(parsed);
    console.log("[DataStudio] TAGS RECEBIDAS:", allTags.length);

    populateDataStudioContextSelect(allTags);

    const contextAfterPopulate = dsSafeTrim(contextSelect?.value || DATASTUDIO_STATE.selectedContext || "all");
    DATASTUDIO_STATE.selectedContext = contextAfterPopulate || "all";

    const filteredTags = (contextAfterPopulate && contextAfterPopulate !== "all")
      ? allTags.filter((tag) => dsContextMatches(tag?.context, contextAfterPopulate))
      : allTags;

    DATASTUDIO_STATE.availableTags = filteredTags;
    updateDataStudioContextInfo();
    renderDataStudioTagsTable(filteredTags);
  } catch (err) {
    console.error("[DataStudio] erro ao buscar tags:", err);
    DATASTUDIO_STATE.availableTags = [];
    updateDataStudioContextInfo();
    renderDataStudioTagsTable([]);
  } finally {
    setDataStudioLoadingTags(false);
  }
}

async function saveDataStudioSelection() {
  setDataStudioSavingSelection(true);
  try {
    const payload = buildDataStudioSelectionPayload();
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
    DATASTUDIO_STATE.selectionId = selectionId;

    const { loadSeriesBtn } = getDataStudioUIElements();
    if (loadSeriesBtn) loadSeriesBtn.disabled = false;
    updateDataStudioExportButton();

    DATASTUDIO_STATE.forceHeroState = false;
    updateDataStudioStageUI();
    window.alert(selectionId ? `Seleção salva! ID ${selectionId}` : "Seleção salva com sucesso.");

    if (selectionId) {
      await fetchDataStudioSeriesBySelection();
    }
  } catch (err) {
    console.error("[DataStudio] erro ao salvar seleção:", err);
    window.alert(`Não foi possível salvar a seleção: ${err.message || err}`);
  } finally {
    setDataStudioSavingSelection(false);
  }
}

async function fetchDataStudioSeriesBySelection() {
  if (!DATASTUDIO_STATE.selectionId) {
    window.alert("Salve uma seleção antes de carregar séries.");
    return;
  }

  setDataStudioLoadingSeries(true);
  try {
    const params = new URLSearchParams({ selection_id: String(DATASTUDIO_STATE.selectionId) });
    const res = await apiFetch(`/datastudio/series?${params.toString()}`);
    if (!res.ok) throw new Error(`Falha ao carregar séries (${res.status})`);

    const data = await res.json();
    const parsed = dsNormalizeApiBody(data);
    DATASTUDIO_STATE.chartData = parsed;
    updateDataStudioStageUI();
    renderDataStudioChart(parsed);
  } catch (err) {
    console.error("[DataStudio] erro ao carregar séries:", err);
    window.alert(`Não foi possível carregar séries: ${err.message || err}`);
    renderDataStudioChart(null);
  } finally {
    setDataStudioLoadingSeries(false);
  }
}

function renderDataStudioChart(seriesPayload) {
  const { chartCanvas } = getDataStudioUIElements();
  if (!chartCanvas || typeof Chart === "undefined") return;

  if (DATASTUDIO_CHART) {
    DATASTUDIO_CHART.destroy();
    DATASTUDIO_CHART = null;
  }

  const labels = [];
  const datasets = [];

  const seriesList = Array.isArray(seriesPayload?.series)
    ? seriesPayload.series
    : (Array.isArray(seriesPayload?.items) ? seriesPayload.items : []);

  const scales = {
    x: { ticks: { color: "#9fb0bf" }, grid: { color: "rgba(255,255,255,.08)" } },
    y: { type: "linear", position: "left", ticks: { color: "#9fb0bf" }, grid: { color: "rgba(255,255,255,.08)" } }
  };
  const axisByUnit = new Map([["_default_", "y"]]);

  if (seriesList.length) {
    const palette = ["#4da3ff", "#39e58c", "#ffd84d", "#ff8a65", "#b39ddb", "#80cbc4"];
    seriesList.forEach((serie, idx) => {
      const points = Array.isArray(serie?.points)
        ? serie.points
        : (Array.isArray(serie?.data) ? serie.data : []);

      if (!labels.length) {
        points.forEach((pt) => {
          const ts = pt?.ts || pt?.timestamp || pt?.x;
          labels.push(ts ? new Date(ts).toLocaleString("pt-BR") : "");
        });
      }

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
        data: points.map((pt) => Number(pt?.value ?? pt?.y ?? null)),
        borderColor: palette[idx % palette.length],
        backgroundColor: palette[idx % palette.length],
        borderWidth: 2,
        tension: 0.25,
        pointRadius: 0,
        pointHoverRadius: 6,
        pointHitRadius: 16,
        fill: false,
        yAxisID: axisByUnit.get(unitKey) || "y"
      });
    });
  } else {
    labels.push("Sem dados");
    datasets.push({
      label: "Data Studio",
      data: [0],
      borderColor: "#4da3ff",
      backgroundColor: "#4da3ff",
      borderWidth: 2,
      tension: 0.25,
      pointRadius: 0,
      pointHoverRadius: 6,
      pointHitRadius: 16,
      fill: false,
      yAxisID: "y"
    });
  }

  const mobile = isMobileViewport();

  DATASTUDIO_CHART = new Chart(chartCanvas.getContext("2d"), {
    type: "line",
    data: { labels, datasets },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      interaction: {
        mode: mobile ? "nearest" : "index",
        intersect: false,
        axis: "x"
      },
      hover: {
        mode: mobile ? "nearest" : "index",
        intersect: false
      },
      elements: {
        point: {
          radius: 0,
          hoverRadius: mobile ? 8 : 6,
          hitRadius: mobile ? 24 : 16
        },
        line: {
          borderWidth: 2
        }
      },
      plugins: {
        legend: {
          labels: {
            color: "#dbe7ef",
            boxWidth: 12,
            usePointStyle: true,
            pointStyle: "line"
          }
        },
        tooltip: {
          enabled: true,
          mode: mobile ? "nearest" : "index",
          intersect: false,
          displayColors: true,
          backgroundColor: "rgba(6, 18, 14, 0.96)",
          borderColor: "rgba(127,208,85,.22)",
          borderWidth: 1,
          titleColor: "#dbe7ef",
          bodyColor: "#dbe7ef",
          padding: 10,
          caretSize: 6
        },
        zoom: {
          limits: {
            x: { minRange: 5 },
            y: { minRange: 1 }
          },
          pan: {
            enabled: true,
            mode: "xy"
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
            mode: "xy"
          }
        }
      },
      scales
    }
  });
  window.dsChartInstance = DATASTUDIO_CHART;
}


function populateDataStudioPlantSelect(plants) {
  const { plantSelect } = getDataStudioUIElements();
  if (!plantSelect) return;

  const list = Array.isArray(plants) ? plants : [];
  const currentValue =
  dsSafeTrim(plantSelect.value) ||
  dsSafeTrim(DATASTUDIO_STATE.selectedPlantId) ||
  "";

  const nextOptions = [
    { value: "", text: "Selecione uma usina" },
    ...list
      .map((p) => {
        const id = p.power_plant_id ?? p.plant_id ?? p.id;
        const name = p.power_plant_name ?? p.name ?? `Usina ${id}`;
        return id == null ? null : { value: String(id), text: String(name) };
      })
      .filter(Boolean)
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

  if (currentValue && nextOptions.some(o => o.value === currentValue)) {
    plantSelect.value = currentValue;
  } else if (!plantSelect.value) {
    plantSelect.value = "";
  }

  DATASTUDIO_STATE.selectedPlantId = dsSafeTrim(plantSelect.value) || null;

  console.log("[DS] plant options:", [...plantSelect.options].map(o => ({
    value: o.value,
    text: o.textContent
  })));
}

function syncDataStudioAggregationUI() {
  const { modeSelect, aggregationSelect, consolidationSelect } = getDataStudioUIElements();
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

function markDataStudioSeriesDirty() {
  DATASTUDIO_STATE.selectionId = null;
  DATASTUDIO_STATE.chartData = null;

  const { loadSeriesBtn, saveSelectionBtn } = getDataStudioUIElements();

  if (loadSeriesBtn) {
    loadSeriesBtn.disabled = true;
    loadSeriesBtn.textContent = "Carregar séries";
  }

  if (saveSelectionBtn) {
    saveSelectionBtn.disabled = false;
  }

  renderDataStudioChart(null);
  updateDataStudioExportButton();
}

function formatDateInputValue(dateObj) {
  const y = dateObj.getFullYear();
  const m = String(dateObj.getMonth() + 1).padStart(2, "0");
  const d = String(dateObj.getDate()).padStart(2, "0");
  return `${y}-${m}-${d}`;
}

function autoAdjustDataStudioDateRangeByMode() {
  const { startDateInput, endDateInput } = getDataStudioUIElements();
  if (!startDateInput || !endDateInput) return;

  const now = new Date();
  const start = new Date(now);

  const mode = dsSafeTrim(DATASTUDIO_STATE.aggregationMode || "historico");
  const period = dsSafeTrim(DATASTUDIO_STATE.consolidationPeriod || "5min");

  if (mode === "historico") {
    start.setDate(now.getDate() - 7);
  } else {
    switch (period) {
      case "5min":
        start.setDate(now.getDate() - 1);
        break;
      case "daily":
      case "hdaily":
        start.setDate(now.getDate() - 30);
        break;
      case "weekly":
      case "hweekly":
        start.setDate(now.getDate() - 90);
        break;
      case "monthly":
      case "hmonthly":
        start.setMonth(now.getMonth() - 12);
        break;
      case "yearly":
      case "hyearly":
        start.setFullYear(now.getFullYear() - 5);
        break;
      default:
        start.setDate(now.getDate() - 30);
        break;
    }
  }

  const startStr = formatDateInputValue(start);
  const endStr = formatDateInputValue(now);

  startDateInput.value = startStr;
  endDateInput.value = endStr;

  DATASTUDIO_STATE.startDate = startStr;
  DATASTUDIO_STATE.endDate = endStr;
}

function wireDataStudioOnce() {
  if (DATASTUDIO_STATE.wired) return;

  const ui = getDataStudioUIElements();
  if (!ui.openTagsBtn && !ui.startDateInput && !ui.endDateInput) return;

  DATASTUDIO_STATE.wired = true;

  const now = new Date();
  const week = new Date();
  week.setDate(now.getDate() - 7);
  const asDate = (d) => `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, "0")}-${String(d.getDate()).padStart(2, "0")}`;

  if (ui.startDateInput && !ui.startDateInput.value) ui.startDateInput.value = asDate(week);
  if (ui.endDateInput && !ui.endDateInput.value) ui.endDateInput.value = asDate(now);

  DATASTUDIO_STATE.startDate = dsSafeTrim(ui.startDateInput?.value);
  DATASTUDIO_STATE.endDate = dsSafeTrim(ui.endDateInput?.value);

  autoAdjustDataStudioDateRangeByMode();

  ui.startDateInput?.addEventListener("change", (e) => {
    DATASTUDIO_STATE.startDate = dsSafeTrim(e.target.value);
    DATASTUDIO_STATE.catalogConfirmed = false;
    markDataStudioSeriesDirty();
  });

  ui.endDateInput?.addEventListener("change", (e) => {
    DATASTUDIO_STATE.endDate = dsSafeTrim(e.target.value);
    DATASTUDIO_STATE.catalogConfirmed = false;
    markDataStudioSeriesDirty();
  });

  ui.plantSelect?.addEventListener("change", (e) => {
    const nextPlantId = dsSafeTrim(e.target.value) || null;
    DATASTUDIO_STATE.selectedPlantId = nextPlantId;

    DATASTUDIO_STATE.selectedTags = [];
    DATASTUDIO_STATE.availableTags = [];
    DATASTUDIO_STATE.selectionId = null;
    DATASTUDIO_STATE.chartData = null;
    DATASTUDIO_STATE.selectedContext = "all";
    DATASTUDIO_STATE.searchText = "";

    if (ui.contextSelect) {
      ui.contextSelect.innerHTML = "";
      const opt = document.createElement("option");
      opt.value = "all";
      opt.textContent = "Todos contextos";
      ui.contextSelect.appendChild(opt);
      ui.contextSelect.value = "all";
    }

    if (ui.searchInput) ui.searchInput.value = "";

    renderDataStudioTagsTable([]);
    renderDataStudioChart(null);
    updateSelectedTagsCounter();
    updateDataStudioExportButton();
    updateDataStudioStageUI();
  });

  ui.dataKindSelect?.addEventListener("change", (e) => {
    DATASTUDIO_STATE.selectedDataKind = dsSafeTrim(e.target.value) || "all";
  });

  ui.sourceSelect?.addEventListener("change", (e) => {
    DATASTUDIO_STATE.selectedSource = dsSafeTrim(e.target.value) || "all";
  });

  ui.contextSelect?.addEventListener("change", (e) => {
    DATASTUDIO_STATE.selectedContext = dsSafeTrim(e.target.value) || "all";
    updateDataStudioContextInfo();
    fetchDataStudioTags();
  });

  ui.searchInput?.addEventListener("input", (e) => {
    DATASTUDIO_STATE.searchText = dsSafeTrim(e.target.value);
  });

  ui.modeSelect?.addEventListener("change", (e) => {
    DATASTUDIO_STATE.aggregationMode = dsSafeTrim(e.target.value) || "historico";

    syncDataStudioAggregationUI();
    autoAdjustDataStudioDateRangeByMode();
    markDataStudioSeriesDirty();
  });

  ui.aggregationSelect?.addEventListener("change", (e) => {
    DATASTUDIO_STATE.aggregationType = dsSafeTrim(e.target.value) || "avg";
    markDataStudioSeriesDirty();
  });

  ui.consolidationSelect?.addEventListener("change", (e) => {
    DATASTUDIO_STATE.consolidationPeriod = dsSafeTrim(e.target.value) || "5min";

    autoAdjustDataStudioDateRangeByMode();
    markDataStudioSeriesDirty();
  });

  ui.openTagsBtn?.addEventListener("click", toggleDataStudioCatalogInline);
  ui.tagsApplyBtn?.addEventListener("click", fetchDataStudioTags);
  ui.confirmSelectionBtn?.addEventListener("click", confirmDataStudioCatalogSelection);

  ui.tagsClearBtn?.addEventListener("click", () => {
    DATASTUDIO_STATE.selectedTags = [];
    DATASTUDIO_STATE.selectionId = null;
    DATASTUDIO_STATE.chartData = null;
    DATASTUDIO_STATE.forceHeroState = false;
    DATASTUDIO_STATE.catalogConfirmed = false;
    DATASTUDIO_STATE.catalogOpen = true;
    renderDataStudioTagsTable(DATASTUDIO_STATE.availableTags);
    updateDataStudioExportButton();
    updateDataStudioStageUI();
  });

  ui.saveSelectionBtn?.addEventListener("click", saveDataStudioSelection);
  ui.loadSeriesBtn?.addEventListener("click", async () => {
    await saveDataStudioSelection();
  });
  ui.exportBtn?.addEventListener("click", exportDataStudioSelection);
  ui.zoomInBtn?.addEventListener("click", () => zoomDataStudioChart(1.2));
  ui.zoomOutBtn?.addEventListener("click", () => zoomDataStudioChart(0.8));
  ui.zoomResetBtn?.addEventListener("click", resetDataStudioChartZoom);

  const dsFullscreenBtn = document.getElementById("dsFullscreenBtn");
  const dsChartWrap = document.getElementById("dsChartWrap");
  if (dsFullscreenBtn && dsChartWrap) {

    // Botão de fechar fixo dentro do chart wrap
    const closeBtn = document.createElement("button");
    closeBtn.id = "dsFullscreenClose";
    closeBtn.className = "ds-fullscreen-close";
    closeBtn.title = "Sair da tela cheia";
    closeBtn.innerHTML = '<i class="fa-solid fa-compress"></i>';
    dsChartWrap.appendChild(closeBtn);

    const exitFullscreen = () => {
      dsChartWrap.classList.remove("ds-chart-fullscreen");
      const icon = dsFullscreenBtn.querySelector("i");
      if (icon) icon.className = "fa-solid fa-expand";
      if (window.dsChartInstance) setTimeout(() => window.dsChartInstance.resize(), 100);
    };

    dsFullscreenBtn.addEventListener("click", () => {
      const isFullscreen = dsChartWrap.classList.toggle("ds-chart-fullscreen");
      const icon = dsFullscreenBtn.querySelector("i");
      if (icon) icon.className = isFullscreen ? "fa-solid fa-compress" : "fa-solid fa-expand";
      if (window.dsChartInstance) setTimeout(() => window.dsChartInstance.resize(), 100);
    });

    closeBtn.addEventListener("click", exitFullscreen);

    document.addEventListener("keydown", (e) => {
      if (e.key === "Escape" && dsChartWrap.classList.contains("ds-chart-fullscreen")) {
        exitFullscreen();
      }
    });
  }
  ui.backToHeroBtn?.addEventListener("click", () => {
    DATASTUDIO_STATE.forceHeroState = true;
    DATASTUDIO_STATE.catalogConfirmed = false;
    DATASTUDIO_STATE.catalogOpen = true;
    updateDataStudioStageUI();
    setTimeout(() => {
      const { catalogSection } = getDataStudioUIElements();
      catalogSection?.scrollIntoView({ behavior: "smooth", block: "start" });
    }, 120);
  });

  wireDataStudioChartOutsideTapOnce();

  syncDataStudioAggregationUI();
  updateSelectedTagsCounter();
  updateDataStudioExportButton();
  updateDataStudioStageUI();
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
      updatePortfolioCardAlarms();
    }
  }

  await refreshVisibleViewData();
  syncTopSummaryLayout();
}

document.addEventListener("DOMContentLoaded", async () => {
  wireDataStudioOnce();
  wirePlantEditModalOnce();

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

let currentEditPlantId = null;
let currentEditPlantName = "";
let _plantEditModalWired = false;
let _plantEditLastFocus = null;
let _portfolioActionMenuWired = false;
let _portfolioActionMenuButton = null;
let _portfolioActionMenuContext = null;

function getOrCreatePlantEditModal() {
  let modal = document.getElementById("plantEditModal");
  if (modal) return modal;

  document.body.insertAdjacentHTML("beforeend", `
    <div id="plantEditModal" class="plant-edit-modal hidden" aria-hidden="true">
      <div class="plant-edit-modal__backdrop"></div>
      <div class="plant-edit-modal__box" role="dialog" aria-modal="true" aria-labelledby="plantEditModalTitle">
        <div class="plant-edit-modal__header">
          <h3 id="plantEditModalTitle">Editar usina</h3>
          <button class="plant-edit-modal__close" type="button" onclick="closePlantEditModal()" aria-label="Fechar">
            <i class="fa-solid fa-xmark"></i>
          </button>
        </div>
        <div class="plant-edit-section">
          <label for="plantEditNameInput">Nome da usina</label>
          <div class="plant-edit-inline">
            <input type="text" id="plantEditNameInput" placeholder="Nome da usina" />
            <button type="button" onclick="savePlantName()" title="Salvar nome da usina" aria-label="Salvar nome da usina">
              <i class="fa-solid fa-check"></i>
            </button>
          </div>
          <div id="plantEditNameFeedback" class="plant-edit-feedback" aria-live="polite"></div>
        </div>
        <div class="plant-edit-section" id="plantEditRatedSection">
          <label for="plantEditRatedInput">Rated Power (kWp)</label>
          <div class="plant-edit-inline">
            <input type="number" id="plantEditRatedInput" placeholder="Ex: 1500.0" min="0" step="0.1" />
            <button type="button" onclick="savePlantRatedPower()" title="Salvar rated power" aria-label="Salvar rated power">
              <i class="fa-solid fa-check"></i>
            </button>
          </div>
          <div id="plantEditRatedFeedback" class="plant-edit-feedback" aria-live="polite"></div>
        </div>
        <hr class="plant-edit-divider" />
        <div class="plant-edit-section">
          <label>Dispositivos</label>
          <div id="plantEditDevicesList" class="plant-edit-devices-list" tabindex="-1" aria-live="polite"></div>
        </div>
      </div>
    </div>
  `);

  return document.getElementById("plantEditModal");
}

function wirePlantEditModalOnce() {
  if (_plantEditModalWired) return;

  const modal = getOrCreatePlantEditModal();
  if (!modal) return;

  modal.querySelector(".plant-edit-modal__backdrop")?.addEventListener("click", closePlantEditModal);
  document.getElementById("plantEditNameInput")?.addEventListener("keydown", event => {
    if (event.key === "Enter") {
      event.preventDefault();
      savePlantName();
    }
  });

  document.getElementById("plantEditRatedInput")?.addEventListener("keydown", event => {
    if (event.key === "Enter") {
      event.preventDefault();
      savePlantRatedPower();
    }
  });

  document.addEventListener("keydown", event => {
    if (event.key === "Escape" && !modal.classList.contains("hidden")) {
      closePlantEditModal();
    }
  });

  _plantEditModalWired = true;
}

function plantEditSetFeedback(el, message = "", type = "") {
  if (!el) return;
  el.textContent = message;
  el.classList.remove("ok", "err");
  if (type) el.classList.add(type);
}

function formatPlantEditDeviceMeta(device) {
  const parts = [];
  const deviceId = device?.device_id ?? device?.id;
  const original = device?.original_name || device?.device_default_name || device?.name;
  const cabin = device?.cabin || device?.cabin_name || device?.electrocenter || device?.electrocenter_name;

  if (deviceId != null) parts.push(`ID ${deviceId}`);
  if (original) parts.push(`Original: ${original}`);
  if (device?.is_active != null) parts.push(device.is_active ? "Ativo" : "Inativo");
  if (cabin) parts.push(`Cabine: ${cabin}`);

  return parts.join(" • ");
}

function renderPlantEditDeviceRows(devices) {
  const list = document.getElementById("plantEditDevicesList");
  if (!list) return;

  list.innerHTML = "";
  const items = Array.isArray(devices) ? devices : [];

  if (items.length === 0) {
    const empty = document.createElement("div");
    empty.className = "plant-edit-empty";
    empty.textContent = "Nenhum dispositivo encontrado.";
    list.appendChild(empty);
    return;
  }

  items.forEach(device => {
    const deviceId = device?.device_id;
    const row = document.createElement("div");
    row.className = "plant-edit-device-row";
    row.dataset.deviceId = String(deviceId ?? "");

    const info = document.createElement("div");
    info.className = "plant-edit-device-info";

    const type = document.createElement("span");
    type.className = "plant-edit-device-type";
    type.textContent = device?.device_type || "Device";
    type.title = type.textContent;

    const meta = document.createElement("span");
    meta.className = "plant-edit-device-meta";
    meta.textContent = formatPlantEditDeviceMeta(device);
    meta.title = meta.textContent;

    info.append(type, meta);

    const input = document.createElement("input");
    input.type = "text";
    input.className = "plant-edit-device-name";
    input.placeholder = "Nome do dispositivo";
    input.value = device?.display_name || device?.device_name || device?.name || "";
    input.addEventListener("keydown", event => {
      if (event.key === "Enter") {
        event.preventDefault();
        saveDeviceName(deviceId);
      }
    });

    const button = document.createElement("button");
    button.type = "button";
    button.className = "plant-edit-device-save";
    button.title = "Salvar nome do dispositivo";
    button.setAttribute("aria-label", "Salvar nome do dispositivo");
    button.innerHTML = '<i class="fa-solid fa-check"></i>';
    button.addEventListener("click", () => saveDeviceName(deviceId));

    const feedback = document.createElement("span");
    feedback.className = "plant-edit-device-feedback";
    feedback.setAttribute("aria-live", "polite");

    row.append(info, input, button, feedback);
    list.appendChild(row);
  });
}

async function loadPlantDevices(plantId) {
  const list = document.getElementById("plantEditDevicesList");
  if (!list) return;

  list.innerHTML = '<div class="plant-edit-empty">Carregando dispositivos...</div>';

  try {
    const devices = await fetchPlantDeviceOptions(plantId);
    if (String(currentEditPlantId) !== String(plantId)) return;
    renderPlantEditDeviceRows(devices);
  } catch (err) {
    console.error("[PlantEdit] erro ao carregar dispositivos:", err);
    list.innerHTML = '<div class="plant-edit-empty plant-edit-empty--error">Erro ao carregar dispositivos.</div>';
  }
}

function openPlantEditModal(event, plantId, plantName, options = {}) {
  if (event) {
    event.preventDefault();
    event.stopPropagation();
  }

  const modal = getOrCreatePlantEditModal();
  if (!modal) return;

  wirePlantEditModalOnce();

  currentEditPlantId = plantId;
  currentEditPlantName = String(plantName ?? "");
  _plantEditLastFocus = document.activeElement;

  const nameInput = document.getElementById("plantEditNameInput");
  const nameFeedback = document.getElementById("plantEditNameFeedback");
  const devicesList = document.getElementById("plantEditDevicesList");
  const title = document.getElementById("plantEditModalTitle");
  const mode = options.mode || "plant";
  const plantSection = nameInput?.closest(".plant-edit-section");
  const devicesSection = devicesList?.closest(".plant-edit-section");
  const divider = modal.querySelector(".plant-edit-divider");

  if (nameInput) nameInput.value = currentEditPlantName;

  const ratedInput = document.getElementById("plantEditRatedInput");
  const ratedFeedback = document.getElementById("plantEditRatedFeedback");
  const ratedSection = document.getElementById("plantEditRatedSection");

  // Buscar rated power atual da planta no array lastValidPlants
  const currentPlant = lastValidPlants.find(p =>
    String(p.power_plant_id ?? p.plant_id ?? p.id) === String(plantId)
  );
  const currentRated = currentPlant?.capacity_dc ?? currentPlant?.rated_power_kwp ?? currentPlant?.rated_power_kw ?? "";
  if (ratedInput) ratedInput.value = currentRated !== "" && currentRated != null ? Number(currentRated) : "";
  plantEditSetFeedback(ratedFeedback);

  // Esconder rated section no modo "devices"
  if (ratedSection) ratedSection.style.display = mode === "devices" ? "none" : "";

  if (title) title.textContent = mode === "devices" ? "Gerenciar dispositivos" : "Editar usina";
  if (plantSection) plantSection.style.display = mode === "devices" ? "none" : "";
  if (devicesSection) devicesSection.style.display = mode === "plant" ? "none" : "";
  if (divider) divider.style.display = "none";
  plantEditSetFeedback(nameFeedback);
  if (devicesList) devicesList.innerHTML = "";

  modal.classList.remove("hidden");
  modal.setAttribute("aria-hidden", "false");
  document.body.classList.add("plant-edit-modal-open");

  window.setTimeout(() => {
    if (mode === "devices") {
      devicesList?.focus?.();
      return;
    }
    nameInput?.focus();
    nameInput?.select();
  }, 0);

  if (mode !== "plant") loadPlantDevices(plantId);
}

function closePlantEditModal() {
  const modal = document.getElementById("plantEditModal");
  if (!modal) return;

  modal.classList.add("hidden");
  modal.setAttribute("aria-hidden", "true");
  document.body.classList.remove("plant-edit-modal-open");

  if (_plantEditLastFocus && document.contains(_plantEditLastFocus)) {
    _plantEditLastFocus.focus?.();
  }
}

function getPlantEditDeviceRow(deviceId) {
  return Array.from(document.querySelectorAll("#plantEditDevicesList .plant-edit-device-row"))
    .find(row => row.dataset.deviceId === String(deviceId));
}

function updatePortfolioPlantName(plantId, plantName) {
  const idText = String(plantId);
  document.querySelectorAll(".plant-card[data-plant-id]").forEach(card => {
    if (String(card.dataset.plantId) !== idText) return;
    card.dataset.plantName = plantName;
    const nameEl = card.querySelector(".plant-card__name");
    if (nameEl) nameEl.textContent = plantName;
  });

  lastValidPlants.forEach(plant => {
    const pid = plant?.power_plant_id ?? plant?.plant_id ?? plant?.id;
    if (String(pid) !== idText) return;
    plant.power_plant_name = plantName;
    if (Object.prototype.hasOwnProperty.call(plant, "plant_name")) plant.plant_name = plantName;
    if (Object.prototype.hasOwnProperty.call(plant, "name")) plant.name = plantName;
  });

  const listView = document.getElementById("portfolioListView");
  if (listView && !listView.classList.contains("hidden")) {
    const plants = typeof portfolioFilterPlants === "function"
      ? portfolioFilterPlants(lastValidPlants)
      : lastValidPlants;
    renderPortfolioTable(plants);
  }
}

async function savePlantName() {
  const nameInput = document.getElementById("plantEditNameInput");
  const feedback = document.getElementById("plantEditNameFeedback");
  const button = document.querySelector(".plant-edit-inline button");
  const newName = (nameInput?.value || "").trim();

  if (!currentEditPlantId) {
    plantEditSetFeedback(feedback, "Usina invalida.", "err");
    return;
  }
  if (!newName) {
    plantEditSetFeedback(feedback, "Informe um nome.", "err");
    nameInput?.focus();
    return;
  }

  if (button) button.disabled = true;
  plantEditSetFeedback(feedback, "Salvando...");

  try {
    const res = await apiFetch(`/plants/${encodeURIComponent(currentEditPlantId)}/name`, {
      method: "PATCH",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ plant_name: newName })
    });
    const data = await res.json().catch(() => ({}));
    const parsed = normalizeApiPayload(data) || {};

    if (!res.ok || parsed.ok === false) {
      throw new Error(parsed.error || `Falha ao salvar (${res.status})`);
    }

    const savedName = parsed.power_plant_name || parsed.plant_name || newName;
    currentEditPlantName = savedName;
    if (nameInput) nameInput.value = savedName;
    updatePortfolioPlantName(currentEditPlantId, savedName);
    plantEditSetFeedback(feedback, "Salvo.", "ok");
  } catch (err) {
    console.error("[PlantEdit] erro ao salvar nome da usina:", err);
    plantEditSetFeedback(feedback, err?.message || "Erro ao salvar.", "err");
  } finally {
    if (button) button.disabled = false;
  }
}

async function savePlantRatedPower() {
  const ratedInput = document.getElementById("plantEditRatedInput");
  const feedback = document.getElementById("plantEditRatedFeedback");
  const button = ratedInput?.closest(".plant-edit-inline")?.querySelector("button");
  const rawValue = (ratedInput?.value ?? "").trim();

  if (!currentEditPlantId) {
    plantEditSetFeedback(feedback, "Usina inválida.", "err");
    return;
  }
  if (rawValue === "" || isNaN(Number(rawValue)) || Number(rawValue) < 0) {
    plantEditSetFeedback(feedback, "Informe um valor válido (>= 0).", "err");
    ratedInput?.focus();
    return;
  }

  if (button) button.disabled = true;
  plantEditSetFeedback(feedback, "Salvando...");

  try {
    const res = await apiFetch(`/plants/${encodeURIComponent(currentEditPlantId)}/name`, {
      method: "PATCH",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ capacity_dc: Number(rawValue) })
    });
    const data = await res.json().catch(() => ({}));
    const parsed = normalizeApiPayload(data) || {};

    if (!res.ok || parsed.ok === false) {
      throw new Error(parsed.error || `Falha ao salvar (${res.status})`);
    }

    const savedValue = parsed.capacity_dc;
    if (ratedInput && savedValue != null) ratedInput.value = savedValue;

    // Atualizar o card no portfólio sem reload
    const idText = String(currentEditPlantId);
    lastValidPlants.forEach(plant => {
      const pid = plant?.power_plant_id ?? plant?.plant_id ?? plant?.id;
      if (String(pid) !== idText) return;
      plant.capacity_dc = savedValue;
      plant.rated_power_kwp = savedValue;
      plant.rated_power_kw = savedValue;
    });
    // Re-render dos cards para refletir o novo rated
    if (typeof _portfolioCurrentView !== "undefined" && _portfolioCurrentView === "card") {
      renderPortfolioCards(typeof portfolioFilterPlants === "function"
        ? portfolioFilterPlants(lastValidPlants) : lastValidPlants);
    }

    plantEditSetFeedback(feedback, "Salvo.", "ok");
  } catch (err) {
    console.error("[PlantEdit] erro ao salvar rated power:", err);
    plantEditSetFeedback(feedback, err?.message || "Erro ao salvar.", "err");
  } finally {
    if (button) button.disabled = false;
  }
}

async function saveDeviceName(deviceId) {
  const row = getPlantEditDeviceRow(deviceId);
  const input = row?.querySelector(".plant-edit-device-name");
  const feedback = row?.querySelector(".plant-edit-device-feedback");
  const button = row?.querySelector(".plant-edit-device-save");
  const newName = (input?.value || "").trim();

  if (!currentEditPlantId || !row) return;
  if (!newName) {
    plantEditSetFeedback(feedback, "Informe um nome.", "err");
    input?.focus();
    return;
  }

  if (button) button.disabled = true;
  plantEditSetFeedback(feedback, "Salvando...");

  try {
    const res = await apiFetch(`/plants/${encodeURIComponent(currentEditPlantId)}/devices/${encodeURIComponent(deviceId)}/name`, {
      method: "PATCH",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ display_name: newName })
    });
    const data = await res.json().catch(() => ({}));
    const parsed = normalizeApiPayload(data) || {};

    if (!res.ok || parsed.ok === false) {
      throw new Error(parsed.error || `Falha ao salvar (${res.status})`);
    }

    const savedName = parsed.display_name || newName;
    if (input) input.value = savedName;
    plantEditSetFeedback(feedback, "Salvo.", "ok");
  } catch (err) {
    console.error("[PlantEdit] erro ao salvar nome do dispositivo:", err);
    plantEditSetFeedback(feedback, err?.message || "Erro.", "err");
  } finally {
    if (button) button.disabled = false;
  }
}

function getPlantPageUrl(plantId, { hash = "", action = "" } = {}) {
  const params = new URLSearchParams({ plant_id: String(plantId) });
  if (action) params.set("action", action);
  return `plant.html?${params.toString()}${hash || ""}`;
}

function navigateToPlantPage(plantId, options = {}) {
  if (plantId == null) return;
  window.location.href = getPlantPageUrl(plantId, options);
}

function ensurePortfolioPlantActionMenu() {
  let menu = document.getElementById("portfolioPlantActionMenu");
  if (!menu) {
    menu = document.createElement("div");
    menu.id = "portfolioPlantActionMenu";
    menu.className = "portfolio-plant-action-menu hidden";
    menu.setAttribute("role", "menu");
    document.body.appendChild(menu);
  }

  if (!_portfolioActionMenuWired) {
    menu.addEventListener("click", event => event.stopPropagation());
    document.addEventListener("click", event => {
      if (menu.classList.contains("hidden")) return;
      if (menu.contains(event.target) || _portfolioActionMenuButton?.contains?.(event.target)) return;
      closePortfolioPlantActionMenu();
    });
    document.addEventListener("keydown", event => {
      if (event.key === "Escape") closePortfolioPlantActionMenu();
    });
    window.addEventListener("resize", closePortfolioPlantActionMenu);
    window.addEventListener("scroll", closePortfolioPlantActionMenu, true);
    _portfolioActionMenuWired = true;
  }

  return menu;
}

function positionPortfolioPlantActionMenu(menu, button) {
  if (!menu || !button) return;

  const rect = button.getBoundingClientRect();
  menu.classList.remove("hidden");

  const width = menu.offsetWidth || 260;
  const height = menu.offsetHeight || 320;
  const margin = 12;

  let left = rect.left;
  let top = rect.bottom + 8;

  if (left + width > window.innerWidth - margin) left = window.innerWidth - width - margin;
  if (top + height > window.innerHeight - margin) top = rect.top - height - 8;

  menu.style.left = `${Math.max(margin, left)}px`;
  menu.style.top = `${Math.max(margin, top)}px`;
}

function buildPortfolioPlantActionItems() {
  return [
    { action: "open", icon: "fa-up-right-from-square", label: "Abrir usina / Ver supervisório" },
    { action: "edit", icon: "fa-pen-to-square", label: "Editar dados da usina" },
    { action: "devices", icon: "fa-microchip", label: "Gerenciar dispositivos" },
    { type: "separator" },
    { action: "inverters", icon: "fa-bolt", label: "Gerenciar inversores / strings" },
    { action: "relay", icon: "fa-tower-broadcast", label: "Gerenciar relé" },
    { action: "multimeter", icon: "fa-gauge-high", label: "Gerenciar multimedidor" },
    { action: "trackers", icon: "fa-satellite-dish", label: "Gerenciar trackers" },
    { type: "separator" },
    { action: "events", icon: "fa-triangle-exclamation", label: "Ver eventos/alarmes" },
    { action: "datastudio", icon: "fa-flask-vial", label: "Abrir Data Studio da usina" },
    { action: "command", icon: "fa-terminal", label: "Console de comandos" }
  ];
}

function renderPortfolioPlantActionMenu(menu, plantId, plantName) {
  menu.innerHTML = "";
  const header = document.createElement("div");
  header.className = "portfolio-plant-action-menu__header";
  header.textContent = plantName || `Usina ${plantId}`;
  menu.appendChild(header);

  buildPortfolioPlantActionItems().forEach(item => {
    if (item.type === "separator") {
      const sep = document.createElement("div");
      sep.className = "portfolio-plant-action-menu__separator";
      sep.setAttribute("role", "separator");
      menu.appendChild(sep);
      return;
    }

    const btn = document.createElement("button");
    btn.type = "button";
    btn.className = "portfolio-plant-action-menu__item";
    btn.setAttribute("role", "menuitem");
    btn.dataset.action = item.action;
    btn.innerHTML = `<i class="fa-solid ${item.icon}" aria-hidden="true"></i><span>${escapeHtml(item.label)}</span>`;
    btn.addEventListener("click", () => runPortfolioPlantAction(item.action, plantId, plantName));
    menu.appendChild(btn);
  });
}

function openPortfolioPlantActionMenu(event, plantId, plantName) {
  event?.preventDefault();
  event?.stopPropagation();

  const button = event?.currentTarget;
  const menu = ensurePortfolioPlantActionMenu();
  const sameButtonOpen =
    _portfolioActionMenuButton === button &&
    !menu.classList.contains("hidden");

  closePortfolioPlantActionMenu();
  if (sameButtonOpen) return;

  _portfolioActionMenuButton = button;
  _portfolioActionMenuContext = { plantId, plantName };
  renderPortfolioPlantActionMenu(menu, plantId, plantName);
  button?.setAttribute("aria-expanded", "true");
  positionPortfolioPlantActionMenu(menu, button);
}

function closePortfolioPlantActionMenu() {
  const menu = document.getElementById("portfolioPlantActionMenu");
  if (menu) {
    menu.classList.add("hidden");
    menu.innerHTML = "";
  }
  _portfolioActionMenuButton?.setAttribute("aria-expanded", "false");
  _portfolioActionMenuButton = null;
  _portfolioActionMenuContext = null;
}

function openPortfolioEventsForPlant(plantId) {
  wireEventsFiltersOnce();
  populateEventsPlantSelect(lastValidPlants);

  const ui = getEventsUIElements();
  if (ui.plantSelect) ui.plantSelect.value = String(plantId);
  if (ui.equipmentSelect) ui.equipmentSelect.value = "all";
  if (ui.desc) ui.desc.value = "";
  if (ui.typeSelect) ui.typeSelect.value = "all";
  if (ui.statusSelect) ui.statusSelect.value = "all";
  if (ui.severitySelect) ui.severitySelect.value = "all";
  ensureDefaultEventsDateTimes();

  EVENTS_STATE.page = 1;
  showView("events");
  void refreshEventsEquipmentOptionsForPlant(plantId);
}

function openPortfolioDataStudioForPlant(plantId) {
  showView("datastudio");
  wireDataStudioOnce();
  populateDataStudioPlantSelect(lastValidPlants);

  const ui = getDataStudioUIElements();
  if (ui.plantSelect) ui.plantSelect.value = String(plantId);
  if (ui.dataKindSelect) ui.dataKindSelect.value = "all";
  if (ui.sourceSelect) ui.sourceSelect.value = "all";
  if (ui.searchInput) ui.searchInput.value = "";

  DATASTUDIO_STATE.selectedPlantId = String(plantId);
  DATASTUDIO_STATE.selectedTags = [];
  DATASTUDIO_STATE.availableTags = [];
  DATASTUDIO_STATE.selectionId = null;
  DATASTUDIO_STATE.chartData = null;
  DATASTUDIO_STATE.selectedDataKind = "all";
  DATASTUDIO_STATE.selectedSource = "all";
  DATASTUDIO_STATE.selectedContext = "all";
  DATASTUDIO_STATE.searchText = "";
  DATASTUDIO_STATE.catalogOpen = true;
  DATASTUDIO_STATE.catalogConfirmed = false;
  DATASTUDIO_STATE.forceHeroState = false;

  if (ui.contextSelect) {
    ui.contextSelect.innerHTML = "";
    const opt = document.createElement("option");
    opt.value = "all";
    opt.textContent = "Todos contextos";
    ui.contextSelect.appendChild(opt);
    ui.contextSelect.value = "all";
  }

  renderDataStudioTagsTable([]);
  renderDataStudioChart(null);
  updateSelectedTagsCounter();
  updateDataStudioExportButton();
  updateDataStudioStageUI();
  fetchDataStudioTags();
}

function runPortfolioPlantAction(action, plantId, plantName) {
  closePortfolioPlantActionMenu();

  switch (action) {
    case "open":
      navigateToPlantPage(plantId);
      break;
    case "edit":
      openPlantEditModal(null, plantId, plantName, { mode: "plant" });
      break;
    case "devices":
      openPlantEditModal(null, plantId, plantName, { mode: "devices" });
      break;
    case "inverters":
      navigateToPlantPage(plantId, { hash: "#sec-inverters" });
      break;
    case "relay":
      navigateToPlantPage(plantId, { hash: "#sec-relay" });
      break;
    case "multimeter":
      navigateToPlantPage(plantId, { hash: "#sec-multimeter" });
      break;
    case "trackers":
      navigateToPlantPage(plantId, { hash: "#sec-trackers" });
      break;
    case "events":
      openPortfolioEventsForPlant(plantId);
      break;
    case "datastudio":
      openPortfolioDataStudioForPlant(plantId);
      break;
    case "command":
      navigateToPlantPage(plantId, { action: "command" });
      break;
  }
}

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
    const isOff = card.classList.contains("plant-card--offline");
    const kind = card.dataset.plantKind || "";
    const isStandby = !sev && kind === "standby";
    card.className = `plant-card${isOff ? " plant-card--offline" : ""}${sev ? ` alarm-${sev}` : ""}${isStandby ? " standby-card" : ""}`;
    const icon = card.querySelector(".plant-card__icon");
    if (icon) icon.className = sev ? `plant-card__icon alarm-${sev}` : `plant-card__icon${isStandby ? " standby-icon" : ""}`;
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
    const plantName = String(plant.power_plant_name ?? plant.plant_name ?? plant.name ?? "\u2014");

    const alarmSeverity = normalizeAlarmSeverity(lastAlarmSeverityByPlant.get(plantId))
      || normalizeAlarmSeverity(lastAlarmSeverityByPlant.get(plantName)) || null;

    const plantState = getPortfolioPlantVisualState(plant);
    const activePower = plantState.activePower;
    const ratedPower = Number(plant.rated_power_kw ?? plant.rated_power_kwp ?? 0);
    const energyToday = plantState.energyToday;
    const pr = plant.pr_daily_pct != null ? Number(plant.pr_daily_pct).toFixed(1) + "%" : "\u2014";
    const irr = plant.irradiance_wm2 != null ? Number(plant.irradiance_wm2).toFixed(0) + " W/m\u00B2" : "\u2014";
    const invAvail = plant.inverter_availability_pct != null ? Number(plant.inverter_availability_pct).toFixed(1) + "%" : "\u2014";

    const isOffline = plantState.isOffline;
    const isGenerating = plantState.kind === "generating";
    let statusDotClass = "plant-card__status-dot";
    let statusText;
    if (plantState.kind === "offline") {
      statusDotClass += " offline";
      statusText = "Sem dados";
    }
    else if (isGenerating){ statusDotClass += " generating"; statusText = "Em gera\u00E7\u00E3o"; }
    else                  { statusDotClass += " standby";   statusText = "Aguardando"; }

    const offlineClass = isOffline ? " plant-card--offline" : "";
    const alarmSuffix  = alarmSeverity ? ` alarm-${alarmSeverity}` : "";
    const standbyIcon  = (!alarmSeverity && plantState.kind === "standby") ? " standby-icon" : "";
    const iconClass    = alarmSeverity ? `plant-card__icon alarm-${alarmSeverity}` : `plant-card__icon${standbyIcon}`;
    const standbyCard  = (!alarmSeverity && plantState.kind === "standby") ? " standby-card" : "";
    const cardClass    = `plant-card${offlineClass}${alarmSuffix}${standbyCard}`;
    const canvasId = "mini-chart-" + plantId;

    const card = document.createElement("div");
    card.className = cardClass;
    card.setAttribute("role", "link");
    card.setAttribute("tabindex", "0");
    card.dataset.plantId = plantId;
    card.dataset.plantName = plantName;
    card.dataset.plantKind = plantState.kind;

    card.innerHTML = `
      <div class="plant-card__top">
        <div class="${iconClass}"><i class="fa-solid fa-seedling"></i></div>
        <div class="plant-card__name">${escapeHtml(plantName)}</div>
      </div>
      <div class="plant-card__stats">
        <div class="plant-card__stat">
          <div class="plant-card__stat-label"><i class="fa-solid fa-bolt"></i> Active Power</div>
          <div class="plant-card__stat-value active">${activePower.toFixed(1)} kW</div>
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
        <button class="plant-card__edit-btn" type="button" data-plant-id="${escapeHtml(plantId)}" title="Ações da usina" aria-label="Ações da usina" aria-haspopup="menu" aria-expanded="false">
          <svg viewBox="0 0 18 18" fill="none" xmlns="http://www.w3.org/2000/svg" aria-hidden="true">
            <path d="M10.5 3.5 L2.5 11.5 L2 15.5 L6 15 L14 7 Z" stroke="currentColor" stroke-width="1.3" stroke-linejoin="round" stroke-linecap="round" fill="currentColor" fill-opacity="0.12"/>
            <path d="M8.5 5.5 L12 9" stroke="currentColor" stroke-width="1.3" stroke-linecap="round"/>
            <path d="M13 2.5 L15 4.5 L14 7 L10.5 3.5 Z" stroke="currentColor" stroke-width="1.3" stroke-linejoin="round" fill="currentColor" fill-opacity="0.3"/>
          </svg>
        </button>
        <div class="${statusDotClass}"></div>
        <span class="plant-card__status-text">${statusText}</span>
      </div>
    `;

    card.querySelector(".plant-card__edit-btn")?.addEventListener("click", event => {
      openPortfolioPlantActionMenu(event, plantId, card.dataset.plantName || plantName);
    });
    card.querySelector(".plant-card__edit-btn")?.addEventListener("keydown", event => {
      if (event.key === "Enter" || event.key === " ") {
        openPortfolioPlantActionMenu(event, plantId, card.dataset.plantName || plantName);
      }
    });

    const openPlant = () => {
      if (plantId != null) window.location.href = `plant.html?plant_id=${encodeURIComponent(plantId)}`;
    };
    card.addEventListener("click", openPlant);
    card.addEventListener("keydown", e => {
      if (e.key === "Enter" || e.key === " ") { e.preventDefault(); openPlant(); }
    });

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

// wirePortfolioControls is now called synchronously inside the main DOMContentLoaded listener (above).
