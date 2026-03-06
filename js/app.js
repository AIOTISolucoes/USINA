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

// Abort controller pra evitar race condition
let eventsAbortController = null;
let ALARMS_RENDER_SEQ = 0;

// ✅ MODO PADRÃO DO EVENTS
let EVENTS_VIEW_MODE = "round_robin";

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
// HELPERS DE DATA (EVENTS) — DATE + START TIME + END TIME
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
  } else {
    HH = isEnd ? 23 : 0;
    MI = isEnd ? 59 : 0;
    SS = isEnd ? 59 : 0;
  }

  const d = new Date(yyyy, mm - 1, dd, HH, MI, SS);
  return isNaN(d.getTime()) ? null : d.toISOString();
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

// =============================================================================
// PARSER DE EQUIPMENT (Inversor2 / INV-02 / Relé3 / Relay 3 / Weather)
// =============================================================================
function parseEquipmentFilter(input) {
  const raw = safeTrim(input);
  if (!raw) return { source: null, device_id: null, equipment_norm: "" };

  const compact = raw.replace(/\s+/g, "").replace(/[^\w]/g, "");
  const lower = compact.toLowerCase();

  const invMatch =
    lower.match(/^inversor(\d+)$/) ||
    lower.match(/^inverter(\d+)$/) ||
    lower.match(/^inv(\d+)$/);

  if (invMatch) {
    return {
      source: "inverter",
      device_id: parseInt(invMatch[1], 10),
      equipment_norm: `Inversor${invMatch[1]}`
    };
  }

  const relayMatch =
    lower.match(/^relay(\d+)$/) ||
    lower.match(/^rele(\d+)$/) ||
    lower.match(/^rel(\d+)$/);

  if (relayMatch) {
    return {
      source: "relay",
      device_id: parseInt(relayMatch[1], 10),
      equipment_norm: `Relay${relayMatch[1]}`
    };
  }

  if (lower === "weather" || lower === "clima") {
    return { source: "weather", device_id: null, equipment_norm: "Weather" };
  }

  return { source: null, device_id: null, equipment_norm: raw };
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

  if (data && data.body) {
    const parsed = typeof data.body === "string" ? JSON.parse(data.body) : data.body;
    return Array.isArray(parsed) ? parsed : [];
  }
  return Array.isArray(data) ? data : [];
}

async function acknowledgeAlarm(alarmId) {
  await apiFetch(`/alarms/${alarmId}/ack`, { method: "POST" });
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
  q,
  source,
  device_id,
  plant_id,
  mode = "round_robin",
  rounds = 5,
  include_total = false,
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
      start_time, end_time, page, page_size, severity, event_type, q, source, device_id, plant_id, mode, rounds, include_total,
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
  const date = document.getElementById("eventsDateInput");
  const startTime = document.getElementById("eventsStartTimeInput");
  const endTime = document.getElementById("eventsEndTimeInput");

  const severitySelect = document.getElementById("eventsSeveritySelect");
  const stateSelect = document.getElementById("eventsStateSelect");

  const equipment = document.getElementById("eventsEquipmentInput");
  const point = document.getElementById("eventsPointInput");
  const desc = document.getElementById("eventsDescriptionInput");

  const applyBtn = document.getElementById("eventsApplyBtn") || findButtonByText("apply");
  const clearBtn = document.getElementById("eventsClearBtn") || findButtonByText("clear");

  const prevBtn = document.getElementById("eventsPrevBtn");
  const nextBtn = document.getElementById("eventsNextBtn");
  const pageLabel = document.getElementById("eventsPageLabel");

  return { date, startTime, endTime, severitySelect, stateSelect, equipment, point, desc, applyBtn, clearBtn, prevBtn, nextBtn, pageLabel };
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

function ensureStateSelectOptions() {
  const ui = getEventsUIElements();
  const sel = ui.stateSelect;
  if (!sel || sel.tagName !== "SELECT") return;

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

  if (!sel.value) sel.value = "all";
}

function ensureDefaultEventsDates() {
  const ui = getEventsUIElements();
  if (!ui.date) return;
  if (!safeTrim(ui.date.value)) ui.date.value = todayYYYYMMDD();
}

// =============================================================================
// filtros: Equipment => source/device_id, texto => q
// =============================================================================
function getEventsFiltersFromUI() {
  const ui = getEventsUIElements();

  const date = safeTrim(ui.date?.value);
  const startT = safeTrim(ui.startTime?.value);
  const endT = safeTrim(ui.endTime?.value);

  let start_time = isoFromDateAndTime(date, startT, false);
  let end_time = isoFromDateAndTime(date, endT, true);

  const fixed = clampEventRange(start_time, end_time);
  start_time = fixed.startISO;
  end_time = fixed.endISO;

  let severity = "all";
  if (ui.severitySelect) severity = String(ui.severitySelect.value || "all").trim().toLowerCase() || "all";

  let event_type = "all";
  if (ui.stateSelect) event_type = String(ui.stateSelect.value || "all").trim().toLowerCase() || "all";

  const equipmentText = safeTrim(ui.equipment?.value);
  const equip = parseEquipmentFilter(equipmentText);

  const source = equip.source;
  const device_id = equip.device_id;

  const pointText = safeTrim(ui.point?.value);
  const descText = safeTrim(ui.desc?.value);
  const qParts = [pointText, descText].map(s => safeTrim(s)).filter(Boolean);
  const q = qParts.join(" ");

  const plant_id = null;

  return { start_time, end_time, plant_id, severity, event_type, q, source, device_id };
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
  ensureStateSelectOptions();

  const ui = getEventsUIElements();

  if (ui.severitySelect) {
    ui.severitySelect.addEventListener("change", () => {
      EVENTS_STATE.page = 1;
      loadEvents(1);
    });
  }

  if (ui.stateSelect) {
    ui.stateSelect.addEventListener("change", () => {
      EVENTS_STATE.page = 1;
      loadEvents(1);
    });
  }

  const textInputs = [ui.equipment, ui.point, ui.desc].filter(Boolean);
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

      if (ui2.equipment) ui2.equipment.value = "";
      if (ui2.point) ui2.point.value = "";
      if (ui2.desc) ui2.desc.value = "";

      if (ui2.stateSelect) ui2.stateSelect.value = "all";
      if (ui2.severitySelect) ui2.severitySelect.value = "all";

      if (ui2.date) ui2.date.value = todayYYYYMMDD();
      if (ui2.startTime) ui2.startTime.value = "";
      if (ui2.endTime) ui2.endTime.value = "";

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
async function renderAlarmsTable(isRecognized = false) {
  const tbody = document.getElementById("alarmsTbody");
  if (!tbody) return;

  const renderSeq = ++ALARMS_RENDER_SEQ;
  tbody.innerHTML = "";

  let alarms = [];
  try {
    alarms = isRecognized
      ? (await fetchAcknowledgedAlarms()).filter(a => {
          const state = a.alarm_state || a.state;
          return state === "ACK" || state === "CLEARED";
        })
      : await fetchActiveAlarms();
  } catch (err) {
    console.error("Erro ao buscar alarmes:", err);
  }

  if (renderSeq !== ALARMS_RENDER_SEQ) return;

  alarms = dedupeAlarms(alarms);

  if (!alarms || alarms.length === 0) {
    tbody.innerHTML = `<tr><td colspan="4" style="text-align:center; opacity:0.6; padding:40px;">${isRecognized ? "Nenhum alerta reconhecido" : "Nenhum alerta ativo"}</td></tr>`;
    return;
  }

  alarms.forEach(alarm => {
    if (renderSeq !== ALARMS_RENDER_SEQ) return;
    const tr = document.createElement("tr");
    const sev = normalizeAlarmSeverity(
      alarm.severity || alarm.alarm_severity || alarm.level || alarm.alarm_level
    ) || "low";

    const timestamp =
      alarm.ack_at ||
      alarm.cleared_at ||
      alarm.last_event_ts ||
      alarm.started_at ||
      "—";

    const tsFormatted = timestamp !== "—" ? new Date(timestamp).toLocaleString("pt-BR") : "—";

    const state = alarm.alarm_state || alarm.state || "—";
    const stateColor =
      state === "ACTIVE" ? "#f44336" :
      state === "ACK" ? "#ff9800" :
      state === "CLEARED" ? "#4caf50" :
      "#ccc";

    const plantLabel = alarm.power_plant_name ? alarm.power_plant_name : "—";
    const deviceLabel = alarm.device_type && alarm.device_name
      ? `${alarm.device_type} • ${alarm.device_name}`
      : (alarm.device_name || alarm.device_id || "—");

    const desc =
      alarm.event_name && String(alarm.event_name).trim() !== ""
        ? alarm.event_name
        : getAlarmDescription(alarm.event_code);

    tr.innerHTML = `
      <td>${plantLabel} • ${deviceLabel}</td>
      <td>${desc}</td>
      <td class="alarm-state-pill" style="font-weight:bold; color:${stateColor};">${state}</td>
      <td>${tsFormatted}</td>
    `;

    if (!isRecognized) {
      tr.classList.add("alarm-row-attention", `alarm-row-attention--${sev}`);
      const alarmId = alarm.alarm_id ?? alarm.id;
      tr.style.cursor = "pointer";
      tr.title = "Clique duplo para reconhecer";
      tr.addEventListener("dblclick", async () => {
        try {
          if (!alarmId) return;
          await acknowledgeAlarm(alarmId);
          await renderAlarmsTable(false);
        } catch (err) {
          console.error("Erro ao reconhecer alarme:", err);
        }
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

  const headers = Array.from(tr.querySelectorAll("th")).map(th => (th.textContent || "").trim().toLowerCase());
  if (headers.includes("severity")) return;

  const th = document.createElement("th");
  th.textContent = "SEVERITY";
  tr.appendChild(th);
}

async function loadEvents(page = 1, { silent = false } = {}) {
  const tbody = document.getElementById("eventsTbody");
  if (!tbody) return;

  if (EVENTS_STATE.loading) return;
  EVENTS_STATE.loading = true;

  try {
    wireEventsFiltersOnce();
    ensureDefaultEventsDates();
    ensureSeveritySelectOptions();
    ensureStateSelectOptions();
    ensureEventsHeaderHasSeverity(tbody);

    if (!silent) {
      tbody.innerHTML = `
        <tr><td colspan="6" style="text-align:center; opacity:0.7; padding:40px;">Carregando...</td></tr>
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
      source: filters.source,
      device_id: filters.device_id,
      q: filters.q,
      plant_id: filters.plant_id,
      mode: EVENTS_VIEW_MODE,
      rounds: EVENTS_ROUNDS,
      include_total: false
    });

    const events = response?.items || [];

    updateEventsPaginationUI({
      page,
      page_size: EVENTS_STATE.page_size,
      total: response?.pagination?.total ?? null,
      total_pages: response?.pagination?.total_pages ?? null
    });

    if (!events.length) {
      tbody.innerHTML = `
        <tr><td colspan="6" style="text-align:center; opacity:0.6; padding:40px;">
          Nenhum evento registrado
        </td></tr>
      `;
      return;
    }

    tbody.innerHTML = "";

    events.forEach(ev => {
      const tr = document.createElement("tr");

      const ts = ev.event_ts ? new Date(ev.event_ts).toLocaleString("pt-BR") : "—";

      const deviceLabel =
        ev.device_type && ev.device_name
          ? `${ev.device_type} • ${ev.device_name}`
          : (ev.device_name || ev.device_id || "—");

      const desc = valueOrDash(ev.event_name);
      const type = valueOrDash(ev.event_type);
      const plant = valueOrDash(ev.power_plant_name);
      const sev = valueOrDash(ev.severity);

      tr.innerHTML = `
        <td>${ts}</td>
        <td>${plant}</td>
        <td>${deviceLabel}</td>
        <td>${desc}</td>
        <td>${type}</td>
        <td style="font-weight:bold; color:${severityColor(sev)};">
          ${sev}
        </td>
      `;

      tbody.appendChild(tr);
    });

    EVENTS_STATE.page = page;
  } catch (err) {
    if (String(err?.name) === "AbortError") return;

    console.error("Erro ao buscar eventos:", err?.message, err?.url, err?.body);
    tbody.innerHTML = `
      <tr><td colspan="6" style="text-align:center; color:#f44336; padding:40px;">
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
  const validPlants = Array.isArray(plants) ? plants : [];

  let totalActivePower = 0;
  let totalRatedPower = 0;

  validPlants.forEach(p => {
    totalActivePower += Number(p.active_power_kw || 0);
    totalRatedPower += Number(p.rated_power_kw || 0);
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
}

function renderPortfolioTable(plants) {
  const tbody = document.getElementById("portfolioTbody");
  if (!tbody) return;

  const validPlants = Array.isArray(plants) ? plants : [];
  if (validPlants.length === 0) return;

  tbody.innerHTML = "";

  validPlants.forEach(plant => {
    const plantId = plant.power_plant_id ?? plant.plant_id ?? plant.id;
    const openPlantPage = () => {
      if (plantId == null) return;
      window.location.href = `plant.html?plant_id=${encodeURIComponent(plantId)}`;
    };

    const tr = document.createElement("tr");
    tr.classList.add("portfolio-row-linkable");
    tr.setAttribute("role", "link");
    tr.setAttribute("tabindex", "0");

    const alarmSeverity = normalizeAlarmSeverity(plant.alarm_severity);
    const plantIconClass = alarmSeverity ? `plant-icon plant-icon--${alarmSeverity}` : "plant-icon plant-icon--ok";

    tr.innerHTML = `
      <td>
        <button class="plant-cell-btn" title="Abrir usina ${valueOrDash(plant.power_plant_name)}">
          <span class="plant-cell">
            <span class="${plantIconClass}" title="${alarmSeverity || "ok"}">
              <i class="fa-solid fa-seedling"></i>
            </span>
            <span class="plant-name-text">${valueOrDash(plant.power_plant_name)}</span>
          </span>
        </button>
      </td>
      <td class="metric-neutral">${Number(plant.rated_power_kw ?? 0).toFixed(1)} kWp</td>
      <td class="metric-active">${Number(plant.active_power_kw ?? 0).toFixed(1)} kW</td>
      <td class="metric-active">${Number(plant.energy_today_kwh ?? 0).toFixed(1)} kWh</td>
      <td>${valueOrDash(plant.irradiance_wm2)} W/m²</td>
      <td>${plant.inverter_availability_pct != null ? (plant.inverter_availability_pct * 100).toFixed(1) + "%" : "—"}</td>
      <td>${plant.relay_availability_pct != null ? (plant.relay_availability_pct * 100).toFixed(1) + "%" : "—"}</td>
      <td>${plant.performance_ratio != null ? Number(plant.performance_ratio).toFixed(1) + "%" : "—"}</td>
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

    tbody.appendChild(tr);
  });
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
  diagram: document.getElementById("diagramView")
};

function showView(viewName) {
  localStorage.setItem("currentView", viewName);
  Object.values(views).forEach(v => { if (v) v.classList.add("hidden"); });
  if (views[viewName]) {
    views[viewName].classList.remove("hidden");
    animateViewEntrance(views[viewName]);
  }

  document.querySelectorAll(".sidebar-btn").forEach(b => b.classList.remove("active"));
  const activeBtn = document.getElementById(`btn${viewName.charAt(0).toUpperCase()}${viewName.slice(1)}`);
  if (activeBtn) activeBtn.classList.add("active");

  const topSummary = document.getElementById("topSummary");
  if (topSummary) topSummary.classList.remove("hidden");

  if (viewName === "events") {
    EVENTS_STATE.page = 1;
    loadEvents(1);
    startEventsAutoRefresh();
  } else {
    stopEventsAutoRefresh();
  }
}

document.getElementById("btnOverview")?.addEventListener("click", () => showView("overview"));

document.getElementById("btnAlarms")?.addEventListener("click", async () => {
  showView("alarms");
  document.querySelectorAll(".tab-btn").forEach(b => b.classList.remove("active"));
  const firstTab = document.querySelector(".tab-btn");
  if (firstTab) firstTab.classList.add("active");
  await renderAlarmsTable(false);
});

document.getElementById("btnEvents")?.addEventListener("click", () => showView("events"));

document.querySelectorAll(".tab-btn").forEach(btn => {
  btn.addEventListener("click", async () => {
    document.querySelectorAll(".tab-btn").forEach(b => b.classList.remove("active"));
    btn.classList.add("active");
    const isRecognized = btn.textContent.includes("RECONHECIDOS");
    await renderAlarmsTable(isRecognized);

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
  try {
    const [plants, alarms] = await Promise.all([
      fetchPlants(),
      fetchActiveAlarms()
    ]);
    if (Array.isArray(plants) && plants.length > 0) lastValidPlants = plants;

    lastAlarmSeverityByPlant = buildPlantAlarmSeverityMap(alarms);

    if (CURRENT_PLANT_ID == null) {
      CURRENT_PLANT_ID = loadSelectedPlantId();
    }

    if (CURRENT_PLANT_ID == null && lastValidPlants.length) {
      CURRENT_PLANT_ID = lastValidPlants[0].power_plant_id ?? lastValidPlants[0].plant_id ?? lastValidPlants[0].id;
      saveSelectedPlantId(CURRENT_PLANT_ID);
    }

    // ✅ chips globais via endpoint (fonte da verdade)
    try {
      const summary = await fetchPlantsSummary();
      refreshTopChipsGlobalFromSummary(summary);
    } catch (e) {
      console.warn("[SUMMARY] falhou, fallback via /plants:", e?.message || e);
      refreshTopChipsGlobalFromPlants(lastValidPlants);
    }

    const selected = lastValidPlants.find(
      p => (p.power_plant_id ?? p.plant_id ?? p.id) === CURRENT_PLANT_ID
    );
    if (selected) updateSummaryUI([selected]);
    else updateSummaryUI(lastValidPlants);

    renderPortfolioTable(lastValidPlants);
    await refreshVisibleViewData();
  } catch (err) {
    console.error("Erro ao atualizar dashboard:", err);

    try {
      const summary = await fetchPlantsSummary();
      refreshTopChipsGlobalFromSummary(summary);
    } catch (e) {
      console.warn("[SUMMARY] falhou, fallback via /plants:", e?.message || e);
      refreshTopChipsGlobalFromPlants(lastValidPlants);
    }

    const selected = lastValidPlants.find(
      p => (p.power_plant_id ?? p.plant_id ?? p.id) === CURRENT_PLANT_ID
    );
    if (selected) updateSummaryUI([selected]);
    else updateSummaryUI(lastValidPlants);

    renderPortfolioTable(lastValidPlants);
    await refreshVisibleViewData();
  }
}

document.addEventListener("DOMContentLoaded", async () => {
  const savedView = localStorage.getItem("currentView") || "overview";
  showView(savedView);

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

  document.querySelector(".logout-icon")?.addEventListener("click", logout);
  document.querySelector(".sidebar-logout")?.addEventListener("click", logout);
});
