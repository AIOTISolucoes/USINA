// =============================================================================
// 🔒 AUTH GUARD (ESTÁVEL E PROFISSIONAL)
// =============================================================================
(function authGuard() {
  const user = localStorage.getItem("user");
  if (!user) {
    window.location.href = "index.html";
    return;
  }

  // Volta para onde o usuário QUERIA ir antes de ser mandado para o login.
  // Quem grava isto é o plant.js, quando a notificação de alarme faz deep-link
  // com a sessão vencida. Fica aqui, e não no login.js, de propósito: o login
  // tem QUATRO caminhos de sucesso (senha, Google, troca de senha, sessão já
  // válida) e todos terminam em resumo.html. Tratando no destino, um lugar só
  // cobre os quatro — e cobre também qualquer caminho novo que apareça.
  try {
    const destino = localStorage.getItem("scada:after_login_redirect");
    if (destino) {
      localStorage.removeItem("scada:after_login_redirect");
      // só caminho interno, para o valor no localStorage nunca virar
      // redirecionamento para fora do site
      if (/^\/?[A-Za-z0-9_\-]+\.html(\?[^#]*)?$/.test(destino)) {
        window.location.replace(destino.replace(/^\//, ""));
        return;
      }
    }
  } catch (e) {}
})();

/**
 * Limpa a sessão e redireciona para o login.
 */
function logout() {
  localStorage.removeItem("user");
  localStorage.removeItem("currentView");
  window.location.href = "index.html";
}

function _dismissAppLoader() {
  const el = document.getElementById("appLoader");
  if (!el) return;
  el.classList.add("fade-out");
  setTimeout(() => el.remove(), 450);
}

// =============================================================================
// API FETCH COM CONTEXTO DO USUÁRIO LOGADO
// =============================================================================
const API_BASE = "https://jgeg9i0js1.execute-api.us-east-1.amazonaws.com";
const INVERTER_NO_COMM_AFTER_MS = 15 * 60 * 1000; // legado (chips usam status do mart)
const DASHBOARD_REFRESH_INTERVAL_MS = 30000;
const DS_DEFAULT_PALETTE = [
  "#4da3ff", "#39e58c", "#ffd84d", "#ff8a65",
  "#b39ddb", "#80cbc4", "#f06292", "#aed581",
  "#ffb74d", "#4dd0e1", "#ce93d8", "#a5d6a7"
];
const DS_SERIES_PALETTE = [];
// paleta VIVA: branding.js chama isto na troca/restauração de tema — a
// versão congelada no parse ficava com as cores do tema antigo pra sempre
window.__refreshDsPalette = () => {
  const base = (window.__BRAND_PALETTE && window.__BRAND_PALETTE.length ? window.__BRAND_PALETTE : DS_DEFAULT_PALETTE)
    .map((c) => (window.__brandRemap ? window.__brandRemap(c) : c));
  DS_SERIES_PALETTE.splice(0, DS_SERIES_PALETTE.length, ...base);
};
window.__refreshDsPalette();
const EVENTS_REFRESH_INTERVAL_MS = 10000;

// Sessão inválida: manda para o login UMA vez. Sem a trava, cada chamada da
// tela (são várias em paralelo) dispararia o próprio redirect.
function forceReloginOnce() {
  if (window.__aiotiRelogin) return;
  window.__aiotiRelogin = true;
  try { localStorage.removeItem("user"); } catch (e) {}
  window.location.replace("index.html?expirou=1");
}

function apiFetch(path, options = {}) {
  const user = JSON.parse(localStorage.getItem("user") || "{}");

  const headers = {
    ...(options.headers || {})
  };

  // X-Is-Superuser continua sendo enviado DE PROPÓSITO, mesmo tendo virado
  // inócuo: a API de 07/08 ignora esse header (era falsificável) e lê
  // is_superuser do banco, pelo token. Mantê-lo deixa este arquivo compatível
  // com a Lambda antiga também — assim o push do site e o deploy da Lambda
  // não precisam ser simultâneos, em qualquer ordem nada quebra.
  if (user.customer_id) headers["X-Customer-Id"] = user.customer_id;
  if (user.is_superuser === true) headers["X-Is-Superuser"] = "true";
  if (user.username) headers["X-Username"] = user.username;
  if (user.token) headers["Authorization"] = `Bearer ${user.token}`;

  return fetch(`${API_BASE}${path}`, {
    ...options,
    headers,
    cache: "no-store"
  }).then(r => {
    // Desde 07/08 toda rota de dado exige o token assinado. Quem estava com
    // sessão antiga no localStorage (salva antes de o token existir) passa a
    // receber 401 aqui. Sem este tratamento a tela ficava vazia e sem dizer
    // por quê — o usuário via "erro" onde a ação certa é relogar.
    if (r.status === 401) {
      r.clone().json().then(d => {
        if (d && (d.code === "token_required" || d.code === "token_invalid")) {
          forceReloginOnce();
        }
      }).catch(() => {});
    }
    return r;
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
function _canSendCommand() {
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
  catalogConfirmed: false,
  _currentIsFavorite: false
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
  if (s === "critical") return "#d50000";
  if (s === "high") return "#f44336";
  if (s === "medium") return "#ff9800";
  if (s === "low") return "#4caf50";
  return "#ccc";
}

// As 4 severidades do catálogo (tb_event_alarm_catalog.severity) precisam
// existir aqui. Até 05/08 esta função só conhecia high e medium: qualquer
// código marcado como "critical" virava null e o alarme era pintado de
// cinza e jogado para o FIM da ordenação, ou seja, o mais grave aparecia
// como o menos grave. Passou a importar quando a tela de catálogo deixou o
// operador escolher a severidade.
function normalizeAlarmSeverity(sev) {
  if (!sev) return null;
  const normalized = String(sev).toLowerCase();
  if (normalized === "critical" || normalized === "high" ||
      normalized === "medium" || normalized === "low") return normalized;
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
  const rank = { critical: 4, high: 3, medium: 2, low: 1 };
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

  if (r.total === 0 && lastValidPlants.length > 0) {
    console.warn("[INV CHIPS - GLOBAL] skipped: computed all zeros");
    return;
  }

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

  if (total === 0 && lastValidPlants.length > 0) {
    console.warn("[INV CHIPS - GLOBAL SUMMARY] skipped: summary returned all zeros");
    return;
  }

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

  // % de carga sobre a capacidade AC (o que a usina consegue entregar);
  // rated DC so como fallback se nao houver capacity_ac cadastrada
  const capacityBase = totalCapacityAc > 0 ? totalCapacityAc : totalRatedPower;
  const loadPct = capacityBase > 0 ? (totalActivePower / capacityBase) * 100 : 0;

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
  if (elPsfRatedAc) elPsfRatedAc.textContent = totalCapacityAc > 0 ? totalCapacityAc.toFixed(1) + " kW" : "—";
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

  const getAlarmPriority = (plant) => {
    const pid = plant.power_plant_id ?? plant.plant_id ?? plant.id;
    const pname = plant.power_plant_name ?? plant.plant_name ?? plant.name;
    const sev = normalizeAlarmSeverity(lastAlarmSeverityByPlant.get(pid))
      || normalizeAlarmSeverity(lastAlarmSeverityByPlant.get(pname))
      || normalizeAlarmSeverity(lastAlarmSeverityByPlant.get(Number(pid)))
      || null;
    if (sev === "critical") return 0;
    if (sev === "high") return 1;
    if (sev === "medium") return 2;
    if (sev === "low") return 3;
    return 4;
  };

  validPlants.sort((a, b) => {
    // Alarmes primeiro: high → medium → sem alarme
    const alarmA = getAlarmPriority(a);
    const alarmB = getAlarmPriority(b);
    if (alarmA !== alarmB) return alarmA - alarmB;

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

function _syncCardMaintBtn(btn, isOn) {
  btn.dataset.maintOn = isOn ? "1" : "0";
  btn.classList.toggle("is-on", isOn);
  btn.title = isOn
    ? "Encerrar manutenção do coletor local"
    : "Marcar coletor local em atualização";
}

async function togglePlantCollectorMaintenance(plantId, plantName, isOn) {
  const turnOn = !isOn;
  const msg = turnOn
    ? `Marcar a usina "${plantName}" como "ATUALIZANDO COLETOR LOCAL"?\n\nO aviso ficará visível para todos os usuários até ser encerrado.`
    : `Encerrar o modo "ATUALIZANDO COLETOR LOCAL" da usina "${plantName}"?`;
  if (!window.confirm(msg)) return;
  try {
    const res = await apiFetch(`/plants/${plantId}/maintenance`, {
      method: "PATCH",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ maintenance: turnOn })
    });
    const data = await res.json().catch(() => null);
    if (!res.ok) throw new Error(data?.error || `HTTP ${res.status}`);
    const btn = document.querySelector(`.plant-card__maint-btn[data-maint-plant-id="${plantId}"]`);
    if (btn) _syncCardMaintBtn(btn, data?.collector_maintenance === true);
    if (typeof refreshDashboard === "function") refreshDashboard();
  } catch (e) {
    console.error("[collector-maintenance] erro", e);
    alert("Falha ao alterar o modo manutenção: " + (e?.message || e));
  }
}

// --- Manutenção em campo (equipe DO CLIENTE) no card do portfólio ---------
// Espelha o can_flag_field_maintenance do api2.py. Se divergir, o botão
// aparece e a rota responde 403: confuso, mas não perigoso.
function canFlagFieldMaintenance() {
  let u = {};
  try { u = JSON.parse(localStorage.getItem("user") || "{}"); } catch { u = {}; }
  const p = u.permissions || {};
  return u.is_superuser === true || u.role_key === "admin_customer" ||
    u.role_key === "operator" || p.admin_customer === true ||
    p.plant_edit === true || p.remote_command === true;
}

function _syncCardFieldBtn(btn, isOn) {
  btn.dataset.fieldOn = isOn ? "1" : "0";
  btn.classList.toggle("is-on", isOn);
  btn.title = isOn
    ? "Encerrar manutenção em campo"
    : "Marcar manutenção em campo (sua equipe na usina)";
  btn.setAttribute("aria-label", btn.title);
}

async function togglePlantFieldMaintenance(plantId, plantName, isOn) {
  const turnOn = !isOn;
  let nota = null;
  if (turnOn) {
    const resp = window.prompt(
      `Marcar a usina "${plantName}" como EM MANUTENÇÃO EM CAMPO?\n\n` +
      "O aviso fica visível para todos até você encerrar, e a queda de geração\n" +
      "no período deixa de ser tratada como falha pelo diagnóstico.\n\n" +
      "Descreva o serviço (opcional):", "");
    if (resp === null) return;
    nota = resp.trim() || null;
  } else if (!window.confirm(`Encerrar a manutenção em campo da usina "${plantName}"?`)) {
    return;
  }
  try {
    const res = await apiFetch(`/plants/${plantId}/maintenance`, {
      method: "PATCH",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ maintenance: turnOn, scope: "field", note: nota })
    });
    const data = await res.json().catch(() => null);
    if (!res.ok) throw new Error(data?.hint || data?.error || `HTTP ${res.status}`);
    const btn = document.querySelector(`.plant-card__field-btn[data-field-plant-id="${plantId}"]`);
    if (btn) _syncCardFieldBtn(btn, data?.field_maintenance === true);
    if (typeof refreshDashboard === "function") refreshDashboard();
  } catch (e) {
    console.error("[field-maintenance] erro", e);
    alert("Falha ao alterar a manutenção em campo: " + (e?.message || e));
  }
}

function getPlantCardStatus(plant) {
  // Modo manutenção do coletor local tem prioridade sobre qualquer status:
  // a equipe está mexendo no CLP, então "offline" aqui NÃO é falha.
  if (plant.collector_maintenance === true) {
    return { colorClass: 'plant-card--maintenance', badge: 'Atualizando coletor local', badgeClass: 'badge--maintenance' };
  }
  // Manutenção da equipe DO CLIENTE. Vem depois do coletor de propósito: se as
  // duas estiverem ligadas, quem manda no que a tela diz é a nossa intervenção,
  // porque durante ela o dado em si pode estar inconsistente — na do cliente o
  // dado está certo, a usina é que está parada por escolha.
  if (plant.field_maintenance === true) {
    return { colorClass: 'plant-card--field-maint', badge: 'Manutenção em campo', badgeClass: 'badge--field-maint' };
  }
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
    const isMaintenance = commStatus.badgeClass === 'badge--maintenance';
    const isFieldMaint = commStatus.badgeClass === 'badge--field-maint';
    if (isMaintenance) tr.classList.add("portfolio-row--maintenance");
    else if (isFieldMaint) tr.classList.add("portfolio-row--field-maint");
    else if (plantState.isOffline || isCommOffline) tr.classList.add("portfolio-row--offline");

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
            <span class="plant-name-text" data-i18n-skip>${valueOrDash(plantName)}</span>
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

// --- Resolução e agregação REAIS (17/08/2026) -------------------------------
// Quem decide a resolução é o backend (datastudio_choose_hist_table), pelo
// tamanho da janela: até 2 dias vira 5 min, até 7 vira 15 min, até 15 vira
// horário, acima disso vira diário. O usuário não escolhe e, até hoje, não
// via. O cliente da Pedra Branca pediu julho inteiro, recebeu balde diário e
// concluiu que a plataforma estava "resumindo" o dado dele.
//
// O backend também já dizia, em `aggregation_resolved`, quando a agregação
// pedida não pôde ser aplicada (ex.: "avg(fallback:integral)"). Esse campo
// existia desde sempre e o front NUNCA o leu — degradação silenciosa, a mesma
// família do bug do assistente de IA em 15/08.
const DS_ROTA_LABEL = {
  hist_5min:   "5 min",
  hist_15min:  "15 min",
  hist_hourly: "Horário",
  hist_daily:  "Diário",
  hist_string: "5 min",
  timeseries:  "Bruto",
};

function dsRotuloResolucao(rota) {
  if (!rota) return null;
  return DS_ROTA_LABEL[rota] || String(rota).replace(/_/g, " ");
}

function dsAplicarResolucaoMedida(payloads) {
  const chip = document.getElementById("dsInfoResolution");
  if (!chip) return;

  const series = [];
  for (const p of payloads) {
    const lista = Array.isArray(p?.series) ? p.series : [];
    for (const s of lista) series.push(s);
  }
  if (!series.length) return;

  const rotas = [...new Set(series.map(s => dsRotuloResolucao(s?.resolved_route)).filter(Boolean))];
  // Rotas diferentes na mesma tela são possíveis (uma série consolidada e
  // outra histórica). Mostrar as duas é mais honesto que escolher uma.
  chip.textContent = rotas.length ? rotas.join(" + ") : "—";
  chip.dataset.medido = "1";

  // Agregação que REALMENTE foi aplicada. `aggregation_resolved` começando com
  // "avg(" significa que o pedido não pôde ser atendido e virou média.
  const degradadas = series.filter(s => {
    const r = String(s?.aggregation_resolved || "");
    return r.startsWith("avg(") && String(s?.aggregation || "") !== "avg";
  });

  let nota = document.getElementById("dsInfoAggNote");
  if (!nota) {
    nota = document.createElement("span");
    nota.id = "dsInfoAggNote";
    nota.style.cssText = "color:#f5b93d;margin-left:8px;font-size:0.85em;";
    chip.parentElement?.appendChild(nota);
  }

  if (degradadas.length) {
    const pedida = degradadas[0].aggregation;
    nota.textContent = `⚠ ${degradadas.length} série(s) sem "${pedida}"; usando média`;
    nota.title = degradadas
      .map(s => `${s.pathname}: ${s.aggregation_resolved}`)
      .join("\n");
  } else {
    nota.textContent = "";
    nota.title = "";
  }
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
      <div class="ds-plant-block__actions">
        <button class="ds-plant-action-btn" data-action="favoritePlant" data-plant="${pid}" type="button" title="Favoritar seleção desta usina">
          <i class="fa-regular fa-star"></i> <span>Favoritar</span>
        </button>
        <button class="ds-plant-action-btn" data-action="exportPlant" data-plant="${pid}" type="button" title="Exportar CSV desta usina">
          <i class="fa-solid fa-download"></i> <span>CSV</span>
        </button>
        <button class="ds-plant-action-btn ds-plant-action-btn--danger" data-action="removePlant" data-plant="${pid}" type="button" title="Remover esta usina">
          <i class="fa-solid fa-xmark"></i>
        </button>
      </div>
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

        <div style="display:flex;align-items:center;gap:8px;flex-wrap:wrap;">
          <div class="ds-v2-found-count" id="dsFoundCount_${pid}" style="flex:1;min-width:120px;">0 medidas encontradas</div>
          <button class="ds-v2-select-all-btn" data-action="selectAllTags" data-plant="${pid}" type="button">Selecionar todas</button>
          <button class="ds-v2-select-all-btn" data-action="deselectAllTags" data-plant="${pid}" type="button" style="display:none;">Desmarcar todas</button>
        </div>
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
  dsUpdateSelectAllButtons(plantId);
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

// --- Select all / deselect all ---

// Limite específico para corrente de string: cada string vira uma consulta à hist_string no
// backend; muitas de uma vez (ainda mais em janelas longas) saturam o banco de 2 vCPU e estouram
// o timeout do gateway (erro 503 → gráfico vazio). Por isso capamos o nº de séries de string.
const DS_MAX_STRING_SERIES = 24;
function _dsIsStringTag(tag) {
  const p = dsSafeTrim(tag?.pathname || tag?.path_name || tag?.tag);
  return /^INV_\d+\.string_current_\d+$/.test(p);
}
function _dsCountSelectedStrings(ps) {
  return (ps?.selectedTags || []).filter(_dsIsStringTag).length;
}

function dsSelectAllTagsForPlant(plantId) {
  const ps = DATASTUDIO_PLANTS[String(plantId)];
  if (!ps) return;
  const visible = Array.isArray(ps.availableTags) ? ps.availableTags : [];
  let added = 0, skippedStrings = 0;
  let stringCount = _dsCountSelectedStrings(ps);
  for (const tag of visible) {
    if (ps.selectedTags.length >= 50) break;
    const pathname = dsSafeTrim(tag?.pathname || tag?.path_name || tag?.tag);
    if (!pathname) continue;
    if (isTagSelectedForPlant(plantId, tag)) continue;
    if (_dsIsStringTag(tag)) {
      if (stringCount >= DS_MAX_STRING_SERIES) { skippedStrings++; continue; }
      stringCount++;
    }
    ps.selectedTags.push({
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
    added++;
  }
  if (added) {
    updateStageUIForPlant(plantId);
    renderDataStudioTagsTableForPlant(plantId, visible);
  }
  if (skippedStrings) {
    window.alert(`Foram adicionadas até ${DS_MAX_STRING_SERIES} correntes de string (limite por vez para não sobrecarregar o servidor). ${skippedStrings} não foram incluídas — plote em lotes menores.`);
  }
  dsUpdateSelectAllButtons(plantId);
}

function dsDeselectAllTagsForPlant(plantId) {
  const ps = DATASTUDIO_PLANTS[String(plantId)];
  if (!ps) return;
  const visible = Array.isArray(ps.availableTags) ? ps.availableTags : [];
  const visibleKeys = new Set(visible.map(t => selectedTagKey(t)).filter(Boolean));
  ps.selectedTags = ps.selectedTags.filter(t => !visibleKeys.has(selectedTagKey(t)));
  updateStageUIForPlant(plantId);
  renderDataStudioTagsTableForPlant(plantId, visible);
  dsUpdateSelectAllButtons(plantId);
}

function dsUpdateSelectAllButtons(plantId) {
  const ps = DATASTUDIO_PLANTS[String(plantId)];
  const visible = Array.isArray(ps?.availableTags) ? ps.availableTags : [];
  const allSelected = visible.length > 0 && visible.every(t => isTagSelectedForPlant(plantId, t));
  const btnSel = document.querySelector(`[data-action="selectAllTags"][data-plant="${plantId}"]`);
  const btnDes = document.querySelector(`[data-action="deselectAllTags"][data-plant="${plantId}"]`);
  if (btnSel) btnSel.style.display = allSelected ? "none" : "";
  if (btnDes) btnDes.style.display = allSelected ? "" : "none";
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
  if (_dsIsStringTag(tag) && _dsCountSelectedStrings(ps) >= DS_MAX_STRING_SERIES) {
    window.alert(`Para não sobrecarregar o servidor, você pode plotar no máximo ${DS_MAX_STRING_SERIES} correntes de string por vez. Remova alguma para adicionar outra.`);
    return false;
  }
  ps.selectedTags.push(tag);
  updateStageUIForPlant(plantId);
  return true;
}

function removeSelectedTagForPlant(plantId, tagOrPath) {
  const ps = DATASTUDIO_PLANTS[String(plantId)];
  if (!ps) return;
  // selectedTagKey devolve "id:<id>" para tags com id e "path:<pathname>" para
  // strings — comparar só pela key nunca casa quando o chip remove por pathname.
  const key = selectedTagKey(tagOrPath);
  const path = dsSafeTrim(typeof tagOrPath === "string" ? tagOrPath : tagOrPath?.pathname);
  ps.selectedTags = ps.selectedTags.filter(t => {
    if (selectedTagKey(t) === key) return false;
    if (path && dsSafeTrim(t?.pathname) === path) return false;
    return true;
  });
  updateStageUIForPlant(plantId);
  _removeSeriesFromRenderedChart(plantId, path);
}

function _removeSeriesFromRenderedChart(plantId, pathname) {
  if (!pathname) return;
  const pid = String(plantId);
  const ps = DATASTUDIO_PLANTS[pid];
  const payload = DATASTUDIO_STATE.seriesByPlant?.[pid];
  if (!ps || !ps.chartInstance || !payload) return;
  const series = _extractSeriesList(payload).filter(s => dsSafeTrim(s?.pathname) !== pathname);
  const newPayload = { ...(typeof payload === "object" && !Array.isArray(payload) ? payload : {}), series };
  DATASTUDIO_STATE.seriesByPlant[pid] = newPayload;
  renderChartForPlant(pid, newPayload);
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
  const showWorkspace = (hasSelection && ps.catalogConfirmed) || ps.loadedFromFavorite;

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

async function confirmCatalogSelectionForPlant(plantId) {
  const ps = DATASTUDIO_PLANTS[String(plantId)];
  if (!ps) return;
  if (!ps.selectedTags.length) {
    window.alert("Selecione ao menos uma medida antes de confirmar.");
    return;
  }
  ps.catalogConfirmed = true;
  ps.catalogOpen = false;
  updateStageUIForPlant(plantId);

  const chartWrap = document.getElementById(`dsChartWrap_${plantId}`);
  if (chartWrap) {
    chartWrap.style.position = "relative";
    let loader = chartWrap.querySelector(".chart-loader");
    if (!loader) {
      loader = document.createElement("div");
      loader.className = "chart-loader";
      loader.innerHTML = '<i class="fa-solid fa-spinner fa-spin"></i>';
      chartWrap.prepend(loader);
    }
    loader.classList.remove("hidden");
  }

  try {
    const payload = buildPayloadForPlant(plantId, ps.selectedTags);
    const res = await apiFetch("/datastudio/selection", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload)
    });
    const data = await res.json().catch(() => ({}));
    const parsed = dsNormalizeApiBody(data);
    const selectionId = parsed?.selection_id ?? parsed?.id ?? null;
    if (selectionId) {
      ps.selectionId = selectionId;
      DATASTUDIO_STATE.selectionIdsByPlant[String(plantId)] = selectionId;
      DATASTUDIO_STATE.selectionId = DATASTUDIO_STATE.selectionId || selectionId;
    }

    if (selectionId) {
      const seriesRes = await apiFetch(`/datastudio/series?selection_id=${selectionId}`);
      if (seriesRes.ok) {
        const seriesData = await seriesRes.json();
        const seriesParsed = dsNormalizeApiBody(seriesData);
        DATASTUDIO_STATE.seriesByPlant[String(plantId)] = seriesParsed;
        renderChartForPlant(String(plantId), seriesParsed);
      }
    }
  } catch (e) {
    console.error("[DataStudio] erro ao confirmar e plotar:", e);
    window.alert(`Erro ao carregar gráfico: ${e.message || e}`);
  } finally {
    const loader = chartWrap?.querySelector(".chart-loader");
    if (loader) loader.classList.add("hidden");
  }
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

    const rawTags = normalizeTagsResponse(parsed);

    // app.tag_catalog pode ter o mesmo pathname em mais de uma linha (ids
    // diferentes) — só uma rota devolve dados. Mantém uma por pathname,
    // priorizando source=historico.
    const byPath = new Map();
    for (const t of rawTags) {
      const p = dsSafeTrim(t?.pathname || t?.path_name || t?.tag);
      if (!p) continue;
      const prev = byPath.get(p);
      if (!prev) { byPath.set(p, t); continue; }
      const rank = x => (dsSafeTrim(x?.source).toLowerCase() === "historico" ? 0 : 1);
      if (rank(t) < rank(prev)) byPath.set(p, t);
    }
    const tags = [...byPath.values()];
    if (tags.length < rawTags.length) {
      console.warn(`[DataStudio] Plant ${plantId}: ${rawTags.length - tags.length} tags duplicadas (mesmo pathname) removidas do catálogo`);
    }

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
    // Sem isto a API responde 401 desde 07/08. Este fetch e cru (precisa do
    // corpo bruto do CSV), entao nao passa pelo apiFetch e o token tem que
    // ser posto na mao - foi assim que este export ficou quebrado.
    if (user.token) headers["Authorization"] = `Bearer ${user.token}`;

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
    updateDsFavoriteToggleBtn();
  } catch (err) {
    console.error("[DataStudio] erro ao salvar seleção:", err);
    window.alert(`Não foi possível salvar a seleção: ${err.message || err}`);
  } finally {
    setDataStudioSavingSelection(false);
  }
}

// ============================================================
// DATA STUDIO — FAVORITOS
// ============================================================

function updateDsFavoriteToggleBtn() {
  const btn = document.getElementById("dsFavoriteToggleBtn");
  if (!btn) return;
  const hasSelectionId = !!DATASTUDIO_STATE.selectionId || Object.keys(DATASTUDIO_STATE.selectionIdsByPlant).length > 0;
  const hasTags = Object.keys(DATASTUDIO_PLANTS).some(pid => DATASTUDIO_PLANTS[pid]?.selectedTags?.length > 0);
  btn.disabled = !hasSelectionId && !hasTags;

  const isFav = !!DATASTUDIO_STATE._currentIsFavorite;
  const icon = btn.querySelector("i");
  if (icon) {
    icon.className = isFav ? "fa-solid fa-star" : "fa-regular fa-star";
  }
  btn.style.color = isFav ? "#ffd84d" : "";
  const label = btn.childNodes[btn.childNodes.length - 1];
  if (label && label.nodeType === 3) {
    label.textContent = isFav ? " Favoritado" : " Favoritar";
  }
}

async function toggleDataStudioFavorite() {
  let selectionId = DATASTUDIO_STATE.selectionId || Object.values(DATASTUDIO_STATE.selectionIdsByPlant)[0];

  // Se não tem seleção salva ainda, salva automaticamente antes de favoritar
  if (!selectionId) {
    const plantIds = Object.keys(DATASTUDIO_PLANTS);
    const anyTags = plantIds.some(pid => DATASTUDIO_PLANTS[pid]?.selectedTags?.length > 0);
    if (!anyTags) {
      window.alert("Selecione medidas antes de favoritar.");
      return;
    }
    await saveDataStudioSelection();
    selectionId = DATASTUDIO_STATE.selectionId || Object.values(DATASTUDIO_STATE.selectionIdsByPlant)[0];
    if (!selectionId) return;
  }

  const makeFavorite = !DATASTUDIO_STATE._currentIsFavorite;

  try {
    const res = await apiFetch("/datastudio/favorite", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        selection_id: selectionId,
        is_favorite: makeFavorite,
      }),
    });
    const data = await res.json();
    if (data?.ok) {
      DATASTUDIO_STATE._currentIsFavorite = data.is_favorite;
      updateDsFavoriteToggleBtn();
    }
  } catch (e) {
    console.warn("[datastudio] favorite toggle falhou:", e);
  }
}

// Favoritar por planta individual
async function toggleFavoriteForPlant(plantId) {
  const ps = DATASTUDIO_PLANTS[String(plantId)];
  if (!ps) return;

  let selectionId = ps.selectionId || DATASTUDIO_STATE.selectionIdsByPlant[String(plantId)];

  // Se não tem seleção salva, salva automaticamente
  if (!selectionId) {
    if (!ps.selectedTags?.length) {
      window.alert("Selecione medidas nesta usina antes de favoritar.");
      return;
    }
    // Salvar seleção só desta planta
    try {
      const payload = buildPayloadForPlant(plantId, ps.selectedTags);
      const res = await apiFetch("/datastudio/selection", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload)
      });
      const data = await res.json().catch(() => ({}));
      const parsed = dsNormalizeApiBody(data);
      selectionId = parsed?.selection_id ?? parsed?.id ?? null;
      if (selectionId) {
        ps.selectionId = selectionId;
        DATASTUDIO_STATE.selectionIdsByPlant[String(plantId)] = selectionId;
      }
    } catch (e) {
      console.warn("[datastudio] save before favorite falhou:", e);
      return;
    }
  }

  if (!selectionId) return;

  const currentlyFav = !!ps._isFavorite;
  const makeFavorite = !currentlyFav;

  try {
    const res = await apiFetch("/datastudio/favorite", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ selection_id: selectionId, is_favorite: makeFavorite }),
    });
    const data = await res.json();
    if (data?.ok) {
      ps._isFavorite = data.is_favorite;
      _updatePlantFavBtn(plantId);
    }
  } catch (e) {
    console.warn("[datastudio] favorite toggle falhou:", e);
  }
}

function _updatePlantFavBtn(plantId) {
  const btn = document.querySelector(`[data-action="favoritePlant"][data-plant="${plantId}"]`);
  if (!btn) return;
  const ps = DATASTUDIO_PLANTS[String(plantId)];
  const isFav = !!ps?._isFavorite;
  const icon = btn.querySelector("i");
  const label = btn.querySelector("span");
  if (icon) icon.className = isFav ? "fa-solid fa-star" : "fa-regular fa-star";
  if (label) label.textContent = isFav ? "Favoritado" : "Favoritar";
  btn.classList.toggle("ds-plant-fav-active", isFav);
}

// Exportar CSV por planta individual
async function exportDataStudioSelectionForPlant(plantId) {
  const selId = DATASTUDIO_STATE.selectionIdsByPlant[String(plantId)];
  if (!selId) {
    window.alert("Salve uma seleção desta usina antes de exportar.");
    return;
  }

  const btn = document.querySelector(`[data-action="exportPlant"][data-plant="${plantId}"]`);
  const oldHtml = btn ? btn.innerHTML : "";
  try {
    if (btn) { btn.disabled = true; btn.innerHTML = '<i class="fa-solid fa-spinner fa-spin"></i>'; }
    const url = `${API_BASE}/datastudio/export?selection_id=${encodeURIComponent(selId)}`;
    const user = JSON.parse(localStorage.getItem("user") || "{}");
    const headers = {};
    if (user.customer_id) headers["X-Customer-Id"] = user.customer_id;
    if (user.is_superuser === true) headers["X-Is-Superuser"] = "true";
    // ver comentario no export de varias usinas: fetch cru precisa do token
    if (user.token) headers["Authorization"] = `Bearer ${user.token}`;
    const res = await fetch(url, { headers, cache: "no-store" });
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    const blob = await res.blob();
    const a = document.createElement("a");
    a.href = URL.createObjectURL(blob);
    a.download = `datastudio_usina_${plantId}.csv`;
    document.body.appendChild(a);
    a.click();
    a.remove();
    URL.revokeObjectURL(a.href);
  } catch (e) {
    console.warn("[datastudio] export falhou:", e);
    window.alert("Falha ao exportar CSV.");
  } finally {
    if (btn) { btn.disabled = false; btn.innerHTML = oldHtml; }
  }
}

// Remover plant block
function removePlantBlock(plantId) {
  const pidStr = String(plantId);
  const pidNum = Number(plantId);

  // Destroi chart
  const ps = DATASTUDIO_PLANTS[pidStr];
  if (ps && ps.chartInstance) { try { ps.chartInstance.destroy(); } catch(e){} }
  delete DATASTUDIO_PLANTS[pidStr];
  delete DATASTUDIO_CHART[pidStr];
  delete DATASTUDIO_STATE.selectionIdsByPlant[pidStr];
  delete DATASTUDIO_STATE.seriesByPlant[pidStr];

  // Remove do selectedPlantIds
  DATASTUDIO_STATE.selectedPlantIds = DATASTUDIO_STATE.selectedPlantIds.filter(id => id !== pidNum);

  // Remove o DOM
  const el = document.getElementById(`dsPlantBlock_${pidStr}`);
  if (el) el.remove();

  // Desmarca checkbox no multiselect
  const dropdown = document.getElementById("dsPlantMultiselectDropdown");
  if (dropdown) {
    const cb = dropdown.querySelector(`input[type='checkbox'][value='${pidNum}']`);
    if (cb) cb.checked = false;
  }
  if (typeof _syncMultiselectLabel === "function") _syncMultiselectLabel();

  // Se não sobrou nenhuma planta, mostra empty state
  if (!DATASTUDIO_STATE.selectedPlantIds.length) {
    const container = document.getElementById("dsPlantBlocks");
    const noPlantState = document.getElementById("dsNoPlantState");
    if (container && noPlantState) {
      container.appendChild(noPlantState);
      noPlantState.style.display = "";
    }
    DATASTUDIO_STATE.selectionId = null;
  }

  updateDataStudioExportButton();
}

async function fetchDataStudioFavorites() {
  try {
    const res = await apiFetch("/datastudio/selections?favorites_only=true");
    const data = await res.json();
    return Array.isArray(data?.selections) ? data.selections : [];
  } catch (e) {
    console.warn("[datastudio] fetch favorites falhou:", e);
    return [];
  }
}

let _dsFavoritesCache = [];

async function renderDsFavoritesPanel() {
  const panel = document.getElementById("dsFavoritesPanel");
  const list = document.getElementById("dsFavoritesList");
  if (!panel || !list) return;

  list.innerHTML = '<p class="ds-favorites-empty">Carregando...</p>';
  panel.style.display = "";

  const favorites = await fetchDataStudioFavorites();
  _dsFavoritesCache = favorites;

  if (!favorites.length) {
    list.innerHTML = '<p class="ds-favorites-empty">Nenhum favorito salvo ainda.</p>';
    return;
  }

  list.innerHTML = "";

  for (const fav of favorites) {
    const item = document.createElement("div");
    item.className = "ds-fav-item";

    const startStr = fav.start_ts ? new Date(fav.start_ts).toLocaleDateString("pt-BR") : "—";
    const endStr = fav.end_ts ? new Date(fav.end_ts).toLocaleDateString("pt-BR") : "—";
    const plantName = fav.power_plant_name || `Usina #${fav.power_plant_id}`;
    const itemsCount = fav.items_count || 0;

    const title = fav.items_labels || fav.selection_name || "Seleção sem nome";

    item.innerHTML = `
      <input type="checkbox" class="ds-fav-item-check" data-fav-id="${fav.id}" />
      <div class="ds-fav-item-info">
        <div class="ds-fav-item-plant">
          <img src="img/logo-plant.svg" class="ds-fav-plant-icon" alt="" />
          <span>${escapeHtml(plantName)}</span>
        </div>
        <div class="ds-fav-item-name">${escapeHtml(title)}</div>
        <div class="ds-fav-item-meta">${startStr} a ${endStr} &middot; ${itemsCount} medida${itemsCount !== 1 ? "s" : ""}</div>
      </div>
      <button class="ds-fav-item-star" title="Desfavoritar" data-id="${fav.id}">
        <i class="fa-solid fa-star"></i>
      </button>
    `;

    // Clicar em qualquer lugar da row toggle o checkbox
    item.addEventListener("click", (e) => {
      // Ignora se clicou na estrela ou no próprio checkbox
      if (e.target.closest(".ds-fav-item-star")) return;
      if (e.target.classList.contains("ds-fav-item-check")) return;
      const cb = item.querySelector(".ds-fav-item-check");
      if (cb) cb.checked = !cb.checked;
    });

    // Clicar na estrela desfavorita
    item.querySelector(".ds-fav-item-star").addEventListener("click", async (e) => {
      e.stopPropagation();
      await toggleFavoriteById(fav.id, false);
      await renderDsFavoritesPanel();
    });

    list.appendChild(item);
  }

  // Botão de carregar selecionados
  const loadBar = document.createElement("div");
  loadBar.className = "ds-favorites-load-bar";
  loadBar.innerHTML = `
    <button class="ds-favorites-load-btn" id="dsFavLoadSelectedBtn" type="button">
      <i class="fa-solid fa-chart-line"></i> Carregar selecionados
    </button>
  `;
  list.appendChild(loadBar);

  document.getElementById("dsFavLoadSelectedBtn")?.addEventListener("click", () => {
    const checked = list.querySelectorAll(".ds-fav-item-check:checked");
    if (!checked.length) {
      window.alert("Selecione ao menos um favorito.");
      return;
    }
    const selectedIds = new Set();
    checked.forEach(cb => selectedIds.add(Number(cb.dataset.favId)));
    const selectedFavs = _dsFavoritesCache.filter(f => selectedIds.has(Number(f.id)));
    console.log("[datastudio] favoritos selecionados:", selectedFavs.length, selectedFavs.map(f => f.id));
    loadFavoriteSelections(selectedFavs);
  });
}

async function toggleFavoriteById(selectionId, makeFavorite) {
  try {
    const res = await apiFetch("/datastudio/favorite", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ selection_id: selectionId, is_favorite: makeFavorite }),
    });
    const data = await res.json();
    return data?.is_favorite ?? null;
  } catch (e) {
    console.warn("[datastudio] favorite toggle falhou:", e);
    return null;
  }
}

async function loadFavoriteSelections(favs) {
  if (!favs.length) return;

  // Fecha o painel
  const panel = document.getElementById("dsFavoritesPanel");
  if (panel) panel.style.display = "none";

  // Limpa estado anterior
  Object.keys(DATASTUDIO_PLANTS).forEach(pid => {
    const ps = DATASTUDIO_PLANTS[pid];
    if (ps && ps.chartInstance) { try { ps.chartInstance.destroy(); } catch(e){} }
  });
  DATASTUDIO_PLANTS = {};
  DATASTUDIO_CHART = {};
  DATASTUDIO_STATE.seriesByPlant = {};
  DATASTUDIO_STATE.selectionIdsByPlant = {};
  DATASTUDIO_STATE.chartData = null;

  // Agrupa favoritos por planta (pode ter múltiplos da mesma usina)
  const favsByPlant = {};
  for (const fav of favs) {
    const pid = String(fav.power_plant_id);
    if (!favsByPlant[pid]) favsByPlant[pid] = [];
    favsByPlant[pid].push(fav);
  }

  const plantIds = Object.keys(favsByPlant).map(Number);

  // O primeiro selection_id fica no state (para o botão favoritar funcionar)
  DATASTUDIO_STATE.selectedPlantIds = plantIds;
  DATASTUDIO_STATE.selectionIdsByPlant = {};
  for (const pid of Object.keys(favsByPlant)) {
    DATASTUDIO_STATE.selectionIdsByPlant[pid] = favsByPlant[pid][0].id;
  }
  DATASTUDIO_STATE.selectionId = favs[0].id;
  DATASTUDIO_STATE._currentIsFavorite = true;

  // Sincroniza checkboxes do multiselect de usinas
  const dropdown = document.getElementById("dsPlantMultiselectDropdown");
  if (dropdown) {
    dropdown.querySelectorAll("input[type='checkbox']").forEach(cb => {
      cb.checked = plantIds.includes(Number(cb.value));
    });
    if (typeof _syncMultiselectLabel === "function") _syncMultiselectLabel();
  }

  renderPlantBlocks();

  // Buscar tags do catálogo para cada usina
  for (const pid of plantIds) {
    const ps = DATASTUDIO_PLANTS[String(pid)];
    if (ps && !ps.catalogTags.length) {
      fetchDataStudioTagsForPlant(String(pid));
    }
  }

  // Forçar workspace visível em cada plant block
  for (const pid of Object.keys(favsByPlant)) {
    const ps = DATASTUDIO_PLANTS[pid];
    if (ps) {
      ps.catalogConfirmed = true;
      ps.loadedFromFavorite = true;
      ps._isFavorite = true;
      updateStageUIForPlant(pid);
      _updatePlantFavBtn(pid);
    }
  }

  // Atualizar datas para hoje (não ficar preso na data antiga)
  if (typeof dsAutoSetDateRange === "function") dsAutoSetDateRange();

  // Mostrar spinners nos chart wraps
  for (const pid of Object.keys(favsByPlant)) {
    const chartWrap = document.getElementById(`dsChartWrap_${pid}`);
    if (chartWrap) {
      chartWrap.style.position = "relative";
      let loader = chartWrap.querySelector(".chart-loader");
      if (!loader) {
        loader = document.createElement("div");
        loader.className = "chart-loader";
        loader.innerHTML = '<i class="fa-solid fa-spinner fa-spin"></i>';
        chartWrap.prepend(loader);
      }
      loader.classList.remove("hidden");
    }
  }

  // Buscar séries de TODOS os favoritos e mergear por planta
  setDataStudioLoadingSeries(true);
  try {
    DATASTUDIO_STATE.seriesByPlant = {};

    const filters = getDataStudioMainFilters();
    const dateOverrides = {};
    if (filters.start_ts) dateOverrides.start_ts = filters.start_ts;
    if (filters.end_ts) dateOverrides.end_ts = filters.end_ts;

    for (const [pid, plantFavs] of Object.entries(favsByPlant)) {
      let mergedSeries = [];

      for (const fav of plantFavs) {
        const params = new URLSearchParams({ selection_id: String(fav.id), ...dateOverrides });
        const res = await apiFetch(`/datastudio/series?${params.toString()}`);
        if (!res.ok) throw new Error(`Falha ao carregar séries para fav ${fav.id} (${res.status})`);
        const data = await res.json();
        const parsed = dsNormalizeApiBody(data);
        const seriesList = _extractSeriesList(parsed);
        mergedSeries = mergedSeries.concat(seriesList);
      }

      const mergedPayload = { series: mergedSeries };
      DATASTUDIO_STATE.seriesByPlant[pid] = mergedPayload;

      const ps = DATASTUDIO_PLANTS[pid];
      if (ps) {
        ps.catalogConfirmed = true;
        const favTags = mergedSeries.map(s => ({ pathname: s.pathname, ...s }));
        ps.selectedTags = favTags;
        updateStageUIForPlant(pid);
      }
      renderChartForPlant(pid, mergedPayload);
    }

    // Mesmo motivo do caminho principal: o favorito tambem passa pelo
    // /datastudio/series e tambem pode cair em balde diario ou ter a
    // agregacao degradada. Corrigir so um dos dois deixaria metade da tela
    // mentindo -- foi exatamente o que aconteceu com o alarme.py em 14/08.
    dsAplicarResolucaoMedida(Object.values(DATASTUDIO_STATE.seriesByPlant));
  } catch (err) {
    console.error("[DataStudio] erro ao carregar favoritos:", err);
    window.alert(`Não foi possível carregar favoritos: ${err.message || err}`);
  } finally {
    setDataStudioLoadingSeries(false);
    for (const pid of Object.keys(favsByPlant)) {
      const loader = document.getElementById(`dsChartWrap_${pid}`)?.querySelector(".chart-loader");
      if (loader) loader.classList.add("hidden");
    }
  }
}

function escapeHtml(str) {
  const div = document.createElement("div");
  div.textContent = str;
  return div.innerHTML;
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

  for (const [plantId] of selEntries) {
    const chartWrap = document.getElementById(`dsChartWrap_${plantId}`);
    if (chartWrap) {
      chartWrap.style.position = "relative";
      let loader = chartWrap.querySelector(".chart-loader");
      if (!loader) {
        loader = document.createElement("div");
        loader.className = "chart-loader";
        loader.innerHTML = '<i class="fa-solid fa-spinner fa-spin"></i>';
        chartWrap.prepend(loader);
      }
      loader.classList.remove("hidden");
    }
  }

  setDataStudioLoadingSeries(true);
  try {
    DATASTUDIO_STATE.seriesByPlant = {};

    const filters = getDataStudioMainFilters();
    const dateOverrides = {};
    if (filters.start_ts) dateOverrides.start_ts = filters.start_ts;
    if (filters.end_ts) dateOverrides.end_ts = filters.end_ts;

    for (const [plantId, selId] of selEntries) {
      const params = new URLSearchParams({ selection_id: String(selId), ...dateOverrides });
      const res = await apiFetch(`/datastudio/series?${params.toString()}`);
      if (!res.ok) throw new Error(`Falha ao carregar séries (${res.status})`);

      const data = await res.json();
      const parsed = dsNormalizeApiBody(data);
      DATASTUDIO_STATE.seriesByPlant[plantId] = parsed;
    }

    for (const [plantId, seriesPayload] of Object.entries(DATASTUDIO_STATE.seriesByPlant)) {
      const ps = DATASTUDIO_PLANTS[plantId];
      if (ps) {
        ps.catalogConfirmed = true;
        updateStageUIForPlant(plantId);
      }
      renderChartForPlant(plantId, seriesPayload);
    }

    // Só aqui a resolução real é conhecida: ela vem na resposta, não da
    // escolha do usuário. Antes disso o chip mostra apenas um palpite.
    dsAplicarResolucaoMedida(Object.values(DATASTUDIO_STATE.seriesByPlant));
  } catch (err) {
    console.error("[DataStudio] erro ao carregar séries:", err);
    window.alert(`Não foi possível carregar séries: ${err.message || err}`);
  } finally {
    setDataStudioLoadingSeries(false);
    for (const [plantId] of selEntries) {
      const loader = document.getElementById(`dsChartWrap_${plantId}`)?.querySelector(".chart-loader");
      if (loader) loader.classList.add("hidden");
    }
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
  DATASTUDIO_STATE._currentIsFavorite = false;

  const loadSeriesBtn = document.getElementById("dsLoadSeriesBtn");
  const saveSelectionBtn = document.getElementById("dsSaveSelectionBtn");
  if (loadSeriesBtn) { loadSeriesBtn.disabled = true; loadSeriesBtn.textContent = "Carregar séries"; }
  if (saveSelectionBtn) saveSelectionBtn.disabled = false;

  updateDsFavoriteToggleBtn();
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
    start.setDate(now.getDate() - 1);
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
      case "selectAllTags":
        dsSelectAllTagsForPlant(plantId);
        break;
      case "deselectAllTags":
        dsDeselectAllTagsForPlant(plantId);
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
      case "favoritePlant":
        toggleFavoriteForPlant(plantId);
        break;
      case "exportPlant":
        exportDataStudioSelectionForPlant(plantId);
        break;
      case "removePlant":
        removePlantBlock(plantId);
        break;
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
  const applyBottomBtn = document.getElementById("dsApplyBottomBtn");

  loadSeriesBtn?.addEventListener("click", () => saveDataStudioSelection());
  saveSelectionBtn?.addEventListener("click", saveDataStudioSelection);
  exportBtn?.addEventListener("click", exportDataStudioSelection);
  applyBottomBtn?.addEventListener("click", () => {
    if (typeof window.applyAllPlants === "function") window.applyAllPlants();
  });

  const exportAllBtn = document.getElementById("dsExportAllBtn");
  exportAllBtn?.addEventListener("click", exportDataStudioSelection);

  // Favoritos
  const favToggleBtn = document.getElementById("dsFavoriteToggleBtn");
  const favListBtn = document.getElementById("dsFavoritesListBtn");
  const favPanelClose = document.getElementById("dsFavoritesPanelClose");

  favToggleBtn?.addEventListener("click", toggleDataStudioFavorite);
  favListBtn?.addEventListener("click", () => {
    const panel = document.getElementById("dsFavoritesPanel");
    if (panel && panel.style.display !== "none") {
      panel.style.display = "none";
    } else {
      renderDsFavoritesPanel();
    }
  });
  favPanelClose?.addEventListener("click", () => {
    const panel = document.getElementById("dsFavoritesPanel");
    if (panel) panel.style.display = "none";
  });

  clearAllBtn?.addEventListener("click", () => {
    DATASTUDIO_STATE.selectionId = null;
    DATASTUDIO_STATE.selectionIdsByPlant = {};
    DATASTUDIO_STATE.seriesByPlant = {};
    DATASTUDIO_STATE.chartData = null;
    DATASTUDIO_STATE.selectedPlantIds = [];
    DATASTUDIO_STATE.selectedPlantId = null;
    DATASTUDIO_STATE._currentIsFavorite = false;
    updateDsFavoriteToggleBtn();

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
  datastudio: document.getElementById("dataStudioView"),
  explorer: document.getElementById("explorerView"),
  tickets: document.getElementById("ticketsView"),
  swagger: document.getElementById("swaggerView")
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

function isPlatformAdmin() {
  const _u = JSON.parse(localStorage.getItem("user") || "{}");
  return _u.is_superuser === true;
}

function showView(viewName) {
  // Explorador de Dados (JSON bruto do MQTT): liberado a qualquer usuário logado;
  // o backend limita cada cliente às SUAS usinas (filtro por customer_id em /raw/query).
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
    datastudio: "btnDataStudio",
    explorer: "btnExplorer",
    tickets: "btnTickets",
    swagger: "btnSwagger"
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

  if (viewName === "explorer") {
    initExplorerOnce();
  }

  if (viewName === "tickets") {
    initTicketsOnce();
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
document.getElementById("btnExplorer")?.addEventListener("click", () => showView("explorer"));
document.getElementById("btnTickets")?.addEventListener("click", () => showView("tickets"));

// Explorador (JSON do MQTT): disponível para todos os usuários logados
// (o backend restringe cada cliente às próprias usinas via customer_id).
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

  const [plantsRes, alarmsRes, summaryRes] = await Promise.allSettled([
    fetchPlants(),
    fetchActiveAlarms(),
    fetchPlantsSummary()
  ]);

  if (plantsRes.status === "fulfilled" && Array.isArray(plantsRes.value) && plantsRes.value.length > 0) {
    plants = plantsRes.value;
    lastValidPlants = plants;
  } else {
    if (plantsRes.status === "rejected") console.error("Erro ao buscar plantas:", plantsRes.reason);
    plants = lastValidPlants.map(p => ({ ...p, updated_at: new Date().toISOString(), last_update: new Date().toISOString() }));
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

  if (alarmsRes.status === "fulfilled" && Array.isArray(alarmsRes.value)) {
    alarms = alarmsRes.value;
  } else {
    if (alarmsRes.status === "rejected") console.error("Erro ao buscar alarmes ativos:", alarmsRes.reason);
  }

  if (alarms.length > 0 || lastAlarmSeverityByPlant.size === 0) {
    lastAlarmSeverityByPlant = buildPlantAlarmSeverityMap(alarms);
  }

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

  if (summaryRes.status === "fulfilled") {
    refreshTopChipsGlobalFromSummary(summaryRes.value);
  } else {
    console.warn("[SUMMARY] falhou, fallback via /plants:", summaryRes.reason);
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

function _hasRestrictedPlantAccess() {
  const u = _getUser();
  if (u.is_superuser) return false;
  const perms = u.permissions || {};
  const ids = perms.allowed_plant_ids;
  return Array.isArray(ids) && ids.length > 0;
}

function _applyPartnerRestrictions() {
  if (!_hasRestrictedPlantAccess()) return;
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

  _applyPartnerRestrictions();

  const savedView = localStorage.getItem("currentView") || "overview";
  showView(savedView);
  syncTopSummaryLayout();

  wirePortfolioControls();
  _dismissAppLoader();
  await refreshDashboard();
  setInterval(refreshDashboard, DASHBOARD_REFRESH_INTERVAL_MS);

  document.addEventListener("visibilitychange", async () => {
    if (!document.hidden) {
      await refreshDashboard();
    }
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
// capacity_ac por usina (vem do GET /plants) — fallback da linha de expectativa
// no mini chart quando a usina não tem PVSyst (mesma regra do gráfico diário)
const _miniChartCapAc = new Map();
let _portfolioRenderGen = 0;
const _miniChartDataCache = new Map(); // plantId → { ts, body }
const _MINI_CHART_CACHE_TTL_MS = 5 * 60 * 1000;
let _miniChartPowerSource = "inverter"; // "inverter" | "meter"
let _miniChartIrrSource = "poa";        // "poa" | "ghi"
let _miniChartAnyMeter = false;
let _miniChartAnyGhi = false;

function miniChartSetPowerSource(source) {
  _miniChartPowerSource = source;
  const invBtn = document.getElementById("miniToggleInverter");
  const meterBtn = document.getElementById("miniToggleMeter");
  if (invBtn) invBtn.classList.toggle("active", source === "inverter");
  if (meterBtn) meterBtn.classList.toggle("active", source === "meter");
  _showMiniChartLoader(() => _reRenderAllMiniCharts());
}

function miniChartSetIrrSource(source) {
  _miniChartIrrSource = source;
  const poaBtn = document.getElementById("miniTogglePoa");
  const ghiBtn = document.getElementById("miniToggleGhi");
  if (poaBtn) poaBtn.classList.toggle("active", source === "poa");
  if (ghiBtn) ghiBtn.classList.toggle("active", source === "ghi");
  _showMiniChartLoader(() => _reRenderAllMiniCharts());
}

function _showMiniChartLoader(renderFn) {
  const loader = document.getElementById("miniChartLoader");
  if (loader) loader.classList.remove("hidden");
  requestAnimationFrame(() => {
    requestAnimationFrame(() => {
      renderFn();
      if (loader) loader.classList.add("hidden");
    });
  });
}

function _reRenderAllMiniCharts() {
  _miniChartDataCache.forEach((cached, plantId) => {
    const canvasId = "mini-chart-" + plantId;
    const canvas = document.getElementById(canvasId);
    if (canvas && cached?.body) {
      _renderMiniChartOnCanvas(canvas, plantId, cached.body);
    }
  });
}

function _updateMiniChartToggleVisibility() {
  const powerWrap = document.getElementById("miniChartPowerToggleWrap");
  const irrWrap = document.getElementById("miniChartIrrToggleWrap");
  if (powerWrap) powerWrap.style.display = _miniChartAnyMeter ? "" : "none";
  if (irrWrap) irrWrap.style.display = _miniChartAnyGhi ? "" : "none";
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
    const isMaintenance = commStatus.badgeClass === 'badge--maintenance';
    const isFieldMaint = commStatus.badgeClass === 'badge--field-maint';
    const isOffline = plantState.isOffline || isCommOffline;
    let statusDotClass = "plant-card__status-dot";
    let statusText;
    if (isMaintenance) { statusDotClass += " maintenance"; statusText = "Atualizando coletor local"; }
    else if (isFieldMaint) { statusDotClass += " field-maint"; statusText = "Manuten\u00E7\u00E3o em campo"; }
    else if (isCommOffline) { statusDotClass += " offline"; statusText = "Sem comunica\u00E7\u00E3o"; }
    else if (plantState.kind === "offline") { statusDotClass += " offline"; statusText = "Desligada"; }
    else if (plantState.kind === "generating") { statusDotClass += " generating"; statusText = "Em gera\u00E7\u00E3o"; }
    else { statusDotClass += " standby"; statusText = "Aguardando"; }

    const dot = card.querySelector(".plant-card__status-dot");
    const txt = card.querySelector(".plant-card__status-text");
    if (dot) dot.className = statusDotClass;
    if (txt) {
      // o capacete entra como ::before desta classe (ver layout.css)
      txt.className = "plant-card__status-text" + (isFieldMaint ? " is-field-maint" : "");
      txt.textContent = statusText;
    }

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

    // Update comm badge — manutenção e "Sem comunicação" não usam badge no topo (o status
    // embaixo já avisa e o badge sumia com o nome da usina). Só a comunicação parcial fica.
    const existingBadge = card.querySelector(".badge--offline, .badge--partial, .badge--maintenance, .badge--field-maint");
    if (commStatus.badge && !isMaintenance && !isFieldMaint && !isCommOffline) {
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

    // Sincroniza o toggle de manutenção do coletor (estado pode mudar por outro admin)
    const maintBtn = card.querySelector(".plant-card__maint-btn");
    if (maintBtn) _syncCardMaintBtn(maintBtn, plant.collector_maintenance === true);

    // Idem para o de campo: duas pessoas da mesma equipe podem estar na usina,
    // e quem não clicou precisa ver o estado mudar sozinho
    const fieldBtn = card.querySelector(".plant-card__field-btn");
    if (fieldBtn) _syncCardFieldBtn(fieldBtn, plant.field_maintenance === true);
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
    if (card.classList.contains('plant-card--maintenance')) return;
    const isOff = card.classList.contains("plant-card--offline");
    card.className = `plant-card${isOff ? " plant-card--offline" : ""}${sev ? ` alarm-${sev}` : ""}`;
    const icon = card.querySelector(".plant-card__icon");
    if (icon) icon.className = sev ? `plant-card__icon alarm-${sev}` : "plant-card__icon";
  });
}

// ======================================================
// BADGE DE ACONTECIMENTOS NO CARD
// Mostra um sinal de exclamação pulsando no topo do card
// para TODA issue que o robô detectou naquela usina
// (usina desligada/potência 0, temperatura alta, PR caindo,
// clipping, mudanças de estado etc.). Vermelho = tem crítico;
// âmbar = só avisos. Tooltip lista os acontecimentos; clique
// abre o relatório do robô.
// ======================================================
let _issuePopEl = null;

function _ensureIssuePop() {
  if (_issuePopEl) return _issuePopEl;
  _issuePopEl = document.createElement("div");
  _issuePopEl.className = "plant-issue-pop hidden";
  document.body.appendChild(_issuePopEl);
  _issuePopEl.addEventListener("mouseleave", () => _issuePopEl.classList.add("hidden"));
  window.addEventListener("scroll", () => _issuePopEl.classList.add("hidden"), { passive: true });
  return _issuePopEl;
}

function _showIssuePop(badge, plantName, plantIssues) {
  if (!plantIssues || !plantIssues.length) return;
  const pop = _ensureIssuePop();
  const sevMeta = s => (String(s || "").toLowerCase() === "critical")
    ? ["crítico", "#ff4d4f"]
    : ["aviso", "#ffb020"];

  pop.innerHTML = `
    <div class="plant-issue-pop__title">
      <i class="fa-solid fa-triangle-exclamation"></i>
      ${plantName || "Usina"} · ${plantIssues.length} acontecimento${plantIssues.length > 1 ? "s" : ""}
    </div>
    ${plantIssues.map(i => {
      const [lbl, color] = sevMeta(i.severity);
      return `
      <div class="plant-issue-pop__item">
        <span class="plant-issue-pop__dot" style="background:${color};box-shadow:0 0 6px ${color};"></span>
        <div class="plant-issue-pop__body">
          <div class="plant-issue-pop__msg">${i.message || i.type || "acontecimento"}</div>
          <div class="plant-issue-pop__meta">${i.device_name ? i.device_name + " · " : ""}<span style="color:${color};">${lbl}</span></div>
        </div>
      </div>`;
    }).join("")}
    <div class="plant-issue-pop__hint">clique no badge para abrir o relatório completo</div>`;

  pop.classList.remove("hidden");
  const r = badge.getBoundingClientRect();
  const w = pop.offsetWidth || 272;
  const h = pop.offsetHeight || 0;
  let left = Math.min(r.right - w, window.innerWidth - w - 8);
  if (left < 8) left = 8;
  let top = r.bottom + 8;
  if (top + h > window.innerHeight - 8) top = Math.max(8, r.top - h - 8);
  pop.style.left = left + "px";
  pop.style.top = top + "px";
}

function updatePlantCardIssueBadges() {
  const grid = document.getElementById("portfolioCardView");
  if (!grid) return;
  const issues = Array.isArray(ROBOT_STATE?.issues) ? ROBOT_STATE.issues : [];
  const byPlant = new Map();
  issues.forEach(i => {
    const pid = String(i.plant_id ?? "");
    if (!pid) return;
    if (!byPlant.has(pid)) byPlant.set(pid, []);
    byPlant.get(pid).push(i);
  });

  grid.querySelectorAll(".plant-card[data-plant-id]").forEach(card => {
    const pid = String(card.dataset.plantId);
    const plantIssues = byPlant.get(pid) || [];
    let badge = card.querySelector(".plant-card__issue-badge");

    if (!plantIssues.length) {
      if (badge) badge.remove();
      return;
    }

    const hasCritical = plantIssues.some(i => (i.severity || "").toLowerCase() === "critical");

    if (!badge) {
      badge = document.createElement("div");
      badge.className = "plant-card__issue-badge";
      badge.setAttribute("role", "button");
      badge.setAttribute("aria-label", "Acontecimentos da usina");
      badge.innerHTML = `<i class="fa-solid fa-exclamation"></i><span class="plant-card__issue-count"></span>`;
      badge.addEventListener("click", (e) => {
        e.stopPropagation();
        try { robotToggleReport(true); } catch (_) {}
      });
      badge.addEventListener("mouseenter", () => {
        _showIssuePop(badge, badge._plantName, badge._issues || []);
      });
      badge.addEventListener("mouseleave", () => {
        setTimeout(() => {
          if (_issuePopEl && !_issuePopEl.matches(":hover") && !badge.matches(":hover")) {
            _issuePopEl.classList.add("hidden");
          }
        }, 160);
      });
      // Fica ao lado do nome da usina (dentro do header do card)
      const topRow = card.querySelector(".plant-card__top");
      const nameEl = card.querySelector(".plant-card__name");
      if (topRow && nameEl) topRow.insertBefore(badge, nameEl.nextSibling);
      else card.appendChild(badge);
    }
    badge._issues = plantIssues;
    badge._plantName = card.dataset.plantName || "";
    badge.classList.toggle("is-critical", hasCritical);
    const countEl = badge.querySelector(".plant-card__issue-count");
    if (countEl) countEl.textContent = plantIssues.length > 1 ? String(plantIssues.length) : "";
  });
}

// ---- Badge do CLP (coletor MQTT) no topo do card ----
// Regra simples (pedido supervisor 16/07): verde = conectado (recebendo dados);
// vermelho = Desconectado (sem dado MQTT há mais de 10 min). Sem métrica de
// alarme/qualidade — alarme de relé não pinta mais o CLP.
function clpBadgeHtml(plant) {
  const clp = plant && typeof plant.clp === "object" && plant.clp ? plant.clp : null;
  const status = (clp && clp.status) || plant?.clp_status || null;
  if (!status) return ""; // backend antigo sem o campo → não mostra nada

  let lastTxt = "";
  if (clp && clp.last_data) {
    const d = new Date(clp.last_data);
    if (!isNaN(d)) {
      lastTxt = " — última leitura " + d.toLocaleString("pt-BR", {
        day: "2-digit", month: "2-digit", hour: "2-digit", minute: "2-digit"
      });
    }
  }

  let cls, label;
  if (status === "offline" || status === "none") {
    cls = "clp--offline";
    label = "CLP · Desconectado — sem receber dados via MQTT há mais de 10 min" + lastTxt;
  } else {
    // "ok" — e "alarm"/"quality" de um backend antigo também contam como
    // conectado: o dado está chegando.
    cls = "clp--ok";
    label = "CLP · Conectado — recebendo dados via MQTT" + lastTxt;
  }

  return `
    <span class="clp-badge ${cls}" title="${label}">
      <svg viewBox="0 0 24 24" aria-hidden="true">
        <rect x="3.5" y="2.5" width="17" height="19" rx="2.5" class="clp-svg-body"/>
        <circle cx="8"  cy="7" r="1.3" class="clp-svg-led clp-svg-led--main"/>
        <circle cx="12" cy="7" r="1.3" class="clp-svg-led"/>
        <circle cx="16" cy="7" r="1.3" class="clp-svg-led"/>
        <rect x="6.5" y="10.2" width="11" height="4.2" rx="1" class="clp-svg-screen"/>
        <rect x="6.6"  y="17" width="2.6" height="2.8" rx="0.6" class="clp-svg-pin"/>
        <rect x="10.7" y="17" width="2.6" height="2.8" rx="0.6" class="clp-svg-pin"/>
        <rect x="14.8" y="17" width="2.6" height="2.8" rx="0.6" class="clp-svg-pin"/>
      </svg>
    </span>`;
}

// ── Modal de diagnóstico do CLP: fluxo Plataforma (dbt) → CLP,
//    com verificado independente em cada nó. Admin vê o JSON MQTT cru. ──
function _clpFmtAge(sec) {
  if (sec == null) return "sem dado";
  if (sec < 90) return "agora mesmo";
  if (sec < 3600) return "há " + Math.round(sec / 60) + " min";
  if (sec < 86400) return "há " + Math.round(sec / 3600) + " h";
  return "há " + Math.floor(sec / 86400) + " dia(s)";
}

function _clpEsc(s) {
  return String(s ?? "").replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;");
}

function _clpCheckHtml(ok) {
  if (ok === true)  return '<span class="clp-diag-check ok"><i class="fa-solid fa-check"></i> OK</span>';
  if (ok === false) return '<span class="clp-diag-check bad"><i class="fa-solid fa-xmark"></i> Problema</span>';
  return '<span class="clp-diag-check na">?</span>';
}

function openClpDiagModal(plantId, plantName) {
  let ov = document.getElementById("clpDiagOverlay");
  if (!ov) {
    ov = document.createElement("div");
    ov.id = "clpDiagOverlay";
    ov.className = "clp-diag-overlay hidden";
    ov.addEventListener("click", (e) => { if (e.target === ov) ov.classList.add("hidden"); });
    document.addEventListener("keydown", (e) => {
      if (e.key === "Escape") ov.classList.add("hidden");
    });
    document.body.appendChild(ov);
  }
  ov.classList.remove("hidden");
  ov.dataset.plantId = plantId != null ? String(plantId) : "";
  ov.dataset.plantName = plantName || "";
  ov.innerHTML = `
    <div class="clp-diag-modal" role="dialog" aria-label="Diagnóstico de conexão do CLP">
      <div class="clp-diag-head">
        <span><i class="fa-solid fa-tower-broadcast"></i> Conexão — ${_clpEsc(plantName || "Usina")}</span>
        <button class="clp-diag-close" aria-label="Fechar"
          onclick="document.getElementById('clpDiagOverlay').classList.add('hidden')">&times;</button>
      </div>
      <div class="clp-diag-body">
        <div class="clp-diag-loading"><i class="fa-solid fa-circle-notch fa-spin"></i> Verificando conexão...</div>
      </div>
    </div>`;

  apiFetch(`/plants/${plantId}/clp/diagnostics`)
    .then(r => { if (!r.ok) throw new Error("HTTP " + r.status); return r.json(); })
    .then(data => _renderClpDiag(ov, data))
    .catch(err => {
      console.error("[CLP diag]", err);
      const body = ov.querySelector(".clp-diag-body");
      if (body) body.innerHTML = `
        <div class="clp-diag-error">
          <i class="fa-solid fa-triangle-exclamation"></i> Não foi possível carregar o diagnóstico.
          <br><small>${_clpEsc(err?.message || err)}</small>
        </div>`;
    });
}

function _renderClpDiag(ov, data) {
  const body = ov.querySelector(".clp-diag-body");
  if (!body) return;
  const clp = data.clp || {};
  const dbt = data.dbt || {};

  const VERDICT = {
    ok:           { cls: "ok",   icon: "fa-circle-check",         text: "Tudo certo: o CLP está enviando dados e a plataforma está processando normalmente." },
    clp_problem:  { cls: "bad",  icon: "fa-plug-circle-xmark",    text: "O CLP/coletor local parou de enviar dados (MQTT). A plataforma está operando normalmente — o problema é no equipamento ou na conexão em campo." },
    dbt_problem:  { cls: "warn", icon: "fa-hourglass-half",       text: "O CLP está enviando dados normalmente, mas a plataforma está com atraso no processamento. Não é problema no equipamento." },
    plant_off:    { cls: "na",   icon: "fa-power-off",            text: "Os inversores já estavam desligados (sem geração) quando o envio de dados parou — a usina aparenta estar desligada, não é falha de comunicação." },
    both_problem: { cls: "bad",  icon: "fa-triangle-exclamation", text: "Sem dados do CLP e plataforma com atraso — é preciso verificar os dois lados." },
    none:         { cls: "na",   icon: "fa-circle-question",      text: "Esta usina não possui CLP/relé de proteção cadastrado." },
  };
  const v = VERDICT[data.verdict] || VERDICT.none;

  const lagMin = (dbt.processing_lag_seconds != null && dbt.processing_lag_seconds > 60)
    ? " · fila de " + Math.round(dbt.processing_lag_seconds / 60) + " min"
    : "";

  let html = `
    <div class="clp-diag-flow">
      <div class="clp-diag-node">
        <div class="clp-diag-node-icon"><i class="fa-solid fa-cloud"></i></div>
        <div class="clp-diag-node-info">
          <div class="clp-diag-node-title">Plataforma AIOTI</div>
          <div class="clp-diag-node-sub">processamento de dados (dbt)</div>
          <div class="clp-diag-node-meta">último ciclo ${_clpFmtAge(dbt.heartbeat_age_seconds)} · dado desta usina ${_clpFmtAge(dbt.processed_age_seconds)}${lagMin}</div>
        </div>
        ${_clpCheckHtml(dbt.ok)}
      </div>
      <div class="clp-diag-arrow ${v.cls}"><i class="fa-solid fa-arrow-down-long"></i></div>
      <div class="clp-diag-node">
        <div class="clp-diag-node-icon"><i class="fa-solid fa-microchip"></i></div>
        <div class="clp-diag-node-info">
          <div class="clp-diag-node-title">CLP · Coletor local</div>
          <div class="clp-diag-node-sub">envio de dados via MQTT</div>
          <div class="clp-diag-node-meta">último dado ${_clpFmtAge(clp.age_seconds)}${clp.relays_total ? " · " + (clp.relays_with_data || 0) + "/" + clp.relays_total + " relé(s)" : ""}${data.verdict === "plant_off" ? " · inversores desligados" : ""}</div>
        </div>
        ${data.verdict === "plant_off"
          ? '<span class="clp-diag-check na"><i class="fa-solid fa-power-off"></i> Desligado</span>'
          : _clpCheckHtml(clp.ok)}
      </div>
    </div>
    <div class="clp-diag-verdict ${v.cls}"><i class="fa-solid ${v.icon}"></i><span>${v.text}</span></div>`;

  if (data.is_admin && Array.isArray(data.raw_mqtt) && data.raw_mqtt.length) {
    // Um bloco por TIPO de dispositivo da usina (inversor, relé, multimedidor,
    // tracker, estação...) com o último payload de cada — tipo sem dado em 48h
    // aparece marcado, o que denuncia coleta que não está entrando no banco.
    html += `<details class="clp-diag-json"><summary><i class="fa-solid fa-code"></i> JSON MQTT (admin)</summary>` +
      data.raw_mqtt.map(m => {
        const who = [m.type_label, m.device_name].filter(Boolean).join(" · ")
          || m.device_name || ("Relé " + m.device_id);
        return `
        <div class="clp-diag-json-item">
          <div class="clp-diag-json-head">${_clpEsc(who)} · ${m.timestamp ? new Date(m.timestamp).toLocaleString("pt-BR") : "⚠ sem leitura recente (2h)"}</div>
          <pre>${_clpEsc(m.payload ? JSON.stringify(m.payload, null, 2) : "— sem payload —")}</pre>
        </div>`;
      }).join("") +
      `</details>`;
  }

  // Aba "Configuração do gateway" (admin) — edita o JSON de config da usina,
  // salva no banco e publica como TRANSAÇÃO V2 (begin/put/validate/commit).
  if (data.is_admin) {
    const _pid = ov.dataset.plantId || "";
    const gc = data.gateway_config || null;
    _CLP_CFG_SAVED = gc && gc.config && typeof gc.config === "object" ? gc.config : null;
    _CLP_CFG_HISTORY = Array.isArray(data.gateway_config_history) ? data.gateway_config_history : [];
    // o último envio é o primeiro do histórico; o campo separado é retrocompatibilidade
    const last = data.gateway_config_last_sent || _CLP_CFG_HISTORY[0] || null;

    // Estado da configuração. Os botões saem do meio do texto e vão para uma
    // fila própria: intercalados, quebravam a leitura em qualquer largura.
    // Dado técnico (entidades, CRC) em mono, como no resto da plataforma.
    const savedInfo = gc
      ? `Salva em ${_clpEsc(new Date(gc.updated_at).toLocaleString("pt-BR"))}` +
        (gc.updated_by ? ` por ${_clpEsc(gc.updated_by)}` : "")
      : "Nenhuma configuração salva ainda.";
    // o nome de quem enviou vem ANTES da data: no fim, ele sobrava sozinho
    // na última linha do balão estreito
    const lastInfo = last
      ? `Último envio${last.sent_by_username ? ` por ${_clpEsc(last.sent_by_username)}` : ""}: ` +
        `${_clpEsc(new Date(last.sent_at).toLocaleString("pt-BR"))} · ` +
        `<code>${last.entity_count || 0}</code> entidades · CRC <code>${_clpEsc(last.crc32 || "—")}</code>`
      : "";
    const metaAcoes = [
      gc ? `<button type="button" class="clp-cfg-tpl clp-cfg-tpl--mini" onclick="_clpCfgRestoreSaved()">
              <i class="fa-solid fa-rotate-left" aria-hidden="true"></i> Restaurar</button>` : "",
      gc ? `<button type="button" class="clp-cfg-tpl clp-cfg-tpl--mini" onclick="_clpCfgDownload()">
              <i class="fa-solid fa-download" aria-hidden="true"></i> Baixar</button>` : "",
      (last && _CLP_CFG_HISTORY.length)
        ? `<button type="button" class="clp-cfg-tpl clp-cfg-tpl--mini" onclick="_clpCfgToggleHistory('${_pid}')">
             <i class="fa-solid fa-clock-rotate-left" aria-hidden="true"></i> Histórico (${_CLP_CFG_HISTORY.length})</button>`
        : "",
    ].filter(Boolean).join("");

    html += `
      <details class="clp-diag-cfg">
        <summary><i class="fa-solid fa-gears"></i> Configuração do gateway (admin)</summary>
        <div class="clp-cfg-inner">
          <div class="clp-cfg-meta">
            <div>${savedInfo}</div>
            ${lastInfo ? `<div>${lastInfo}</div>` : ""}
            ${metaAcoes ? `<div class="clp-cfg-meta-acoes">${metaAcoes}</div>` : ""}
          </div>
          ${_clpCfgFeedbackHtml(data.gateway_config_feedback, last)}
          <div class="clp-cfg-history" id="clpCfgHistory"></div>

          <button type="button" class="clp-cfg-abrir" onclick="_clpAbreConfigurador('${_pid}')">
            <i class="fa-solid fa-table-columns" aria-hidden="true"></i>
            <span class="clp-cfg-abrir-txt">
              <strong>${_CLP_CFG_SAVED ? "Abrir o configurador" : "Configurar este gateway"}</strong>
              <small>Equipamentos, canais, campos, tópicos, comandos, sequências e PID.
                O catálogo de modelos já traz o mapa Modbus pronto.</small>
            </span>
            <i class="fa-solid fa-arrow-right clp-cfg-abrir-seta" aria-hidden="true"></i>
          </button>

          <details class="clp-cfg-manual">
            <summary><i class="fa-solid fa-code" aria-hidden="true"></i> Colar o JSON à mão</summary>
            <div class="clp-cfg-manual-inner">
              <p class="clp-cfg-hint">
                A configuração vai para o gateway quebrada em várias mensagens, e nada chega aos
                equipamentos antes da última delas. Se alguma for recusada, a configuração antiga
                continua valendo.
              </p>
              <label class="clp-cfg-label" for="clpCfgPayload">
                Configuração (JSON)
                <span>
                  <button type="button" class="clp-cfg-tpl" onclick="_clpLoadConfigTemplate()">
                    <i class="fa-solid fa-file-code" aria-hidden="true"></i> Modelo padrão
                  </button>
                  <button type="button" class="clp-cfg-tpl" onclick="_clpCfgPreview('${_pid}')">
                    <i class="fa-solid fa-eye" aria-hidden="true"></i> Pré-visualizar
                  </button>
                </span>
              </label>
              <textarea id="clpCfgPayload" class="clp-cfg-area" rows="10" spellcheck="false"
                placeholder='{ "plant": {...}, "channels": [...], "templates": [...], "devices": [...] }'>${
                  _CLP_CFG_SAVED ? _clpEsc(JSON.stringify(_CLP_CFG_SAVED, null, 2)) : ""
                }</textarea>
              <div class="clp-cfg-preview" id="clpCfgPreview"></div>
              <label class="clp-cfg-check" title="Template importado chega com o mapa não validado em campo; sem isso o gateway recusa a transação inteira">
                <input type="checkbox" id="clpCfgAllowUnverified" checked>
                <span>Aceitar mapa de template ainda não validado em campo</span>
              </label>
              <button class="clp-cfg-btn clp-cfg-btn--ghost" onclick="_clpCfgSave('${_pid}')">
                <i class="fa-solid fa-floppy-disk"></i> Salvar no banco
              </button>
              <p class="clp-cfg-hint">Para publicar no gateway, confirme com o seu usuário e senha:</p>
              <div class="clp-cfg-auth">
                <input id="clpCfgUser" class="clp-cfg-input" placeholder="usuário" autocomplete="off"
                       aria-label="usuário">
                <input id="clpCfgPass" class="clp-cfg-input" type="password" placeholder="senha"
                       autocomplete="new-password" aria-label="senha">
              </div>
              <button class="clp-cfg-btn" onclick="_clpSendConfig('${_pid}')">
                <i class="fa-solid fa-satellite-dish"></i> Publicar no gateway
              </button>
              <div id="clpCfgResult" class="clp-cfg-result"></div>
            </div>
          </details>
        </div>
      </details>`;
  }

  body.innerHTML = html;
}

// Modelo de configuração do gateway enviado pelo Igor em 24/07 ("exatamente
// esse vai ser o padrão"). O print do WhatsApp cortou depois de "commands" —
// as chaves seguintes entram quando ele mandar o arquivo inteiro. Serve de
// ponto de partida no textarea; o gateway é quem valida o conteúdo.
const CLP_CONFIG_TEMPLATE = {
  alarms: [],
  auto_reclosing: {
    allowed_protections: [],
    allowed_schedule: {},
    blocking_conditions: [],
    delay_ms: 30000,
    device_id: "",
    enabled: false,
    max_attempts: 0,
    metadata: {},
    require_communication: true,
    require_voltage_normal: true,
    retry_interval_ms: 60000,
    stability_ms: 60000
  },
  channels: [
    {
      data_bits: 8,
      enabled: false,
      id: "tcp-inversores",
      ip: "192.0.2.190",
      metadata: {},
      name: "Rede TCP dos inversores",
      parity: "none",
      poll_interval_ms: 1000,
      port: 502,
      retries: 1,
      stop_bits: 1,
      timeout_ms: 1000,
      transport: "tcp"
    }
  ],
  commands: []
};

let _CLP_CFG_SAVED = null;
let _CLP_CFG_HISTORY = [];

// Abre o configurador gráfico (gateway.html) na usina do modal. O segmento do
// tópico é o NOME da usina no cadastro — vai junto para a tela já avisar quando
// o "plant.id" do JSON não bater com ele (a ingestão casa exato).
function _clpAbreConfigurador(plantId) {
  const card = document.querySelector(`.plant-card[data-plant-id="${plantId}"]`);
  const nome = (card && (card.dataset.plantTopicName || card.dataset.plantName)) || "";
  const url = `gateway.html?plant_id=${encodeURIComponent(plantId)}` +
    (nome ? `&plant=${encodeURIComponent(nome)}` : "");
  window.open(url, "_blank", "noopener");
}

// ---- Resgate da configuração -----------------------------------------------
// Pedido do Igor 25/07: poder voltar para uma configuração que já funcionou.
// A do banco é a "atual"; o histórico guarda o JSON de cada envio, então dá
// para recuperar qualquer um deles se um envio novo der ruim.

function _clpCfgRestoreSaved() {
  const el = document.getElementById("clpCfgPayload");
  const res = document.getElementById("clpCfgResult");
  if (!el || !_CLP_CFG_SAVED) return;
  if (el.value.trim() && !confirm("Substituir o que está na caixa pela configuração salva no banco?")) return;
  el.value = JSON.stringify(_CLP_CFG_SAVED, null, 2);
  if (res) res.innerHTML = `<span class="clp-cfg-ok"><i class="fa-solid fa-check"></i> Configuração salva recarregada.</span>`;
}

function _clpCfgDownload() {
  let cfg;
  try {
    cfg = _clpCfgRead();
  } catch (e) {
    cfg = _CLP_CFG_SAVED;
  }
  if (!cfg) return;
  const nome = `config-gateway-${(cfg.plant && cfg.plant.id) || "usina"}-${new Date().toISOString().slice(0, 10)}.json`;
  const blob = new Blob([JSON.stringify(cfg, null, 2)], { type: "application/json" });
  const a = document.createElement("a");
  a.href = URL.createObjectURL(blob);
  a.download = nome;
  a.click();
  setTimeout(() => URL.revokeObjectURL(a.href), 1000);
}

function _clpCfgToggleHistory(plantId) {
  const box = document.getElementById("clpCfgHistory");
  if (!box) return;
  if (box.innerHTML.trim()) { box.innerHTML = ""; return; }

  box.innerHTML = _CLP_CFG_HISTORY.map(h => {
    const quando = h.sent_at ? new Date(h.sent_at).toLocaleString("pt-BR") : "—";
    const btn = h.has_snapshot
      ? `<button type="button" class="clp-cfg-tpl clp-cfg-tpl--mini" onclick="_clpCfgRestoreFromLog('${plantId}', ${h.id})">
           <i class="fa-solid fa-rotate-left"></i> Usar esta
         </button>`
      : `<span class="clp-cfg-history-nosnap">sem cópia do JSON</span>`;
    return `<div class="clp-cfg-history-item">
      <div>
        <strong>${_clpEsc(quando)}</strong> · ${h.entity_count || 0} entidades ·
        CRC <code>${_clpEsc(h.crc32 || "—")}</code>
        ${h.sent_by_username ? `· ${_clpEsc(h.sent_by_username)}` : ""}
      </div>
      ${btn}
    </div>`;
  }).join("");
}

function _clpCfgRestoreFromLog(plantId, logId) {
  const res = document.getElementById("clpCfgResult");
  const el = document.getElementById("clpCfgPayload");
  if (!el) return;
  if (el.value.trim() && !confirm("Substituir o que está na caixa por este envio anterior?")) return;

  if (res) res.innerHTML = `<span class="clp-cfg-wait"><i class="fa-solid fa-circle-notch fa-spin"></i> Recuperando...</span>`;
  apiFetch(`/plants/${plantId}/clp/config`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ action: "restore", log_id: logId }),
  })
    .then(async (r) => {
      const d = await r.json().catch(() => ({}));
      if (!r.ok || d.ok === false) throw new Error(d.error || ("HTTP " + r.status));
      return d;
    })
    .then((d) => {
      el.value = JSON.stringify(d.config, null, 2);
      const quando = d.sent_at ? new Date(d.sent_at).toLocaleString("pt-BR") : "";
      if (res) res.innerHTML = `<span class="clp-cfg-ok"><i class="fa-solid fa-check"></i> ` +
        `Configuração de ${_clpEsc(quando)} carregada na caixa. ` +
        `Confira e publique para aplicar no gateway.</span>`;
      document.getElementById("clpCfgHistory").innerHTML = "";
    })
    .catch((err) => {
      if (res) res.innerHTML = `<span class="clp-cfg-err"><i class="fa-solid fa-triangle-exclamation"></i> ${_clpEsc(err.message || err)}</span>`;
    });
}

function _clpCfgRead() {
  const el = document.getElementById("clpCfgPayload");
  const raw = (el && el.value || "").trim();
  if (!raw) throw new Error("Cole ou carregue a configuração.");
  return JSON.parse(raw);
}

// Monta a transação V2 com o emissor oficial do gateway (js/gateway_v2.js) e
// mostra o que seria publicado, sem mandar nada.
function _clpCfgBuild(plantId) {
  if (typeof buildPublications !== "function") {
    throw new Error("emissor do gateway não carregado (js/gateway_v2.js)");
  }
  const cfg = _clpCfgAplicaMapaNaoValidado(_clpCfgRead());
  const usina = _clpCfgPlantSegment(plantId, cfg);
  // request_id NOVO a cada envio: o gateway ignora um begin com id que ele já
  // viu (idempotência), e aí o envio "some" sem erro nenhum. O sufixo aleatório
  // cobre dois cliques dentro do mesmo milissegundo.
  // ⚠️ A API de referência do Igor (27/07) valida com ^[A-Za-z0-9._-]{1,16}$ —
  // o limite caiu de 63 para 16. O formato antigo ("cfg-" + Date.now() + "-" +
  // aleatório) dava 23 caracteres e seria recusado. Base36 do relógio resolve:
  // 1 + 8 + 5 = 14, ainda único e compatível com o firmware antigo (16 < 63).
  const rid = ("c" + Date.now().toString(36) +
    Math.random().toString(36).slice(2, 7)).slice(0, 16);
  const user = JSON.parse(localStorage.getItem("user") || "{}");
  return {
    cfg,
    rid,
    txn: buildPublications(cfg, rid, { usina, originUser: user.username || "plataforma" }),
  };
}

// Template importado chega ao gateway com o mapa "não validado em campo", e a
// validação recusa a transação inteira a não ser que o device permita. O campo
// mora no metadata de cada device; quase nenhum JSON traz isso preenchido.
function _clpCfgDevicesSemMapaLiberado(cfg) {
  const devs = Array.isArray(cfg && cfg.devices) ? cfg.devices : [];
  return devs.filter(d => !(d && d.metadata && d.metadata.allow_unverified_map === true)).length;
}

// Resultado REAL do último envio, vindo do tópico de feedback do gateway
// (IoT Rule -> app.gateway_config_feedback). Até 27/07 a tela dizia "publicado"
// e ficava por isso mesmo, mesmo quando o CC100 recusava tudo em silêncio.
// AUSÊNCIA de feedback também é diagnóstico: significa que o gateway não
// respondeu nada — ou está fora do ar, ou o tópico não bate.
function _clpCfgFeedbackHtml(lista, ultimoEnvio) {
  if (!Array.isArray(lista) || !lista.length) {
    return ultimoEnvio
      ? `<div class="clp-cfg-fb clp-cfg-fb--mudo"><i class="fa-solid fa-circle-question"></i> ` +
        `O gateway não respondeu nada neste envio. Confirme se ele está online e se a regra ` +
        `de feedback está ativa.</div>`
      : "";
  }
  const f = lista[0];
  const st = String(f.status_final || "").toLowerCase();
  // "partial_success" é FALHA na referência do Igor, não sucesso parcial benigno
  const ruim = ["failed", "validation_error", "partial_success"].includes(st);
  const classe = ruim ? "clp-cfg-fb--erro" : (st === "success" ? "clp-cfg-fb--ok" : "clp-cfg-fb--neutro");
  const icone = ruim ? "fa-circle-xmark" : (st === "success" ? "fa-circle-check" : "fa-circle-info");
  const num = v => (v === null || v === undefined ? "—" : v);
  const casa = ultimoEnvio && String(ultimoEnvio.request_id || "") === String(f.request_id || "");

  return `<div class="clp-cfg-fb ${classe}">` +
    `<i class="fa-solid ${icone}"></i> ` +
    `<strong>Resposta do gateway:</strong> ${_clpEsc(f.status_final || "sem status")}` +
    ` · recebidas ${num(f.received)} · aplicadas ${num(f.applied)} · recusadas ${num(f.rejected)}` +
    (f.devices_configured != null ? ` · ${f.devices_configured} entidades confirmadas` : "") +
    (f.ultima_mensagem ? `<br><span class="clp-cfg-fb-msg">${_clpEsc(f.ultima_mensagem)}</span>` : "") +
    (casa ? "" : `<br><span class="clp-cfg-fb-msg">(de um envio anterior — ` +
      `${_clpEsc(String(f.request_id || ""))})</span>`) +
    `</div>`;
}

// Capacidades do staging do CC100 (tabela "Capacidades" da documentacao do
// gateway V16). Estourar qualquer uma faz o validate recusar a TRANSACAO
// INTEIRA, e a doc e explicita: "config invalida nunca vira ativa". Como a
// plataforma ainda nao assina o topico de feedback, sem esta checagem o erro
// aparece como "publicou e nao aplicou" - o mesmo sintoma das outras 4
// armadilhas. Barato conferir aqui antes de mandar 493 mensagens.
const CLP_CFG_LIMITES = {
  channels: 32, templates: 100, devices: 255,
  sequences: 128, pid: 16, reclose_rules: 32, metadata: 1024,
  requests_por_template: 32, fields_por_template: 192,
  commands_por_device: 6, status_map_por_template: 16,
  passos_por_sequencia: 16,
};

function _clpCfgLimitesEstourados(cfg) {
  const problemas = [];
  const arr = k => (Array.isArray(cfg && cfg[k]) ? cfg[k] : []);

  // Limites globais (contagem simples da lista)
  [
    ["channels", "canais Modbus"], ["templates", "templates"],
    ["devices", "equipamentos"], ["sequences", "sequencias"],
    ["pid", "controladores PID"], ["metadata", "entradas de metadados"],
  ].forEach(([chave, rotulo]) => {
    const n = arr(chave).length;
    const max = CLP_CFG_LIMITES[chave];
    if (n > max) problemas.push(`${n} ${rotulo} (maximo ${max})`);
  });

  // Limites POR PAI: agrupa pelo id do template/device e olha o maior grupo.
  const porPai = (lista, campoPai) => {
    const c = {};
    lista.forEach(x => {
      const pai = x && (x[campoPai] != null ? String(x[campoPai]) : "");
      if (pai) c[pai] = (c[pai] || 0) + 1;
    });
    let topo = ["", 0];
    Object.entries(c).forEach(e => { if (e[1] > topo[1]) topo = e; });
    return topo;
  };
  [
    ["requests", "template_id", "requests_por_template", "requests no template"],
    ["fields", "template_id", "fields_por_template", "campos no template"],
    ["status_map", "template_id", "status_map_por_template", "correlacoes de status no template"],
    ["commands", "device_id", "commands_por_device", "comandos no equipamento"],
    ["sequence_steps", "sequence_id", "passos_por_sequencia", "passos na sequencia"],
  ].forEach(([chave, campoPai, limite, rotulo]) => {
    const [pai, n] = porPai(arr(chave), campoPai);
    const max = CLP_CFG_LIMITES[limite];
    if (n > max) problemas.push(`${n} ${rotulo} "${pai}" (maximo ${max})`);
  });

  // Regras de religamento: ora vem como lista, ora dentro de auto_reclosing.
  const reclose = arr("reclose_rules").length ||
    (cfg && cfg.auto_reclosing && Array.isArray(cfg.auto_reclosing.rules)
      ? cfg.auto_reclosing.rules.length : 0);
  if (reclose > CLP_CFG_LIMITES.reclose_rules) {
    problemas.push(`${reclose} regras de religamento (maximo ${CLP_CFG_LIMITES.reclose_rules})`);
  }
  return problemas;
}

function _clpCfgAplicaMapaNaoValidado(cfg) {
  const el = document.getElementById("clpCfgAllowUnverified");
  if (!el || !el.checked || !Array.isArray(cfg.devices)) return cfg;
  const copia = JSON.parse(JSON.stringify(cfg));
  copia.devices.forEach(d => {
    if (!d || typeof d !== "object") return;
    if (!d.metadata || typeof d.metadata !== "object") d.metadata = {};
    d.metadata.allow_unverified_map = true;
  });
  return copia;
}

// O segmento do tópico é o NOME da usina no cadastro (a ingestão casa exato).
function _clpCfgPlantSegment(plantId, cfg) {
  const card = document.querySelector(`.plant-card[data-plant-id="${plantId}"]`);
  const fromCard = card && (card.dataset.plantTopicName || card.dataset.plantName);
  const fromCfg = cfg && ((cfg.plant && cfg.plant.id) || (cfg.general && cfg.general.plant_id));
  const seg = String(fromCfg || fromCard || "").trim();
  if (!seg) throw new Error('defina "plant".id no JSON com o nome da usina do tópico');
  return seg;
}

function _clpCfgPreview(plantId) {
  const box = document.getElementById("clpCfgPreview");
  if (!box) return;
  try {
    const { txn } = _clpCfgBuild(plantId);
    const porEntidade = {};
    txn.publications.filter(p => p.kind === "put")
      .forEach(p => { porEntidade[p.entity] = (porEntidade[p.entity] || 0) + 1; });
    const resumo = Object.entries(porEntidade)
      .map(([k, v]) => `${v}× ${_clpEsc(k)}`).join(" · ") || "nenhuma entidade";
    const alerta = txn.issues.length
      ? `<div class="clp-cfg-err">${txn.issues.length} entidade(s) passam do limite de 511 bytes e ` +
        `seriam recusadas: ${_clpEsc(txn.issues.join("; "))}</div>`
      : "";
    // Aviso do mapa não validado: é a recusa mais comum e não dá erro nenhum
    // aqui — a transação sai inteira e o gateway rejeita no validate.
    const semMapa = _clpCfgDevicesSemMapaLiberado(_clpCfgRead());
    const avisoMapa = (semMapa && !document.getElementById("clpCfgAllowUnverified")?.checked)
      ? `<div class="clp-cfg-err">${semMapa} equipamento(s) sem "mapa não validado" liberado. ` +
        `Se o template for importado, o gateway recusa a configuração inteira. ` +
        `Marque a opção abaixo ou valide o mapa em campo.</div>`
      : "";
    // Capacidades do staging: se estourar, o gateway recusa TUDO no validate.
    const estourados = _clpCfgLimitesEstourados(_clpCfgRead());
    const avisoLimite = estourados.length
      ? `<div class="clp-cfg-err">Passa da capacidade do gateway: ` +
        `${_clpEsc(estourados.join("; "))}. O validate recusaria a configuração inteira.</div>`
      : "";
    box.innerHTML =
      `<div><strong>${txn.publications.length}</strong> mensagens · ` +
      `<strong>${txn.entityCount}</strong> entidades <span class="clp-cfg-hint">` +
      `(conferência local — quem compila de verdade é o servidor)</span></div>` +
      `<div class="clp-cfg-preview-list">${resumo}</div>` +
      `<div class="clp-cfg-preview-topic">${_clpEsc(txn.topics.chunk)}</div>` +
      alerta + avisoMapa + avisoLimite;
  } catch (e) {
    box.innerHTML = `<div class="clp-cfg-err"><i class="fa-solid fa-triangle-exclamation"></i> ${_clpEsc(e.message || e)}</div>`;
  }
}

// Salva o JSON no banco sem publicar (não pede senha: não toca no equipamento).
function _clpCfgSave(plantId) {
  const res = document.getElementById("clpCfgResult");
  if (!res) return;
  let cfg;
  try {
    cfg = _clpCfgRead();
  } catch (e) {
    res.innerHTML = `<span class="clp-cfg-err">JSON inválido: ${_clpEsc(e.message)}</span>`;
    return;
  }
  res.innerHTML = `<span class="clp-cfg-wait"><i class="fa-solid fa-circle-notch fa-spin"></i> Salvando...</span>`;
  apiFetch(`/plants/${plantId}/clp/config`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ action: "save", config: cfg }),
  })
    .then(async (r) => {
      const d = await r.json().catch(() => ({}));
      if (!r.ok || d.ok === false) throw new Error(d.error || ("HTTP " + r.status));
      return d;
    })
    .then(() => {
      _CLP_CFG_SAVED = cfg;
      res.innerHTML = `<span class="clp-cfg-ok"><i class="fa-solid fa-check"></i> Configuração salva no banco.</span>`;
    })
    .catch((err) => {
      res.innerHTML = `<span class="clp-cfg-err"><i class="fa-solid fa-triangle-exclamation"></i> ${_clpEsc(err.message || err)}</span>`;
    });
}

function _clpLoadConfigTemplate() {
  const el = document.getElementById("clpCfgPayload");
  const res = document.getElementById("clpCfgResult");
  if (!el) return;
  el.value = JSON.stringify(CLP_CONFIG_TEMPLATE, null, 2);
  if (res) {
    res.innerHTML = `<span class="clp-cfg-wait">Modelo carregado. Confira os valores antes de publicar. ` +
      `As seções que vierem depois de "commands" ainda precisam ser completadas.</span>`;
  }
}

// Publica a configuração como TRANSAÇÃO V2 (begin → put ×N → validate → commit).
// O emissor roda aqui no front (js/gateway_v2.js, cópia da referência do Igor) e o
// backend só repassa as mensagens ao IoT Core, na ordem e sem reserializar — o
// gateway confere CRC32/SHA-256 sobre os bytes exatos.
// Re-valida usuário+senha operacional no backend (mesma segurança do comando remoto).
function _clpSendConfig(plantId) {
  const userEl  = document.getElementById("clpCfgUser");
  const passEl  = document.getElementById("clpCfgPass");
  const resEl   = document.getElementById("clpCfgResult");
  if (!resEl) return;

  const username = ((userEl && userEl.value) || "").trim();
  const password = (passEl && passEl.value) || "";
  if (!username || !password) {
    resEl.innerHTML = `<span class="clp-cfg-err">Informe usuário e senha.</span>`;
    return;
  }

  let built;
  try {
    built = _clpCfgBuild(plantId);
  } catch (e) {
    resEl.innerHTML = `<span class="clp-cfg-err"><i class="fa-solid fa-triangle-exclamation"></i> ${_clpEsc(e.message || e)}</span>`;
    return;
  }

  const { cfg, rid, txn } = built;
  if (txn.issues.length) {
    resEl.innerHTML = `<span class="clp-cfg-err"><i class="fa-solid fa-triangle-exclamation"></i> ` +
      `${txn.issues.length} entidade(s) passam de 511 bytes e o gateway recusaria. ` +
      `Veja a pré-visualização.</span>`;
    return;
  }

  if (!confirm(
    `Enviar a configuração (${txn.entityCount} entidades) ao gateway?\n\n` +
    `O backend compila e publica uma mensagem por vez, esperando o gateway ` +
    `confirmar cada uma. Isso leva alguns minutos.\n\n` +
    `O commit aplica a configuração no equipamento.`
  )) return;

  _clpCfgApplyV2(plantId, cfg, username, password, passEl, resEl);
}

// Caminho NOVO (contrato do Igor, 27/07). O front manda a configuração de ALTO
// NÍVEL e não compila nada: quem compila é o backend, com o mqtt_entities.py
// dele. A API responde 202 e o envio real acontece num worker assíncrono, que
// espera o ACK de cada mensagem — por isso aqui só resta acompanhar.
async function _clpCfgApplyV2(plantId, cfg, username, password, passEl, resEl) {
  const espera = ms => new Promise(r => setTimeout(r, ms));
  try {
    resEl.innerHTML = `<span class="clp-cfg-wait"><i class="fa-solid fa-circle-notch fa-spin"></i> ` +
      `Compilando a configuração...</span>`;

    const r = await apiFetch(`/plants/${plantId}/clp/config`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ action: "apply_v2", configuration: cfg, username, password }),
    });
    const d = await r.json().catch(() => ({}));
    if (!r.ok || d.ok === false) throw new Error(d.error || `HTTP ${r.status}`);
    if (passEl) passEl.value = "";

    const rid = d.request_id;
    const total = Number(d.messages) || 0;
    let ultimo = "";

    // Polling do job. Sem teto de tentativas: quem decide o fim é o backend
    // (success/failed), e uma transação grande pode passar de 10 minutos.
    for (;;) {
      await espera(1500);
      const jr = await apiFetch(`/plants/${plantId}/clp/config`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ action: "job", request_id: rid }),
      });
      const j = await jr.json().catch(() => ({}));
      if (!jr.ok) throw new Error(j.error || `HTTP ${jr.status}`);

      const enviadas = Number(j.entities_sent) || 0;
      const pct = total ? Math.min(100, Math.round((enviadas / total) * 100)) : 0;
      const etapa = { begin: "abrindo a transação", put: "enviando entidades",
                      validate: "o gateway está validando", commit: "aplicando",
                      finished: "concluído" }[j.stage] || (j.stage || "");

      if (j.status === "success") {
        resEl.innerHTML = `<span class="clp-cfg-ok"><i class="fa-solid fa-check"></i> ` +
          `Configuração aplicada e confirmada pelo gateway ` +
          `(${enviadas} de ${total} mensagens).</span>`;
        _renderClpDiag && setTimeout(() => { try { openClpDiagModal(plantId); } catch (e) {} }, 1200);
        return;
      }
      if (j.status === "failed") {
        resEl.innerHTML = `<span class="clp-cfg-err"><i class="fa-solid fa-triangle-exclamation"></i> ` +
          `O gateway recusou: ${_clpEsc(j.message || "sem detalhe")}<br>` +
          `<small>Parou em ${enviadas} de ${total}. A configuração anterior continua valendo ` +
          `(o backend publicou abort).</small></span>`;
        return;
      }

      const msg = `${pct}% · ${enviadas} de ${total} mensagens · ${etapa}`;
      if (msg !== ultimo) {
        ultimo = msg;
        resEl.innerHTML = `<span class="clp-cfg-wait"><i class="fa-solid fa-circle-notch fa-spin"></i> ` +
          `${_clpEsc(msg)}</span>` +
          `<div class="clp-cfg-bar"><div class="clp-cfg-bar-fill" style="width:${pct}%"></div></div>` +
          `<small class="clp-cfg-hint">Pode fechar esta janela — o envio continua no servidor.</small>`;
      }
    }
  } catch (err) {
    resEl.innerHTML = `<span class="clp-cfg-err"><i class="fa-solid fa-triangle-exclamation"></i> ` +
      `${_clpEsc(err.message || err)}</span>`;
  }
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
    const relayAvail = plant.relay_availability_pct != null ? Number(plant.relay_availability_pct).toFixed(1) + "%" : "\u2014";
    const prAcc = plant.pr_accumulated_pct != null ? Number(plant.pr_accumulated_pct).toFixed(1) + "%" : "\u2014";

    // Perda de hoje em R$ (pedido do cliente, 24/08). Quem calcula e o
    // api2.py; aqui so se desenha.
    //
    // \ud83d\udd11 O indicador SOME quando nao da para calcular, em vez de mostrar
    // traco. Usina sem capacidade cadastrada ou sem irradiacao medida entra
    // com null de proposito \u2014 e "R$ \u2014" num campo de dinheiro parece defeito
    // do sistema, enquanto a ausencia do campo nao promete nada.
    const perdaHoje = (() => {
      const brl = plant.perda_hoje_brl;
      if (brl == null) return { html: "" };
      const kwh = Number(plant.perda_hoje_kwh || 0);
      const valor = Number(brl).toLocaleString("pt-BR", {
        minimumFractionDigits: 2, maximumFractionDigits: 2
      });
      const zerado = Number(brl) <= 0;
      return {
        html: `
        <div class="plant-card__stat" title="Quanto a usina deixou de gerar hoje, comparando com PR de refer\u00eancia de ${String(plant.perda_pr_referencia_pct ?? 80).replace(".", ",")}% \u00b7 tarifa R$ ${String(plant.perda_tarifa_kwh_brl ?? 0.49).replace(".", ",")}/kWh">
          <div class="plant-card__stat-label"><i class="fa-solid fa-arrow-trend-down"></i> Perda hoje</div>
          <div class="plant-card__stat-value${zerado ? "" : " plant-card__stat-value--perda"}">R$ ${valor}<span class="plant-card__stat-sub">${kwh.toFixed(1)} kWh</span></div>
        </div>`
      };
    })();

    const isMaintenance = commStatus.badgeClass === 'badge--maintenance';
    const isFieldMaint = commStatus.badgeClass === 'badge--field-maint';
    const isOffline = plantState.isOffline || isCommOffline;
    const isGenerating = plantState.kind === "generating";
    let statusDotClass = "plant-card__status-dot";
    let statusText;
    if (isMaintenance) {
      statusDotClass += " maintenance";
      statusText = "Atualizando coletor local";
    }
    else if (isFieldMaint) {
      statusDotClass += " field-maint";
      statusText = "Manutenção em campo";
    }
    else if (isCommOffline) {
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
    _miniChartCapAc.set(String(plantId), Number(plant.capacity_ac ?? 0) || 0);

    // Manutenção e "Sem comunicação" NÃO ganham badge no topo: o status embaixo já avisa
    // e o badge inline engolia o espaço da linha, sumindo com o nome da usina (ex.: PedraBranca).
    // Só a comunicação parcial (que não aparece no status de baixo) mantém o badge no topo.
    const commBadgeHtml = commStatus.badge && !isMaintenance && !isFieldMaint && !isCommOffline
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
        <!-- data-i18n-skip: nome proprio vindo do banco. Ver NOMES_PROPRIOS_I18N.md -->
        <div class="plant-card__name" data-i18n-skip>${plantName}</div>
        <button
          class="plant-card__field-btn"
          data-field-plant-id="${plantId}"
          type="button"
          style="display:none;"
        >
          <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M2 17h20"/><path d="M4 17a8 8 0 0 1 16 0"/><path d="M9 9.5V5.2A1.2 1.2 0 0 1 10.2 4h3.6A1.2 1.2 0 0 1 15 5.2v4.3"/><path d="M2 17v1.5A1.5 1.5 0 0 0 3.5 20h17a1.5 1.5 0 0 0 1.5-1.5V17"/></svg>
        </button>
        ${commBadgeHtml}
      </div>
      <div class="plant-card__stats">
        <div class="plant-card__stat">
          <div class="plant-card__stat-label"><i class="fa-solid fa-bolt"></i> Potência Ativa</div>
          <div class="plant-card__stat-value active">${activePowerDisplay}</div>
        </div>
        <div class="plant-card__stat">
          <div class="plant-card__stat-label"><i class="fa-solid fa-layer-group"></i> Pot. Nominal</div>
          <div class="plant-card__stat-value muted">${ratedPower.toFixed(1)} kWp</div>
        </div>
        <div class="plant-card__stat">
          <div class="plant-card__stat-label"><i class="fa-solid fa-bolt-lightning"></i> Energia Diária</div>
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
        <div class="plant-card__stat">
          <div class="plant-card__stat-label"><i class="fa-solid fa-tower-broadcast"></i> Relé Disp.</div>
          <div class="plant-card__stat-value">${relayAvail}</div>
        </div>
        <div class="plant-card__stat">
          <div class="plant-card__stat-label"><i class="fa-solid fa-chart-line"></i> PR Acum.</div>
          <div class="plant-card__stat-value">${prAcc}</div>
        </div>
        ${perdaHoje.html}
      </div>
      <div class="plant-card__chart-area">
        <div class="plant-card__chart-wrap">
          <canvas id="${canvasId}"></canvas>
        </div>
        <div class="plant-card__chart-legend">
          <span class="pcc-leg pcc-leg--power">Potência Ativa</span>
          <span class="pcc-leg pcc-leg--irr">Irrad. POA</span>
          <span class="pcc-leg pcc-leg--pr">PR</span>
        </div>
      </div>
      <div class="plant-card__status">
        <div class="${statusDotClass}"></div>
        <span class="plant-card__status-text${isFieldMaint ? ' is-field-maint' : ''}">${statusText}</span>
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
      <button
        class="plant-card__maint-btn"
        title="Manutenção do coletor local"
        data-maint-plant-id="${plantId}"
        aria-label="Manutenção do coletor local"
        style="display:none;"
      >
        <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M14.7 6.3a1 1 0 0 0 0 1.4l1.6 1.6a1 1 0 0 0 1.4 0l3.77-3.77a6 6 0 0 1-7.94 7.94l-6.91 6.91a2.12 2.12 0 0 1-3-3l6.91-6.91a6 6 0 0 1 7.94-7.94l-3.76 3.76z"/></svg>
      </button>
      ${clpBadgeHtml(plant)}
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

    // Toggle de manutenção do coletor local — SOMENTE superuser (admin AIOTI)
    const _maintBtn = card.querySelector(".plant-card__maint-btn");
    if (_maintBtn && _u.is_superuser === true) {
      _maintBtn.style.display = "";
      _syncCardMaintBtn(_maintBtn, plant.collector_maintenance === true);
      _maintBtn.addEventListener("click", (e) => {
        e.stopPropagation();
        togglePlantCollectorMaintenance(plantId, plantName, _maintBtn.dataset.maintOn === "1");
      });
    }

    // Manutenção em campo (equipe DO CLIENTE) — ao lado do nome, não na fila de
    // ícones do canto. Lá já moram lápis, chave inglesa e CLP; um quarto em
    // right:106px apertaria o nome da usina, que é o bug da PedraBranca.
    const _fieldBtn = card.querySelector(".plant-card__field-btn");
    if (_fieldBtn && canFlagFieldMaintenance()) {
      _fieldBtn.style.display = "";
      _syncCardFieldBtn(_fieldBtn, plant.field_maintenance === true);
      _fieldBtn.addEventListener("click", (e) => {
        e.stopPropagation();
        togglePlantFieldMaintenance(plantId, plantName, _fieldBtn.dataset.fieldOn === "1");
      });
    }

    // Badge do CLP → modal de diagnóstico Plataforma (dbt) → CLP
    const _clpBadge = card.querySelector(".clp-badge");
    if (_clpBadge) {
      _clpBadge.setAttribute("role", "button");
      _clpBadge.setAttribute("tabindex", "0");
      _clpBadge.addEventListener("click", (e) => {
        e.stopPropagation();
        openClpDiagModal(plantId, plantName);
      });
      _clpBadge.addEventListener("keydown", (e) => {
        if (e.key === "Enter" || e.key === " ") {
          e.preventDefault(); e.stopPropagation();
          openClpDiagModal(plantId, plantName);
        }
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

  // Reaplica os badges de acontecimentos (o re-render zera o grid)
  try { updatePlantCardIssueBadges(); } catch (_) {}
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
  const meterRaw = body?.meterPower || [];
  const irrGhiRaw = body?.irradianceGhi || [];
  const hasMeter = Array.isArray(meterRaw) && meterRaw.some(v => v != null);
  const hasGhi = Array.isArray(irrGhiRaw) && irrGhiRaw.some(v => v != null);

  if (hasMeter && !_miniChartAnyMeter) { _miniChartAnyMeter = true; _updateMiniChartToggleVisibility(); }
  if (hasGhi && !_miniChartAnyGhi) { _miniChartAnyGhi = true; _updateMiniChartToggleVisibility(); }

  const useMeter = _miniChartPowerSource === "meter" && hasMeter;
  const useGhi = _miniChartIrrSource === "ghi" && hasGhi;

  const powerRaw = useMeter
    ? meterRaw
    : (body?.activePower || body?.active_power_kw || body?.power_kw || []);
  const irrRaw = useGhi
    ? irrGhiRaw
    : (body?.irradiance || body?.irradiance_wm2 || []);
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
  const maxI   = irrRaw.length   ? seriesMax(iNums)  : 0;

  // Expectativa (mesma regra do gráfico diário da usina): curva PVSyst quando
  // a usina tem simulação no banco; senão linha reta no capacity AC.
  const expRaw = body?.expectedPower || [];
  const hasPvsystExpected = Array.isArray(expRaw) && expRaw.some(v => v != null && Number(v) > 0);
  const capAc = _miniChartCapAc.get(String(plantId)) || 0;
  let expNums = null, expLabel = null;
  if (hasPvsystExpected) {
    expNums = toNums(expRaw);
    expLabel = "Esperado";
  } else if (capAc > 0 && labels.length) {
    expNums = labels.map(() => capAc);
    expLabel = "Capacity AC";
  }

  const maxE   = expNums ? seriesMax(expNums) : 0;
  const maxP   = powerRaw.length ? Math.max(seriesMax(pNums), maxE) : maxE;

  const datasets = [];

  const powerLabel = useMeter ? "Multimedidor" : "Active Power";
  const powerBorderColor = useMeter ? "rgba(77,163,255,0.9)" : "rgba(127,208,85,0.9)";
  const powerBgColor = useMeter ? "rgba(77,163,255,0.07)" : "rgba(127,208,85,0.07)";
  const powerHoverColor = useMeter ? "#4da3ff" : "#7fd055";
  const irrLabel = useGhi ? "Irrad. GHI" : "Irrad. POA";

  if (powerRaw.length) {
    datasets.push({
      label: powerLabel, _raw: powerRaw, _unit: "kW",
      data: pNums, yAxisID: "y",
      borderColor: powerBorderColor,
      backgroundColor: powerBgColor,
      borderWidth: 1.5, pointRadius: 0, pointHoverRadius: 4,
      pointHoverBackgroundColor: powerHoverColor,
      tension: 0.4, fill: true,
    });
  }

  if (irrRaw.length) {
    datasets.push({
      label: irrLabel, _raw: irrRaw, _unit: "W/m\u00B2",
      data: iNums, yAxisID: "y1",
      borderColor: "rgba(255,200,50,0.85)",
      backgroundColor: "transparent",
      borderWidth: 1.5, pointRadius: 0, pointHoverRadius: 4,
      pointHoverBackgroundColor: "#ffc832",
      tension: 0.4, fill: false,
    });
  }

  if (expNums) {
    datasets.push({
      label: expLabel, _raw: expNums, _unit: "kW",
      data: expNums, yAxisID: "y",
      borderColor: "rgba(205,213,225,0.55)",
      backgroundColor: "transparent",
      borderWidth: 1, borderDash: [4, 3],
      pointRadius: 0, pointHoverRadius: 3,
      pointHoverBackgroundColor: "#cdd5e1",
      tension: hasPvsystExpected ? 0.4 : 0,
      fill: false, spanGaps: true,
      order: 3, // atrás das séries de potência/irradiância
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
          // max > 0 sempre: com a série toda zerada (usina parada), max 0
          // degenera a escala e a linha vai pro TOPO (ticket #11)
          min: 0, max: maxP > 0 ? maxP * 1.12 : 1,
          grid: { display: false },
          ticks: { ...tickStyle("rgba(57,229,140,0.55)"), callback: v => fmtTick(v, "kW") },
          border: { display: false },
        },
        y1: {
          type: "linear", position: "right",
          display: irrRaw.length > 0,
          // piso de 100 W/m² no teto: sensor travado em ~0.5 (Pedra Branca)
          // auto-ajustava a escala e a linha "colava" no topo (ticket #11)
          min: 0, max: Math.max(maxI * 1.12, 100),
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

let _pemCabinsCache = [];

async function _pemLoadDevices(plantId) {
  const list = document.getElementById("pemDevicesList");
  if (!list) return;
  list.innerHTML = `<div class="plant-edit-empty">Carregando…</div>`;
  try {
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

    const [devRes, cabRes] = await Promise.all([
      apiFetch(`/plants/${plantId}/devices/options`),
      apiFetch(`/plants/${plantId}/cabin-groups`)
    ]);
    const devData = await devRes.json();
    const cabData = await cabRes.json();
    const items  = devData?.items || [];
    _pemCabinsCache = cabData?.items || [];

    if (!items.length) { list.innerHTML = `<div class="plant-edit-empty">Nenhum dispositivo.</div>`; return; }
    const canCmd = _canSendCommand();

    const cabinOptions = (currentCabinId) => {
      let html = `<option value="">Sem cabine</option>`;
      for (const c of _pemCabinsCache) {
        const sel = c.id === currentCabinId ? " selected" : "";
        html += `<option value="${c.id}"${sel}>${c.name}</option>`;
      }
      return html;
    };

    list.innerHTML = items.map(d => {
      const isInverter = (d.device_type || "").toLowerCase() === "inverter";
      const cabinSelect = isInverter && _pemCabinsCache.length
        ? `<select class="pem-cabin-select pem-ctrl" data-device-id="${d.device_id}"
             title="Cabine">${cabinOptions(d.cabin_id)}</select>`
        : "";
      return `
      <div class="plant-edit-device-row" data-device-id="${d.device_id}">
        <div class="plant-edit-device-info">
          <span class="plant-edit-device-type">${d.device_type || "—"}</span>
          <span class="plant-edit-device-meta">#${d.device_id}${d.cabin_name ? ` · ${d.cabin_name}` : ""}</span>
        </div>
        <div class="plant-edit-inline">
          <input class="plant-edit-device-name" type="text"
            value="${(d.device_name || "").replace(/"/g,'&quot;')}"
            placeholder="Nome do dispositivo"/>
          ${cabinSelect}
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
      </div>`;
    }).join("");

    _pemWireCommandButtons(list);

    list.querySelectorAll(".plant-edit-device-row").forEach(row => {
      const did     = row.dataset.deviceId;
      const inp     = row.querySelector(".plant-edit-device-name");
      const saveBtn = row.querySelector(".plant-edit-device-save");
      const delBtn  = row.querySelector(".pem-del-device-btn");
      const cabSel  = row.querySelector(".pem-cabin-select");
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

      cabSel?.addEventListener("change", async () => {
        const cabinId = cabSel.value ? parseInt(cabSel.value) : null;
        fb.textContent = "Vinculando…"; fb.className = "plant-edit-device-feedback";
        try {
          const r = await apiFetch(`/plants/${_PLANT_EDIT_ID}/devices/${did}/cabin`, {
            method: "PATCH", headers: {"Content-Type":"application/json"},
            body: JSON.stringify({cabin_id: cabinId})
          });
          if (!r.ok) throw new Error((await r.json())?.error || `HTTP ${r.status}`);
          const cabName = cabinId ? _pemCabinsCache.find(c => c.id === cabinId)?.name : null;
          const meta = row.querySelector(".plant-edit-device-meta");
          if (meta) meta.textContent = `#${did}${cabName ? ` · ${cabName}` : ""}`;
          fb.textContent = cabinId ? `✓ Vinculado à ${cabName}` : "✓ Desvinculado";
          fb.className = "plant-edit-device-feedback ok";
        } catch(e) { fb.textContent = e.message || "Erro."; fb.className = "plant-edit-device-feedback err"; }
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
      <div class="plant-edit-device-row" data-cabin-id="${c.id}">
        <div class="plant-edit-device-info">
          <span class="plant-edit-device-meta">${c.inverter_count} inv.</span>
        </div>
        <div class="plant-edit-inline">
          <input class="plant-edit-device-name" type="text"
            value="${(c.name || "").replace(/"/g,'&quot;')}"
            placeholder="Nome da cabine"/>
          <button class="plant-edit-device-save" type="button" title="Salvar nome">
            <i class="fa-solid fa-check"></i>
          </button>
          <button class="pem-del-cabin-btn" type="button" title="Excluir cabine">
            <i class="fa-solid fa-trash"></i>
          </button>
        </div>
        <div class="plant-edit-device-feedback" id="pem-cabfb-${c.id}"></div>
      </div>`).join("");

    list.querySelectorAll(".plant-edit-device-row").forEach(row => {
      const cid     = row.dataset.cabinId;
      const inp     = row.querySelector(".plant-edit-device-name");
      const saveBtn = row.querySelector(".plant-edit-device-save");
      const delBtn  = row.querySelector(".pem-del-cabin-btn");
      const fb      = row.querySelector(".plant-edit-device-feedback");

      saveBtn?.addEventListener("click", async () => {
        const n = inp.value.trim();
        if (!n) { fb.textContent = "Nome vazio."; fb.className = "plant-edit-device-feedback err"; return; }
        saveBtn.disabled = true; fb.textContent = "Salvando…"; fb.className = "plant-edit-device-feedback";
        try {
          const r = await apiFetch(`/plants/${_PLANT_EDIT_ID}/cabin-groups/${cid}`, {
            method: "PATCH", headers: {"Content-Type":"application/json"},
            body: JSON.stringify({name: n})
          });
          if (!r.ok) throw new Error((await r.json())?.error || `HTTP ${r.status}`);
          fb.textContent = "✓ Salvo!"; fb.className = "plant-edit-device-feedback ok";
        } catch(e) { fb.textContent = e.message || "Erro."; fb.className = "plant-edit-device-feedback err"; }
        finally    { saveBtn.disabled = false; }
      });

      delBtn?.addEventListener("click", async () => {
        if (!confirm(`Excluir cabine #${cid}? Inversores vinculados serão desvinculados.`)) return;
        delBtn.disabled = true; fb.textContent = "Excluindo…"; fb.className = "plant-edit-device-feedback";
        try {
          const r = await apiFetch(`/plants/${_PLANT_EDIT_ID}/cabin-groups/${cid}`, { method: "DELETE" });
          if (!r.ok) throw new Error((await r.json())?.error || `HTTP ${r.status}`);
          fb.textContent = "✓ Removida!"; fb.className = "plant-edit-device-feedback ok";
          setTimeout(() => row.remove(), 900);
        } catch(e) { fb.textContent = e.message || "Erro."; fb.className = "plant-edit-device-feedback err"; delBtn.disabled = false; }
      });
    });
  } catch(e) {
    console.error("[_pemLoadCabins]", e);
    list.innerHTML = `<div class="plant-edit-empty plant-edit-empty--error">Erro ao carregar cabines.</div>`;
  }
}

async function pemAddDevice() {
  if (!_PLANT_EDIT_ID) return;
  const selEl  = document.getElementById("pemDeviceTypeSelect");
  const nameEl = document.getElementById("pemDeviceNameInput");
  const qtyEl  = document.getElementById("pemDeviceQtyInput");
  const fb     = document.getElementById("pemAddDeviceFeedback");
  const dtId   = selEl?.value;
  const displayBase = nameEl?.value?.trim() || "";
  const qty    = Math.max(1, Math.min(100, parseInt(qtyEl?.value) || 1));
  if (!dtId) { fb.textContent = "Selecione o tipo."; fb.className = "plant-edit-feedback err"; return; }

  fb.textContent = qty > 1 ? `Adicionando 0/${qty}…` : "Adicionando…";
  fb.className = "plant-edit-feedback";

  const created = [];
  const errors = [];
  for (let i = 0; i < qty; i++) {
    const dname = displayBase ? (qty > 1 ? `${displayBase} ${i + 1}` : displayBase) : undefined;
    try {
      const payload = {device_type_id: parseInt(dtId)};
      if (dname) payload.display_name = dname;
      const r = await apiFetch(`/plants/${_PLANT_EDIT_ID}/devices`, {
        method: "POST", headers: {"Content-Type":"application/json"},
        body: JSON.stringify(payload)
      });
      const d = await r.json();
      if (!r.ok) throw new Error(d?.error || `HTTP ${r.status}`);
      created.push(d.device_id);
      if (qty > 1) fb.textContent = `Adicionando ${i + 1}/${qty}…`;
    } catch(e) {
      errors.push(`${dname || `#${i+1}`}: ${e.message}`);
    }
  }

  if (errors.length) {
    fb.textContent = `${created.length} criados, ${errors.length} erro(s): ${errors[0]}`;
    fb.className = "plant-edit-feedback err";
  } else if (qty === 1) {
    fb.textContent = `✓ Dispositivo #${created[0]} adicionado.`;
    fb.className = "plant-edit-feedback ok";
  } else {
    fb.textContent = `✓ ${created.length} dispositivos adicionados.`;
    fb.className = "plant-edit-feedback ok";
  }
  if (nameEl) nameEl.value = "";
  if (qtyEl) qtyEl.value = "1";
  _pemLoadDevices(_PLANT_EDIT_ID);
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
async function openPlantCreateModal() {
  const modal = document.getElementById("plantCreateModal");
  if (!modal) return;
  ["plantCreateNameInput","plantCreateDcInput","plantCreateAcInput",
   "plantCreateLocationInput"].forEach(id => {
    const el = document.getElementById(id);
    if (el) el.value = "";
  });
  const fb = document.getElementById("plantCreateFeedback");
  if (fb) { fb.textContent = ""; fb.className = "plant-edit-feedback"; }

  const _u = JSON.parse(localStorage.getItem("user") || "{}");
  const customerSection = document.getElementById("plantCreateCustomerSection");
  const custSelect = document.getElementById("plantCreateCustomerInput");
  if (customerSection) {
    customerSection.style.display = _u.is_superuser ? "" : "none";
    if (_u.is_superuser && custSelect) {
      custSelect.innerHTML = `<option value="">Carregando...</option>`;
      try {
        const res = await apiFetch("/plants/customers");
        const data = await res.json();
        const items = data?.items || [];
        custSelect.innerHTML = `<option value="">Selecione o cliente</option>` +
          items.map(c => `<option value="${c.id}">${c.name} (${c.plant_count} usinas)</option>`).join("");
      } catch(e) {
        custSelect.innerHTML = `<option value="">Erro ao carregar</option>`;
      }
    }
  }

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
    setTimeout(() => {
      closePlantCreateModal();
      openPlantEditModal(data.plant_id, data.plant_name, payload.capacity_dc || 0);
      switchPlantEditTab("devices");
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
    panel.classList.remove("ronda-expanded");
    panel.style.width = ""; panel.style.maxHeight = "";
    _appRondaSwitchTab("diag");
    if (badge && ROBOT_STATE.issues.length > 0) badge.classList.remove("hidden");
    ROBOT_STATE.reportOpen = false;
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
  try { updatePlantCardIssueBadges(); } catch (_) {}

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

    // Som de notificação — só após o primeiro refresh (não toca no load)
    if (_robotSoundPrimed) {
      try {
        const hasCritical = issues.some(i => (i.severity || "").toLowerCase() === "critical");
        window.NotifySound?.play(hasCritical ? "critical" : "warning");
      } catch (_) {}
    }
  }
  _robotSoundPrimed = true;
}

let _robotSoundPrimed = false;

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

/* ── Ronda Diária (aba no robô do portfólio) ── */
let _APP_RONDA_DATA = null;
let _APP_RONDA_LOADING = false;
let _APP_RONDA_PLANTS = [];

function _appRondaSwitchTab(tab) {
  document.querySelectorAll(".robot-tab").forEach(b => b.classList.toggle("active", b.dataset.tab === tab));
  const list = document.getElementById("robotReportList");
  const ronda = document.getElementById("robotRondaContent");
  const reportEl = document.getElementById("robotReportContent");
  const panel = document.getElementById("robotReport");
  const prefsFooter = panel ? panel.querySelector(".robot-prefs-footer") : null;
  if (tab === "diag") {
    if (list) list.classList.remove("hidden");
    if (ronda) ronda.classList.add("hidden");
    if (reportEl) reportEl.classList.add("hidden");
    if (prefsFooter) prefsFooter.style.display = "";
    if (panel) { panel.classList.remove("ronda-expanded"); panel.style.width = ""; panel.style.maxHeight = ""; }
  } else if (tab === "ronda") {
    if (list) list.classList.add("hidden");
    if (ronda) { ronda.classList.remove("hidden"); ronda.scrollTop = 0; }
    if (reportEl) reportEl.classList.add("hidden");
    if (prefsFooter) prefsFooter.style.display = "none";
    if (panel) panel.classList.add("ronda-expanded");
    _appRondaEnsurePlants();
  } else if (tab === "report") {
    if (list) list.classList.add("hidden");
    if (ronda) ronda.classList.add("hidden");
    if (reportEl) { reportEl.classList.remove("hidden"); reportEl.scrollTop = 0; }
    if (prefsFooter) prefsFooter.style.display = "none";
    if (panel) panel.classList.add("ronda-expanded");
    _appReportEnsurePlants();
  }
}

async function _appRondaEnsurePlants() {
  const el = document.getElementById("robotRondaContent");
  if (!el) return;
  if (_APP_RONDA_PLANTS.length > 0 && !_APP_RONDA_DATA) {
    _appRondaRenderPicker(el);
    return;
  }
  if (_APP_RONDA_PLANTS.length > 0) return;
  el.innerHTML = '<div class="ronda-loading"><i class="fa-solid fa-spinner fa-spin"></i><br>Carregando usinas...</div>';
  try {
    _APP_RONDA_PLANTS = await fetchPlants();
    _appRondaRenderPicker(el);
  } catch (e) {
    el.innerHTML = `<div class="ronda-error"><i class="fa-solid fa-triangle-exclamation"></i> Erro: ${e.message}</div>`;
  }
}

function _appRondaRenderPicker(el) {
  const plants = _APP_RONDA_PLANTS;
  if (!plants.length) {
    el.innerHTML = '<div class="ronda-error">Nenhuma usina encontrada</div>';
    return;
  }
  const yesterday = new Date(); yesterday.setDate(yesterday.getDate() - 1);
  const defaultDate = yesterday.toISOString().slice(0, 10);
  const opts = plants.map(p => {
    const id = p.power_plant_id ?? p.plant_id ?? p.id;
    const name = p.power_plant_name ?? p.plant_name ?? p.name ?? `Usina ${id}`;
    return `<option value="${id}">${name}</option>`;
  }).join("");

  el.innerHTML = `<div class="ronda-toolbar" style="border-top:none; border-bottom:1px solid rgba(255,255,255,0.06); justify-content:flex-start;">
    <select class="ronda-plant-select" id="appRondaPlantSelect">${opts}</select>
    <input type="date" class="ronda-date-picker" id="appRondaDatePicker" value="${defaultDate}" max="${new Date().toISOString().slice(0,10)}">
    <button class="ronda-btn" id="appRondaLoadBtn"><i class="fa-solid fa-search"></i> Carregar</button>
  </div>
  <div id="appRondaBody" style="flex:1;overflow-y:auto;"></div>`;

  document.getElementById("appRondaLoadBtn")?.addEventListener("click", () => {
    const plantId = document.getElementById("appRondaPlantSelect")?.value;
    const date = document.getElementById("appRondaDatePicker")?.value;
    if (plantId) _appRondaFetch(plantId, date);
  });
}

async function _appRondaFetch(plantId, dateStr) {
  const bodyEl = document.getElementById("appRondaBody");
  if (!bodyEl) return;
  _APP_RONDA_LOADING = true;
  bodyEl.innerHTML = '<div class="ronda-loading"><i class="fa-solid fa-spinner fa-spin"></i><br>Carregando ronda...</div>';
  try {
    let url = `${API_BASE}/plants/${plantId}/realtime?view=daily-round`;
    if (dateStr) url += `&date=${dateStr}`;
    const res = await apiFetch(url.replace(API_BASE, ""));
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    let data = await res.json();
    if (data && data.body) data = typeof data.body === "string" ? JSON.parse(data.body) : data.body;
    _APP_RONDA_DATA = data;
    _appRondaRenderContent(data, bodyEl);
  } catch (e) {
    console.error("[RONDA-APP]", e);
    bodyEl.innerHTML = `<div class="ronda-error"><i class="fa-solid fa-triangle-exclamation"></i> Erro: ${e.message}</div>`;
  } finally {
    _APP_RONDA_LOADING = false;
  }
}

function _appRondaFmt(v, dec) {
  if (v == null || v === "") return "—";
  const n = Number(v);
  return isNaN(n) ? String(v) : n.toLocaleString("pt-BR", { minimumFractionDigits: dec || 0, maximumFractionDigits: dec || 0 });
}

function _appRondaPerfClass(cls) {
  if (!cls || cls === "sem_dados") return "";
  return `ronda-perf-${cls}`;
}

function _appRondaPerfLabel(cls) {
  const map = { acima: "Acima", normal: "Normal", abaixo: "Abaixo", sem_dados: "—" };
  return map[cls] || cls || "—";
}

function _rondaInfoBtn(text) {
  return `<span tabindex="0" onclick="this.nextElementSibling.classList.toggle('hidden')" style="display:inline-flex;align-items:center;justify-content:center;width:17px;height:17px;border-radius:50%;background:rgba(57,229,140,0.12);color:#39e58c;font-size:9px;font-weight:800;cursor:pointer;margin-left:5px;flex-shrink:0;border:1px solid rgba(57,229,140,0.3);vertical-align:middle;line-height:1;transition:all .15s;" onmouseover="this.style.background='rgba(57,229,140,0.25)';this.style.boxShadow='0 0 8px rgba(57,229,140,0.3)'" onmouseout="this.style.background='rgba(57,229,140,0.12)';this.style.boxShadow='none'">?</span><div class="ronda-info-pop hidden" style="display:block;background:linear-gradient(135deg,rgba(10,18,12,0.97),rgba(15,25,18,0.97));border:1px solid rgba(57,229,140,0.2);border-radius:10px;padding:12px 14px;font-size:10.5px;line-height:1.7;color:rgba(255,255,255,0.8);margin:6px 0 8px;max-width:420px;box-shadow:0 6px 24px rgba(0,0,0,0.5),0 0 12px rgba(57,229,140,0.08);">${text}</div>`;
}
function _rondaFormula(f) { return `<span style="display:inline-block;background:rgba(57,229,140,0.08);border:1px solid rgba(57,229,140,0.15);border-radius:4px;padding:2px 6px;font-family:'JetBrains Mono',monospace;font-size:9.5px;color:#39e58c;margin:2px 0;">${f}</span>`; }
function _rondaLabel(l) { return `<span style="color:#39e58c;font-weight:700;">${l}</span>`; }
function _rondaNote(n) { return `<span style="display:block;margin-top:4px;padding-top:4px;border-top:1px solid rgba(255,255,255,0.06);color:rgba(255,255,255,0.45);font-size:9.5px;">${n}</span>`; }

function _appRondaRenderContent(data, el) {
  if (!data) return;
  const ps = data.plant_summary || {};
  const w = data.weather || {};
  const invs = data.inverters || [];
  const sb = data.string_box || [];
  const alarms = data.alarms || [];
  let html = "";

  html += `<div class="ronda-section">
    <div class="ronda-section-title"><i class="fa-solid fa-solar-panel"></i> Resumo da Usina — ${ps.power_plant_name || ""}</div>
    <div class="ronda-kpi-grid">
      <div class="ronda-kpi"><span class="ronda-kpi-label">Geração ${_rondaInfoBtn(`${_rondaLabel("Geração Diária (kWh)")}<br>Energia ativa total produzida pela usina no dia.<br><br>${_rondaLabel("Fonte dos dados:")}<br>Contadores de energia dos inversores. Quando indisponível, usa integração trapezoidal da curva de potência ativa.${_rondaNote("Horário de cálculo: período solar completo do dia.")}`)}</span><span class="ronda-kpi-value">${_appRondaFmt(ps.generation_kwh, 1)} kWh</span></div>
      <div class="ronda-kpi"><span class="ronda-kpi-label">PR Diário ${_rondaInfoBtn(`${_rondaLabel("Performance Ratio — Diário")}<br>Eficiência global da usina, descontando a irradiação disponível.<br><br>${_rondaLabel("Fórmula:")}<br>${_rondaFormula("PR = (Geração real [kWh]) / (Cap. DC [kWp] × Irradiação [kWh/m²]) × 100")}<br><br>${_rondaLabel("Referência:")}<br>● <span style='color:#39e58c'>≥ 75%</span> — Bom&ensp;● <span style='color:#eab308'>60–75%</span> — Atenção&ensp;● <span style='color:#ef4444'>< 60%</span> — Crítico${_rondaNote("Irradiação: sensor POA (prioridade), GHI ou genérico.")}`)}</span><span class="ronda-kpi-value ${(ps.pr_daily_pct || 0) >= 75 ? "val-good" : (ps.pr_daily_pct || 0) >= 60 ? "val-warn" : "val-bad"}">${_appRondaFmt(ps.pr_daily_pct, 1)}%</span></div>
      <div class="ronda-kpi"><span class="ronda-kpi-label">PR Acumulado ${_rondaInfoBtn(`${_rondaLabel("Performance Ratio — Acumulado no Mês")}<br>Consolida o PR de todos os dias do mês até a data selecionada.<br><br>${_rondaLabel("Fórmula:")}<br>${_rondaFormula("PR Acum. = Σ(Geração diária) / Σ(Cap. DC × Irradiação diária) × 100")}${_rondaNote("Dias sem irradiação são excluídos do cálculo.")}`)}</span><span class="ronda-kpi-value">${_appRondaFmt(ps.pr_accumulated_pct, 1)}%</span></div>
      <div class="ronda-kpi"><span class="ronda-kpi-label">Fator Capac. Diário ${_rondaInfoBtn(`${_rondaLabel("Fator de Capacidade — Diário")}<br>Quanto a usina gerou em relação ao máximo teórico (potência nominal × 24h).<br><br>${_rondaLabel("Fórmula:")}<br>${_rondaFormula("FC = (Geração real [kWh]) / (Cap. DC [kWp] × 24h) × 100")}<br><br>${_rondaLabel("Referência:")}<br>Usinas fotovoltaicas no Nordeste: tipicamente <b>15–25%</b>.${_rondaNote("Diferente do PR, o FC não desconta a irradiação — reflete a proporção de uso em 24h.")}`)}</span><span class="ronda-kpi-value">${_appRondaFmt(ps.capacity_factor_daily_pct, 1)}%</span></div>
      <div class="ronda-kpi"><span class="ronda-kpi-label">Início Geração</span><span class="ronda-kpi-value">${ps.gen_start_time || "—"}</span></div>
      <div class="ronda-kpi"><span class="ronda-kpi-label">Últ. Atualização</span><span class="ronda-kpi-value">${ps.gen_end_time || "—"}</span></div>
    </div>
  </div>`;

  html += `<div class="ronda-section">
    <div class="ronda-section-title"><i class="fa-solid fa-cloud-sun"></i> Estação Solarimétrica</div>
    <div class="ronda-kpi-grid">
      <div class="ronda-kpi"><span class="ronda-kpi-label">POA Méd</span><span class="ronda-kpi-value">${_appRondaFmt(w.poa_avg_wm2, 1)} W/m²</span></div>
      <div class="ronda-kpi"><span class="ronda-kpi-label">GHI Méd</span><span class="ronda-kpi-value">${_appRondaFmt(w.ghi_avg_wm2, 1)} W/m²</span></div>
      <div class="ronda-kpi"><span class="ronda-kpi-label">POA Acum.</span><span class="ronda-kpi-value">${_appRondaFmt(w.poa_acc_wh_m2, 1)} Wh/m²</span></div>
      <div class="ronda-kpi"><span class="ronda-kpi-label">GHI Acum.</span><span class="ronda-kpi-value">${_appRondaFmt(w.ghi_acc_wh_m2, 1)} Wh/m²</span></div>
      <div class="ronda-kpi"><span class="ronda-kpi-label">Temp. Média</span><span class="ronda-kpi-value">${_appRondaFmt(w.air_temp_avg_c, 1)} °C</span></div>
      <div class="ronda-kpi"><span class="ronda-kpi-label">Temp. Máx</span><span class="ronda-kpi-value">${_appRondaFmt(w.air_temp_max_c, 1)} °C</span></div>
      <div class="ronda-kpi"><span class="ronda-kpi-label">Vento Méd</span><span class="ronda-kpi-value">${_appRondaFmt(w.wind_speed_avg, 1)} m/s</span></div>
      <div class="ronda-kpi"><span class="ronda-kpi-label">Chuva</span><span class="ronda-kpi-value">${w.rain_detected ? "Sim" : "Não"}</span></div>
    </div>
  </div>`;

  if (invs.length) {
    html += `<div class="ronda-section">
      <div class="ronda-section-title"><i class="fa-solid fa-bolt"></i> Inversores ${_rondaInfoBtn(`${_rondaLabel("Métricas por Inversor")}<br><br>${_rondaLabel("Pot Méd")} — Potência ativa média (kW) durante horário de geração.<br>${_rondaLabel("Energia")} — Energia total gerada no dia (kWh).<br>${_rondaLabel("PR")} — Performance Ratio individual do inversor:<br>${_rondaFormula("PR inv = Energia inv / (Pot. nominal × Irradiação) × 100")}<br>${_rondaLabel("Perf.")} — Potência média vs média própria dos últimos 30 dias.<br>${_rondaLabel("vs Média")} — PR do inversor vs média do PR de toda a frota.<br><br>${_rondaLabel("Classificação (limiar ±15%):")}<br>● <span style='color:#39e58c'>Acima</span> — > +15% da referência<br>● <span style='color:rgba(255,255,255,0.7)'>Normal</span> — dentro de ±15%<br>● <span style='color:#ef4444'>Abaixo</span> — < −15% da referência`)}</div>
      <div style="overflow-x:auto;"><table class="ronda-inv-table">
        <thead><tr><th>Inv</th><th>Pot Méd</th><th>Energia</th><th>PR</th><th>Perf.</th><th>vs Média</th></tr></thead>
        <tbody>`;
    invs.forEach(inv => {
      const perfCls = _appRondaPerfClass(inv.power_performance);
      const prCls = _appRondaPerfClass(inv.pr_vs_fleet);
      html += `<tr>
        <td>${inv.inverter_name || "Inv" + inv.device_id}</td>
        <td>${_appRondaFmt(inv.avg_power_kw, 1)} kW</td>
        <td>${_appRondaFmt(inv.energy_daily_kwh, 0)} kWh</td>
        <td>${_appRondaFmt(inv.pr_inverter_pct, 1)}%</td>
        <td><span class="ronda-perf-badge ${perfCls}">${_appRondaPerfLabel(inv.power_performance)}</span></td>
        <td><span class="ronda-perf-badge ${prCls}">${_appRondaPerfLabel(inv.pr_vs_fleet)}</span></td>
      </tr>`;
    });
    html += `</tbody></table></div></div>`;
  }

  if (sb && sb.length) {
    html += `<div class="ronda-section"><div class="ronda-section-title"><i class="fa-solid fa-plug-circle-check"></i> Corrente Strings ${_rondaInfoBtn(`${_rondaLabel("Análise de Corrente por String")}<br><br>${_rondaLabel("O que mostra:")}<br>Corrente média (A) de cada string no horário solar (6h–18h).<br><br>${_rondaLabel("Referência:")}<br>Média de corrente de todas as strings <b>ativas</b> (> 0.1A) do mesmo inversor, no mesmo dia.<br><br>${_rondaLabel("Fórmula da variação:")}<br>${_rondaFormula("Var% = ((I string − I méd inv) / I méd inv) × 100")}<br><br>${_rondaLabel("Escala de cores:")}<br>● <span style='color:#39e58c'>Verde</span> — variação ≥ −5% (normal)<br>● <span style='color:#eab308'>Amarelo</span> — entre −5% e −15% (atenção)<br>● <span style='color:#ef4444'>Vermelho</span> — abaixo de −15% (anomalia)${_rondaNote("Strings zeradas (< 0.1A) são excluídas da média de referência do inversor.")}`)}</div>`;
    sb.forEach(inv => {
      const name = inv.device_name || inv.inverter_name || ("Inv" + inv.device_id);
      const strings = inv.strings || [];
      if (!strings.length) return;
      const active = strings.filter(s => (s.avg_current || 0) >= 0.1);
      const invAvg = active.length > 0 ? active.reduce((s, x) => s + Number(x.avg_current || 0), 0) / active.length : 0;
      html += `<div style="margin-bottom:10px;">
        <span style="font-size:11px;font-weight:600;color:rgba(255,255,255,0.7);">${name}</span>
        <span style="font-size:9.5px;color:rgba(255,255,255,0.35);margin-left:8px;">méd inv: ${invAvg.toFixed(1)}A</span>
        <div style="display:grid;grid-template-columns:repeat(auto-fill,minmax(180px,1fr));gap:3px 12px;margin-top:3px;">`;
      strings.forEach(s => {
        const avg = Number(s.avg_current || 0);
        let vPct = s.variation_pct != null ? Number(s.variation_pct) : null;
        if (vPct == null && invAvg > 0 && avg >= 0.1) vPct = ((avg - invAvg) / invAvg) * 100;
        const sign = vPct != null ? (vPct >= 0 ? "+" : "") : "";
        const color = vPct == null ? "rgba(255,255,255,0.4)" : vPct >= -5 ? "#39e58c" : vPct >= -15 ? "#eab308" : "#ef4444";
        const varLabel = vPct != null ? `${sign}${vPct.toFixed(1)}%` : "—";
        html += `<div style="display:flex;align-items:center;gap:6px;font-size:10px;">
          <span style="width:26px;color:rgba(255,255,255,0.45);">S${s.string_index}</span>
          <span style="color:rgba(255,255,255,0.7);min-width:40px;">${avg.toFixed(1)}A</span>
          <span style="font-weight:700;color:${color};min-width:45px;text-align:right;">${varLabel}</span>
        </div>`;
      });
      html += `</div></div>`;
    });
    html += `</div>`;
  }

  const trks = data.trackers || [];
  if (trks.length) {
    html += `<div class="ronda-section"><div class="ronda-section-title"><i class="fa-solid fa-arrows-rotate"></i> Trackers (${trks.length})</div>`;
    trks.forEach(t => {
      const dev = t.avg_deviation != null ? t.avg_deviation.toFixed(1) + "°" : "—";
      const devColor = (t.avg_deviation || 0) > 5 ? "#ef4444" : (t.avg_deviation || 0) > 2 ? "#eab308" : "#39e58c";
      const pos = t.current_position != null ? t.current_position.toFixed(1) + "°" : "—";
      const tgt = t.current_target != null ? t.current_target.toFixed(1) + "°" : "—";
      html += `<div style="display:flex;align-items:center;gap:8px;padding:3px 0;font-size:11px;">
        <span style="min-width:70px;font-weight:600;color:rgba(255,255,255,0.8);">${t.tracker_name || "Tracker"}</span>
        <span style="color:rgba(255,255,255,0.5);">Pos: ${pos}</span>
        <span style="color:rgba(255,255,255,0.5);">Alvo: ${tgt}</span>
        <span style="font-weight:700;color:${devColor};">Desvio: ${dev}</span>
      </div>`;
    });
    html += `</div>`;
  }

  html += `<div class="ronda-toolbar">
    <button class="ronda-btn report-btn-pdf" id="appRondaDlPdf"><i class="fa-solid fa-file-pdf"></i> PDF</button>
    <button class="ronda-expand-btn" id="appRondaExpandBtn"><i class="fa-solid fa-expand"></i> Expandir</button>
  </div>`;

  el.innerHTML = html;
  document.getElementById("appRondaDlPdf")?.addEventListener("click", () => _appRondaDownloadPdf(data));
  document.getElementById("appRondaExpandBtn")?.addEventListener("click", () => _appRondaOpenFullPanel(data));
}

function _appRondaDownloadCsv(data) {
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
  lines.push("Últ. Atualização Geração," + (ps.gen_end_time || ""));
  lines.push("");
  lines.push("ESTAÇÃO SOLARIMÉTRICA");
  lines.push("Irrad Media W/m2," + (w.irradiance_avg_wm2 || ""));
  lines.push("Irrad Max W/m2," + (w.irradiance_max_wm2 || ""));
  lines.push("Temp Media C," + (w.air_temp_avg_c || ""));
  lines.push("Vento m/s," + (w.wind_speed_avg || ""));
  lines.push("Chuva," + (w.rain_detected ? "Sim" : "Nao"));
  lines.push("");
  if (invs.length) {
    lines.push("INVERSORES");
    lines.push("Nome,Pot Media kW,Energia kWh,PR %,Performance,vs Media");
    invs.forEach(inv => lines.push([inv.inverter_name||"", inv.avg_power_kw||"", inv.energy_daily_kwh||"", inv.pr_inverter_pct||"", inv.power_performance||"", inv.pr_vs_fleet||""].join(",")));
    lines.push("");
  }
  if (alarms.length) {
    lines.push("ALARMES");
    lines.push("Timestamp,Dispositivo,Tipo,Descrição,Severidade");
    alarms.forEach(a => lines.push([(a.timestamp||"").replace(/,/g,";"), a.device_name||"", a.code||"", (a.description||"").replace(/,/g,";"), a.severity||""].join(",")));
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

// Rasteriza o elemento do relatório limitando o tamanho do canvas: com scale fixo 2 um relatório
// comprido estoura o teto de área de canvas do navegador (Safari/iOS ~16 Mpx) e o toDataURL devolve
// um PNG vazio → jsPDF: "Incomplete or corrupt PNG file". Aqui a escala é reduzida p/ caber, com
// fallback p/ JPEG (menor em memória) e, em último caso, erro claro pro usuário.
async function _pdfRenderCanvas(bodyEl) {
  const cw = bodyEl.scrollWidth || bodyEl.clientWidth || 1;
  const ch = bodyEl.scrollHeight || bodyEl.clientHeight || 1;
  const MAX_DIM = 8000;                 // teto por dimensão (px)
  const MAX_AREA = 24 * 1024 * 1024;    // teto de área seguro entre navegadores (~24 Mpx)
  let scale = Math.min(2, MAX_DIM / cw, MAX_DIM / ch, Math.sqrt(MAX_AREA / (cw * ch)));
  if (!isFinite(scale) || scale <= 0) scale = 1;
  scale = Math.max(0.5, scale);
  const canvas = await html2canvas(bodyEl, { backgroundColor: "#1a1d23", scale, scrollY: 0, scrollX: 0, windowHeight: ch + 200 });
  let imgData = canvas.toDataURL("image/png");
  let imgFmt = "PNG";
  if (!imgData || imgData.length < 1000) {            // PNG vazio → tenta JPEG (bem menor)
    imgData = canvas.toDataURL("image/jpeg", 0.92);
    imgFmt = "JPEG";
  }
  if (!imgData || imgData.length < 1000) {
    throw new Error("relatório grande demais para o navegador gerar de uma vez. Tente reduzir o período ou gerar por seção.");
  }
  return { canvas, imgData, imgFmt };
}

async function _pdfCaptureFull(bodyEl, panelEl, filename, orientation) {
  const overlay = document.createElement("div");
  overlay.style.cssText = "position:fixed;inset:0;z-index:99999;background:rgba(0,0,0,0.7);display:flex;align-items:center;justify-content:center;font-size:14px;color:#39e58c;font-family:'Inter',sans-serif;";
  overlay.innerHTML = '<div><i class="fa-solid fa-spinner fa-spin" style="margin-right:8px;"></i>Gerando PDF...</div>';
  document.body.appendChild(overlay);
  const savedOverflow = bodyEl.style.overflow;
  const savedMaxH = bodyEl.style.maxHeight;
  const savedH = bodyEl.style.height;
  const savedPanelH = panelEl ? panelEl.style.height : "";
  bodyEl.style.overflow = "visible";
  bodyEl.style.maxHeight = "none";
  bodyEl.style.height = "auto";
  if (panelEl) { panelEl.style.height = "auto"; panelEl.style.overflow = "visible"; }
  await new Promise(r => setTimeout(r, 200));
  try {
    if (typeof html2canvas === "undefined") { const sc = document.createElement("script"); sc.src = "https://cdnjs.cloudflare.com/ajax/libs/html2canvas/1.4.1/html2canvas.min.js"; document.head.appendChild(sc); await new Promise((r, j) => { sc.onload = r; sc.onerror = j; }); }
    if (typeof jspdf === "undefined" && typeof jsPDF === "undefined") { const sc = document.createElement("script"); sc.src = "https://cdnjs.cloudflare.com/ajax/libs/jspdf/2.5.1/jspdf.umd.min.js"; document.head.appendChild(sc); await new Promise((r, j) => { sc.onload = r; sc.onerror = j; }); }
    const JP = (typeof jsPDF !== "undefined") ? jsPDF : (typeof jspdf !== "undefined" ? jspdf.jsPDF : window.jspdf.jsPDF);
    const { canvas, imgData, imgFmt } = await _pdfRenderCanvas(bodyEl);
    const pdf = new JP({ orientation: orientation || "landscape", unit: "mm", format: "a4" });
    const pW = pdf.internal.pageSize.getWidth(), pH = pdf.internal.pageSize.getHeight(), m = 10, uW = pW - 2 * m;
    const imgH = (canvas.height / canvas.width) * uW;
    let yOff = 0;
    while (yOff < imgH) { if (yOff > 0) pdf.addPage(); pdf.addImage(imgData, imgFmt, m, m - yOff, uW, imgH); pdf.setFontSize(8); pdf.setTextColor(150); pdf.text(window.__BRANDING_PDF_FOOTER || "Gerado automaticamente pela plataforma AIOTI Solar SCADA", pW / 2, pH - 5, { align: "center" }); yOff += pH - 2 * m; }
    pdf.save(filename);
  } catch (e) { console.error("[PDF]", e); alert("Erro ao gerar PDF: " + e.message); }
  finally {
    bodyEl.style.overflow = savedOverflow;
    bodyEl.style.maxHeight = savedMaxH;
    bodyEl.style.height = savedH;
    if (panelEl) { panelEl.style.height = savedPanelH; panelEl.style.overflow = ""; }
    overlay.remove();
  }
}

async function _appRondaDownloadPdf(data) {
  if (!data) return;
  const panel = document.getElementById("rondaFullPanel");
  if (!panel || panel.classList.contains("hidden")) { _appRondaOpenFullPanel(data); await new Promise(r => setTimeout(r, 500)); }
  const bodyEl = document.getElementById("rondaFullBody");
  if (!bodyEl) return;
  const ps = data.plant_summary || {};
  await _pdfCaptureFull(bodyEl, panel, `Ronda_Diaria_${(ps.power_plant_name || "usina").replace(/\s+/g, "_")}_${data.date || "hoje"}.pdf`, "portrait");
}

function _appRondaOpenFullPanel(data) {
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
  if (datePicker) { datePicker.value = data.date || ""; datePicker.max = new Date().toISOString().slice(0, 10); }
  const dlBtn = document.getElementById("rondaFullDownloadPdf");
  if (dlBtn) dlBtn.onclick = () => _appRondaDownloadPdf(data);
  const closeBtn = document.getElementById("rondaFullClose");
  if (closeBtn) closeBtn.onclick = () => { panel.classList.add("hidden"); document.body.style.overflow = ""; };

  const svgSolar = '<svg viewBox="0 0 24 24" fill="none" stroke="#facc15" stroke-width="2"><circle cx="12" cy="12" r="4"/><path d="M12 2v2M12 20v2M4.93 4.93l1.41 1.41M17.66 17.66l1.41 1.41M2 12h2M20 12h2M4.93 19.07l1.41-1.41M17.66 6.34l1.41-1.41"/></svg>';
  const svgWeather = '<svg viewBox="0 0 24 24" fill="none" stroke="#60a5fa" stroke-width="2"><path d="M3 15a4 4 0 0 0 4 4h9a5 5 0 0 0 .5-9.97A7 7 0 0 0 3 11.5"/></svg>';
  const svgBolt = '<svg viewBox="0 0 24 24" fill="none" stroke="#39e58c" stroke-width="2"><path d="M13 2L3 14h9l-1 8 10-12h-9l1-8z"/></svg>';
  const svgString = '<svg viewBox="0 0 24 24" fill="none" stroke="#a855f7" stroke-width="2"><rect x="2" y="7" width="20" height="10" rx="2"/><path d="M6 7V5a2 2 0 0 1 2-2h8a2 2 0 0 1 2 2v2M6 17v2a2 2 0 0 0 2 2h8a2 2 0 0 0 2-2v-2"/></svg>';
  const svgAlarm = '<svg viewBox="0 0 24 24" fill="none" stroke="#ef4444" stroke-width="2"><path d="M18 8A6 6 0 0 0 6 8c0 7-3 9-3 9h18s-3-2-3-9"/><path d="M13.73 21a2 2 0 0 1-3.46 0"/></svg>';
  const prCls = (ps.pr_daily_pct || 0) >= 75 ? "val-good" : (ps.pr_daily_pct || 0) >= 60 ? "val-warn" : "val-bad";

  let body = '<div class="ronda-full-grid">';
  body += `<div class="ronda-card"><div class="ronda-card-header"><div class="ronda-card-icon icon-solar">${svgSolar}</div><div><div class="ronda-card-title">Resumo da Usina</div><div class="ronda-card-subtitle">${data.date || ""}</div></div></div><div class="ronda-card-body"><div class="ronda-full-kpi-row"><div class="ronda-full-kpi"><div class="ronda-full-kpi-label">Geração</div><div class="ronda-full-kpi-value">${_appRondaFmt(ps.generation_kwh, 1)}<span class="ronda-full-kpi-unit">kWh</span></div></div><div class="ronda-full-kpi"><div class="ronda-full-kpi-label">PR Diário</div><div class="ronda-full-kpi-value ${prCls}">${_appRondaFmt(ps.pr_daily_pct, 1)}<span class="ronda-full-kpi-unit">%</span></div></div><div class="ronda-full-kpi"><div class="ronda-full-kpi-label">PR Acum.</div><div class="ronda-full-kpi-value">${_appRondaFmt(ps.pr_accumulated_pct, 1)}<span class="ronda-full-kpi-unit">%</span></div></div></div><div class="ronda-full-kpi-row" style="margin-top:8px;"><div class="ronda-full-kpi"><div class="ronda-full-kpi-label">Fator Capac. Diário</div><div class="ronda-full-kpi-value">${_appRondaFmt(ps.capacity_factor_daily_pct, 1)}<span class="ronda-full-kpi-unit">%</span></div></div><div class="ronda-full-kpi"><div class="ronda-full-kpi-label">Início</div><div class="ronda-full-kpi-value" style="font-size:16px;">${ps.gen_start_time || "—"}</div></div><div class="ronda-full-kpi"><div class="ronda-full-kpi-label">Últ. Atualização</div><div class="ronda-full-kpi-value" style="font-size:16px;">${ps.gen_end_time || "—"}</div></div></div></div></div>`;
  body += `<div class="ronda-card"><div class="ronda-card-header"><div class="ronda-card-icon icon-weather">${svgWeather}</div><div><div class="ronda-card-title">Estação Solarimétrica</div><div class="ronda-card-subtitle">${w.irradiance_classification ? "Irradiância: " + w.irradiance_classification : ""}</div></div></div><div class="ronda-card-body">
    <div class="ronda-full-kpi-row">
      <div class="ronda-full-kpi"><div class="ronda-full-kpi-label">POA Méd</div><div class="ronda-full-kpi-value">${_appRondaFmt(w.poa_avg_wm2, 1)}<span class="ronda-full-kpi-unit">W/m²</span></div></div>
      <div class="ronda-full-kpi"><div class="ronda-full-kpi-label">GHI Méd</div><div class="ronda-full-kpi-value">${_appRondaFmt(w.ghi_avg_wm2, 1)}<span class="ronda-full-kpi-unit">W/m²</span></div></div>
      <div class="ronda-full-kpi"><div class="ronda-full-kpi-label">Temp. Média</div><div class="ronda-full-kpi-value">${_appRondaFmt(w.air_temp_avg_c, 1)}<span class="ronda-full-kpi-unit">°C</span></div></div>
    </div>
    <div class="ronda-full-kpi-row" style="margin-top:8px;">
      <div class="ronda-full-kpi"><div class="ronda-full-kpi-label">POA Acum.</div><div class="ronda-full-kpi-value">${_appRondaFmt(w.poa_acc_wh_m2, 1)}<span class="ronda-full-kpi-unit">Wh/m²</span></div></div>
      <div class="ronda-full-kpi"><div class="ronda-full-kpi-label">GHI Acum.</div><div class="ronda-full-kpi-value">${_appRondaFmt(w.ghi_acc_wh_m2, 1)}<span class="ronda-full-kpi-unit">Wh/m²</span></div></div>
      <div class="ronda-full-kpi"><div class="ronda-full-kpi-label">Vento</div><div class="ronda-full-kpi-value">${_appRondaFmt(w.wind_speed_avg, 1)}<span class="ronda-full-kpi-unit">m/s</span></div></div>
    </div>
    <div class="ronda-full-kpi-row" style="margin-top:8px;">
      <div class="ronda-full-kpi"><div class="ronda-full-kpi-label">Temp. Máx</div><div class="ronda-full-kpi-value">${_appRondaFmt(w.air_temp_max_c, 1)}<span class="ronda-full-kpi-unit">°C</span></div></div>
      <div class="ronda-full-kpi"><div class="ronda-full-kpi-label">Chuva</div><div class="ronda-full-kpi-value">${w.rain_detected ? "Sim" : "Não"}</div></div>
    </div>
  </div></div>`;

  if (invs.length) {
    body += `<div class="ronda-card span-full"><div class="ronda-card-header"><div class="ronda-card-icon icon-bolt">${svgBolt}</div><div><div class="ronda-card-title">Inversores</div><div class="ronda-card-subtitle">${invs.length} unidades</div></div></div><div class="ronda-card-body" style="padding:0;"><div style="overflow-x:auto;"><table class="ronda-full-inv-table"><thead><tr><th>Inversor</th><th>Pot. Média</th><th>Pot. Máx</th><th>Energia</th><th>PR</th><th>Temp. Média</th><th>Performance</th><th>vs Média (Pot)</th><th>vs Média (PR)</th><th>Disponib.</th></tr></thead><tbody>`;
    invs.forEach(inv => {
      const pC = inv.power_performance && inv.power_performance !== "sem_dados" ? `ronda-full-perf-${inv.power_performance}` : "";
      const fPC = inv.power_vs_fleet && inv.power_vs_fleet !== "sem_dados" ? `ronda-full-perf-${inv.power_vs_fleet}` : "";
      const fPrC = inv.pr_vs_fleet && inv.pr_vs_fleet !== "sem_dados" ? `ronda-full-perf-${inv.pr_vs_fleet}` : "";
      const arrow = inv.power_performance === "acima" ? "▲" : inv.power_performance === "abaixo" ? "▼" : "";
      body += `<tr><td style="font-weight:600;">${inv.inverter_name || "Inv" + inv.device_id}</td><td>${_appRondaFmt(inv.avg_power_kw, 1)} kW</td><td>${_appRondaFmt(inv.max_power_kw, 1)} kW</td><td>${_appRondaFmt(inv.energy_daily_kwh, 0)} kWh</td><td style="font-weight:700;">${_appRondaFmt(inv.pr_inverter_pct, 1)}%</td><td>${_appRondaFmt(inv.avg_temp_c, 1)} °C</td><td><span class="ronda-full-perf-badge ${pC}">${arrow} ${_appRondaPerfLabel(inv.power_performance)}</span></td><td><span class="ronda-full-perf-badge ${fPC}">${_appRondaPerfLabel(inv.power_vs_fleet)}</span></td><td><span class="ronda-full-perf-badge ${fPrC}">${_appRondaPerfLabel(inv.pr_vs_fleet)}</span></td><td>${_appRondaFmt(inv.running_pct, 1)}%</td></tr>`;
    });
    body += `</tbody></table></div></div></div>`;
  }

  if (sb && sb.length) {
    body += `<div class="ronda-card span-full"><div class="ronda-card-header"><div class="ronda-card-icon icon-string">${svgString}</div><div><div class="ronda-card-title">Corrente Strings — vs Média do Inversor</div><div class="ronda-card-subtitle">Corrente média de cada string vs média das strings ativas do mesmo inversor (horário solar 6h-18h)</div></div></div><div class="ronda-card-body">`;
    sb.forEach(inv => {
      const invName = inv.device_name || inv.inverter_name || ("Inv" + inv.device_id);
      const strings = inv.strings || [];
      if (!strings.length) return;
      const active = strings.filter(s => (s.avg_current || 0) >= 0.1);
      const invAvg = active.length > 0 ? active.reduce((s, x) => s + Number(x.avg_current || 0), 0) / active.length : 0;
      body += `<div style="margin-bottom:12px;"><div style="font-size:11.5px;font-weight:700;color:rgba(255,255,255,0.8);margin-bottom:2px;">${invName} <span style="font-weight:400;font-size:10px;color:rgba(255,255,255,0.4);">méd inv: ${invAvg.toFixed(1)}A</span></div>`;
      body += `<div style="display:grid;grid-template-columns:repeat(auto-fill,minmax(200px,1fr));gap:3px 14px;">`;
      strings.forEach(s => {
        const avg = Number(s.avg_current || 0);
        let vPct = s.variation_pct != null ? Number(s.variation_pct) : null;
        if (vPct == null && invAvg > 0 && avg >= 0.1) vPct = ((avg - invAvg) / invAvg) * 100;
        const sign = vPct != null ? (vPct >= 0 ? "+" : "") : "";
        const color = vPct == null ? "rgba(255,255,255,0.4)" : vPct >= -5 ? "#39e58c" : vPct >= -15 ? "#eab308" : "#ef4444";
        const varLabel = vPct != null ? `${sign}${vPct.toFixed(1)}%` : "—";
        body += `<div style="display:flex;align-items:center;gap:6px;font-size:10.5px;"><span style="width:30px;color:rgba(255,255,255,0.45);">S${s.string_index}</span><span style="color:rgba(255,255,255,0.7);min-width:45px;">${avg.toFixed(1)}A</span><span style="font-weight:700;color:${color};min-width:50px;text-align:right;">${varLabel}</span></div>`;
      });
      body += `</div></div>`;
    });
    body += `</div></div>`;
  }

  const _fTrks = data.trackers || [];
  if (_fTrks.length) {
    const svgTracker = '<svg viewBox="0 0 24 24" fill="none" stroke="#facc15" stroke-width="2"><rect x="3" y="11" width="18" height="2" rx="1"/><path d="M12 6v5M8 4l4 2 4-2"/><circle cx="12" cy="19" r="2"/></svg>';
    body += `<div class="ronda-card span-full"><div class="ronda-card-header"><div class="ronda-card-icon icon-solar">${svgTracker}</div><div><div class="ronda-card-title">Trackers</div><div class="ronda-card-subtitle">${_fTrks.length} unidades</div></div></div><div class="ronda-card-body">`;
    body += `<div style="display:grid;grid-template-columns:repeat(auto-fill,minmax(280px,1fr));gap:12px;">`;
    _fTrks.forEach(t => {
      const pos = t.current_position != null ? t.current_position : 0;
      const tgt = t.current_target != null ? t.current_target : 0;
      const dev = t.avg_deviation != null ? t.avg_deviation : 0;
      const devColor = dev > 5 ? "#ef4444" : dev > 2 ? "#eab308" : "#39e58c";
      const devStatus = dev > 5 ? "Desvio alto" : dev > 2 ? "Desvio moderado" : "Normal";
      const angle = Math.max(-60, Math.min(60, pos));
      body += `<div style="background:rgba(255,255,255,0.03);border:1px solid rgba(255,255,255,0.06);border-radius:10px;padding:12px;text-align:center;">
        <div style="font-weight:700;font-size:12px;color:rgba(255,255,255,0.85);margin-bottom:8px;">${t.tracker_name || "Tracker"}</div>
        <svg viewBox="0 0 120 80" width="120" height="80" style="display:block;margin:0 auto;">
          <line x1="60" y1="75" x2="60" y2="30" stroke="rgba(255,255,255,0.15)" stroke-width="3" stroke-linecap="round"/>
          <g transform="rotate(${angle}, 60, 30)">
            <rect x="20" y="26" width="80" height="6" rx="2" fill="${devColor}" opacity="0.8">
              <animateTransform attributeName="transform" type="rotate" from="${angle - 2} 60 30" to="${angle + 2} 60 30" dur="3s" repeatCount="indefinite" additive="sum" values="${angle - 2} 60 30;${angle + 2} 60 30;${angle - 2} 60 30"/>
            </rect>
            <circle cx="60" cy="29" r="4" fill="#facc15" opacity="0.6"/>
          </g>
          <circle cx="60" cy="75" r="3" fill="rgba(255,255,255,0.2)"/>
          <text x="60" y="18" text-anchor="middle" font-size="8" fill="rgba(255,255,255,0.4)">☀</text>
        </svg>
        <div style="display:flex;justify-content:center;gap:14px;font-size:10px;margin-top:6px;">
          <span style="color:rgba(255,255,255,0.5);">Pos: <strong style="color:rgba(255,255,255,0.85);">${pos.toFixed(1)}°</strong></span>
          <span style="color:rgba(255,255,255,0.5);">Alvo: <strong style="color:rgba(255,255,255,0.85);">${tgt.toFixed(1)}°</strong></span>
        </div>
        <div style="font-size:10px;margin-top:4px;font-weight:700;color:${devColor};">Desvio: ${dev.toFixed(1)}° — ${devStatus}</div>
        ${t.deviation_pct != null ? `<div style="font-size:9px;color:rgba(255,255,255,0.4);margin-top:2px;">${t.deviation_pct}% do tempo com desvio > 5°</div>` : ""}
      </div>`;
    });
    body += `</div></div></div>`;
  }

  body += "</div>";

  const bodyEl = document.getElementById("rondaFullBody");
  if (bodyEl) bodyEl.innerHTML = body;
  document.addEventListener("keydown", function _esc(e) { if (e.key === "Escape") { panel.classList.add("hidden"); document.body.style.overflow = ""; document.removeEventListener("keydown", _esc); } });
}

/* ── Relatório de Performance (aba no robô do portfólio) ── */
let _APP_REPORT_DATA = null;
let _APP_REPORT_LOADING = false;
let _APP_REPORT_PLANTS = [];

async function _appReportEnsurePlants() {
  const el = document.getElementById("robotReportContent");
  if (!el) return;
  if (_APP_REPORT_PLANTS.length > 0 && !_APP_REPORT_DATA) { _appReportRenderPicker(el); return; }
  if (_APP_REPORT_PLANTS.length > 0) return;
  el.innerHTML = '<div class="ronda-loading"><i class="fa-solid fa-spinner fa-spin"></i><br>Carregando usinas...</div>';
  try {
    _APP_REPORT_PLANTS = _APP_RONDA_PLANTS.length > 0 ? _APP_RONDA_PLANTS : await fetchPlants();
    if (!_APP_RONDA_PLANTS.length) _APP_RONDA_PLANTS = _APP_REPORT_PLANTS;
    _appReportRenderPicker(el);
  } catch (e) {
    el.innerHTML = `<div class="ronda-error"><i class="fa-solid fa-triangle-exclamation"></i> Erro: ${e.message}</div>`;
  }
}

function _appReportRenderPicker(el) {
  const plants = _APP_REPORT_PLANTS;
  if (!plants.length) { el.innerHTML = '<div class="ronda-error">Nenhuma usina encontrada</div>'; return; }
  const today = new Date().toISOString().slice(0, 10);
  const weekAgo = new Date(Date.now() - 7 * 86400000).toISOString().slice(0, 10);
  const opts = plants.map(p => {
    const id = p.power_plant_id ?? p.plant_id ?? p.id;
    const name = p.power_plant_name ?? p.plant_name ?? p.name ?? `Usina ${id}`;
    return `<option value="${id}">${name}</option>`;
  }).join("");
  el.innerHTML = `<div class="ronda-toolbar" style="border-top:none; border-bottom:1px solid rgba(255,255,255,0.06); justify-content:flex-start; flex-wrap:wrap;">
    <select class="ronda-plant-select" id="appReportPlantSelect">${opts}</select>
    <input type="date" class="ronda-date-picker" id="appReportStartDate" value="${weekAgo}" max="${today}" title="Início">
    <span style="color:rgba(255,255,255,0.3);font-size:11px;align-self:center;">~</span>
    <input type="date" class="ronda-date-picker" id="appReportEndDate" value="${today}" max="${today}" title="Fim">
    <button class="ronda-btn" id="appReportLoadBtn"><i class="fa-solid fa-search"></i> Gerar</button>
  </div>
  <div id="appReportBody" style="padding:8px 10px;"></div>`;

  document.getElementById("appReportLoadBtn")?.addEventListener("click", () => _appReportFetch());
}

async function _appReportFetch() {
  const plantId = document.getElementById("appReportPlantSelect")?.value;
  const startDate = document.getElementById("appReportStartDate")?.value;
  const endDate = document.getElementById("appReportEndDate")?.value;
  const bodyEl = document.getElementById("appReportBody");
  if (!plantId || !bodyEl) return;

  if (startDate && endDate) {
    const diff = (new Date(endDate) - new Date(startDate)) / 86400000;
    if (diff > 30) { bodyEl.innerHTML = '<div class="ronda-error">Período máximo: 30 dias</div>'; return; }
    if (diff < 0) { bodyEl.innerHTML = '<div class="ronda-error">Data fim deve ser >= início</div>'; return; }
  }

  _APP_REPORT_LOADING = true;
  bodyEl.innerHTML = '<div class="ronda-loading"><i class="fa-solid fa-spinner fa-spin"></i><br>Gerando relatório...</div>';
  try {
    let url = `${API_BASE}/plants/${plantId}/realtime?view=report`;
    if (startDate) url += `&start=${startDate}`;
    if (endDate) url += `&end=${endDate}`;
    const user = JSON.parse(localStorage.getItem("user") || "{}");
    const headers = {};
    if (user.customer_id) headers["X-Customer-Id"] = user.customer_id;
    if (user.is_superuser === true) headers["X-Is-Superuser"] = "true";
    if (user.username) headers["X-Username"] = user.username;
    // ver comentario no export do datastudio: fetch cru precisa do token
    if (user.token) headers["Authorization"] = `Bearer ${user.token}`;
    const res = await fetch(url, { headers, cache: "no-store" });
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    let data = await res.json();
    if (data && data.body) data = typeof data.body === "string" ? JSON.parse(data.body) : data.body;
    _APP_REPORT_DATA = data;
    _appReportRenderMini(data, bodyEl);
  } catch (e) {
    console.error("[REPORT-APP]", e);
    bodyEl.innerHTML = `<div class="ronda-error"><i class="fa-solid fa-triangle-exclamation"></i> Erro: ${e.message}</div>`;
  } finally {
    _APP_REPORT_LOADING = false;
  }
}

function _rpFmt(v, dec) { return v != null ? Number(v).toLocaleString("pt-BR", { minimumFractionDigits: dec, maximumFractionDigits: dec }) : "—"; }

function _rpSparklineSVG(values, color, w, h) {
  if (!values || !values.length) return "";
  const max = Math.max(...values, 1);
  const min = Math.min(...values, 0);
  const range = max - min || 1;
  const pts = values.map((v, i) => {
    const x = (i / Math.max(values.length - 1, 1)) * w;
    const y = h - ((v - min) / range) * (h - 4) - 2;
    return `${x.toFixed(1)},${y.toFixed(1)}`;
  }).join(" ");
  const areaPath = `M0,${h} L${pts.split(" ").map((p, i) => { const [x, y] = p.split(","); return `${x},${y}`; }).join(" L")} L${w},${h} Z`;
  const len = values.length * w / (values.length - 1 || 1);
  return `<svg viewBox="0 0 ${w} ${h}" width="${w}" height="${h}" style="display:block;">
    <defs><linearGradient id="rsg${color.replace('#','')}" x1="0" y1="0" x2="0" y2="1"><stop offset="0%" stop-color="${color}" stop-opacity="0.18"/><stop offset="100%" stop-color="${color}" stop-opacity="0"/></linearGradient></defs>
    <path d="${areaPath}" fill="url(#rsg${color.replace('#','')})" />
    <polyline points="${pts}" fill="none" stroke="${color}" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" style="--line-length:${len};stroke-dasharray:${len};stroke-dashoffset:${len};animation:reportLineDraw 1.2s ease-in-out forwards;"/>
  </svg>`;
}

function _appReportRenderMini(data, el) {
  const p = data.period || {};
  const s = data.summary || {};
  const invs = data.inverters || [];
  const alarms = data.alarms_summary || [];
  const trend = data.daily_trend || [];
  const diag = data.diagnostic_text || [];

  const genValues = trend.map(d => d.generation_kwh || 0);

  let html = "";

  html += `<div class="ronda-section">
    <div class="ronda-section-title"><i class="fa-solid fa-solar-panel"></i> Resumo do Período — ${p.power_plant_name || ""}</div>
    <div class="ronda-section-title" style="font-size:9px;margin-top:-4px;margin-bottom:6px;">${p.start || ""} ~ ${p.end || ""} (${p.days || 0} dias)</div>
    <div class="ronda-kpi-grid" style="grid-template-columns:1fr 1fr 1fr;">
      <div class="ronda-kpi"><span class="ronda-kpi-label">Geração Total</span><span class="ronda-kpi-value" style="font-family:'Space Mono',monospace;">${_rpFmt(s.total_generation_kwh, 1)} <small style="font-size:10px;color:rgba(255,255,255,0.4);">kWh</small></span></div>
      <div class="ronda-kpi"><span class="ronda-kpi-label">PR Médio</span><span class="ronda-kpi-value ${(s.avg_pr_pct||0) >= 75 ? 'val-good' : (s.avg_pr_pct||0) >= 60 ? 'val-warn' : 'val-bad'}">${_rpFmt(s.avg_pr_pct, 1)}%</span></div>
      <div class="ronda-kpi"><span class="ronda-kpi-label">FC Médio</span><span class="ronda-kpi-value">${_rpFmt(s.avg_capacity_factor_pct, 1)}%</span></div>
    </div>
  </div>`;

  if (genValues.length > 1) {
    html += `<div class="ronda-section">
      <div class="ronda-section-title"><i class="fa-solid fa-chart-line"></i> Tendência</div>
      ${_rpSparklineSVG(genValues, "#39e58c", 280, 50)}
    </div>`;
  }

  if (invs.length) {
    const sorted = [...invs].sort((a, b) => (a.avg_pr_pct || 0) - (b.avg_pr_pct || 0));
    const worst = sorted[0];
    const best = sorted[sorted.length - 1];
    html += `<div class="ronda-section">
      <div class="ronda-section-title"><i class="fa-solid fa-bolt"></i> Inversores — Destaque</div>`;
    if (worst && worst.vs_fleet === "abaixo") {
      html += `<div style="display:flex;align-items:center;gap:6px;font-size:11px;padding:2px 0;"><span style="color:#ef4444;font-weight:700;">▼</span> <span style="color:rgba(255,255,255,0.8);">${worst.inverter_name}</span> <span style="font-family:'Space Mono',monospace;color:#ef4444;">PR ${_rpFmt(worst.avg_pr_pct,1)}%</span> <span class="ronda-perf-badge ronda-perf-abaixo">abaixo</span></div>`;
    }
    if (best && best.vs_fleet === "acima") {
      html += `<div style="display:flex;align-items:center;gap:6px;font-size:11px;padding:2px 0;"><span style="color:#39e58c;font-weight:700;">▲</span> <span style="color:rgba(255,255,255,0.8);">${best.inverter_name}</span> <span style="font-family:'Space Mono',monospace;color:#39e58c;">PR ${_rpFmt(best.avg_pr_pct,1)}%</span> <span class="ronda-perf-badge ronda-perf-acima">acima</span></div>`;
    }
    html += `</div>`;
  }

  const _mTrks = data.trackers || [];
  if (_mTrks.length) {
    html += `<div class="ronda-section"><div class="ronda-section-title"><i class="fa-solid fa-arrows-rotate"></i> Trackers (${_mTrks.length})</div>`;
    _mTrks.forEach(t => {
      const dev = t.avg_deviation != null ? t.avg_deviation.toFixed(1) + "°" : "—";
      const devColor = (t.avg_deviation || 0) > 5 ? "#ef4444" : (t.avg_deviation || 0) > 2 ? "#eab308" : "#39e58c";
      html += `<div style="display:flex;align-items:center;gap:8px;padding:2px 0;font-size:11px;"><span style="min-width:70px;font-weight:600;color:rgba(255,255,255,0.8);">${t.tracker_name || "Tracker"}</span><span style="font-weight:700;color:${devColor};">Desvio: ${dev}</span><span style="color:rgba(255,255,255,0.4);">${t.deviation_pct != null ? t.deviation_pct + "% >5°" : ""}</span></div>`;
    });
    html += `</div>`;
  }

  html += `<div class="ronda-toolbar">
    <button class="ronda-btn" id="appReportExpandBtn"><i class="fa-solid fa-expand"></i> Expandir</button>
    <button class="ronda-btn report-btn-pdf" id="appReportPdfBtn"><i class="fa-solid fa-file-pdf"></i> PDF</button>
  </div>`;

  el.innerHTML = html;
  document.getElementById("appReportExpandBtn")?.addEventListener("click", () => _appReportOpenFullPanel(data));
  document.getElementById("appReportPdfBtn")?.addEventListener("click", () => _appReportDownloadPdf(data));
}

function _rpCompBarHTML(label, curVal, prevVal, curLabel, prevLabel, unit, delta) {
  const maxVal = Math.max(curVal || 0, prevVal || 0, 1);
  const curPct = ((curVal || 0) / maxVal * 100).toFixed(1);
  const prevPct = ((prevVal || 0) / maxVal * 100).toFixed(1);
  const dColor = delta != null ? (delta >= 0 ? "#39e58c" : "#ef4444") : "rgba(255,255,255,0.4)";
  const dSign = delta != null ? (delta >= 0 ? "+" : "") : "";
  return `<div style="margin-bottom:10px;">
    <div style="font-size:10px;color:rgba(255,255,255,0.5);margin-bottom:3px;">${label}</div>
    <div style="display:flex;align-items:center;gap:8px;margin-bottom:2px;">
      <span style="width:60px;font-size:10px;color:rgba(255,255,255,0.6);">${curLabel}</span>
      <div style="flex:1;height:10px;background:rgba(255,255,255,0.06);border-radius:5px;overflow:hidden;">
        <div style="height:100%;width:${curPct}%;background:linear-gradient(90deg,#39e58c,#7FD055);border-radius:5px;animation:reportBarGrow 0.6s ease-out both;"></div>
      </div>
      <span style="min-width:70px;text-align:right;font-size:11px;font-family:'Space Mono',monospace;color:rgba(255,255,255,0.85);">${_rpFmt(curVal, 1)} ${unit}</span>
    </div>
    <div style="display:flex;align-items:center;gap:8px;">
      <span style="width:60px;font-size:10px;color:rgba(255,255,255,0.4);">${prevLabel}</span>
      <div style="flex:1;height:10px;background:rgba(255,255,255,0.06);border-radius:5px;overflow:hidden;">
        <div style="height:100%;width:${prevPct}%;background:rgba(255,255,255,0.12);border-radius:5px;animation:reportBarGrow 0.6s ease-out 0.1s both;"></div>
      </div>
      <span style="min-width:70px;text-align:right;font-size:11px;font-family:'Space Mono',monospace;color:rgba(255,255,255,0.5);">${_rpFmt(prevVal, 1)} ${unit}</span>
    </div>
    ${delta != null ? `<div style="text-align:right;font-size:10px;font-weight:700;color:${dColor};margin-top:2px;">${dSign}${delta.toFixed(1)}%</div>` : ""}
  </div>`;
}

function _rpTrendSVG(data) {
  const trend = data.daily_trend || [];
  if (trend.length < 2) return "";
  const W = 800, H = 250, PAD = 50, PADR = 50;
  const genVals = trend.map(d => d.generation_kwh || 0);
  const prVals = trend.map(d => d.pr_pct);
  const maxGen = Math.max(...genVals, 1);
  const maxPr = 100;

  function toXY(vals, maxV, i) {
    const x = PAD + (i / (trend.length - 1)) * (W - PAD - PADR);
    const y = H - PAD - ((vals[i] || 0) / maxV) * (H - 2 * PAD);
    return [x.toFixed(1), y.toFixed(1)];
  }

  let gridLines = "";
  for (let i = 0; i <= 4; i++) {
    const y = PAD + (i / 4) * (H - 2 * PAD);
    const genLabel = Math.round(maxGen * (1 - i / 4));
    const prLabel = Math.round(maxPr * (1 - i / 4));
    gridLines += `<line x1="${PAD}" y1="${y}" x2="${W - PADR}" y2="${y}" stroke="rgba(255,255,255,0.05)" stroke-dasharray="4 4"/>`;
    gridLines += `<text x="${PAD - 6}" y="${y + 3}" text-anchor="end" font-size="9" fill="rgba(255,255,255,0.35)">${genLabel}</text>`;
    gridLines += `<text x="${W - PADR + 6}" y="${y + 3}" text-anchor="start" font-size="9" fill="rgba(255,255,255,0.35)">${prLabel}%</text>`;
  }

  let dateLabels = "";
  trend.forEach((d, i) => {
    const x = PAD + (i / (trend.length - 1)) * (W - PAD - PADR);
    const label = d.date ? d.date.slice(5).replace("-", "/") : "";
    if (trend.length <= 10 || i % Math.ceil(trend.length / 10) === 0) {
      dateLabels += `<text x="${x}" y="${H - 10}" text-anchor="middle" font-size="9" fill="rgba(255,255,255,0.35)">${label}</text>`;
    }
  });

  const genPts = genVals.map((_, i) => toXY(genVals, maxGen, i).join(",")).join(" ");
  const genArea = `M${PAD},${H - PAD} ` + genVals.map((_, i) => `L${toXY(genVals, maxGen, i).join(",")}`).join(" ") + ` L${W - PADR},${H - PAD} Z`;
  const prPts = prVals.map((v, i) => v != null ? toXY(prVals, maxPr, i).join(",") : null).filter(Boolean).join(" ");

  return `<svg viewBox="0 0 ${W} ${H}" style="width:100%;height:auto;display:block;">
    <defs>
      <linearGradient id="rpGenGrad" x1="0" y1="0" x2="0" y2="1">
        <stop offset="0%" stop-color="#39e58c" stop-opacity="0.15"/>
        <stop offset="100%" stop-color="#39e58c" stop-opacity="0"/>
      </linearGradient>
    </defs>
    ${gridLines}
    ${dateLabels}
    <text x="${PAD - 6}" y="${PAD - 10}" text-anchor="end" font-size="10" fill="rgba(255,255,255,0.4)">kWh</text>
    <text x="${W - PADR + 6}" y="${PAD - 10}" text-anchor="start" font-size="10" fill="rgba(255,255,255,0.4)">PR%</text>
    <path d="${genArea}" fill="url(#rpGenGrad)"/>
    <polyline points="${genPts}" fill="none" stroke="#39e58c" stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round" filter="url(#rpGlow)"/>
    ${prPts ? `<polyline points="${prPts}" fill="none" stroke="#60a5fa" stroke-width="2" stroke-dasharray="6 3" stroke-linecap="round"/>` : ""}
    <defs><filter id="rpGlow"><feDropShadow dx="0" dy="0" stdDeviation="3" flood-color="#39e58c" flood-opacity="0.3"/></filter></defs>
    ${trend.map((d, i) => {
      const [gx, gy] = toXY(genVals, maxGen, i);
      const pr = prVals[i] != null ? prVals[i].toFixed(1) + "%" : "—";
      const dt = d.date ? d.date.slice(5).replace("-", "/") : "";
      return `<g class="rp-dot" style="cursor:pointer;" onmouseover="this.querySelector('.rp-tip').style.display='block';this.querySelector('circle').setAttribute('r','6')" onmouseout="this.querySelector('.rp-tip').style.display='none';this.querySelector('circle').setAttribute('r','4')">
        <circle cx="${gx}" cy="${gy}" r="4" fill="#39e58c" stroke="#1a1d23" stroke-width="2" opacity="0.7"/>
        <g class="rp-tip" style="display:none;">
          <rect x="${Number(gx) - 60}" y="${Number(gy) - 52}" width="120" height="42" rx="6" fill="rgba(10,18,12,0.95)" stroke="rgba(57,229,140,0.3)" stroke-width="1"/>
          <text x="${gx}" y="${Number(gy) - 36}" text-anchor="middle" font-size="9" font-weight="700" fill="#39e58c">${dt}</text>
          <text x="${gx}" y="${Number(gy) - 24}" text-anchor="middle" font-size="9" fill="rgba(255,255,255,0.85)">Ger: ${(genVals[i] || 0).toFixed(1)} kWh</text>
          <text x="${gx}" y="${Number(gy) - 14}" text-anchor="middle" font-size="9" fill="#60a5fa">PR: ${pr}</text>
        </g>
      </g>`;
    }).join("")}
  </svg>`;
}

function _rpMiniSparkline(values, color) {
  if (!values || values.length < 2) return "";
  const w = 40, h = 16;
  const max = Math.max(...values, 1);
  const min = Math.min(...values, 0);
  const range = max - min || 1;
  const pts = values.map((v, i) => {
    const x = (i / (values.length - 1)) * w;
    const y = h - ((v - min) / range) * (h - 2) - 1;
    return `${x.toFixed(1)},${y.toFixed(1)}`;
  }).join(" ");
  return `<svg viewBox="0 0 ${w} ${h}" width="${w}" height="${h}"><polyline points="${pts}" fill="none" stroke="${color}" stroke-width="1.5" stroke-linecap="round"/></svg>`;
}

function _rpHeatmapHTML(sbData, dates) {
  if (!sbData || !sbData.length) return "";
  let html = "";
  sbData.forEach(inv => {
    const invName = inv.inverter_name || ("Inv" + inv.device_id);
    const avgI = inv.avg_inverter_current;
    html += `<div style="margin-bottom:14px;">
      <div style="font-size:11.5px;font-weight:700;color:rgba(255,255,255,0.8);margin-bottom:4px;">${invName} <span style="font-weight:400;font-size:10px;color:rgba(255,255,255,0.4);">méd: ${avgI != null ? avgI.toFixed(1) + "A" : "—"}</span></div>`;
    html += `<div style="overflow-x:auto;"><table style="border-collapse:collapse;font-size:10px;">`;
    html += `<thead><tr><th style="padding:2px 6px;color:rgba(255,255,255,0.35);text-align:left;">String</th>`;
    const allDates = new Set();
    (inv.strings || []).forEach(s => (s.daily || []).forEach(d => allDates.add(d.date)));
    const sortedDates = [...allDates].sort();
    sortedDates.forEach((d, di) => {
      html += `<th style="padding:2px 4px;color:rgba(255,255,255,0.3);font-weight:400;animation:reportHeatIn 0.3s ease ${di * 50}ms both;">${d.slice(5).replace("-","/")}</th>`;
    });
    html += `</tr></thead><tbody>`;

    (inv.strings || []).forEach(s => {
      html += `<tr><td style="padding:2px 6px;color:rgba(255,255,255,0.55);">S${s.string_index}</td>`;
      const byDate = {};
      (s.daily || []).forEach(d => { byDate[d.date] = d; });
      sortedDates.forEach((d, di) => {
        const cell = byDate[d];
        let bg = "rgba(255,255,255,0.06)";
        let title = "sem dados";
        if (cell) {
          if (cell.status === "normal") { bg = "#39e58c"; title = `${cell.avg_current}A (${cell.variation_pct != null ? (cell.variation_pct >= 0 ? "+" : "") + cell.variation_pct + "%" : ""})`; }
          else if (cell.status === "warning") { bg = "#eab308"; title = `${cell.avg_current}A (${cell.variation_pct}%)`; }
          else if (cell.status === "critical") { bg = "#ef4444"; title = `${cell.avg_current}A (${cell.variation_pct}%)`; }
          else { bg = "rgba(255,255,255,0.06)"; title = `${cell.avg_current}A (zerada)`; }
        }
        html += `<td style="padding:2px 4px;animation:reportHeatIn 0.3s ease ${di * 50}ms both;" title="${title}"><div style="width:14px;height:14px;border-radius:3px;background:${bg};opacity:0.85;"></div></td>`;
      });
      html += `</tr>`;
    });
    html += `</tbody></table></div></div>`;
  });
  return html;
}

function _appReportOpenFullPanel(data) {
  const panel = document.getElementById("reportFullPanel");
  if (!panel || !data) return;
  panel.classList.remove("hidden");
  document.body.style.overflow = "hidden";

  const p = data.period || {};
  const s = data.summary || {};
  const mc = data.monthly_comparison || {};
  const trend = data.daily_trend || [];
  const invs = data.inverters || [];
  const sb = data.string_box_heatmap || [];
  const w = data.weather || {};
  const alarms = data.alarms_summary || [];
  const diag = data.diagnostic_text || [];

  const nameEl = document.getElementById("reportFullPlantName");
  if (nameEl) nameEl.textContent = p.power_plant_name ? `— ${p.power_plant_name} (${p.start} ~ ${p.end})` : "";

  const closeBtn = document.getElementById("reportFullClose");
  if (closeBtn) closeBtn.onclick = () => { panel.classList.add("hidden"); document.body.style.overflow = ""; };

  document.getElementById("reportFullPdf")?.addEventListener("click", () => _appReportDownloadPdf(data), { once: true });

  const svgSolar = '<svg viewBox="0 0 24 24" fill="none" stroke="#facc15" stroke-width="2"><circle cx="12" cy="12" r="4"/><path d="M12 2v2M12 20v2M4.93 4.93l1.41 1.41M17.66 17.66l1.41 1.41M2 12h2M20 12h2M4.93 19.07l1.41-1.41M17.66 6.34l1.41-1.41"/></svg>';
  const svgBars = '<svg viewBox="0 0 24 24" fill="none" stroke="#60a5fa" stroke-width="2"><rect x="3" y="12" width="4" height="9" rx="1"/><rect x="10" y="7" width="4" height="14" rx="1"/><rect x="17" y="3" width="4" height="18" rx="1"/></svg>';
  const svgTrend = '<svg viewBox="0 0 24 24" fill="none" stroke="#39e58c" stroke-width="2"><polyline points="22 12 18 12 15 21 9 3 6 12 2 12"/></svg>';
  const svgBolt = '<svg viewBox="0 0 24 24" fill="none" stroke="#39e58c" stroke-width="2"><path d="M13 2L3 14h9l-1 8 10-12h-9l1-8z"/></svg>';
  const svgString = '<svg viewBox="0 0 24 24" fill="none" stroke="#a855f7" stroke-width="2"><rect x="2" y="7" width="20" height="10" rx="2"/><path d="M6 7V5a2 2 0 0 1 2-2h8a2 2 0 0 1 2 2v2M6 17v2a2 2 0 0 0 2 2h8a2 2 0 0 0 2-2v-2"/></svg>';
  const svgWeather = '<svg viewBox="0 0 24 24" fill="none" stroke="#60a5fa" stroke-width="2"><path d="M3 15a4 4 0 0 0 4 4h9a5 5 0 0 0 .5-9.97A7 7 0 0 0 3 11.5"/></svg>';
  const svgAlarm = '<svg viewBox="0 0 24 24" fill="none" stroke="#ef4444" stroke-width="2"><path d="M18 8A6 6 0 0 0 6 8c0 7-3 9-3 9h18s-3-2-3-9"/><path d="M13.73 21a2 2 0 0 1-3.46 0"/></svg>';
  const svgDiag = '<svg viewBox="0 0 24 24" fill="none" stroke="#39e58c" stroke-width="2"><path d="M16 4h2a2 2 0 0 1 2 2v14a2 2 0 0 1-2 2H6a2 2 0 0 1-2-2V6a2 2 0 0 1 2-2h2"/><rect x="8" y="2" width="8" height="4" rx="1" ry="1"/><path d="M9 14l2 2 4-4"/></svg>';

  let ci = 0;
  function cardDelay() { return `animation:reportCardIn 0.35s ease-out ${(ci++) * 0.05}s both;`; }

  let body = '<div class="ronda-full-grid" style="max-width:1400px;">';

  // Card: Resumo
  const infoGen = _rondaInfoBtn(`${_rondaLabel('Geração Total')} — Soma da energia diária (kWh) de todos os dias do período selecionado.<br>${_rondaFormula('Geração Total = Σ generation_daily_kwh')}<br>${_rondaNote('Fonte: fct_power_plant_metrics_daily. Prioridade: daily_active_energy > daily_energy > delta acumulador > integração trapezoidal.')}`);
  const infoPR = _rondaInfoBtn(`${_rondaLabel('PR Médio')} — Performance Ratio médio do período. Mede eficiência real vs teórica.<br>${_rondaFormula('PR = Média(Geração_dia / (Potência_Nominal × Irradiação_dia)) × 100')}<br>🟢 ≥ 75% — Bom&emsp;🟡 60–75% — Atenção&emsp;🔴 < 60% — Crítico<br>${_rondaNote('Média aritmética dos PRs diários do período. Dias sem irradiação são excluídos.')}`);
  const infoFC = _rondaInfoBtn(`${_rondaLabel('Fator de Capacidade Médio')} — Relação entre energia gerada e máximo teórico em 24h.<br>${_rondaFormula('FC = Média(Geração_dia / (Pot_Nominal × 24h)) × 100')}<br>${_rondaNote('Indica o aproveitamento da capacidade instalada. Valores típicos para solar: 15-25%.')}`);
  body += `<div class="ronda-card" style="${cardDelay()}">
    <div class="ronda-card-header"><div class="ronda-card-icon icon-solar">${svgSolar}</div><div><div class="ronda-card-title">Resumo do Período</div><div class="ronda-card-subtitle">${p.start || ""} ~ ${p.end || ""} (${p.days || 0} dias)</div></div></div>
    <div class="ronda-card-body">
      <div class="ronda-full-kpi-row">
        <div class="ronda-full-kpi"><div class="ronda-full-kpi-label">Geração Total ${infoGen}</div><div class="ronda-full-kpi-value" style="animation:reportKpiGlow 3s ease-in-out infinite;">${_rpFmt(s.total_generation_kwh, 1)}<span class="ronda-full-kpi-unit">kWh</span></div></div>
        <div class="ronda-full-kpi"><div class="ronda-full-kpi-label">PR Médio ${infoPR}</div><div class="ronda-full-kpi-value ${(s.avg_pr_pct||0)>=75?'val-good':(s.avg_pr_pct||0)>=60?'val-warn':'val-bad'}">${_rpFmt(s.avg_pr_pct,1)}<span class="ronda-full-kpi-unit">%</span></div></div>
        <div class="ronda-full-kpi"><div class="ronda-full-kpi-label">FC Médio ${infoFC}</div><div class="ronda-full-kpi-value">${_rpFmt(s.avg_capacity_factor_pct,1)}<span class="ronda-full-kpi-unit">%</span></div></div>
      </div>
      <div class="ronda-full-kpi-row" style="margin-top:8px;">
        <div class="ronda-full-kpi"><div class="ronda-full-kpi-label">Irrad. Total</div><div class="ronda-full-kpi-value">${_rpFmt(s.total_irradiation_kwh_m2,2)}<span class="ronda-full-kpi-unit">kWh/m²</span></div></div>
        <div class="ronda-full-kpi"><div class="ronda-full-kpi-label">Irrad. Média</div><div class="ronda-full-kpi-value">${_rpFmt(s.avg_irradiance_wm2,0)}<span class="ronda-full-kpi-unit">W/m²</span></div></div>
        <div class="ronda-full-kpi"><div class="ronda-full-kpi-label">Dias Oper.</div><div class="ronda-full-kpi-value">${s.operating_days || 0}<span class="ronda-full-kpi-unit">/ ${p.days||0}</span></div></div>
      </div>
    </div>
  </div>`;

  // Card: Comparativo Mensal
  const curLbl = mc.current_month ? mc.current_month.replace("-", "/") : "Atual";
  const prevLbl = mc.previous_month ? mc.previous_month.replace("-", "/") : "Anterior";
  body += `<div class="ronda-card" style="${cardDelay()}">
    <div class="ronda-card-header"><div class="ronda-card-icon icon-weather">${svgBars}</div><div><div class="ronda-card-title">Comparativo Mensal ${_rondaInfoBtn(`${_rondaLabel('Comparativo Mensal')} — Compara os indicadores do mês atual com o mês anterior.<br>${_rondaFormula('Delta% = ((Atual - Anterior) / |Anterior|) × 100')}<br>🟢 Positivo = melhoria&emsp;🔴 Negativo = queda<br>${_rondaNote('Geração, PR e Fator de Capacidade são calculados com as médias de cada mês completo.')}`)}</div><div class="ronda-card-subtitle">${curLbl} vs ${prevLbl}</div></div></div>
    <div class="ronda-card-body">
      ${_rpCompBarHTML("Geração", mc.current_generation_kwh, mc.previous_generation_kwh, curLbl, prevLbl, "kWh", mc.delta_generation_pct)}
      ${_rpCompBarHTML("PR", mc.current_pr_pct, mc.previous_pr_pct, curLbl, prevLbl, "%", mc.delta_pr_pct)}
      ${_rpCompBarHTML("Fator Capac.", mc.current_fc_pct, mc.previous_fc_pct, curLbl, prevLbl, "%", mc.delta_fc_pct)}
    </div>
  </div>`;

  // Card: Tendência (full width)
  if (trend.length > 1) {
    body += `<div class="ronda-card span-full" style="${cardDelay()}">
      <div class="ronda-card-header"><div class="ronda-card-icon icon-bolt">${svgTrend}</div><div><div class="ronda-card-title">Tendência Diária ${_rondaInfoBtn(`${_rondaLabel('Gráfico de Tendência')} — Evolução diária da geração e PR no período selecionado.<br>🟢 ${_rondaLabel('Linha verde')}: Geração (kWh) — eixo esquerdo<br>🔵 ${_rondaLabel('Linha azul tracejada')}: PR (%) — eixo direito<br>${_rondaNote('Passe o mouse sobre os pontos para ver valores exatos de cada dia. A área verde sombreada indica volume de geração.')}`)}</div><div class="ronda-card-subtitle">Geração (kWh) e PR (%)</div></div></div>
      <div class="ronda-card-body" style="padding:10px 12px;">${_rpTrendSVG(data)}</div>
    </div>`;
  }

  // Card: Inversores (full width)
  if (invs.length) {
    body += `<div class="ronda-card span-full" style="${cardDelay()}">
      <div class="ronda-card-header"><div class="ronda-card-icon icon-bolt">${svgBolt}</div><div><div class="ronda-card-title">Performance por Inversor ${_rondaInfoBtn(`${_rondaLabel('Performance por Inversor')} — Médias de cada inversor no período.<br>${_rondaFormula('PR Inv = Energia_Total / (Cap_por_Inv × Irrad_Média × Dias) × 100')}<br>${_rondaLabel('vs Média')}: compara o PR do inversor com a média da frota (±10%).<br>🟢 Acima (+10%)&emsp;🔵 Normal (±10%)&emsp;🔴 Abaixo (-10%)<br>${_rondaLabel('Tend.')}: sparkline de energia diária — mostra se o inversor está estável, subindo ou caindo.<br>${_rondaNote('Disponibilidade = % de amostras com estado "rodando". Inversores inativos são excluídos.')}`)}</div><div class="ronda-card-subtitle">${invs.length} unidades — médias do período</div></div></div>
      <div class="ronda-card-body" style="padding:0;"><div style="overflow-x:auto;">
        <table class="ronda-full-inv-table">
          <thead><tr><th>Inversor</th><th>Pot. Média</th><th>Energia</th><th>PR Méd</th><th>vs Média</th><th>Disponib.</th><th>Tend.</th></tr></thead>
          <tbody>`;
    invs.forEach(inv => {
      const vsCls = inv.vs_fleet && inv.vs_fleet !== "sem_dados" ? `ronda-full-perf-${inv.vs_fleet}` : "";
      const arrow = inv.vs_fleet === "acima" ? "▲" : inv.vs_fleet === "abaixo" ? "▼" : "";
      const sparkColor = inv.vs_fleet === "abaixo" ? "#ef4444" : inv.vs_fleet === "acima" ? "#39e58c" : "#60a5fa";
      body += `<tr>
        <td style="font-weight:600;">${inv.inverter_name || "Inv" + inv.device_id}</td>
        <td>${_rpFmt(inv.avg_power_kw, 1)} kW</td>
        <td>${_rpFmt(inv.total_energy_kwh, 0)} kWh</td>
        <td style="font-weight:700;">${_rpFmt(inv.avg_pr_pct, 1)}%</td>
        <td><span class="ronda-full-perf-badge ${vsCls}">${arrow} ${inv.vs_fleet === "sem_dados" ? "—" : inv.vs_fleet}</span></td>
        <td>${_rpFmt(inv.availability_pct, 1)}%</td>
        <td>${_rpMiniSparkline(inv.daily_energy || [], sparkColor)}</td>
      </tr>`;
    });
    body += `</tbody></table></div></div></div>`;
  }

  // Card: String Box Heatmap (full width)
  if (sb && sb.length) {
    body += `<div class="ronda-card span-full" style="${cardDelay()}">
      <div class="ronda-card-header"><div class="ronda-card-icon icon-string">${svgString}</div><div><div class="ronda-card-title">String Box — Heatmap ${_rondaInfoBtn(`${_rondaLabel('Heatmap de Strings')} — Cada quadrado = corrente média de uma string em um dia (6h-18h).<br>${_rondaFormula('Variação% = ((Corrente_String - Média_Inversor) / Média_Inversor) × 100')}<br>🟢 ≥ -5% — Normal&emsp;🟡 -5% a -15% — Atenção&emsp;🔴 < -15% — Crítico&emsp;⬜ Zerada/offline<br>${_rondaNote('Passe o mouse sobre os quadrados para ver o valor exato. Strings zeradas por 2+ dias geram alerta no diagnóstico.')}`)}</div><div class="ronda-card-subtitle">Corrente vs média do inversor por dia (6h-18h)</div></div></div>
      <div class="ronda-card-body">${_rpHeatmapHTML(sb)}</div>
    </div>`;
  }

  // Card: Weather
  body += `<div class="ronda-card" style="${cardDelay()}">
    <div class="ronda-card-header"><div class="ronda-card-icon icon-weather">${svgWeather}</div><div><div class="ronda-card-title">Estação Solarimétrica</div><div class="ronda-card-subtitle">Médias do período</div></div></div>
    <div class="ronda-card-body">
      <div class="ronda-full-kpi-row">
        <div class="ronda-full-kpi"><div class="ronda-full-kpi-label">Irrad. Média</div><div class="ronda-full-kpi-value">${_rpFmt(w.avg_irradiance_wm2, 0)}<span class="ronda-full-kpi-unit">W/m²</span></div></div>
        <div class="ronda-full-kpi"><div class="ronda-full-kpi-label">Irrad. Máx</div><div class="ronda-full-kpi-value">${_rpFmt(w.max_irradiance_wm2, 0)}<span class="ronda-full-kpi-unit">W/m²</span></div></div>
      </div>
      <div class="ronda-full-kpi-row" style="margin-top:8px;">
        <div class="ronda-full-kpi"><div class="ronda-full-kpi-label">Temp. Média</div><div class="ronda-full-kpi-value">${_rpFmt(w.avg_temp_c, 1)}<span class="ronda-full-kpi-unit">°C</span></div></div>
        <div class="ronda-full-kpi"><div class="ronda-full-kpi-label">Temp. Máx</div><div class="ronda-full-kpi-value">${_rpFmt(w.max_temp_c, 1)}<span class="ronda-full-kpi-unit">°C</span></div></div>
      </div>
      <div class="ronda-full-kpi-row" style="margin-top:8px;">
        <div class="ronda-full-kpi"><div class="ronda-full-kpi-label">Vento</div><div class="ronda-full-kpi-value">${_rpFmt(w.avg_wind_speed, 1)}<span class="ronda-full-kpi-unit">m/s</span></div></div>
        <div class="ronda-full-kpi"><div class="ronda-full-kpi-label">Chuva</div><div class="ronda-full-kpi-value">${w.rain_days || 0}<span class="ronda-full-kpi-unit">/ ${w.total_days || 0} dias</span></div></div>
      </div>
      ${w.daily_irradiance && w.daily_irradiance.length > 1 ? `<div style="margin-top:10px;">${_rpSparklineSVG(w.daily_irradiance, "#facc15", 260, 40)}</div>` : ""}
    </div>
  </div>`;

  // Card: Trackers (if any)
  const _rpTrks = data.trackers || [];
  if (_rpTrks.length) {
    const svgTrk = '<svg viewBox="0 0 24 24" fill="none" stroke="#facc15" stroke-width="2"><rect x="3" y="11" width="18" height="2" rx="1"/><path d="M12 6v5M8 4l4 2 4-2"/><circle cx="12" cy="19" r="2"/></svg>';
    body += `<div class="ronda-card span-full" style="${cardDelay()}">
      <div class="ronda-card-header"><div class="ronda-card-icon icon-solar">${svgTrk}</div><div><div class="ronda-card-title">Trackers ${_rondaInfoBtn(`${_rondaLabel('Trackers')} — Posicionamento dos seguidores solares no período.<br>${_rondaFormula('Desvio = |Posição Atual - Posição Alvo|')}<br>🟢 ≤ 2° Normal&emsp;🟡 2°-5° Moderado&emsp;🔴 > 5° Alto<br>${_rondaNote('Sparkline mostra evolução do desvio médio diário. % de tempo com desvio > 5° indica frequência do problema.')}`)}</div><div class="ronda-card-subtitle">${_rpTrks.length} unidades</div></div></div>
      <div class="ronda-card-body"><div style="overflow-x:auto;">
        <table class="ronda-full-inv-table">
          <thead><tr><th>Tracker</th><th>Pos. Média</th><th>Alvo Médio</th><th>Desvio Méd</th><th>Desvio Máx</th><th>% Desvio>5°</th><th>Tend.</th></tr></thead>
          <tbody>`;
    _rpTrks.forEach(t => {
      const devColor = (t.avg_deviation||0) > 5 ? "#ef4444" : (t.avg_deviation||0) > 2 ? "#eab308" : "#39e58c";
      const sparkColor = (t.avg_deviation||0) > 5 ? "#ef4444" : "#facc15";
      body += `<tr>
        <td style="font-weight:600;">${t.tracker_name || "Tracker"}</td>
        <td>${_rpFmt(t.avg_position, 1)}°</td>
        <td>${_rpFmt(t.avg_target, 1)}°</td>
        <td style="font-weight:700;color:${devColor};">${_rpFmt(t.avg_deviation, 1)}°</td>
        <td>${_rpFmt(t.max_deviation, 1)}°</td>
        <td>${_rpFmt(t.deviation_pct, 1)}%</td>
        <td>${_rpMiniSparkline(t.daily_deviation || [], sparkColor)}</td>
      </tr>`;
    });
    body += `</tbody></table></div></div></div>`;
  }

  // Card: Diagnóstico (full width)
  if (diag.length) {
    body += `<div class="ronda-card span-full" style="${cardDelay()}">
      <div class="ronda-card-header"><div class="ronda-card-icon icon-bolt">${svgDiag}</div><div><div class="ronda-card-title">Diagnóstico do Período</div><div class="ronda-card-subtitle">Gerado automaticamente</div></div></div>
      <div class="ronda-card-body" style="background:rgba(57,229,140,0.03);border:1px solid rgba(57,229,140,0.1);border-radius:8px;margin:8px;padding:14px 16px;">`;
    diag.forEach(d => {
      const icon = d.type === "warning" ? '<span style="color:#eab308;margin-right:4px;">&#9888;</span>' : d.type === "ok" ? '<span style="color:#39e58c;margin-right:4px;">&#10003;</span>' : '<span style="color:#60a5fa;margin-right:4px;">&#9432;</span>';
      body += `<p style="margin:0 0 8px 0;font-size:12.5px;line-height:1.7;color:rgba(255,255,255,0.75);font-family:'Inter',sans-serif;">${icon}${d.text}</p>`;
    });
    body += `</div></div>`;
  }

  body += "</div>";

  const bodyEl = document.getElementById("reportFullBody");
  if (bodyEl) bodyEl.innerHTML = body;

  document.addEventListener("keydown", function _rpEsc(e) {
    if (e.key === "Escape") { panel.classList.add("hidden"); document.body.style.overflow = ""; document.removeEventListener("keydown", _rpEsc); }
  });
}

function _appReportDownloadCsv(data) {
  if (!data) return;
  const p = data.period || {};
  const trend = data.daily_trend || [];
  const invs = data.inverters || [];
  let csv = "Relatório de Performance\n";
  csv += `Usina,${p.power_plant_name || ""}\n`;
  csv += `Período,${p.start || ""} ~ ${p.end || ""}\n\n`;
  csv += "Data,Geração (kWh),PR (%),FC (%),Irradiação (kWh/m²)\n";
  trend.forEach(d => { csv += `${d.date},${d.generation_kwh ?? ""},${d.pr_pct ?? ""},${d.capacity_factor_pct ?? ""},${d.irradiation_kwh_m2 ?? ""}\n`; });
  csv += "\nInversor,Pot. Média (kW),Energia (kWh),PR (%),vs Média,Disponib. (%)\n";
  invs.forEach(inv => { csv += `${inv.inverter_name},${inv.avg_power_kw ?? ""},${inv.total_energy_kwh ?? ""},${inv.avg_pr_pct ?? ""},${inv.vs_fleet ?? ""},${inv.availability_pct ?? ""}\n`; });
  const blob = new Blob(["﻿" + csv], { type: "text/csv;charset=utf-8;" });
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url;
  a.download = `Relatorio_${(p.power_plant_name||"usina").replace(/\s+/g,"_")}_${p.start}_${p.end}.csv`;
  a.click();
  URL.revokeObjectURL(url);
}

async function _appReportDownloadPdf(data) {
  if (!data) return;
  const panel = document.getElementById("reportFullPanel");
  if (!panel || panel.classList.contains("hidden")) { _appReportOpenFullPanel(data); await new Promise(r => setTimeout(r, 500)); }
  const bodyEl = document.getElementById("reportFullBody");
  if (!bodyEl) return;
  const p = data.period || {};
  await _pdfCaptureFull(bodyEl, panel, `Relatorio_${(p.power_plant_name||"usina").replace(/\s+/g,"_")}_${p.start}_${p.end}.pdf`, "landscape");
}

function _wireAppRobotResize() {
  const panel = document.getElementById("robotReport");
  const handleLeft = document.getElementById("robotReportResizeLeft");
  const handleTop = document.getElementById("robotReportResizeTop");
  if (!panel) return;
  let dragging = null, startX = 0, startY = 0, startW = 0, startH = 0;
  function onDown(axis, e) { e.preventDefault(); e.stopPropagation(); dragging = axis; startX = e.clientX; startY = e.clientY; const r = panel.getBoundingClientRect(); startW = r.width; startH = r.height; panel.classList.add("ronda-resizing"); document.addEventListener("pointermove", onMove); document.addEventListener("pointerup", onUp); }
  function onMove(e) { if (!dragging) return; if (dragging === "x") { panel.style.width = Math.max(300, Math.min(window.innerWidth - 40, startW + (startX - e.clientX))) + "px"; } else { panel.style.maxHeight = Math.max(200, Math.min(window.innerHeight - 100, startH + (startY - e.clientY))) + "px"; } }
  function onUp() { dragging = null; panel.classList.remove("ronda-resizing"); document.removeEventListener("pointermove", onMove); document.removeEventListener("pointerup", onUp); }
  if (handleLeft) handleLeft.addEventListener("pointerdown", e => onDown("x", e));
  if (handleTop) handleTop.addEventListener("pointerdown", e => onDown("y", e));
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

  document.querySelectorAll(".robot-tab").forEach(btn => {
    btn.addEventListener("click", e => { e.stopPropagation(); _appRondaSwitchTab(btn.dataset.tab); });
  });
  _wireAppRobotResize();

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

// =============================================================================
// EXPLORADOR DE DADOS (CLIENTE-FRIENDLY)
// =============================================================================
let _explorerInited = false;
let _explorerOffset = 0;

function initExplorerOnce() {
  if (_explorerInited) return;
  _explorerInited = true;

  const plantSel = document.getElementById("explorerPlantSelect");
  const dataType = document.getElementById("explorerDataType");
  const loadBtn = document.getElementById("explorerLoadBtn");
  const prevBtn = document.getElementById("explorerPrevBtn");
  const nextBtn = document.getElementById("explorerNextBtn");
  const exportBtn = document.getElementById("explorerExportCsv");

  _explorerPopulatePlants(plantSel);

  const now = new Date();
  const yesterday = new Date(now.getTime() - 24 * 60 * 60 * 1000);
  const fmt = d => d.toISOString().slice(0, 16);
  const startInput = document.getElementById("explorerStartDate");
  const endInput = document.getElementById("explorerEndDate");
  if (!startInput.value) startInput.value = fmt(yesterday);
  if (!endInput.value) endInput.value = fmt(now);

  const tableContainer = document.getElementById("explorerTableContainer");
  if (tableContainer) {
    let isDragging = false, startX = 0, scrollLeft = 0;

    tableContainer.addEventListener("mousedown", e => {
      isDragging = true;
      startX = e.pageX - tableContainer.offsetLeft;
      scrollLeft = tableContainer.scrollLeft;
      tableContainer.style.cursor = "grabbing";
    });

    tableContainer.addEventListener("mouseleave", () => {
      isDragging = false;
      tableContainer.style.cursor = "";
    });

    tableContainer.addEventListener("mouseup", () => {
      isDragging = false;
      tableContainer.style.cursor = "";
    });

    tableContainer.addEventListener("mousemove", e => {
      if (!isDragging) return;
      e.preventDefault();
      const x = e.pageX - tableContainer.offsetLeft;
      tableContainer.scrollLeft = scrollLeft - (x - startX);
    });
  }

  loadBtn.addEventListener("click", () => { _explorerOffset = 0; _explorerLoad(); });
  prevBtn.addEventListener("click", () => {
    const limit = parseInt(document.getElementById("explorerLimit").value) || 50;
    _explorerOffset = Math.max(0, _explorerOffset - limit);
    _explorerLoad();
  });
  nextBtn.addEventListener("click", () => {
    const limit = parseInt(document.getElementById("explorerLimit").value) || 50;
    _explorerOffset += limit;
    _explorerLoad();
  });
  exportBtn.addEventListener("click", _explorerExportCsv);

  const legendToggle = document.getElementById("explorerLegendToggle");
  if (legendToggle) {
    legendToggle.addEventListener("click", () => {
      const body = document.getElementById("explorerLegendBody");
      const arrow = document.querySelector(".explorer-legend-arrow");
      if (!body) return;
      const open = body.style.display !== "none";
      body.style.display = open ? "none" : "";
      if (arrow) arrow.classList.toggle("open", !open);
    });
  }
}

async function _explorerPopulatePlants(sel) {
  try {
    const res = await apiFetch("/plants");
    const data = await res.json();
    const plants = data.items || data || [];
    plants.forEach(p => {
      const opt = document.createElement("option");
      opt.value = p.power_plant_id;
      opt.textContent = p.power_plant_name || p.display_name || `Usina ${p.power_plant_id}`;
      sel.appendChild(opt);
    });
  } catch (e) {
    console.error("[Explorer] erro ao carregar usinas:", e);
  }
}

async function _explorerLoad() {
  const plantId = document.getElementById("explorerPlantSelect").value;
  const dataType = document.getElementById("explorerDataType").value;

  if (!plantId) {
    _explorerShowError("Selecione uma usina para consultar.");
    return;
  }

  _explorerShowLoading();

  try {
    const result = await _explorerLoadRaw(plantId, dataType);
    if (result) _explorerShowResults(result);

  } catch (e) {
    console.error("[Explorer] erro:", e);
    _explorerShowError("Erro ao carregar dados: " + e.message);
  }
}

async function _explorerLoadRaw(plantId, tableName) {
  const plantName = document.getElementById("explorerPlantSelect").selectedOptions[0]?.textContent || "";
  const limit = parseInt(document.getElementById("explorerLimit").value) || 50;
  const startVal = document.getElementById("explorerStartDate").value;
  const endVal = document.getElementById("explorerEndDate").value;

  const friendlyTable = {
    raw_inverter: "Inversores", raw_relay: "Reles", raw_meter: "Multimedidores",
    raw_weather_station: "Est. Meteorologica", raw_tracker: "Trackers",
    raw_transformer: "Transformadores", raw_nobreak: "Nobreaks", raw_logger: "Loggers"
  };

  let effectiveStart = startVal;
  if (!effectiveStart) {
    const yesterday = new Date(Date.now() - 24 * 60 * 60 * 1000);
    effectiveStart = yesterday.toISOString().slice(0, 16);
  }

  let url = `/raw/query?table=${encodeURIComponent(tableName)}&plant_id=${encodeURIComponent(plantId)}&limit=${limit}&offset=${_explorerOffset}`;
  url += `&start=${encodeURIComponent(effectiveStart)}`;
  if (endVal) url += `&end=${encodeURIComponent(endVal)}`;

  let res;
  try {
    res = await apiFetch(url);
  } catch (fetchErr) {
    console.error("[Explorer] fetch /raw/query falhou:", fetchErr);
    throw new Error(
      "Dados brutos indisponiveis no momento. A rota /raw/query precisa ser adicionada no API Gateway. " +
      "Use o Data Studio para consultar dados historicos processados."
    );
  }

  if (!res.ok) {
    let errMsg = `HTTP ${res.status}`;
    try {
      const errBody = await res.json();
      errMsg = errBody.error || errBody.message || errMsg;
    } catch {}
    if (res.status === 404) {
      throw new Error(
        "Dados brutos indisponiveis: a rota /raw/query nao esta configurada no API Gateway. " +
        "Entre em contato com o suporte tecnico para habilitar esta funcionalidade."
      );
    }
    throw new Error(`Erro ao consultar dados brutos: ${errMsg}`);
  }

  const data = await res.json();
  const items = data.items || [];
  const total = data.total || 0;

  const SKIP_RAW_KEYS = new Set(["mqtt_topic", "device_type", "received_at", "timestamp"]);

  const RAW_LABELS = {
    active_power: "Potencia Ativa (kW)", power_dc: "Potencia DC (kW)",
    voltage_dc: "Tensao DC (V)", frequency: "Frequencia (Hz)",
    efficiency: "Eficiencia (%)", power_factor: "Fator de Potencia",
    total_energy: "Energia Total (kWh)", apparent_power: "Pot. Aparente (kVA)",
    reactive_power: "Pot. Reativa (kvar)", isolation: "Isolamento (MOhm)",
    temperature: "Temperatura (C)", irradiance: "Irradiancia (W/m2)",
    wind_speed: "Vel. Vento (m/s)", humidity: "Umidade (%)",
    ambient_temperature: "Temp. Ambiente (C)", module_temperature: "Temp. Modulo (C)",
  };

  const allKeys = new Set();
  items.forEach(item => {
    Object.keys(item.json_data || {}).forEach(k => {
      if (!SKIP_RAW_KEYS.has(k.toLowerCase())) allKeys.add(k);
    });
  });

  const idKeys = [];
  const otherKeys = [];
  allKeys.forEach(k => {
    if (/^ID\d+$/i.test(k)) idKeys.push(k);
    else otherKeys.push(k);
  });
  idKeys.sort((a, b) => parseInt(a.replace(/\D/g, "")) - parseInt(b.replace(/\D/g, "")));
  const sortedKeys = [...otherKeys, ...idKeys];

  const rows = items.map(item => {
    const jd = item.json_data || {};
    const base = {
      "Data/Hora": _explorerFormatTs(item.timestamp),
      "Dispositivo": item.device_id,
    };
    sortedKeys.forEach(k => {
      const v = jd[k];
      const label = RAW_LABELS[k.toLowerCase()] || _explorerFriendlyKey(k);
      if (v == null) { base[label] = "—"; return; }
      base[label] = typeof v === "number" && !Number.isInteger(v) ? parseFloat(v).toFixed(2) : v;
    });
    return base;
  });

  const hasMore = data.has_more === true;

  return {
    type: "table",
    title: `Dados Brutos: ${friendlyTable[tableName] || tableName} - ${plantName}`,
    rows,
    count: items.length > 0 ? `${_explorerOffset + 1}-${_explorerOffset + items.length}` : 0,
    pagination: { total: hasMore ? _explorerOffset + limit + 1 : _explorerOffset + items.length, limit, offset: _explorerOffset, hasMore }
  };
}

function _explorerFriendlyKey(k) {
  return k.replace(/_/g, " ").replace(/\b\w/g, c => c.toUpperCase());
}

function _explorerFormatTs(ts) {
  if (!ts) return "—";
  try {
    const d = new Date(ts);
    return d.toLocaleString("pt-BR", { timeZone: "America/Fortaleza" });
  } catch { return ts; }
}

function _explorerStatusLabel(s) {
  const map = { RUNNING: "Em Operacao", OFF: "Desligado", NO_COMM: "Sem Comunicacao", UNKNOWN: "Desconhecido" };
  return map[s] || s;
}

function _explorerShowLoading() {
  document.getElementById("explorerEmpty").style.display = "none";
  document.getElementById("explorerError").style.display = "none";
  document.getElementById("explorerResults").style.display = "none";
  document.getElementById("explorerLoading").style.display = "";
}

function _explorerShowError(msg) {
  document.getElementById("explorerEmpty").style.display = "none";
  document.getElementById("explorerLoading").style.display = "none";
  document.getElementById("explorerResults").style.display = "none";
  document.getElementById("explorerError").style.display = "";
  document.getElementById("explorerErrorMsg").textContent = msg;
}

function _explorerShowResults(result) {
  document.getElementById("explorerEmpty").style.display = "none";
  document.getElementById("explorerLoading").style.display = "none";
  document.getElementById("explorerError").style.display = "none";
  document.getElementById("explorerResults").style.display = "";

  document.getElementById("explorerResultsTitle").textContent = result.title;
  document.getElementById("explorerResultsCount").textContent =
    result.count > 0 ? `${result.count} registro(s)` : "";

  const tableEl = document.getElementById("explorerTableContainer");
  const paginationEl = document.getElementById("explorerPagination");

  tableEl.style.display = "none";
  paginationEl.style.display = "none";

  if (result.type === "table" && result.rows.length > 0) {
    tableEl.style.display = "";
    const cols = Object.keys(result.rows[0]);
    document.getElementById("explorerTableHead").innerHTML =
      "<tr>" + cols.map(c => `<th>${c}</th>`).join("") + "</tr>";
    document.getElementById("explorerTableBody").innerHTML =
      result.rows.map(row => "<tr>" + cols.map(c => `<td>${row[c] ?? "—"}</td>`).join("") + "</tr>").join("");
  } else if (result.type === "table" && result.rows.length === 0) {
    tableEl.style.display = "";
    document.getElementById("explorerTableHead").innerHTML = "";
    document.getElementById("explorerTableBody").innerHTML =
      '<tr><td style="padding:20px;text-align:center;opacity:.6">Nenhum registro encontrado para o periodo selecionado.</td></tr>';
  }

  if (result.pagination) {
    const p = result.pagination;
    paginationEl.style.display = "";
    const pageNum = Math.floor(p.offset / p.limit) + 1;
    document.getElementById("explorerPageInfo").textContent = `Pagina ${pageNum}`;
    document.getElementById("explorerPrevBtn").disabled = p.offset === 0;
    document.getElementById("explorerNextBtn").disabled = !p.hasMore;
  }

  window._explorerLastResult = result;

  const dataType = document.getElementById("explorerDataType").value;
  const hasIdCols = result.rows.length > 0 && Object.keys(result.rows[0]).some(k => /^ID\d+$/i.test(k));
  if (hasIdCols) {
    _explorerLoadLegend(dataType);
  } else {
    const lgEl = document.getElementById("explorerLegend");
    if (lgEl) lgEl.style.display = "none";
  }
}

function _explorerExportCsv() {
  const result = window._explorerLastResult;
  if (!result || result.type !== "table" || !result.rows.length) return;

  const cols = Object.keys(result.rows[0]);
  const sep = ";";
  const csvLines = [cols.join(sep)];
  result.rows.forEach(row => {
    csvLines.push(cols.map(c => {
      let v = row[c];
      if (v == null) return "";
      if (typeof v === "number") return String(v).replace(".", ",");
      v = String(v);
      if (v.includes(sep) || v.includes('"') || v.includes("\n")) v = '"' + v.replace(/"/g, '""') + '"';
      return v;
    }).join(sep));
  });

  const blob = new Blob(["﻿" + csvLines.join("\r\n")], { type: "text/csv;charset=utf-8;" });
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url;
  a.download = `explorador_${new Date().toISOString().slice(0,10)}.csv`;
  a.click();
  URL.revokeObjectURL(url);
}

let _explorerLegendCache = {};

async function _explorerLoadLegend(tableName) {
  const legendEl = document.getElementById("explorerLegend");
  const gridEl = document.getElementById("explorerLegendGrid");
  const bodyEl = document.getElementById("explorerLegendBody");
  const arrowEl = document.querySelector(".explorer-legend-arrow");
  if (!legendEl || !gridEl) return;

  legendEl.style.display = "none";
  if (bodyEl) bodyEl.style.display = "none";
  if (arrowEl) arrowEl.classList.remove("open");

  if (_explorerLegendCache[tableName]) {
    _explorerRenderLegend(_explorerLegendCache[tableName]);
    return;
  }

  try {
    const res = await apiFetch(`/raw/id-legend?table=${encodeURIComponent(tableName)}`);
    if (!res.ok) return;
    const data = await res.json();
    const items = data.items || [];
    if (items.length === 0) return;
    _explorerLegendCache[tableName] = items;
    _explorerRenderLegend(items);
  } catch (e) {
    console.warn("[Explorer] legenda indisponível:", e);
  }
}

function _explorerRenderLegend(items) {
  const legendEl = document.getElementById("explorerLegend");
  const gridEl = document.getElementById("explorerLegendGrid");
  if (!legendEl || !gridEl || !items.length) return;

  legendEl.style.display = "";
  gridEl.innerHTML = items.map(it => {
    const badgeClass = (it.type || "event").toLowerCase();
    const sevClass = (it.severity || "").toLowerCase() === "high" ? " high" : "";
    return `<div class="explorer-legend-item">
      <span class="explorer-legend-code">${it.code}</span>
      <span class="explorer-legend-desc">${it.description}</span>
      <span class="explorer-legend-badge ${badgeClass}${sevClass}">${it.type || ""}</span>
    </div>`;
  }).join("");
}

// =============================================================================
// SISTEMA DE TICKETS / SUPORTE
// =============================================================================
let _ticketsInited = false;
let _tkCurrentFilter = "all";
let _tkViewingId = null;
const TK_SEEN_KEY = "tk_last_seen";
let _tkBadgeInterval = null;
let _tkPrevUnseen = 0;

function _tkGetLastSeen() {
  return localStorage.getItem(TK_SEEN_KEY) || "2000-01-01T00:00:00Z";
}
function _tkMarkSeen() {
  localStorage.setItem(TK_SEEN_KEY, new Date().toISOString());
  _tkPrevUnseen = 0;
  const b = document.getElementById("tkBadge");
  if (b) b.style.display = "none";
  const btn = document.getElementById("btnTickets");
  if (btn) btn.classList.remove("tk-btn-pulse");
  _tkUpdateBellPanel([]);
}

function _tkPlaySound() {
  // Centralizado: respeita a preferência de mudo e o desbloqueio de áudio
  if (window.NotifySound) { try { NotifySound.play("ticket"); } catch (_) {} return; }
  try {
    const ac = new (window.AudioContext || window.webkitAudioContext)();
    const notes = [880, 1108.73, 1318.51];
    notes.forEach((freq, i) => {
      const osc = ac.createOscillator();
      const gain = ac.createGain();
      osc.type = "sine";
      osc.frequency.value = freq;
      gain.gain.setValueAtTime(0.15, ac.currentTime + i * 0.12);
      gain.gain.exponentialRampToValueAtTime(0.001, ac.currentTime + i * 0.12 + 0.3);
      osc.connect(gain);
      gain.connect(ac.destination);
      osc.start(ac.currentTime + i * 0.12);
      osc.stop(ac.currentTime + i * 0.12 + 0.3);
    });
  } catch (e) { /* silent */ }
}

function _tkUpdateBellPanel(items) {
  const badge = document.getElementById("notifBellBadge");
  const list = document.getElementById("notifPanelList");
  const platformUnseen = (typeof _notifAllUpdates !== "undefined" ? _notifAllUpdates : []).filter(u => {
    const lastSeen = parseInt(localStorage.getItem("platform_last_seen_update") || "0");
    return u.id > lastSeen;
  }).length;
  const totalUnseen = items.length + platformUnseen;

  if (badge) {
    if (totalUnseen > 0) {
      badge.textContent = totalUnseen > 9 ? "9+" : totalUnseen;
      badge.style.display = "";
    } else {
      badge.style.display = "none";
    }
  }

  if (list && items.length > 0) {
    const ticketHtml = items.map(t => {
      const preview = t.last_comment ? t.last_comment.substring(0, 80) + (t.last_comment.length > 80 ? "…" : "") : "";
      const statusLabel = {open: "Aberto", in_progress: "Em Andamento", resolved: "Resolvido"}[t.status] || t.status;
      const who = t.username ? `<span class="notif-item-date">${_tkEsc(t.username)}</span>` : "";
      return `<div class="notif-item unread" style="cursor:pointer;" onclick="document.getElementById('btnTickets')?.click();">
        <div class="notif-item-title"><i class="fa-solid fa-headset" style="margin-right:6px;color:#39e58c;"></i>Ticket #${t.id} — ${_tkEsc(t.title)}</div>
        <div class="notif-item-desc">${preview ? _tkEsc(preview) : statusLabel}</div>
        ${who}
      </div>`;
    }).join("");

    const existingTickets = list.querySelectorAll('.tk-notif-section');
    existingTickets.forEach(el => el.remove());

    list.insertAdjacentHTML("afterbegin",
      `<div class="tk-notif-section">${ticketHtml}</div>`
    );
  }
}

async function _tkPollUnseen() {
  try {
    const since = _tkGetLastSeen();
    const res = await apiFetch(`/tickets/unseen?since=${encodeURIComponent(since)}`);
    if (!res.ok) return;
    const data = await res.json();
    const count = data.unseen || 0;
    const items = data.items || [];

    const b = document.getElementById("tkBadge");
    const btn = document.getElementById("btnTickets");

    if (count > 0) {
      if (b) { b.textContent = count > 9 ? "9+" : count; b.style.display = "flex"; }
      if (btn) btn.classList.add("tk-btn-pulse");
      _tkUpdateBellPanel(items);
      if (count > _tkPrevUnseen) _tkPlaySound();
    } else {
      if (b) b.style.display = "none";
      if (btn) btn.classList.remove("tk-btn-pulse");
    }
    _tkPrevUnseen = count;
  } catch (e) { /* silent */ }
}
function _tkStartPolling() {
  if (_tkBadgeInterval) return;
  _tkPollUnseen();
  _tkBadgeInterval = setInterval(_tkPollUnseen, 30000);
}

function initTicketsOnce() {
  _tkMarkSeen();
  if (_ticketsInited) { tkLoadList(); return; }
  _ticketsInited = true;

  const user = _getUser();
  const subtitle = document.getElementById("tkSubtitle");
  if (subtitle && user.is_superuser) subtitle.textContent = "Todos os chamados";

  // Novo ticket
  document.getElementById("tkBtnNew")?.addEventListener("click", tkOpenModal);
  document.getElementById("tkModalClose")?.addEventListener("click", tkCloseModal);
  document.getElementById("tkModalCancel")?.addEventListener("click", tkCloseModal);
  document.getElementById("tkModalOverlay")?.addEventListener("click", e => {
    if (e.target.id === "tkModalOverlay") tkCloseModal();
  });
  document.getElementById("tkModalSubmit")?.addEventListener("click", tkSubmitTicket);

  // File upload preview
  const fileInput = document.getElementById("tkFile");
  const fileDrop = document.getElementById("tkFileDrop");
  if (fileInput) {
    fileInput.addEventListener("change", () => tkPreviewFile(fileInput));
  }
  if (fileDrop) {
    fileDrop.addEventListener("dragover", e => { e.preventDefault(); fileDrop.style.borderColor = "rgba(57,229,140,0.5)"; });
    fileDrop.addEventListener("dragleave", () => { fileDrop.style.borderColor = ""; });
    fileDrop.addEventListener("drop", e => {
      e.preventDefault();
      fileDrop.style.borderColor = "";
      if (e.dataTransfer.files.length) {
        fileInput.files = e.dataTransfer.files;
        tkPreviewFile(fileInput);
      }
    });
  }

  // Reply file
  document.getElementById("tkReplyFile")?.addEventListener("change", function() {
    const nameEl = document.getElementById("tkReplyFileName");
    if (nameEl) nameEl.textContent = this.files[0]?.name || "";
  });

  // Filtros
  document.querySelectorAll(".tk-filter-btn").forEach(btn => {
    btn.addEventListener("click", () => {
      document.querySelectorAll(".tk-filter-btn").forEach(b => b.classList.remove("active"));
      btn.classList.add("active");
      _tkCurrentFilter = btn.dataset.status;
      tkLoadList();
    });
  });

  // Back
  document.getElementById("tkBackBtn")?.addEventListener("click", tkBackToList);

  // Send reply
  document.getElementById("tkSendReply")?.addEventListener("click", tkSendComment);

  // Populate plant select
  tkPopulatePlants();

  tkLoadList();
}

async function tkPopulatePlants() {
  const sel = document.getElementById("tkPlant");
  if (!sel) return;
  let plants = lastValidPlants || [];
  if (!plants.length) {
    try {
      const res = await apiFetch("/plants");
      if (res.ok) {
        const data = await res.json();
        plants = Array.isArray(data) ? data : (data.plants || data.items || []);
      }
    } catch (e) { console.warn("[tickets] plantas:", e); }
  }
  if (sel.options.length > 1) return;
  plants.forEach(p => {
    const o = document.createElement("option");
    o.value = p.power_plant_id || p.id;
    o.textContent = p.power_plant_name || p.name;
    sel.appendChild(o);
  });
}

async function tkLoadList() {
  const list = document.getElementById("tkList");
  if (!list) return;
  list.innerHTML = '<div class="tk-empty">Carregando...</div>';
  try {
    const qs = _tkCurrentFilter && _tkCurrentFilter !== "all" ? `?status=${_tkCurrentFilter}` : "";
    const res = await apiFetch(`/tickets${qs}`);
    if (!res.ok) throw new Error("Erro ao carregar tickets");
    const data = await res.json();
    const items = data.items || [];
    if (!items.length) {
      list.innerHTML = '<div class="tk-empty">Nenhum chamado encontrado.</div>';
      return;
    }
    list.innerHTML = items.map(t => {
      const prioClass = `tk-priority-${t.priority || "medium"}`;
      const prioLabel = {low: "Baixa", medium: "Média", high: "Alta"}[t.priority] || "Média";
      const statusLabel = {open: "Aberto", in_progress: "Em Andamento", resolved: "Resolvido"}[t.status] || t.status;
      const d = t.created_at ? _tkDate(t.created_at).toLocaleDateString("pt-BR") : "";
      return `<div class="tk-card" data-id="${t.id}">
        <div class="tk-card-status ${t.status}"></div>
        <div class="tk-card-body">
          <div class="tk-card-top">
            <div class="tk-card-title">${_tkEsc(t.title)}</div>
            <span class="tk-card-id">#${t.id}</span>
          </div>
          <div class="tk-card-desc">${_tkEsc(t.description || "")}</div>
          <div class="tk-card-meta">
            <span><i class="fa-solid fa-circle-dot"></i> ${statusLabel}</span>
            <span class="${prioClass}"><i class="fa-solid fa-flag"></i> ${prioLabel}</span>
            ${t.plant_name ? `<span><i class="fa-solid fa-solar-panel"></i> ${_tkEsc(t.plant_name)}</span>` : ""}
            <span><i class="fa-regular fa-calendar"></i> ${d}</span>
            ${t.comment_count ? `<span><i class="fa-regular fa-comment"></i> ${t.comment_count}</span>` : ""}
          </div>
        </div>
      </div>`;
    }).join("");

    list.querySelectorAll(".tk-card").forEach(card => {
      card.addEventListener("click", () => tkOpenDetail(parseInt(card.dataset.id)));
    });
  } catch (e) {
    console.warn("[tickets]", e);
    list.innerHTML = '<div class="tk-empty">Erro ao carregar chamados.</div>';
  }
}

async function tkOpenDetail(id) {
  _tkViewingId = id;
  const listEl = document.getElementById("tkList");
  const filtersEl = document.querySelector(".tk-filters");
  const headerEl = document.querySelector(".tk-header");
  const detailEl = document.getElementById("tkDetail");
  if (listEl) listEl.classList.add("hidden");
  if (filtersEl) filtersEl.classList.add("hidden");
  if (headerEl) headerEl.classList.add("hidden");
  if (detailEl) detailEl.classList.remove("hidden");

  const detailHeader = document.getElementById("tkDetailHeader");
  const timeline = document.getElementById("tkTimeline");
  detailHeader.innerHTML = '<div class="tk-empty">Carregando...</div>';
  timeline.innerHTML = "";

  try {
    const res = await apiFetch(`/tickets/${id}`);
    if (!res.ok) throw new Error();
    const data = await res.json();
    const t = data.ticket;
    const comments = data.comments || [];
    const user = _getUser();

    const statusLabel = {open: "Aberto", in_progress: "Em Andamento", resolved: "Resolvido"}[t.status] || t.status;
    const prioLabel = {low: "Baixa", medium: "Média", high: "Alta"}[t.priority] || "Média";
    const d = t.created_at ? _tkDate(t.created_at).toLocaleString("pt-BR") : "";

    let statusHtml;
    if (user.is_superuser) {
      statusHtml = `<select class="tk-status-select" id="tkStatusSelect" data-id="${t.id}">
        <option value="open" ${t.status === "open" ? "selected" : ""}>Aberto</option>
        <option value="in_progress" ${t.status === "in_progress" ? "selected" : ""}>Em Andamento</option>
        <option value="resolved" ${t.status === "resolved" ? "selected" : ""}>Resolvido</option>
      </select>`;
    } else {
      statusHtml = `<span class="tk-status-badge ${t.status}"><i class="fa-solid fa-circle-dot"></i> ${statusLabel}</span>`;
    }

    detailHeader.innerHTML = `
      <div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:8px">
        <div class="tk-detail-title">${_tkEsc(t.title)} <span class="tk-card-id">#${t.id}</span></div>
        ${statusHtml}
      </div>
      <div class="tk-detail-info">
        <span><i class="fa-solid fa-flag"></i> ${prioLabel}</span>
        ${t.plant_name ? `<span><i class="fa-solid fa-solar-panel"></i> ${_tkEsc(t.plant_name)}</span>` : ""}
        <span><i class="fa-regular fa-calendar"></i> ${d}</span>
        <span><i class="fa-regular fa-user"></i> ${_tkEsc(t.username || "")}</span>
      </div>
      <div class="tk-detail-desc">${_tkEsc(t.description || "")}</div>
      ${t.image_url ? `<img src="${_tkEsc(t.image_url)}" class="tk-detail-img" alt="Anexo">` : ""}
    `;

    if (user.is_superuser) {
      document.getElementById("tkStatusSelect")?.addEventListener("change", async function() {
        const sel = this;
        const newStatus = sel.value;
        const tid = sel.dataset.id;
        sel.disabled = true;
        try {
          const r = await apiFetch(`/tickets/${tid}`, {
            method: "PUT",
            headers: {"Content-Type": "application/json"},
            body: JSON.stringify({status: newStatus})
          });
          if (r.ok) {
            const lbl = {open: "Aberto", in_progress: "Em Andamento", resolved: "Resolvido"}[newStatus] || newStatus;
            sel.style.borderColor = "rgba(57,229,140,0.5)";
            setTimeout(() => { sel.style.borderColor = ""; }, 1500);
          }
        } catch (e) { console.warn("[status update]", e); }
        sel.disabled = false;
      });
    }

    timeline.innerHTML = comments.map(c => {
      const cd = c.created_at ? _tkDate(c.created_at).toLocaleString("pt-BR") : "";
      const role = c.is_admin ? "admin" : "user";
      const authorLabel = c.is_admin ? `<i class="fa-solid fa-headset"></i> ${_tkEsc(c.author)} (Suporte)` : `<i class="fa-regular fa-user"></i> ${_tkEsc(c.author)}`;
      return `<div class="tk-timeline-item ${role}">
        <div class="tk-timeline-author">
          <span>${authorLabel}</span>
          <span class="tk-timeline-date">${cd}</span>
        </div>
        ${c.text ? `<div class="tk-timeline-text">${_tkEsc(c.text)}</div>` : ""}
        ${c.image_url ? `<img src="${_tkEsc(c.image_url)}" class="tk-timeline-img" alt="Anexo">` : ""}
      </div>`;
    }).join("");

  } catch (e) {
    console.warn("[ticket detail]", e);
    detailHeader.innerHTML = '<div class="tk-empty">Erro ao carregar chamado.</div>';
  }
}

function tkBackToList() {
  _tkViewingId = null;
  const listEl = document.getElementById("tkList");
  const filtersEl = document.querySelector(".tk-filters");
  const headerEl = document.querySelector(".tk-header");
  const detailEl = document.getElementById("tkDetail");
  if (listEl) listEl.classList.remove("hidden");
  if (filtersEl) filtersEl.classList.remove("hidden");
  if (headerEl) headerEl.classList.remove("hidden");
  if (detailEl) detailEl.classList.add("hidden");
  tkLoadList();
}

async function tkSendComment() {
  if (!_tkViewingId) return;
  const textEl = document.getElementById("tkReplyText");
  const fileEl = document.getElementById("tkReplyFile");
  const text = (textEl?.value || "").trim();
  const file = fileEl?.files?.[0];

  if (!text && !file) return;

  let image_url = null;
  if (file) {
    image_url = await tkUploadImage(file);
    if (!image_url) return;
  }

  try {
    const res = await apiFetch(`/tickets/${_tkViewingId}/comments`, {
      method: "POST",
      headers: {"Content-Type": "application/json"},
      body: JSON.stringify({text, image_url})
    });
    if (!res.ok) throw new Error();
    if (textEl) textEl.value = "";
    if (fileEl) fileEl.value = "";
    const nameEl = document.getElementById("tkReplyFileName");
    if (nameEl) nameEl.textContent = "";
    tkOpenDetail(_tkViewingId);
  } catch (e) {
    console.warn("[send comment]", e);
  }
}

function tkOpenModal() {
  document.getElementById("tkModalOverlay")?.classList.remove("hidden");
  document.getElementById("tkTitle").value = "";
  document.getElementById("tkDesc").value = "";
  document.getElementById("tkFile").value = "";
  document.getElementById("tkFilePreview")?.classList.add("hidden");
  document.getElementById("tkFileLabel").textContent = "Clique ou arraste uma imagem";
}

function tkCloseModal() {
  document.getElementById("tkModalOverlay")?.classList.add("hidden");
}

function tkPreviewFile(input) {
  const preview = document.getElementById("tkFilePreview");
  const label = document.getElementById("tkFileLabel");
  if (!input.files[0]) return;
  label.textContent = input.files[0].name;
  const reader = new FileReader();
  reader.onload = e => {
    preview.innerHTML = `<img src="${e.target.result}" alt="Preview">`;
    preview.classList.remove("hidden");
  };
  reader.readAsDataURL(input.files[0]);
}

async function tkSubmitTicket() {
  const title = (document.getElementById("tkTitle")?.value || "").trim();
  const desc = (document.getElementById("tkDesc")?.value || "").trim();
  const plantId = document.getElementById("tkPlant")?.value || null;
  const priority = document.getElementById("tkPriority")?.value || "medium";
  const fileInput = document.getElementById("tkFile");

  if (!title) { alert("Preencha o título do chamado."); return; }

  const submitBtn = document.getElementById("tkModalSubmit");
  if (submitBtn) { submitBtn.disabled = true; submitBtn.innerHTML = '<i class="fa-solid fa-spinner fa-spin"></i> Enviando...'; }

  let image_url = null;
  if (fileInput?.files?.[0]) {
    image_url = await tkUploadImage(fileInput.files[0]);
  }

  try {
    const res = await apiFetch("/tickets", {
      method: "POST",
      headers: {"Content-Type": "application/json"},
      body: JSON.stringify({title, description: desc, plant_id: plantId || null, priority, image_url})
    });
    if (!res.ok) throw new Error();
    tkCloseModal();
    tkLoadList();
  } catch (e) {
    console.warn("[create ticket]", e);
    alert("Erro ao criar chamado.");
  } finally {
    if (submitBtn) { submitBtn.disabled = false; submitBtn.innerHTML = '<i class="fa-solid fa-paper-plane"></i> Enviar Chamado'; }
  }
}

async function tkUploadImage(file) {
  try {
    const ct = file.type || "application/octet-stream";
    const res = await apiFetch("/tickets/upload", {
      method: "POST",
      headers: {"Content-Type": "application/json"},
      body: JSON.stringify({filename: file.name, content_type: ct})
    });
    if (!res.ok) {
      const errBody = await res.text();
      console.error("[upload] presign failed:", res.status, errBody);
      throw new Error("presign " + res.status);
    }
    const data = await res.json();
    console.log("[upload] presigned OK, uploading to S3...", data.upload_url?.substring(0, 80));

    const uploadRes = await fetch(data.upload_url, {
      method: "PUT",
      headers: {"Content-Type": ct},
      body: file
    });
    if (!uploadRes.ok) {
      const s3err = await uploadRes.text();
      console.error("[upload] S3 PUT failed:", uploadRes.status, s3err);
      throw new Error("s3 " + uploadRes.status);
    }
    return data.s3_key;
  } catch (e) {
    console.warn("[upload]", e);
    alert("Erro ao enviar imagem.");
    return null;
  }
}

function _tkEsc(s) {
  const d = document.createElement("div");
  d.textContent = s || "";
  return d.innerHTML;
}

// timestamps de ticket/aviso vêm do banco em UTC SEM timezone
// ("2026-07-16T20:35:34") — o new Date() trata como hora local e
// mostra 3h adiantado. Sem indicador de fuso, assume UTC.
function _tkDate(s) {
  if (!s) return null;
  const hasTz = /(Z|[+-]\d{2}:?\d{2})$/.test(String(s));
  const d = new Date(hasTz ? s : String(s) + "Z");
  return isNaN(d) ? new Date(s) : d;
}

// =============================================================================
// SISTEMA DE ATUALIZAÇÕES (SININHO + MODAL)
// =============================================================================
const NOTIF_STORAGE_KEY = "platform_last_seen_update";

// Quem chegou por `?infra=1` clicou num aviso de usina parada. As novidades da
// plataforma não podem abrir por cima disso — nesse momento elas são ruído.
let _NOTIF_VEIO_POR_INFRA = false;

async function initPlatformUpdates() {
  try {
    _NOTIF_VEIO_POR_INFRA = new URLSearchParams(location.search).get("infra") === "1";
  } catch (e) { /* URL exótica não pode derrubar o sininho */ }

  const bellBtn = document.getElementById("notifBellBtn");
  const panel = document.getElementById("notifPanel");
  const closeBtn = document.getElementById("notifPanelClose");
  const modalOverlay = document.getElementById("notifModalOverlay");
  const modalCloseBtn = document.getElementById("notifModalCloseBtn");
  if (!bellBtn) return;

  bellBtn.addEventListener("click", (e) => {
    e.stopPropagation();
    const open = panel.style.display !== "none";
    panel.style.display = open ? "none" : "";
  });

  if (closeBtn) closeBtn.addEventListener("click", () => { panel.style.display = "none"; });

  const prefsBtn = document.getElementById("notifPrefsBtn");
  if (prefsBtn) prefsBtn.addEventListener("click", (e) => {
    e.stopPropagation();
    panel.style.display = "none";
    openNotifCenter();
  });

  const infraStrip = document.getElementById("notifInfraStrip");
  if (infraStrip) infraStrip.addEventListener("click", (e) => {
    e.stopPropagation();
    panel.style.display = "none";
    openInfraHealth();
  });

  document.addEventListener("click", (e) => {
    if (panel.style.display !== "none" && !e.target.closest(".notif-bell-wrap")) {
      panel.style.display = "none";
    }
  });

  if (modalCloseBtn) {
    modalCloseBtn.addEventListener("click", () => {
      modalOverlay.style.display = "none";
      _notifMarkAllSeen();
    });
  }
  if (modalOverlay) {
    modalOverlay.addEventListener("click", (e) => {
      if (e.target === modalOverlay) {
        modalOverlay.style.display = "none";
        _notifMarkAllSeen();
      }
    });
  }

  await _notifFetchAndRender();

  // Saúde da infra: a faixa acompanha o sininho e se atualiza no ritmo do cron.
  await _infraFetch();
  setInterval(_infraFetch, INFRA_REFRESH_MS);

  // O push de infraestrutura aponta para cá (`?infra=1`). Sem isto o clique na
  // notificação abria a usina e a pessoa não via nem o histórico nem o que
  // mais caiu junto — que é o dado que aponta para o CLP.
  if (_NOTIF_VEIO_POR_INFRA) openInfraHealth();
}

let _notifAllUpdates = [];

async function _notifFetchAndRender() {
  try {
    const res = await apiFetch("/platform/updates?limit=20");
    if (!res.ok) return;
    const data = await res.json();
    _notifAllUpdates = data.items || [];
  } catch (e) {
    console.warn("[updates] fetch falhou:", e);
    return;
  }

  if (!_notifAllUpdates.length) return;

  const lastSeen = parseInt(localStorage.getItem(NOTIF_STORAGE_KEY) || "0");
  const unseen = _notifAllUpdates.filter(u => u.id > lastSeen);

  _notifRenderBadge(unseen.length);
  _notifRenderPanel(_notifAllUpdates, lastSeen);

  if (unseen.length > 0) {
    _notifShowModal(unseen);
  }
}

function _notifRenderBadge(count) {
  const badge = document.getElementById("notifBellBadge");
  if (!badge) return;
  if (count > 0) {
    badge.textContent = count > 9 ? "9+" : String(count);
    badge.style.display = "";
  } else {
    badge.style.display = "none";
  }
}

function _notifRenderPanel(updates, lastSeen) {
  const list = document.getElementById("notifPanelList");
  if (!list) return;

  if (!updates.length) {
    list.innerHTML = '<p class="notif-empty">Nenhuma atualização.</p>';
    return;
  }

  list.innerHTML = updates.map(u => {
    const isUnread = u.id > lastSeen;
    const d = u.created_at ? _tkDate(u.created_at).toLocaleDateString("pt-BR") : "";
    return `<div class="notif-item${isUnread ? " unread" : ""}">
      <div class="notif-item-title">${_notifEsc(u.title)}</div>
      <div class="notif-item-desc">${_notifEsc(u.description || "")}</div>
      ${d ? `<div class="notif-item-date">${d}</div>` : ""}
    </div>`;
  }).join("");
}

function _notifShowModal(unseen) {
  if (_NOTIF_VEIO_POR_INFRA) return;
  const overlay = document.getElementById("notifModalOverlay");
  const body = document.getElementById("notifModalBody");
  if (!overlay || !body) return;

  body.innerHTML = unseen.map(u => {
    const d = u.created_at ? _tkDate(u.created_at).toLocaleDateString("pt-BR") : "";
    return `<div class="notif-modal-entry">
      <div class="notif-modal-entry-title">${_notifEsc(u.title)}</div>
      <div class="notif-modal-entry-desc">${_notifEsc(u.description || "")}</div>
      ${d ? `<div class="notif-modal-entry-date">${d}</div>` : ""}
    </div>`;
  }).join("");

  overlay.style.display = "";
}

function _notifMarkAllSeen() {
  if (!_notifAllUpdates.length) return;
  const maxId = Math.max(..._notifAllUpdates.map(u => u.id));
  localStorage.setItem(NOTIF_STORAGE_KEY, String(maxId));
  _notifRenderBadge(0);
  _notifRenderPanel(_notifAllUpdates, maxId);
}

function _notifEsc(s) {
  const d = document.createElement("div");
  d.textContent = s;
  return d.innerHTML;
}

// =============================================================================
// CENTRAL DE NOTIFICAÇÕES (engrenagem do sininho)
// Pedido do supervisor 24/07: os alarmes notificam demais (tracker chega a
// 150x/dia, falha de comunicação direto) — cada usuário escolhe o que quer
// receber no celular/PC. Grava em app_user.notif_prefs (mesmo jsonb do robô,
// com merge no backend) e o push_notifier.py da EC2 respeita na hora de enviar.
// =============================================================================
let _NOTIF_ALARM_CATALOG = [];

function _notifCenterPrefs() {
  const p = _robotGetNotifPrefs();
  return {
    push_muted: p.push_muted === true,
    // Chave que o infra_watch.py já lê desde 02/08 e que até agora ninguém
    // tinha como marcar: quem quer alarme de equipamento mas não quer aviso
    // de infraestrutura desliga só esta e continua recebendo o resto.
    push_infra_muted: p.push_infra_muted === true,
    push_min_severity: p.push_min_severity || "all",
    push_disabled_codes: Array.isArray(p.push_disabled_codes) ? p.push_disabled_codes.map(String) : [],
  };
}

// Escreve as preferências salvas nos controles. Roda duas vezes de propósito:
// na abertura (com o que está no localStorage, para a tela não piscar) e de
// novo quando o servidor responde, que é a fonte de verdade.
function _notifCenterApplyPrefs() {
  const cur = _notifCenterPrefs();
  const mutedEl = document.getElementById("notifCenterMuted");
  const infraEl = document.getElementById("notifCenterInfraMuted");
  const sevEl = document.getElementById("notifCenterSeverity");
  if (mutedEl) mutedEl.checked = cur.push_muted;
  if (infraEl) infraEl.checked = cur.push_infra_muted;
  if (sevEl) sevEl.value = cur.push_min_severity;
}

async function openNotifCenter() {
  document.getElementById("notifCenterOverlay")?.remove();

  const overlay = document.createElement("div");
  overlay.id = "notifCenterOverlay";
  overlay.className = "notif-center-overlay";
  overlay.innerHTML = `
    <div class="notif-center-box" role="dialog" aria-modal="true" aria-labelledby="notifCenterTitle">
      <div class="notif-center-head">
        <h3 id="notifCenterTitle"><i class="fa-solid fa-bell"></i> Central de notificações</h3>
        <button type="button" class="notif-center-close" aria-label="Fechar">&times;</button>
      </div>
      <p class="notif-center-hint">
        Vale para as notificações no celular e no computador (push) deste usuário,
        em qualquer aparelho.
      </p>

      <label class="notif-center-switch">
        <input type="checkbox" id="notifCenterMuted">
        <span>Pausar todas as notificações de alarme</span>
      </label>

      <label class="notif-center-switch">
        <input type="checkbox" id="notifCenterInfraMuted">
        <span>Pausar avisos de usina sem comunicação
          <small>(infraestrutura — separado dos alarmes de equipamento)</small></span>
      </label>

      <div class="notif-center-field">
        <span>Notificar a partir de</span>
        <select id="notifCenterSeverity">
          <option value="all">Todos os alarmes</option>
          <option value="medium">Média e alta</option>
          <option value="high">Somente alta</option>
        </select>
      </div>

      <div class="notif-center-list-head">
        <span>Alarmes que notificam</span>
        <div class="notif-center-bulk">
          <button type="button" id="notifCenterAll">Marcar todos</button>
          <button type="button" id="notifCenterNone">Limpar</button>
        </div>
      </div>
      <input type="search" id="notifCenterSearch" class="notif-center-search" placeholder="Buscar alarme…">
      <div class="notif-center-list" id="notifCenterList">
        <p class="notif-center-loading">Carregando alarmes…</p>
      </div>

      <div class="notif-center-feedback" id="notifCenterFeedback"></div>
      <div class="notif-center-actions">
        <button type="button" class="notif-center-btn" id="notifCenterCancel">Cancelar</button>
        <button type="button" class="notif-center-btn primary" id="notifCenterSave">Salvar</button>
      </div>
    </div>`;

  document.body.appendChild(overlay);

  const close = () => overlay.remove();
  overlay.addEventListener("click", (e) => { if (e.target === overlay) close(); });
  overlay.querySelector(".notif-center-close")?.addEventListener("click", close);
  document.getElementById("notifCenterCancel")?.addEventListener("click", close);
  document.getElementById("notifCenterSave")?.addEventListener("click", _notifCenterSave);

  _notifCenterApplyPrefs();

  document.getElementById("notifCenterSearch")?.addEventListener("input", (e) => {
    _notifCenterRenderList(String(e.target.value || "").toLowerCase());
  });
  document.getElementById("notifCenterAll")?.addEventListener("click", () => {
    overlay.querySelectorAll(".notif-center-code").forEach(c => { c.checked = true; });
  });
  document.getElementById("notifCenterNone")?.addEventListener("click", () => {
    overlay.querySelectorAll(".notif-center-code").forEach(c => { c.checked = false; });
  });

  await _notifCenterLoadCatalog();
}

async function _notifCenterLoadCatalog() {
  const list = document.getElementById("notifCenterList");
  try {
    const user = JSON.parse(localStorage.getItem("user") || "{}");
    const r = await apiFetch("/users/notif-prefs", { headers: { "X-Username": user.username || "" } });
    if (!r.ok) throw new Error(`HTTP ${r.status}`);
    const d = await r.json();
    const payload = d?.body ? (typeof d.body === "string" ? JSON.parse(d.body) : d.body) : d;
    _NOTIF_ALARM_CATALOG = Array.isArray(payload?.alarm_catalog) ? payload.alarm_catalog : [];
    if (payload?.prefs && typeof payload.prefs === "object") {
      localStorage.setItem(ROBOT_NOTIF_PREFS_KEY, JSON.stringify(payload.prefs));
      _notifCenterApplyPrefs();
    }
  } catch (e) {
    console.warn("[notif-center] catálogo indisponível:", e);
    if (list) {
      list.innerHTML = '<p class="notif-center-loading">Não foi possível carregar a lista de alarmes. ' +
        'As demais opções continuam valendo.</p>';
    }
    return;
  }
  _notifCenterRenderList("");
}

function _notifCenterRenderList(filter) {
  const list = document.getElementById("notifCenterList");
  if (!list) return;
  if (!_NOTIF_ALARM_CATALOG.length) {
    list.innerHTML = '<p class="notif-center-loading">Nenhum alarme no catálogo.</p>';
    return;
  }
  // Preserva o que já estiver marcado na tela antes de redesenhar (busca)
  const onScreen = new Map();
  list.querySelectorAll(".notif-center-code").forEach(c => onScreen.set(c.value, c.checked));
  const disabled = new Set(_notifCenterPrefs().push_disabled_codes);

  const rows = _NOTIF_ALARM_CATALOG.filter(a => {
    if (!filter) return true;
    const hay = `${a.code} ${a.description_pt || ""}`.toLowerCase();
    return hay.includes(filter);
  });

  if (!rows.length) {
    list.innerHTML = '<p class="notif-center-loading">Nenhum alarme encontrado.</p>';
    return;
  }

  list.innerHTML = rows.map(a => {
    const code = String(a.code);
    const checked = onScreen.has(code) ? onScreen.get(code) : !disabled.has(code);
    const sev = String(a.severity || "").toLowerCase();
    const sevClass = sev === "high" ? "high" : (sev === "medium" ? "medium" : "low");
    return `<label class="notif-center-row">
      <input type="checkbox" class="notif-center-code" value="${_notifEsc(code)}" ${checked ? "checked" : ""}>
      <span class="notif-center-sev ${sevClass}"></span>
      <span class="notif-center-desc">${_notifEsc(a.description_pt || code)}</span>
      <span class="notif-center-codetag">${_notifEsc(code)}</span>
    </label>`;
  }).join("");
}

async function _notifCenterSave() {
  const fb = document.getElementById("notifCenterFeedback");
  const overlay = document.getElementById("notifCenterOverlay");
  if (!overlay) return;

  // Desmarcado = não notifica. Só conta o que está na tela + o que o filtro
  // da busca escondeu (esses seguem com a preferência anterior).
  const shown = new Set();
  const disabled = new Set(_notifCenterPrefs().push_disabled_codes);
  overlay.querySelectorAll(".notif-center-code").forEach(c => {
    shown.add(c.value);
    if (c.checked) disabled.delete(c.value);
    else disabled.add(c.value);
  });

  const prefs = {
    ..._robotGetNotifPrefs(),
    push_muted: document.getElementById("notifCenterMuted")?.checked === true,
    push_infra_muted: document.getElementById("notifCenterInfraMuted")?.checked === true,
    push_min_severity: document.getElementById("notifCenterSeverity")?.value || "all",
    push_disabled_codes: Array.from(disabled),
  };

  if (fb) { fb.textContent = "Salvando…"; fb.className = "notif-center-feedback"; }
  try {
    localStorage.setItem(ROBOT_NOTIF_PREFS_KEY, JSON.stringify(prefs));
    const user = JSON.parse(localStorage.getItem("user") || "{}");
    const r = await apiFetch("/users/notif-prefs", {
      method: "POST",
      headers: { "Content-Type": "application/json", "X-Username": user.username || "" },
      body: JSON.stringify({ prefs, merge: true }),
    });
    if (!r.ok) throw new Error(`HTTP ${r.status}`);
    if (fb) { fb.textContent = "✓ Preferências salvas."; fb.className = "notif-center-feedback ok"; }
    setTimeout(() => overlay.remove(), 800);
  } catch (e) {
    if (fb) {
      fb.textContent = "Salvo neste aparelho, mas o servidor não respondeu — as notificações " +
        "no celular podem continuar como estavam.";
      fb.className = "notif-center-feedback err";
    }
  }
}

// =============================================================================
// SAÚDE DA INFRAESTRUTURA (assistente de infra — elo 1)
// O detector roda no cron da EC2 desde 02/08 e o push avisa a cada transição.
// Faltava o lugar de OLHAR: até aqui o histórico só existia por SQL ou pelo
// `--report` via SSH. Um cartão por USINA, nunca um por achado — a Pacajus2
// abriu 5 achados (um por tabela) para UM problema só, e repetir isso na tela
// desfaz o agrupamento que o push já faz de propósito.
// =============================================================================
const INFRA_REFRESH_MS = 5 * 60 * 1000;   // mesma cadência do cron da EC2
let _INFRA_STATE = null;

function _infraUnwrap(d) {
  // Algumas respostas chegam embrulhadas em {body}. Mesmo tratamento do
  // catálogo de alarmes — melhor aceitar as duas formas do que depender disso.
  if (d && d.body) return typeof d.body === "string" ? JSON.parse(d.body) : d.body;
  return d;
}

async function _infraFetch() {
  try {
    const r = await apiFetch("/infra/health?days=7");
    if (!r.ok) throw new Error(`HTTP ${r.status}`);
    _INFRA_STATE = _infraUnwrap(await r.json());
  } catch (e) {
    console.warn("[infra] indisponível:", e);
    _INFRA_STATE = null;
  }
  _infraRenderStrip();
  return _INFRA_STATE;
}

function _infraDur(s) {
  const n = Math.max(0, Math.round(Number(s) || 0));
  if (n < 90) return `${n} s`;
  const min = Math.round(n / 60);
  if (min < 90) return `${min} min`;
  // Horas até 3 dias: numa parada de usina "51,6 h" diz muito mais do que
  // "2 dias", e é esse número que vai para a conversa com o cliente.
  const h = n / 3600;
  if (h < 72) return `${h.toFixed(1).replace(".", ",")} h`;
  return `${(h / 24).toFixed(1).replace(".", ",")} dias`;
}

// Nome da usina quando existe UMA só naquele estado. Antes a faixa pegava
// sempre `incidents[0]`, que é o cartão de maior duração e pode ser de outro
// estado que não o anunciado.
function _infraNomeAberto(st, escopo) {
  const g = (st?.incidents || []).filter(x => (x.scope || "silent") === escopo);
  return g.length === 1 ? g[0].plant_name : "";
}

function _infraWhen(iso) {
  if (!iso) return "";
  const d = _tkDate(iso);
  return isNaN(d) ? "" : d.toLocaleString("pt-BR", { day: "2-digit", month: "2-digit",
                                                    hour: "2-digit", minute: "2-digit" });
}

// A faixa dentro do sininho. É o único ponto da tela que diz, sem clique
// nenhum, se a plataforma está recebendo dado das usinas.
function _infraRenderStrip() {
  const strip = document.getElementById("notifInfraStrip");
  const bell = document.getElementById("notifBellBtn");
  if (!strip) return;

  const st = _INFRA_STATE;
  if (!st || st.available === false) {
    strip.style.display = "none";
    bell?.classList.remove("infra-alert");
    return;
  }

  const down = st.summary?.plants_down || 0;
  const parcial = st.summary?.plants_partial || 0;
  const atraso = st.summary?.plants_lagging || 0;
  const stale = st.watch?.stale === true;
  strip.style.display = "";
  strip.className = "notif-infra-strip " +
    (down ? "down" : ((parcial || atraso) ? "unknown" : (stale ? "unknown" : "ok")));
  // Sino vermelho só quando a usina inteira calou. Equipamento isolado fora do
  // ar é aviso, não emergência — pintar tudo de vermelho foi o que fez o
  // cliente achar que a Acopiara tinha parado enquanto ela gerava normal.
  bell?.classList.toggle("infra-alert", down > 0);

  let icone, texto;
  if (down) {
    icone = "fa-plug-circle-xmark";
    texto = down === 1
      ? `${_notifEsc(_infraNomeAberto(st, "silent") || "1 usina")} sem comunicação`
      : `${down} usinas sem comunicação`;
  } else if (parcial) {
    icone = "fa-triangle-exclamation";
    texto = parcial === 1
      ? `${_notifEsc(_infraNomeAberto(st, "partial") || "1 usina")} com equipamentos sem comunicação`
      : `${parcial} usinas com equipamentos sem comunicação`;
  } else if (atraso) {
    icone = "fa-clock";
    texto = atraso === 1
      ? `${_notifEsc(_infraNomeAberto(st, "lag") || "1 usina")} publicando com atraso`
      : `${atraso} usinas publicando com atraso`;
  } else if (stale) {
    // Vigia calado não é "tudo bem": é "não sei". A diferença entre as duas
    // coisas é justamente o que faz um painel de monitoramento valer algo.
    icone = "fa-triangle-exclamation";
    texto = `Vigilância sem sinal há ${_infraDur(st.watch?.quiet_s)}`;
  } else {
    icone = "fa-plug-circle-check";
    texto = "Infraestrutura: tudo comunicando";
  }
  strip.innerHTML = `<i class="fa-solid ${icone}"></i><span>${texto}</span>` +
                    `<i class="fa-solid fa-chevron-right notif-infra-go"></i>`;
}

async function openInfraHealth() {
  document.getElementById("infraHealthOverlay")?.remove();

  const overlay = document.createElement("div");
  overlay.id = "infraHealthOverlay";
  overlay.className = "notif-center-overlay";
  overlay.innerHTML = `
    <div class="notif-center-box infra-box" role="dialog" aria-modal="true" aria-labelledby="infraHealthTitle">
      <div class="notif-center-head">
        <h3 id="infraHealthTitle"><i class="fa-solid fa-heart-pulse"></i> Saúde da infraestrutura</h3>
        <button type="button" class="notif-center-close" aria-label="Fechar">&times;</button>
      </div>
      <div class="infra-body" id="infraHealthBody">
        <p class="notif-center-loading">Consultando…</p>
      </div>` +
      (_pwCanSee()
        ? `<div style="padding:10px 14px 12px;border-top:1px solid rgba(255,255,255,0.08);">
             <button type="button" id="infraOpenPipelineBtn"
               style="width:100%;background:rgba(255,255,255,0.06);border:1px solid rgba(255,255,255,0.12);
                      color:#e8ecf5;border-radius:8px;padding:9px 12px;font-size:12px;cursor:pointer;
                      display:flex;align-items:center;justify-content:center;gap:8px;">
               <i class="fa-solid fa-diagram-project"></i> Processamento e servidor
             </button>
           </div>`
        : "") +
    `</div>`;

  document.body.appendChild(overlay);
  const close = () => overlay.remove();
  overlay.addEventListener("click", (e) => { if (e.target === overlay) close(); });
  overlay.querySelector(".notif-center-close")?.addEventListener("click", close);
  document.getElementById("infraOpenPipelineBtn")?.addEventListener("click", openPipelineWatch);

  await _infraFetch();
  _infraRenderModal();
}

// ===========================================================================
// PAINEL DO PIPELINE E DA MAQUINA (elos 5-6) -- o que foi pedido como "Airflow"
//
// 🔑 Nao existe Airflow neste pipeline: quem agenda sao linhas de crontab.
// Este painel entrega a CAPACIDADE pedida (ver o que rodou, o que falhou e se
// a maquina aguenta) lendo o que o pipeline_watch.py grava a cada 5 min.
//
// Restrito a admin no BACKEND (403). O gate aqui so evita oferecer um botao
// que iria falhar -- esconder botao nunca foi seguranca.
// ===========================================================================
function _pwCanSee() {
  let u = {};
  try { u = JSON.parse(localStorage.getItem("user") || "{}"); } catch { u = {}; }
  if (u.is_superuser === true || u.is_superuser === "true") return true;
  const p = (u && typeof u.permissions === "object" && u.permissions) ? u.permissions : {};
  return p.infra_alerts === true || p.infra_alerts === "true";
}

function _pwNum(v, dec) {
  if (v === null || v === undefined || isNaN(Number(v))) return "—";
  return Number(v).toLocaleString("pt-BR", { minimumFractionDigits: dec || 0,
                                             maximumFractionDigits: dec || 0 });
}

function _pwBytes(v) {
  const n = Number(v);
  if (!isFinite(n) || n <= 0) return "—";
  const gb = n / 1073741824;
  if (gb >= 1) return `${gb.toFixed(1).replace(".", ",")} GB`;
  return `${(n / 1048576).toFixed(0)} MB`;
}

async function openPipelineWatch() {
  document.getElementById("pipelineWatchOverlay")?.remove();
  const overlay = document.createElement("div");
  overlay.id = "pipelineWatchOverlay";
  overlay.className = "notif-center-overlay";
  overlay.innerHTML = `
    <div class="notif-center-box infra-box" role="dialog" aria-modal="true" aria-labelledby="pwTitle">
      <div class="notif-center-head">
        <h3 id="pwTitle"><i class="fa-solid fa-diagram-project"></i> Processamento e servidor</h3>
        <button type="button" class="notif-center-close" aria-label="Fechar">&times;</button>
      </div>
      <div class="infra-body" id="pwBody">
        <p class="notif-center-loading">Consultando…</p>
      </div>
    </div>`;
  document.body.appendChild(overlay);
  const close = () => overlay.remove();
  overlay.addEventListener("click", (e) => { if (e.target === overlay) close(); });
  overlay.querySelector(".notif-center-close")?.addEventListener("click", close);

  let st = null;
  try {
    const r = await apiFetch("/infra/health?view=pipeline&hours=24");
    if (r.status === 403) { _pwRender({ _forbidden: true }); return; }
    if (!r.ok) throw new Error(`HTTP ${r.status}`);
    st = _infraUnwrap(await r.json());
  } catch (e) {
    console.warn("[pipeline-watch] indisponível:", e);
    st = null;
  }
  _pwRender(st);
}

function _pwRender(st) {
  const body = document.getElementById("pwBody");
  if (!body) return;

  if (st && st._forbidden) {
    body.innerHTML = `<p class="notif-center-loading">Painel restrito a administradores.</p>`;
    return;
  }
  if (!st) {
    body.innerHTML = `<p class="notif-center-loading">Não foi possível consultar o
      processamento agora.</p>`;
    return;
  }
  // "Ainda nao instalado" e diferente de "esta tudo bem". Dizer qual dos dois
  // e a razao de existir de um painel de monitoramento.
  if (st.available === false) {
    body.innerHTML = `<p class="notif-center-loading">O vigia do processamento ainda
      não está instalado.<br><small>${_notifEsc(st.hint || "")}</small></p>`;
    return;
  }

  const w = st.watcher || {};
  const s = st.summary || {};
  const m = st.metrics || {};
  const val = (k) => (m[k] && m[k].value !== undefined) ? m[k].value : null;

  let html = "";

  // 1. O vigia esta vivo? Se ele parar, TODO o resto desta tela vira retrato
  // velho -- e retrato velho parece "tudo bem". Por isso vem primeiro.
  html += `<div class="notif-infra-strip ${w.alive ? "ok" : "unknown"}" style="margin-bottom:10px;">
    <i class="fa-solid ${w.alive ? "fa-circle-check" : "fa-triangle-exclamation"}"></i>
    <span>${w.alive
      ? `Vigia ativo, última verificação há ${_infraDur(w.age_s)}`
      : `Vigia sem sinal${w.age_s != null ? " há " + _infraDur(w.age_s) : ""} — os números abaixo podem estar velhos`}</span>
  </div>`;

  // 2. Placar dos achados abertos.
  const crit = s.critical || 0, warn = s.warning || 0, info = s.info || 0;
  html += `<div style="display:flex;gap:14px;font-size:11.5px;margin-bottom:12px;">
    <span style="color:${crit ? "#ef4444" : "rgba(255,255,255,0.45)"};">● ${crit} crítico(s)</span>
    <span style="color:${warn ? "#eab308" : "rgba(255,255,255,0.45)"};">● ${warn} atenção</span>
    <span style="color:${info ? "#3b82f6" : "rgba(255,255,255,0.45)"};">● ${info} informativo</span>
    ${s.resolved ? `<span style="color:rgba(255,255,255,0.45);margin-left:auto;">${s.resolved} resolvido(s)</span>` : ""}
  </div>`;

  // 3. Os pipelines, um cartao cada. E aqui que se ve "o que rodou e o que
  // falhou", que era o pedido.
  const pls = st.pipelines || [];
  if (pls.length) {
    html += `<div class="ronda-section-title" style="font-size:10px;letter-spacing:.06em;
      text-transform:uppercase;color:rgba(255,255,255,0.5);margin-bottom:6px;">Processamento (dbt)</div>`;
    pls.forEach(p => {
      const falhas = Number(p.seq_falhas || 0);
      const skip = Number(p.skip_pct || 0);
      const ruim = falhas > 0 || skip > 50;
      const cor = falhas > 0 ? "#ef4444" : (skip > 50 ? "#eab308" : "#39e58c");
      html += `<div style="display:flex;align-items:center;gap:10px;padding:7px 9px;margin-bottom:5px;
        border-radius:8px;background:rgba(255,255,255,0.04);border-left:3px solid ${cor};">
        <span style="flex:1;font-size:12px;font-weight:600;">${_notifEsc(p.name || "")}</span>
        <span style="font-size:10.5px;color:rgba(255,255,255,0.6);font-family:'Space Mono',monospace;">
          ${p.duracao_s != null ? _pwNum(p.duracao_s, 0) + "s" : "—"}
        </span>
        <span style="font-size:10.5px;color:rgba(255,255,255,0.6);">
          ${p.idade_ultimo_ok_s != null ? "há " + _infraDur(p.idade_ultimo_ok_s) : "sem registro"}
        </span>
        <span style="font-size:10.5px;color:${ruim ? cor : "rgba(255,255,255,0.45)"};font-weight:${ruim ? 700 : 400};">
          ${falhas > 0 ? falhas + " falha(s) seguidas" : (skip > 0 ? _pwNum(skip, 0) + "% pulado" : "ok")}
        </span>
      </div>`;
    });
  }

  // 4. A maquina. Percentuais so; o numero cru de bytes vai nas tabelas.
  const host = [
    ["CPU", val("host:cpu_pct"), "%", 80],
    ["Espera de disco", val("host:iowait_pct"), "%", 40],
    ["Disco usado", val("host:disk_pct"), "%", 85],
    ["Memória usada", val("host:mem_used_pct"), "%", 90],
    ["Carga por núcleo", val("host:load_por_nucleo"), "", 1.5],
  ].filter(x => x[1] !== null);
  if (host.length) {
    html += `<div class="ronda-section-title" style="font-size:10px;letter-spacing:.06em;
      text-transform:uppercase;color:rgba(255,255,255,0.5);margin:12px 0 6px;">Servidor</div>
      <div style="display:grid;grid-template-columns:1fr 1fr;gap:6px;">`;
    host.forEach(([nome, v, un, teto]) => {
      const alto = Number(v) >= teto;
      html += `<div style="background:rgba(255,255,255,0.04);border-radius:8px;padding:7px 9px;">
        <div style="font-size:9.5px;color:rgba(255,255,255,0.5);">${nome}</div>
        <div style="font-size:14px;font-weight:700;font-family:'Space Mono',monospace;
                    color:${alto ? "#ef4444" : "#e8ecf5"};">${_pwNum(v, un ? 1 : 2)}${un}</div>
      </div>`;
    });
    html += `</div>`;
  }

  // 5. Tabelas que mais ocupam. Serve para responder "esta crescendo?", que e
  // a pergunta que um retrato do instante nunca responde.
  const rels = Object.keys(m).filter(k => k.indexOf("relacao:") === 0)
    .map(k => [k.replace("relacao:", "").replace(":bytes", ""), val(k)])
    .filter(x => x[1] != null).sort((a, b) => b[1] - a[1]).slice(0, 5);
  if (rels.length) {
    html += `<div class="ronda-section-title" style="font-size:10px;letter-spacing:.06em;
      text-transform:uppercase;color:rgba(255,255,255,0.5);margin:12px 0 6px;">Maiores tabelas</div>`;
    rels.forEach(([nome, v]) => {
      html += `<div style="display:flex;justify-content:space-between;font-size:11px;
        padding:4px 2px;border-bottom:1px solid rgba(255,255,255,0.05);">
        <span style="color:rgba(255,255,255,0.7);">${_notifEsc(nome)}</span>
        <span style="font-family:'Space Mono',monospace;">${_pwBytes(v)}</span>
      </div>`;
    });
  }

  // 6. Achados abertos, por ultimo e por extenso.
  const abertos = (st.findings || []).filter(f => f.open);
  if (abertos.length) {
    html += `<div class="ronda-section-title" style="font-size:10px;letter-spacing:.06em;
      text-transform:uppercase;color:rgba(255,255,255,0.5);margin:12px 0 6px;">O que precisa de atenção</div>`;
    abertos.forEach(f => {
      const cor = f.severity === "critical" ? "#ef4444"
                : f.severity === "warning" ? "#eab308" : "#3b82f6";
      html += `<div style="padding:7px 9px;margin-bottom:5px;border-radius:8px;
        background:rgba(255,255,255,0.04);border-left:3px solid ${cor};">
        <div style="font-size:11.5px;line-height:1.4;">${_notifEsc(f.summary || f.key || "")}</div>
        <div style="font-size:9.5px;color:rgba(255,255,255,0.45);margin-top:3px;">
          ${_notifEsc(f.family || "")}${f.duration_s != null ? " · há " + _infraDur(f.duration_s) : ""}
        </div>
      </div>`;
    });
  } else {
    html += `<p style="font-size:11.5px;color:rgba(255,255,255,0.5);margin-top:12px;">
      Nenhum problema aberto no processamento nem no servidor.</p>`;
  }

  body.innerHTML = html;
}

function _infraRenderModal() {
  const body = document.getElementById("infraHealthBody");
  if (!body) return;
  const st = _INFRA_STATE;

  if (!st) {
    body.innerHTML = `<p class="notif-center-loading">Não foi possível consultar a
      saúde da infraestrutura agora. Os avisos no celular continuam valendo.</p>`;
    return;
  }
  if (st.available === false) {
    body.innerHTML = `<p class="notif-center-loading">O monitoramento ainda não está
      instalado neste banco.<br><small>${_notifEsc(st.hint || "")}</small></p>`;
    return;
  }

  const down = st.summary?.plants_down || 0;
  const parcial = st.summary?.plants_partial || 0;
  const atraso = st.summary?.plants_lagging || 0;
  const partes = [];

  // O título tem que dizer o que de fato está acontecendo. "Usina sem
  // comunicação" com a usina gerando gera chamado e desconfiança na tela.
  let heroCls = "ok", heroIco = "fa-plug-circle-check";
  let heroTit = "Todas as usinas comunicando";
  let heroSub = "Nenhum problema aberto no caminho do dado.";
  if (down) {
    heroCls = "down"; heroIco = "fa-plug-circle-xmark";
    heroTit = down === 1 ? "1 usina sem comunicação" : `${down} usinas sem comunicação`;
    heroSub = "A equipe AIOTI já foi avisada.";
  } else if (parcial) {
    heroCls = "warn"; heroIco = "fa-triangle-exclamation";
    heroTit = parcial === 1 ? "1 usina com equipamentos sem comunicação"
                            : `${parcial} usinas com equipamentos sem comunicação`;
    heroSub = "As usinas continuam gerando e enviando dados. " +
              "Parte dos equipamentos parou de responder e a equipe AIOTI já foi avisada.";
  } else if (atraso) {
    heroCls = "warn"; heroIco = "fa-clock";
    heroTit = atraso === 1 ? "1 usina publicando com atraso"
                           : `${atraso} usinas publicando com atraso`;
    heroSub = "Os dados estão chegando, só que mais devagar que o normal dela.";
  }

  partes.push(`<div class="infra-hero ${heroCls}">
      <i class="fa-solid ${heroIco}"></i>
      <div>
        <strong>${heroTit}</strong>
        <span>${heroSub}</span>
      </div>
    </div>`);

  if (st.watch?.stale === true) {
    partes.push(`<div class="infra-warn"><i class="fa-solid fa-triangle-exclamation"></i>
      <span>O monitoramento não registra atividade há ${_infraDur(st.watch.quiet_s)}.
      Ou a frota inteira parou, ou a própria vigilância parou — vale conferir.</span></div>`);
  }

  if (down || parcial || atraso) {
    partes.push(`<div class="infra-sec-title">Agora</div>`);
    partes.push(st.incidents.map(_infraCard).join(""));
  }

  const hist = st.history || [];
  partes.push(`<div class="infra-sec-title">Últimos ${st.days || 7} dias</div>`);
  partes.push(hist.length
    ? hist.map(_infraCard).join("")
    : `<p class="infra-empty">Nenhuma interrupção registrada no período.</p>`);

  if (st.technical && st.watch?.scopes) {
    partes.push(`<div class="infra-foot">Vigiando ${st.watch.scopes} escopos ·
      ritmo recalculado em ${_infraWhen(st.watch.computed_at) || "—"}</div>`);
  }

  body.innerHTML = partes.join("");
}

const _INFRA_ESCOPO = {
  silent:  { txt: "Usina sem comunicação",              cls: "crit" },
  partial: { txt: "Parte dos equipamentos calou",       cls: "warn" },
  lag:     { txt: "Publicando com atraso",              cls: "warn" },
};

function _infraCard(g) {
  const aberto = g.open === true;
  const esc = _INFRA_ESCOPO[g.scope] || _INFRA_ESCOPO.silent;
  const sev = aberto ? esc.cls : "done";
  // started_at é quando a usina calou, não quando o detector percebeu — a
  // Acopiara caiu 26 h antes do detector existir e apareceria como "10 min".
  // O verbo muda com o escopo: "a usina parou" e "um tracker parou" não são a
  // mesma frase, e era exatamente essa confusão que chegava ao cliente.
  const verbo = g.scope === "lag" ? "atrasou desde" : "parou";
  const quando = aberto
    ? `${verbo} ${_infraWhen(g.started_at)} · há ${_infraDur(g.duration_s)}`
    : `${_infraWhen(g.started_at)} → ${_infraWhen(g.closed_at)} · ${_infraDur(g.duration_s)} fora`;

  // Detector que ainda não estava de pé é informação de operação: sem esta
  // linha o incidente parece ter sido acompanhado desde o primeiro minuto.
  let atraso = "";
  if (g.detected_at && g.started_at) {
    const gap = (_tkDate(g.detected_at) - _tkDate(g.started_at)) / 1000;
    if (gap > 1800) {
      atraso = `<div class="infra-src">Detectado só ${_infraWhen(g.detected_at)} ·
                  ${_infraDur(gap)} depois do início</div>`;
    }
  }

  // Nome de tabela só para a equipe: é o que aponta para o CLP. O cliente vê a
  // usina e o tempo, sem `raw_thermalrelay` nenhum (mesma regra do push).
  const fontes = Array.isArray(g.sources) && g.sources.length
    ? `<div class="infra-src">${g.sources.length === 1 ? "Fonte" : `${g.sources.length} fontes`}:
         ${_notifEsc(g.sources.join(", "))}</div>`
    : "";

  const link = g.plant_id
    ? `<a class="infra-link" href="plant.html?plant_id=${encodeURIComponent(g.plant_id)}">
         Abrir usina <i class="fa-solid fa-arrow-right"></i></a>`
    : "";

  const escopo = aberto
    ? `<div class="infra-scope ${esc.cls}">${esc.txt}</div>`
    : "";

  return `<div class="infra-card ${sev}">
      <div class="infra-card-top">
        <span class="infra-plant">${_notifEsc(g.plant_name || `Usina ${g.plant_id}`)}</span>
        <span class="infra-when">${quando}</span>
      </div>
      ${escopo}
      ${fontes}
      ${atraso}
      ${link}
    </div>`;
}

document.addEventListener("DOMContentLoaded", () => {
  setTimeout(initPlatformUpdates, 1500);
  setTimeout(_tkStartPolling, 2000);
});