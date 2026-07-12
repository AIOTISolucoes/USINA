// =============================================================================
// OS.JS - AIOTI Kanban v3
// =============================================================================

const COLUMNS = [
  { status: "pendente", color: "#f59e0b" },
  { status: "em_processo", color: "#3b82f6" },
  { status: "em_verificacao", color: "#a855f7" },
  { status: "concluida", color: "#2aff7b" }
]

const COLUMN_STATUSES = new Set(COLUMNS.map((item) => item.status))
const STATUS_LABELS = {
  pendente: "Tarefa pendente",
  em_processo: "Em processo",
  em_verificacao: "Em verifica\u00e7\u00e3o",
  concluida: "Conclu\u00edda",
  cancelada: "Cancelada"
}

const STATUS_CREATE_TOAST = {
  pendente: "Uma nova Tarefa Pendente foi gerada",
  em_processo: "Uma nova OS em Processo foi gerada",
  em_verificacao: "Uma nova OS em Verifica\u00e7\u00e3o foi gerada",
  concluida: "Uma nova OS Conclu\u00edda foi gerada"
}

const TASK_STATUS_LABELS = {
  nao_iniciada: "N\u00e3o iniciada",
  em_andamento: "Em andamento",
  em_verificacao: "Em verifica\u00e7\u00e3o",
  concluida: "Conclu\u00edda",
  cancelada: "Cancelada"
}

const WEEK_DAY_COLUMNS = [
  { label: "Segunda-feira", aliases: ["segunda-feira", "segunda feira", "segunda", "seg", "mon", "monday"] },
  { label: "Ter\u00e7a-feira", aliases: ["terca-feira", "terca feira", "terca", "ter", "tue", "tuesday"] },
  { label: "Quarta-feira", aliases: ["quarta-feira", "quarta feira", "quarta", "qua", "wed", "wednesday"] },
  { label: "Quinta-feira", aliases: ["quinta-feira", "quinta feira", "quinta", "qui", "thu", "thursday"] },
  { label: "Sexta-feira", aliases: ["sexta-feira", "sexta feira", "sexta", "sex", "fri", "friday"] },
  { label: "S\u00e1bado", aliases: ["sabado", "sabado-feira", "sab", "sat", "saturday"] }
]

const KB = {
  currentOs: null,
  currentOsDetail: null,
  currentTask: null,
  currentTaskTab: "task",
  currentTaskFilter: "all",
  currentStep: 1,
  assetFilters: {},
  assetSearch: "",
  selectedAsset: null,
  subtasks: [],
  resources: [],
  failedChecked: false,
  serviceChecked: false,
  alreadyDoneChecked: false,
  selectedResponsavel: null,
  respUsers: [],
  _searchTimer: null,
  _assetTimer: null,
  _respTimer: null,
  _autoRefresh: null,
  _statusTarget: null,
  workOrdersByStatus: {},
  workOrderCache: new Map(),
  detailRequestSeq: 0,
  drag: {
    id: null,
    fromStatus: null,
    workOrder: null,
    ignoreClickUntil: 0
  },
  taskRegisterOpen: false,
  taskRegisterDraft: { duration: "", note: "" },
  pendingAttachment: null,
  attachmentsLoading: false,
  wizardPendingAttachments: [],
  opts: null,
  classifications: null,
  osNotes: { workOrderId: null, loading: false, items: [], error: null },
  osNoteDrafts: { comment: "", feedback: "", feedbackRating: 0 }
}

const OS_RATING_LABELS = { 1: "Muito Ruim", 2: "Ruim", 3: "MÃ©dio", 4: "Bom", 5: "Ã“timo" }

document.addEventListener("DOMContentLoaded", () => {
  initUser()
  bindTopbar()
  bindBoardDragAndDrop()
  bindFab()
  bindWizard()
  bindAssetDrawer()
  bindRespDrawer()
  bindDetailDrawer()
  bindTaskModalShell()
  bindStatusPopover()
  loadAllColumns()
  loadWizardOptions()
  document.getElementById("delModalCancel")?.addEventListener("click", closeDeleteModal)
  document.getElementById("delModalOverlay")?.addEventListener("click", closeDeleteModal)
  document.getElementById("delModalConfirm")?.addEventListener("click", confirmDeleteWorkOrder)
  document.getElementById("delModalPass")?.addEventListener("keydown", (e) => { if (e.key === "Enter") confirmDeleteWorkOrder() })

  KB._autoRefresh = setInterval(() => loadAllColumns({ silent: true }), 45000)

  setInterval(() => {
    document.querySelectorAll(".kb-card[data-status='em_processo'], .kb-card[data-status='em_verificacao']").forEach((card) => {
      const id = card.dataset.id
      const wo = KB.workOrderCache?.get(id)
      if (!wo) return
      const pct = getCardProgressValue(wo)
      const fill = card.querySelector(".kb-progress-fill")
      const pctEl = card.querySelector(".kb-progress-pct")
      if (fill) fill.style.width = pct + "%"
      if (pctEl) pctEl.textContent = pct + "%"
      const elapsed = getElapsedSeconds(wo)
      const clockEl = card.querySelector(".kb-elapsed")
      if (clockEl) clockEl.textContent = formatElapsed(elapsed)
    })
  }, 30000)
})

// =============================================================================
// Wizard options (dynamic from API)
// =============================================================================

async function loadWizardOptions() {
  function populate(selectId, names) {
    const el = document.getElementById(selectId)
    if (!el) return
    // clear all options except the first placeholder ("Selecionar...")
    while (el.options.length > 1) el.remove(1)
    const seen = new Set()
    names.forEach((name) => {
      if (seen.has(name)) return
      seen.add(name)
      const opt = document.createElement("option")
      opt.value = name
      opt.textContent = name
      el.appendChild(opt)
    })
  }

  try {
    const [opts, classifications] = await Promise.all([
      apiJson("/os-options"),
      apiJson("/os-classifications"),
    ])

    KB.opts = opts
    KB.classifications = classifications

    populate("f-task-type", (opts.task_type || []).map((i) => i.name))
    populate("f-criticality", (opts.criticality || []).map((i) => i.name))

    const level1 = (classifications.items || []).filter((i) => i.level === 1 || !i.level)
    populate("f-class1", level1.map((i) => i.name))

    const level2Names = level1.flatMap((i) => (i.children || []).map((c) => c.name))
    populate("f-class2", level2Names)
  } catch (_err) {
    console.warn("[loadWizardOptions] falha ao carregar opÃ§Ãµes:", _err?.message || _err)
  }
}

// =============================================================================
// User / theme
// =============================================================================

function initUser() {
  const user = JSON.parse(localStorage.getItem("user") || "{}")
  const name = user.name || user.username || "?"
  const initials = avatarInitials(name)
  const userAvatar = document.getElementById("userAvatar")
  const userNameDisplay = document.getElementById("userNameDisplay")
  const requestedBy = document.getElementById("f-requested-by")

  if (userAvatar) userAvatar.textContent = initials
  if (userNameDisplay) userNameDisplay.textContent = name
  if (requestedBy) requestedBy.value = name

  document.getElementById("logoutBtn")?.addEventListener("click", () => {
    if (typeof logout === "function") logout()
    else {
      localStorage.clear()
      window.location = "index.html"
    }
  })
}

// =============================================================================
// Access control
// =============================================================================

function getCurrentUser() {
  return JSON.parse(localStorage.getItem("user") || "{}")
}

function isSuperuser() {
  return getCurrentUser().is_superuser === true
}

// =============================================================================
// API helpers
// =============================================================================

async function readJsonResponse(res) {
  const text = await res.text()
  if (!text) return null
  try {
    return JSON.parse(text)
  } catch (_error) {
    return { raw: text }
  }
}

async function apiJson(path, options = {}) {
  const res = await apiFetch(path, options)
  const data = await readJsonResponse(res)

  if (!res.ok) {
    const message =
      (data && typeof data === "object" && (data.error || data.message || data.detail)) ||
      `HTTP ${res.status}`
    const err = new Error(message)
    err.status = res.status
    err.data = data
    throw err
  }

  return data
}

function apiUserHeaders(extra = {}) {
  const user = JSON.parse(localStorage.getItem("user") || "{}")
  const headers = { ...extra }
  if (user.customer_id) headers["X-Customer-Id"] = user.customer_id
  if (user.is_superuser === true) headers["X-Is-Superuser"] = "true"
  if (user.username) headers["X-Username"] = user.username
  return headers
}

function getApiBase() {
  return typeof API_BASE === "string" ? API_BASE : ""
}

// =============================================================================
// Topbar / board
// =============================================================================

function bindTopbar() {
  document.getElementById("kbSearchInput")?.addEventListener("input", () => {
    clearTimeout(KB._searchTimer)
    KB._searchTimer = setTimeout(() => loadAllColumns(), 350)
  })

  document.getElementById("kbRefreshBtn")?.addEventListener("click", () => loadAllColumns())

  document.querySelectorAll(".kb-col-refresh").forEach((btn) => {
    btn.addEventListener("click", () => loadColumn(btn.dataset.col))
  })
}

function bindBoardDragAndDrop() {
  document.querySelectorAll(".kb-col-body").forEach((body) => {
    body.addEventListener("dragover", handleColumnDragOver)
    body.addEventListener("dragenter", handleColumnDragOver)
    body.addEventListener("drop", handleColumnDrop)
  })
}

async function loadAllColumns({ silent = false } = {}) {
  const q = getBoardSearch()
  await Promise.all(COLUMNS.map((col) => loadColumn(col.status, { silent, q })))
  loadWorkOrderSummary()
}

async function loadWorkOrderSummary() {
  try {
    const data = await apiJson("/work-orders/summary")
    const s = data.by_status || {}
    const p = data.planning || {}
    const r = data.reliability || {}
    const cats = data.categories || []

    const set = (id, val) => {
      const el = document.getElementById(id)
      if (el) el.textContent = val ?? "â€”"
    }

    set("osKpiTotal",      s.total        ?? "â€”")
    set("osKpiPendentes",  s.pendente     ?? "â€”")
    set("osKpiEmProcesso", s.em_processo  ?? "â€”")
    set("osKpiAtrasadas",  p.atrasadas    ?? "â€”")
    set("osKpiParadas",    p.paradas      ?? "â€”")
    set("osKpiConcluidas", s.concluida    ?? "â€”")
    set("osKpiMtbf",       r.mtbf_hours != null ? r.mtbf_hours.toFixed(1) + "h" : "â€”")
    set("osKpiMttr",       r.mttr_hours != null ? r.mttr_hours.toFixed(1) + "h" : "â€”")
    set("osKpiCustoTotal", formatCurrencyBRL(p.custo_total || 0))

    const breakdown = document.getElementById("osCategoryBreakdown")
    if (breakdown) {
      breakdown.innerHTML = cats.length ? cats.map((c) => `
        <div class="os-category-chip">
          <span class="os-cat-name">${esc(c.categoria)}</span>
          <span class="os-cat-total">${c.total}</span>
          <span class="os-cat-detail">${c.em_processo} em processo Â· ${c.paradas} paradas</span>
        </div>
      `).join("") : ""
    }
  } catch (e) {
    console.warn("[summary]", e)
  }
}

async function refreshColumns(statuses, { silent = true } = {}) {
  const q = getBoardSearch()
  const targets = [...new Set((statuses || []).filter((status) => COLUMN_STATUSES.has(status)))]
  if (!targets.length) return
  await Promise.all(targets.map((status) => loadColumn(status, { silent, q })))
}

async function loadColumn(status, { silent = false, q = "" } = {}) {
  const col = document.getElementById(`col-${status}`)
  const loading = document.getElementById(`loading-${status}`)
  const countEl = document.getElementById(`count-${status}`)
  if (!col) return

  if (!silent) {
    loading?.classList.remove("hidden")
    col.innerHTML = ""
    if (loading) col.appendChild(loading)
  }

  const params = new URLSearchParams({ status })
  if (q) params.set("q", q)

  try {
    const data = await apiJson(`/work-orders?${params.toString()}`)
    const items = normalizeWorkOrderList(data)
    KB.workOrdersByStatus[status] = items
    items.forEach(cacheWorkOrder)
    if (countEl) countEl.textContent = String(items.length)
    renderColumn(status, items)
  } catch (error) {
    console.error("[KB] load column", status, error)
    if (!silent) {
      col.innerHTML =
        '<div class="kb-empty"><i class="fa-solid fa-triangle-exclamation"></i><span>Erro ao carregar</span></div>'
    }
  } finally {
    loading?.classList.add("hidden")
  }
}

function renderColumn(status, items) {
  const col = document.getElementById(`col-${status}`)
  if (!col) return

  col.innerHTML = ""
  if (!items.length) {
    col.innerHTML =
      '<div class="kb-empty"><i class="fa-regular fa-rectangle-list"></i><span>Nenhuma OS aqui</span></div>'
    return
  }

  items.forEach((os) => col.appendChild(buildCard(os)))
}

function buildCard(workOrder) {
  const card = document.createElement("div")
  const progress = getCardProgressValue(workOrder)
  const status = workOrder.status || "pendente"
  const assignee = workOrder.responsavel_name || workOrder.assignee_name || workOrder.requested_by || "Sem responsavel"
  const scheduledDate = workOrder.scheduled_date || null
  const assetCode = workOrder.asset_code || workOrder.asset_location || ""
  const detailEnabled = canOpenDetailFromCard(workOrder)

  const isAdmin = isSuperuser()
  const currentUser = getCurrentUser()
  const canDelete = currentUser?.permissions?.os_delete === true || isAdmin
  card.className = "kb-card"
  card.style.setProperty("--card-accent", getStatusColor(status))
  card.dataset.id = String(getWorkOrderId(workOrder) || "")
  card.dataset.status = status
  card.dataset.detailEnabled = String(detailEnabled)
  card.setAttribute("draggable", "true")

  const priorityLabel = workOrder.criticality || workOrder.criticality_name || ""
  const priorityClass = {
    "baixa": "kb-priority--low",
    "media": "kb-priority--mid", "mÃ©dio": "kb-priority--mid", "medio": "kb-priority--mid",
    "alta": "kb-priority--high",
    "critica": "kb-priority--crit", "crÃ­tica": "kb-priority--crit",
    "muito alta": "kb-priority--crit", "muito_alta": "kb-priority--crit",
  }[(priorityLabel || "").toLowerCase()] || "kb-priority--mid"
  const categoryText = workOrder.classification_2_name || workOrder.classification_1_name || workOrder.task_type_name || ""

  card.innerHTML = `
    <div class="kb-card-stripe"></div>
    <div class="kb-card-inner">
      <div class="kb-card-row1">
        <button type="button" class="kb-card-os-num" data-open-detail>OS #${esc(workOrder.os_number || workOrder.id || "?")}</button>
        ${priorityLabel ? `<span class="kb-priority ${priorityClass}">${esc(priorityLabel)}</span>` : ""}
        ${!scheduledDate ? `<span class="kb-badge-unplanned">N\u00e3o planejado</span>` : ""}
        ${status === "cancelada" ? `<span class="kb-badge-cancelled">Cancelado</span>` : ""}
      </div>
      <div class="kb-card-title">${esc(workOrder.task_description || workOrder.title || "Sem descri\u00e7\u00e3o")}</div>
      <div class="kb-card-asset-row">
        <i class="fa-solid fa-microchip"></i>
        <span>${esc(workOrder.asset_name || "Ativo n\u00e3o informado")}${assetCode ? ` &middot; <code>${esc(assetCode)}</code>` : ""}</span>
      </div>
      ${categoryText ? `<div class="kb-card-category"><i class="fa-solid fa-tag"></i>${esc(categoryText)}</div>` : ""}
      <div class="kb-card-progress">
        <div class="kb-progress-track">
          <div class="kb-progress-fill ${isOverdue(workOrder) ? "kb-progress-fill--overdue" : ""}" style="width:${progress}%"></div>
        </div>
        <span class="kb-progress-pct">${progress}%</span>
      </div>
      <div class="kb-card-meta">
        <span class="kb-meta-chip">
          <i class="fa-regular fa-clock"></i>${esc(formatDurationCompact(workOrder.estimated_duration || workOrder.estimated_duration_minutes))}
        </span>
        <span class="kb-meta-chip ${isOverdue(workOrder) ? "kb-meta-chip--overdue" : ""}">
          <i class="fa-regular fa-calendar"></i>${scheduledDate ? fmtDate(scheduledDate) : "Sem data"}
        </span>
        ${workOrder.status === "em_processo" ? `<span class="kb-meta-chip kb-meta-chip--elapsed kb-elapsed" data-wo-id="${esc(String(getWorkOrderId(workOrder)))}"><i class="fa-solid fa-stopwatch"></i>${formatElapsed(getElapsedSeconds(workOrder))}</span>` : ""}
      </div>
      <div class="kb-card-footer">
        <div class="kb-card-assignee">
          <div class="kb-assignee-av" title="${esc(assignee)}">${avatarInitials(assignee)}</div>
          <span class="kb-assignee-name">${esc((assignee || "").split(" ")[0])}</span>
        </div>
        <div class="kb-card-actions">
          ${canDelete ? `<button type="button" class="kb-card-action-btn kb-card-action-btn--delete" data-action="delete" title="Excluir OS"><i class="fa-solid fa-trash"></i></button>` : ""}
          <button type="button" class="kb-card-action-btn" data-action="menu" title="OpÃ§Ãµes"><i class="fa-solid fa-ellipsis-vertical"></i></button>
        </div>
      </div>
    </div>
  `

  card.addEventListener("click", (event) => {
    if (Date.now() < KB.drag.ignoreClickUntil) return
    if (event.target.closest("[data-action]")) return
    if (!detailEnabled) return
    openDetail(workOrder)
  })

  card.querySelector("[data-open-detail]")?.addEventListener("click", (event) => {
    event.preventDefault()
    event.stopPropagation()
    openDetail(workOrder)
  })

  card.querySelector('[data-action="share"]')?.addEventListener("click", async (event) => {
    event.stopPropagation()
    await shareWorkOrder(workOrder)
  })

  card.querySelector('[data-action="menu"]')?.addEventListener("click", (event) => {
    event.stopPropagation()
    openStatusPopover(event.currentTarget, workOrder)
  })

  card.querySelector('[data-action="delete"]')?.addEventListener("click", (event) => {
    event.stopPropagation()
    KB.currentOsDetail = workOrder
    openDeleteModal()
  })

  card.addEventListener("dragstart", (event) => handleCardDragStart(event, workOrder, card))
  card.addEventListener("dragend", () => handleCardDragEnd(card))

  return card
}

async function shareWorkOrder(workOrder) {
  const title = `OS #${workOrder.os_number || workOrder.id || "-"}`
  const text = `${title} - ${workOrder.task_description || workOrder.asset_name || "Sem descricao"}`

  if (navigator.share) {
    try {
      await navigator.share({ title, text })
      showToast("Detalhes da OS compartilhados", "success")
      return
    } catch (_error) {}
  }

  try {
    await navigator.clipboard.writeText(text)
    showToast("Detalhes da OS copiados", "success")
  } catch (_error) {
    showToast("Nao foi possivel compartilhar a OS", "error")
  }
}

function handleCardDragStart(event, workOrder, card) {
  if (!getWorkOrderId(workOrder)) {
    event.preventDefault()
    return
  }
  const id = getWorkOrderId(workOrder)
  if (!id) {
    event.preventDefault()
    return
  }

  KB.drag.id = id
  KB.drag.fromStatus = workOrder.status || null
  KB.drag.workOrder = workOrder

  card.classList.add("kb-card--dragging")
  event.dataTransfer.effectAllowed = "move"
  event.dataTransfer.setData("text/plain", String(id))
}

function handleCardDragEnd(card) {
  card.classList.remove("kb-card--dragging")
  KB.drag.ignoreClickUntil = Date.now() + 150
  clearDropTargets()
  KB.drag.id = null
  KB.drag.fromStatus = null
  KB.drag.workOrder = null
}

function handleColumnDragOver(event) {
  if (!KB.drag.id) return
  event.preventDefault()
  event.dataTransfer.dropEffect = "move"
  const status = event.currentTarget.closest(".kb-col")?.dataset.status
  highlightDropTarget(status)
}

async function handleColumnDrop(event) {
  event.preventDefault()
  const targetStatus = event.currentTarget.closest(".kb-col")?.dataset.status
  const { id, fromStatus, workOrder } = KB.drag
  clearDropTargets()

  if (!id || !targetStatus || !fromStatus || targetStatus === fromStatus) return

  await changeStatus(id, targetStatus, {
    fromStatus,
    workOrder,
    reloadStatuses: [fromStatus, targetStatus],
    successMessage: buildMoveToastMessage(workOrder, targetStatus)
  })
}

function highlightDropTarget(status) {
  document.querySelectorAll(".kb-col").forEach((col) => {
    col.classList.toggle("kb-col--drop-target", col.dataset.status === status)
  })
}

function clearDropTargets() {
  document.querySelectorAll(".kb-col").forEach((col) => col.classList.remove("kb-col--drop-target"))
}

function canOpenDetailFromCard(workOrder) {
  return !!getWorkOrderId(workOrder)
}

// =============================================================================
// Status popover
// =============================================================================

function bindStatusPopover() {
  document.getElementById("statusPopover")?.addEventListener("click", async (event) => {
    const button = event.target.closest(".sp-btn")
    if (!button || !KB._statusTarget) return

    hideStatusPopover()
    await changeStatus(getWorkOrderId(KB._statusTarget), button.dataset.status, {
      fromStatus: KB._statusTarget.status,
      workOrder: KB._statusTarget,
      reloadStatuses: [KB._statusTarget.status, button.dataset.status]
    })
  })
}

function openStatusPopover(anchor, workOrder) {
  const popover = document.getElementById("statusPopover")
  if (!popover || !workOrder) return

  const rect = anchor.getBoundingClientRect()
  popover.style.top = `${rect.bottom + 8}px`
  popover.style.right = `${Math.max(12, window.innerWidth - rect.right)}px`
  popover.classList.remove("hidden")

  KB._statusTarget = workOrder

  const hide = (event) => {
    if (!popover.contains(event.target)) {
      hideStatusPopover()
      document.removeEventListener("click", hide, true)
    }
  }

  setTimeout(() => document.addEventListener("click", hide, true), 10)
}

function hideStatusPopover() {
  document.getElementById("statusPopover")?.classList.add("hidden")
  KB._statusTarget = null
}

async function changeStatus(id, status, { fromStatus, workOrder, reloadStatuses, successMessage } = {}) {
  if (!id || !status) return

  try {
    await apiJson(`/work-orders/${id}/status`, {
      method: "PATCH",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ status })
    })

    if (KB.currentOsDetail && getWorkOrderId(KB.currentOsDetail) === id) {
      KB.currentOsDetail.status = status
      syncPrimaryTaskStatus(KB.currentOsDetail, status)
      renderOsDetail(KB.currentOsDetail)
    }

    showToast(successMessage || `Status atualizado para ${STATUS_LABELS[status] || status}`, "success")
    await refreshColumns(reloadStatuses || [fromStatus, status])
    loadWorkOrderSummary()

    if (KB.currentOsDetail && getWorkOrderId(KB.currentOsDetail) === id) {
      await refreshCurrentWorkOrderDetail({ preserveTaskId: KB.currentTask?.id })
    }
  } catch (error) {
    console.error("[KB] change status", error)
    showToast("Erro ao atualizar status", "error")
  }
}

function buildMoveToastMessage(workOrder, status) {
  const number = workOrder?.os_number || workOrder?.id || "-"
  return `OS #${number} movida para ${STATUS_LABELS[status] || status}`
}

// =============================================================================
// Detail drawer
// =============================================================================

function bindDetailDrawer() {
  document.getElementById("osdCloseBtn")?.addEventListener("click", closeDetail)
  document.getElementById("osdBackBtn")?.addEventListener("click", closeDetail)
  document.getElementById("osDetailOverlay")?.addEventListener("click", closeDetail)
  document.getElementById("osdStatusBtn")?.addEventListener("click", (event) => {
    if (KB.currentOsDetail) openStatusPopover(event.currentTarget, KB.currentOsDetail)
  })
  document.getElementById("osdSaveBtn")?.addEventListener("click", saveCurrentWorkOrderDetail)
  document.getElementById("osdDeleteBtn")?.addEventListener("click", openDeleteModal)
}

async function openDetail(workOrder) {
  if (!workOrder) return

  const normalizedWorkOrder = normalizeWorkOrderDetail(workOrder)
  const workOrderId = getWorkOrderId(normalizedWorkOrder)
  const requestSeq = ++KB.detailRequestSeq

  KB.currentOs = normalizedWorkOrder
  KB.currentOsDetail = normalizedWorkOrder
  cacheWorkOrder(normalizedWorkOrder)

  document.getElementById("osDetailOverlay")?.classList.remove("hidden")
  document.getElementById("osDetailDrawer")?.classList.remove("hidden")

  const osdStatusBtn = document.getElementById("osdStatusBtn")
  const osdSaveBtn = document.getElementById("osdSaveBtn")
  if (osdStatusBtn) osdStatusBtn.style.display = ""
  if (osdSaveBtn) osdSaveBtn.style.display = ""
  const currentUser = getCurrentUser()
  const canDelete = currentUser?.permissions?.os_delete === true || currentUser?.is_superuser === true
  const osdDeleteBtn = document.getElementById("osdDeleteBtn")
  if (osdDeleteBtn) osdDeleteBtn.style.display = canDelete ? "" : "none"

  if (workOrderId) renderOsDetail(KB.currentOsDetail)
  else renderOsDetailLoading(KB.currentOsDetail)

  try {
    const detail = await fetchWorkOrderDetail(workOrderId, normalizedWorkOrder)
    if (requestSeq !== KB.detailRequestSeq) return

    const normalizedDetail = normalizeWorkOrderDetail(detail || normalizedWorkOrder)
    if (getWorkOrderId(normalizedDetail) !== getWorkOrderId(KB.currentOsDetail)) return

    KB.currentOsDetail = normalizedDetail
    cacheWorkOrder(KB.currentOsDetail)
    renderOsDetail(KB.currentOsDetail)
  } catch (error) {
    if (requestSeq !== KB.detailRequestSeq) return
    console.warn("[OS] detail fallback", error)
    renderOsDetail(KB.currentOsDetail)
  }
}

function closeDetail() {
  closeTaskModal()
  KB.detailRequestSeq += 1
  document.getElementById("osDetailOverlay")?.classList.add("hidden")
  document.getElementById("osDetailDrawer")?.classList.add("hidden")
  document.getElementById("osdBody").innerHTML = ""
  KB.currentOs = null
  KB.currentOsDetail = null
  KB.osNotes = { workOrderId: null, loading: false, items: [], error: null }
  KB.osNoteDrafts = { comment: "", feedback: "", feedbackRating: 0 }
}

function openDeleteModal() {
  if (!KB.currentOsDetail) return
  const id = KB.currentOsDetail.os_number || KB.currentOsDetail.id
  const el = document.getElementById("delModalOsId")
  if (el) el.textContent = `OS #${id}`
  const user = getCurrentUser()
  const userInput = document.getElementById("delModalUser")
  if (userInput) userInput.value = user.username || ""
  document.getElementById("delModalPass") && (document.getElementById("delModalPass").value = "")
  document.getElementById("delModalError")?.classList.add("hidden")
  document.getElementById("delModalOverlay")?.classList.remove("hidden")
  document.getElementById("delModal")?.classList.remove("hidden")
  setTimeout(() => document.getElementById("delModalPass")?.focus(), 100)
}

function closeDeleteModal() {
  document.getElementById("delModalOverlay")?.classList.add("hidden")
  document.getElementById("delModal")?.classList.add("hidden")
}

async function confirmDeleteWorkOrder() {
  const username = document.getElementById("delModalUser")?.value?.trim()
  const password = document.getElementById("delModalPass")?.value
  const confirmBtn = document.getElementById("delModalConfirm")
  const errorEl = document.getElementById("delModalError")
  const errorText = document.getElementById("delModalErrorText")

  if (!username || !password) {
    errorEl?.classList.remove("hidden")
    if (errorText) errorText.textContent = "Preencha usuÃ¡rio e senha"
    return
  }

  if (confirmBtn) { confirmBtn.disabled = true; confirmBtn.innerHTML = '<i class="fa-solid fa-spinner fa-spin"></i> Verificando...' }
  errorEl?.classList.add("hidden")

  try {
    const authRes = await apiFetch("/auth/login", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ username, password })
    })
    const authData = await authRes.json()
    if (!authData.ok && !authData.token && !authData.access_token) {
      errorEl?.classList.remove("hidden")
      if (errorText) errorText.textContent = "Credenciais invÃ¡lidas"
      return
    }

    const id = getWorkOrderId(KB.currentOsDetail)
    const fromStatus = KB.currentOsDetail.status
    const currentUser = getCurrentUser()
    await apiJson(`/work-orders/${id}`, {
      method: "DELETE",
      headers: { "X-Username": currentUser.username || "" }
    })

    closeDeleteModal()
    closeDetail()
    showToast("OS excluÃ­da com sucesso", "success")
    await refreshColumns([fromStatus])
    loadWorkOrderSummary()
  } catch (err) {
    errorEl?.classList.remove("hidden")
    if (errorText) errorText.textContent = err.message || "Erro ao excluir"
  } finally {
    if (confirmBtn) { confirmBtn.disabled = false; confirmBtn.innerHTML = '<i class="fa-solid fa-trash"></i> Confirmar exclusÃ£o' }
  }
}

function renderOsDetailLoading(workOrder) {
  const number = workOrder?.os_number || workOrder?.id || "-"
  const osdNum = document.getElementById("osdNum")
  if (osdNum) {
    osdNum.innerHTML = `<span class="osd-num-kicker">Ordem de Servi\u00e7o</span><span class="osd-num-value">#${esc(number)}</span>`
  }
  document.getElementById("osdBody").innerHTML =
    '<div class="osd-panel"><div class="kb-loading"><span class="kb-spinner"></span></div></div>'
}

function renderOsDetail(detail) {
  if (!detail) return

  const osdNum = document.getElementById("osdNum")
  const body = document.getElementById("osdBody")
  if (!body || !osdNum) return

  const tasks = getFilteredTasks(detail)
  const taskTotal = (detail.tasks || []).length
  const progress = getProgressValue(detail)
  const responsible = detail.responsavel_name || detail.assignee_name || detail.requested_by || "Sem responsavel"
  const responsibleMeta = detail.responsavel_email || detail.created_by || detail.requested_by || "Sem informa\u00e7\u00e3o adicional"
  const scheduledDate = detail.scheduled_date || detail.incident_date || null

  osdNum.innerHTML = `<span class="osd-num-kicker">Ordem de Servi\u00e7o</span><span class="osd-num-value">#${esc(detail.os_number || detail.id || "-")}</span>`

  body.innerHTML = `
    <section class="osd-panel">
      <div class="osd-person-row">
        <div class="osd-person">
          <div class="osd-avatar">${avatarInitials(responsible)}</div>
          <div class="osd-person-copy">
            <div class="osd-person-name">${esc(responsible)} <span class="osd-person-caret"><i class="fa-solid fa-chevron-down"></i></span></div>
            <div class="osd-person-sub">${esc(responsibleMeta)}</div>
          </div>
        </div>
        <div class="osd-person-right">#${esc(detail.os_number || detail.id || "-")}</div>
      </div>
      <div class="osd-meta-strip">
        <div class="osd-meta-item ${isOverdue(detail) ? "overdue" : ""}"><i class="fa-regular fa-calendar"></i>${esc(scheduledDate ? fmtDateLong(scheduledDate) : "Sem data programada")}</div>
        <div class="osd-meta-item"><i class="fa-regular fa-clock"></i>${esc(formatDurationCompact(detail.estimated_duration))}</div>
        <span class="osd-status-pill ${esc(detail.status || "pendente")}">${esc(STATUS_LABELS[detail.status] || detail.status || "Pendente")}</span>
      </div>
      <div class="osd-progress-head">
        <div>
          <div class="osd-progress-label">Progresso</div>
          <div class="osd-progress-numbers">
            <span class="osd-progress-value">${progress}%</span>
            ${isSuperuser()
              ? `<input id="osdTotalCost" class="osd-cost-input" type="number" min="0" step="0.01" value="${(detail.total_cost || detail.cost_total || 0).toFixed(2)}" placeholder="0,00" />`
              : `<span class="osd-progress-cost">Custo total: ${esc(formatCurrencyBRL(detail.total_cost || detail.cost_total || 0))}</span>`
            }
          </div>
        </div>
      </div>
      <div class="osd-progress-shell"><div class="osd-progress-fill" style="width:${progress}%"></div></div>
    </section>

    <section class="osd-section-card">
      <div class="osd-section-title">Datas</div>
      <div class="osd-field-grid">
        <div class="osd-field">
          <label for="osdScheduledDate">Data programada</label>
          <input id="osdScheduledDate" class="osd-input" type="datetime-local" value="${esc(toDatetimeLocalInputValue(detail.scheduled_date))}" />
        </div>
        <div class="osd-field">
          <label for="osdStartDate">Data inicial</label>
          <input id="osdStartDate" class="osd-input" type="datetime-local" value="${esc(toDatetimeLocalInputValue(detail.start_date))}" />
        </div>
        <div class="osd-field">
          <label for="osdEndDate">Data final</label>
          <input id="osdEndDate" class="osd-input" type="datetime-local" value="${esc(toDatetimeLocalInputValue(detail.end_date))}" />
        </div>
      </div>
    </section>

    <section class="osd-section-card">
      <div class="osd-section-title">Observa\u00e7\u00e3o</div>
      <textarea id="osdObservation" class="osd-textarea" placeholder="Digite observa\u00e7\u00f5es da ordem de servi\u00e7o...">${esc(detail.observations || "")}</textarea>
    </section>

    <section class="osd-section-card">
      <div class="osd-section-title">Classifica\u00e7\u00e3o</div>
      <div class="osd-field-grid">
        <div class="osd-field">
          <label for="osdTaskType">Tipo de tarefa</label>
          <select id="osdTaskType" class="osd-input">
            <option value="">Selecionar...</option>
            ${((KB.opts?.task_type || []).map((i) => i.name)).map((n) => `<option value="${esc(n)}"${detail.task_type === n ? " selected" : ""}>${esc(n)}</option>`).join("")}
          </select>
        </div>
        <div class="osd-field">
          <label for="osdCriticality">Criticidade</label>
          <select id="osdCriticality" class="osd-input">
            <option value="">Selecionar...</option>
            ${((KB.opts?.criticality || []).map((i) => i.name)).map((n) => `<option value="${esc(n)}"${detail.criticality === n ? " selected" : ""}>${esc(n)}</option>`).join("")}
          </select>
        </div>
        <div class="osd-field">
          <label for="osdClassification1">Classifica\u00e7\u00e3o 1</label>
          <select id="osdClassification1" class="osd-input">
            <option value="">Selecionar...</option>
            ${(((KB.classifications?.items || []).filter((i) => i.level === 1 || !i.level)).map((i) => i.name)).map((n) => `<option value="${esc(n)}"${detail.classification_1 === n ? " selected" : ""}>${esc(n)}</option>`).join("")}
          </select>
        </div>
        <div class="osd-field">
          <label for="osdClassification2">Classifica\u00e7\u00e3o 2</label>
          <select id="osdClassification2" class="osd-input">
            <option value="">Selecionar...</option>
            ${((KB.classifications?.items || []).filter((i) => i.level === 1 || !i.level).flatMap((i) => (i.children || []).map((c) => c.name))).map((n) => `<option value="${esc(n)}"${detail.classification_2 === n ? " selected" : ""}>${esc(n)}</option>`).join("")}
          </select>
        </div>
      </div>
    </section>

    <section class="osd-section-card">
      <div class="osd-section-head">
        <div>
          <div class="osd-section-title">Tarefas</div>
          <div class="osd-section-meta">Total: ${taskTotal}</div>
        </div>
        <select id="osdTaskFilter" class="task-select osd-filter">
          <option value="all" ${KB.currentTaskFilter === "all" ? "selected" : ""}>Todas</option>
          <option value="open" ${KB.currentTaskFilter === "open" ? "selected" : ""}>N\u00e3o iniciadas</option>
          <option value="running" ${KB.currentTaskFilter === "running" ? "selected" : ""}>Em andamento</option>
          <option value="done" ${KB.currentTaskFilter === "done" ? "selected" : ""}>Conclu\u00eddas</option>
        </select>
      </div>
      <div class="osd-task-list">
        ${tasks.length ? tasks.map((task) => renderOsDetailTaskCard(task)).join("") : '<div class="task-empty">Nenhuma tarefa encontrada para este filtro.</div>'}
      </div>
    </section>

    <section class="osd-section-card">
      <div class="osd-section-head">
        <div>
          <div class="osd-section-title">Feedback</div>
          <div class="osd-section-meta" id="osdFeedbackMeta">Retorno da execuÃ§Ã£o</div>
        </div>
      </div>
      <div id="osdFeedbackList" class="osd-note-list"></div>
      <div class="osd-rating" id="osdFeedbackRating">
        ${[1, 2, 3, 4, 5].map((n) => `<button type="button" class="osd-star ${KB.osNoteDrafts.feedbackRating >= n ? "active" : ""}" data-rating="${n}" title="${OS_RATING_LABELS[n]}"><i class="fa-solid fa-star"></i></button>`).join("")}
        <span class="osd-rating-label" id="osdFeedbackRatingLabel">${KB.osNoteDrafts.feedbackRating ? OS_RATING_LABELS[KB.osNoteDrafts.feedbackRating] : "Avalie a execuÃ§Ã£o"}</span>
      </div>
      <textarea id="osdFeedbackInput" class="osd-textarea osd-note-input" placeholder="Escreva o feedback da execuÃ§Ã£o...">${esc(KB.osNoteDrafts.feedback || "")}</textarea>
      <div class="osd-note-actions">
        <button type="button" id="osdFeedbackBtn" class="wz-btn-primary osd-note-btn"><i class="fa-solid fa-clipboard-check"></i> Registrar feedback</button>
      </div>
    </section>

    <section class="osd-section-card">
      <div class="osd-section-head">
        <div>
          <div class="osd-section-title">ComentÃ¡rios</div>
          <div class="osd-section-meta" id="osdCommentsMeta"></div>
        </div>
      </div>
      <div id="osdCommentList" class="osd-note-list"></div>
      <textarea id="osdCommentInput" class="osd-textarea osd-note-input" placeholder="Escreva um comentÃ¡rio...">${esc(KB.osNoteDrafts.comment || "")}</textarea>
      <div class="osd-note-actions">
        <button type="button" id="osdCommentBtn" class="wz-btn-primary osd-note-btn"><i class="fa-solid fa-comment"></i> Comentar</button>
      </div>
    </section>
  `

  document.getElementById("osdTaskFilter")?.addEventListener("change", (event) => {
    KB.currentTaskFilter = event.target.value
    renderOsDetail(detail)
  })

  body.querySelectorAll("[data-task-id]").forEach((element) => {
    element.addEventListener("click", () => {
      const taskId = element.dataset.taskId
      const task = findTaskById(detail, taskId)
      if (task) openTaskModal(task)
    })
  })

  document.getElementById("osdFeedbackBtn")?.addEventListener("click", () => submitWorkOrderNote("feedback"))
  document.getElementById("osdCommentBtn")?.addEventListener("click", () => submitWorkOrderNote("comment"))
  document.getElementById("osdFeedbackInput")?.addEventListener("input", (event) => { KB.osNoteDrafts.feedback = event.target.value })
  document.getElementById("osdCommentInput")?.addEventListener("input", (event) => { KB.osNoteDrafts.comment = event.target.value })
  document.querySelectorAll("#osdFeedbackRating .osd-star").forEach((btn) => {
    btn.addEventListener("click", () => {
      KB.osNoteDrafts.feedbackRating = Number(btn.dataset.rating) || 0
      updateFeedbackRatingWidget()
    })
  })
  loadWorkOrderNotes(detail)
}

function renderOsDetailTaskCard(task) {
  const taskStatus = normalizeTaskStatus(task.status)
  return `
    <button type="button" class="osd-task-card" data-task-id="${esc(task.id)}">
      <div class="osd-task-asset-row">
        <div>
          <div class="osd-task-title">${esc(task.asset_name || "Ativo")}${task.asset_code ? ` { ${esc(task.asset_code)} }` : ""}</div>
          <div class="osd-task-sub">${esc(task.asset_location ? `// ${task.asset_location}` : "// Sem local informado")}</div>
        </div>
        <span class="osd-task-arrow"><i class="fa-solid fa-chevron-right"></i></span>
      </div>
      <div class="osd-task-main-row">
        <div>
          <div class="osd-task-title">${esc(task.name || "Sem titulo")}</div>
          <div class="osd-task-meta">
            <span>Criticidade: ${esc(formatCriticality(task.criticality).label)}</span>
            <span>Tipo de tarefa: ${esc(task.task_type || "---")}</span>
            <span>Classifica\u00e7\u00e3o 1: ${esc(task.classification_1 || "---")}</span>
            <span>Classifica\u00e7\u00e3o 2: ${esc(task.classification_2 || "---")}</span>
            <span>N\u00famero de solicita\u00e7\u00e3o: ${esc(task.request_number || "---")}</span>
            <span>Data programada: ${esc(task.scheduled_date ? fmtDateLong(task.scheduled_date) : "---")}</span>
            <span>Dura\u00e7\u00e3o estimada: ${esc(formatDurationLong(task.estimated_duration))}</span>
          </div>
        </div>
      </div>
      <div class="osd-task-footer">
        <div class="osd-task-counts">Recursos ${task.resources.length} | Anexos ${task.attachments.length}</div>
        <span class="osd-task-status ${esc(taskStatus)}">${esc(TASK_STATUS_LABELS[taskStatus] || taskStatus)}</span>
      </div>
    </button>
  `
}

// =============================================================================
// Notas da OS (feedback + comentarios) â€” "OS".os_work_order_note via /work-orders/{id}/notes
// =============================================================================

async function loadWorkOrderNotes(detail) {
  const workOrderId = getWorkOrderId(detail)
  if (!workOrderId) return

  if (String(KB.osNotes.workOrderId) === String(workOrderId) && !KB.osNotes.error) {
    renderWorkOrderNotes()
    return
  }

  KB.osNotes = { workOrderId, loading: true, items: [], error: null }
  renderWorkOrderNotes()

  try {
    const data = await apiJson(`/work-orders/${workOrderId}/notes`)
    if (String(KB.osNotes.workOrderId) !== String(workOrderId)) return
    const items = Array.isArray(data?.items) ? data.items : []
    KB.osNotes = { workOrderId, loading: false, items, error: null }
  } catch (error) {
    console.warn("[OS] notes", error)
    if (String(KB.osNotes.workOrderId) !== String(workOrderId)) return
    KB.osNotes = { workOrderId, loading: false, items: [], error: error.message || "Erro ao carregar" }
  }
  renderWorkOrderNotes()
}

function renderWorkOrderNotes() {
  const feedbackList = document.getElementById("osdFeedbackList")
  const commentList = document.getElementById("osdCommentList")
  if (!feedbackList || !commentList) return
  if (KB.currentOsDetail && String(getWorkOrderId(KB.currentOsDetail)) !== String(KB.osNotes.workOrderId)) return

  if (KB.osNotes.loading) {
    const loading = '<div class="osd-note-empty"><span class="kb-spinner"></span></div>'
    feedbackList.innerHTML = loading
    commentList.innerHTML = loading
    return
  }

  if (KB.osNotes.error) {
    const err = `<div class="osd-note-empty">Erro ao carregar: ${esc(KB.osNotes.error)}</div>`
    feedbackList.innerHTML = err
    commentList.innerHTML = err
    return
  }

  const feedbacks = KB.osNotes.items.filter((note) => note.note_type === "feedback")
  const comments = KB.osNotes.items.filter((note) => note.note_type !== "feedback")

  feedbackList.innerHTML = feedbacks.length
    ? feedbacks.map(renderWorkOrderNoteItem).join("")
    : '<div class="osd-note-empty">Nenhum feedback registrado.</div>'
  commentList.innerHTML = comments.length
    ? comments.map(renderWorkOrderNoteItem).join("")
    : '<div class="osd-note-empty">Nenhum comentÃ¡rio ainda.</div>'

  const fbMeta = document.getElementById("osdFeedbackMeta")
  if (fbMeta) fbMeta.textContent = feedbacks.length ? `Total: ${feedbacks.length}` : "Retorno da execuÃ§Ã£o"
  const cmMeta = document.getElementById("osdCommentsMeta")
  if (cmMeta) cmMeta.textContent = comments.length ? `Total: ${comments.length}` : ""
}

function noteAuthorLabel(note) {
  if (note.author_name) return note.author_name
  const user = getCurrentUser()
  if (note.created_by_user_id && user?.id && String(note.created_by_user_id) === String(user.id)) {
    return user.username || "VocÃª"
  }
  if (note.created_by_user_id) return `UsuÃ¡rio #${note.created_by_user_id}`
  return "Sem autor"
}

function fmtNoteDateTime(value) {
  if (!value) return ""
  const date = new Date(value)
  if (Number.isNaN(date.getTime())) return String(value)
  return date.toLocaleString("pt-BR", { day: "2-digit", month: "2-digit", year: "numeric", hour: "2-digit", minute: "2-digit" })
}

function updateFeedbackRatingWidget() {
  const rating = KB.osNoteDrafts.feedbackRating || 0
  document.querySelectorAll("#osdFeedbackRating .osd-star").forEach((btn) => {
    btn.classList.toggle("active", Number(btn.dataset.rating) <= rating)
  })
  const label = document.getElementById("osdFeedbackRatingLabel")
  if (label) label.textContent = rating ? OS_RATING_LABELS[rating] : "Avalie a execuÃ§Ã£o"
}

function renderNoteStars(rating) {
  const value = Number(rating) || 0
  if (!value) return ""
  const stars = [1, 2, 3, 4, 5]
    .map((n) => `<i class="fa-solid fa-star ${n <= value ? "on" : ""}"></i>`)
    .join("")
  return `<div class="osd-note-stars">${stars}<span>${OS_RATING_LABELS[value] || ""}</span></div>`
}

function renderWorkOrderNoteItem(note) {
  const author = noteAuthorLabel(note)
  const isFeedback = note.note_type === "feedback"
  const user = getCurrentUser()
  const mine = !isFeedback && note.created_by_user_id && user?.id &&
    String(note.created_by_user_id) === String(user.id)
  return `
    <div class="osd-note-item ${isFeedback ? "feedback" : "chat"} ${mine ? "mine" : ""}">
      <div class="osd-note-head">
        <span class="osd-note-author"><span class="osd-note-avatar">${avatarInitials(author)}</span>${esc(author)}</span>
        <span class="osd-note-date">${esc(fmtNoteDateTime(note.created_at))}</span>
      </div>
      ${isFeedback ? renderNoteStars(note.rating) : ""}
      ${note.content ? `<div class="osd-note-text">${esc(note.content)}</div>` : ""}
    </div>
  `
}

async function submitWorkOrderNote(noteType) {
  if (!KB.currentOsDetail) return
  const workOrderId = getWorkOrderId(KB.currentOsDetail)
  if (!workOrderId) return

  const inputId = noteType === "feedback" ? "osdFeedbackInput" : "osdCommentInput"
  const btnId = noteType === "feedback" ? "osdFeedbackBtn" : "osdCommentBtn"
  const input = document.getElementById(inputId)
  const content = (input?.value || "").trim()
  const rating = noteType === "feedback" ? (KB.osNoteDrafts.feedbackRating || 0) : 0

  if (noteType === "feedback" && !rating) {
    showToast("Selecione uma avaliaÃ§Ã£o (estrelas) para o feedback", "error")
    return
  }
  if (noteType === "comment" && !content) {
    showToast("Escreva o comentÃ¡rio antes de enviar", "error")
    return
  }

  const btn = document.getElementById(btnId)
  const prevHtml = btn?.innerHTML
  if (btn) { btn.disabled = true; btn.innerHTML = '<i class="fa-solid fa-spinner fa-spin"></i> Enviando' }

  try {
    const user = getCurrentUser()
    await apiJson(`/work-orders/${workOrderId}/notes`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        content,
        note_type: noteType,
        created_by_user_id: user?.id || null,
        rating: rating || null
      })
    })

    if (input) input.value = ""
    if (noteType === "feedback") {
      KB.osNoteDrafts.feedback = ""
      KB.osNoteDrafts.feedbackRating = 0
      updateFeedbackRatingWidget()
    } else {
      KB.osNoteDrafts.comment = ""
    }
    showToast(noteType === "feedback" ? "Feedback registrado" : "ComentÃ¡rio adicionado", "success")

    KB.osNotes = { workOrderId: null, loading: false, items: [], error: null }
    await loadWorkOrderNotes(KB.currentOsDetail)
  } catch (error) {
    showToast(`Erro ao salvar: ${error.message}`, "error")
  } finally {
    if (btn) { btn.disabled = false; btn.innerHTML = prevHtml }
  }
}

async function saveCurrentWorkOrderDetail() {
  if (!KB.currentOsDetail) return

  const nextDetail = cloneWorkOrderDetail(KB.currentOsDetail)
  nextDetail.observations = document.getElementById("osdObservation")?.value?.trim() || ""
  nextDetail.scheduled_date = readDatetimeLocalValue("osdScheduledDate")
  nextDetail.start_date = readDatetimeLocalValue("osdStartDate")
  nextDetail.end_date = readDatetimeLocalValue("osdEndDate")
  const osdTaskType = document.getElementById("osdTaskType")?.value || ""
  const osdCriticality = document.getElementById("osdCriticality")?.value || ""
  const osdClassification1 = document.getElementById("osdClassification1")?.value || ""
  const osdClassification2 = document.getElementById("osdClassification2")?.value || ""
  if (osdTaskType) nextDetail.task_type = osdTaskType
  if (osdCriticality) nextDetail.criticality = osdCriticality
  nextDetail.classification_1 = osdClassification1
  nextDetail.classification_2 = osdClassification2
  const costInput = document.getElementById("osdTotalCost")
  if (costInput && isSuperuser()) {
    nextDetail.total_cost = parseFloat(costInput.value) || 0
  }
  if (nextDetail.tasks?.[0]) {
    if (osdTaskType) nextDetail.tasks[0].task_type = osdTaskType
    if (osdCriticality) nextDetail.tasks[0].criticality = osdCriticality
    nextDetail.tasks[0].classification_1 = osdClassification1
    nextDetail.tasks[0].classification_2 = osdClassification2
  }
  syncPrimaryTaskToDetail(nextDetail)

  await persistWorkOrderDetail(nextDetail, "OS atualizada com sucesso")
}

async function fetchWorkOrderDetail(id, fallback = null) {
  if (!id) return normalizeWorkOrderDetail(fallback)

  try {
    const data = await apiJson(`/work-orders/${id}`)
    return data?.item || data?.work_order || data
  } catch (error) {
    const cached = KB.workOrderCache.get(String(id))
    if (cached) return cached
    if (fallback) return fallback
    throw error
  }
}

async function refreshCurrentWorkOrderDetail({ preserveTaskId = null } = {}) {
  if (!KB.currentOsDetail) return

  const detail = await fetchWorkOrderDetail(getWorkOrderId(KB.currentOsDetail), KB.currentOsDetail)
  KB.currentOsDetail = normalizeWorkOrderDetail(detail)
  cacheWorkOrder(KB.currentOsDetail)
  renderOsDetail(KB.currentOsDetail)

  if (preserveTaskId && KB.currentTask) {
    const refreshedTask = findTaskById(KB.currentOsDetail, preserveTaskId)
    if (refreshedTask) {
      KB.currentTask = refreshedTask
      renderTaskModal()
    }
  }
}

async function persistWorkOrderDetail(nextDetail, successMessage) {
  const saveBtn = document.getElementById("osdSaveBtn")
  const taskId = KB.currentTask?.id || null
  const previousStatus = KB.currentOsDetail?.status || nextDetail.status
  const nextStatus = nextDetail.status || previousStatus

  if (saveBtn) {
    saveBtn.disabled = true
    saveBtn.innerHTML = '<i class="fa-solid fa-spinner fa-spin"></i> Salvando'
  }

  try {
    const payload = buildWorkOrderPatchPayload(nextDetail)
    const response = await apiJson(`/work-orders/${getWorkOrderId(nextDetail)}`, {
      method: "PATCH",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload)
    })

    const updated = normalizeWorkOrderDetail(response?.item || response?.work_order || { ...nextDetail, ...(response || {}) })
    KB.currentOs = updated
    KB.currentOsDetail = updated
    cacheWorkOrder(updated)

    renderOsDetail(updated)
    if (KB.currentTask) {
      KB.currentTask = findTaskById(updated, taskId) || updated.tasks?.[0] || null
      renderTaskModal()
    }

    showToast(successMessage || "OS atualizada", "success")
    await refreshColumns(previousStatus === nextStatus ? [nextStatus] : [previousStatus, nextStatus])
  } catch (error) {
    console.error("[OS] patch", error)
    showToast(`Erro ao salvar OS: ${error.message}`, "error")
  } finally {
    if (saveBtn) {
      saveBtn.disabled = false
      saveBtn.innerHTML = '<i class="fa-solid fa-floppy-disk"></i> Salvar'
    }
  }
}

// =============================================================================
// Task modal
// =============================================================================

function bindTaskModalShell() {
  document.getElementById("taskModalOverlay")?.addEventListener("click", closeTaskModal)
  document.getElementById("taskAttachmentInput")?.addEventListener("change", handleAttachmentFileSelected)
}

function openTaskModal(task) {
  if (!task || !KB.currentOsDetail) return

  KB.currentTask = task
  KB.currentTaskTab = "task"
  KB.taskRegisterOpen = false
  KB.taskRegisterDraft = { duration: "", note: "" }
  KB.pendingAttachment = null

  document.getElementById("taskModalOverlay")?.classList.remove("hidden")
  document.getElementById("taskModal")?.classList.remove("hidden")
  renderTaskModal()
}

function closeTaskModal() {
  document.getElementById("taskModalOverlay")?.classList.add("hidden")
  document.getElementById("taskModal")?.classList.add("hidden")
  document.getElementById("taskModalHeader").innerHTML = ""
  document.getElementById("taskModalBody").innerHTML = ""
  document.getElementById("taskModalFooter").innerHTML = ""
  KB.currentTask = null
  KB.currentTaskTab = "task"
  KB.taskRegisterOpen = false
  KB.taskRegisterDraft = { duration: "", note: "" }
  KB.pendingAttachment = null
  KB.attachmentsLoading = false
}

function renderTaskModal() {
  if (!KB.currentTask || !KB.currentOsDetail) return

  const task = KB.currentTask
  const header = document.getElementById("taskModalHeader")
  const body = document.getElementById("taskModalBody")
  const footer = document.getElementById("taskModalFooter")
  if (!header || !body || !footer) return

  header.innerHTML = `
    <div class="task-modal-head-left">
      <button class="asset-drawer-back" id="taskModalBackBtn"><i class="fa-solid fa-arrow-left"></i></button>
      <div class="task-modal-title">${esc(task.asset_name || "Ativo")}${task.asset_code ? ` { ${esc(task.asset_code)} }` : ""}</div>
    </div>
    <div class="task-modal-head-right">
      <button class="wz-btn-primary" id="taskModalSaveBtn"><i class="fa-solid fa-floppy-disk"></i></button>
    </div>
  `

  body.innerHTML = `
    <div class="task-tabs">
      ${renderTaskTabButton("task", "fa-solid fa-house", "Tarefa")}
      ${renderTaskTabButton("subtasks", "fa-solid fa-list", "SubTarefas")}
      ${renderTaskTabButton("resources", "fa-solid fa-screwdriver-wrench", "Recursos")}
      ${renderTaskTabButton("attachments", "fa-solid fa-paperclip", "Anexos")}
    </div>
    <div class="task-panel">
      ${renderTaskPanel(task)}
    </div>
  `

  footer.innerHTML = `
    ${KB.taskRegisterOpen ? renderTaskRegisterPanel() : ""}
    <div class="task-modal-footer-row">
      <button class="wz-btn-primary" id="taskStartBtn" ${task.status === "em_andamento" || task.status === "concluida" ? "disabled" : ""}>
        <i class="fa-solid fa-play"></i> ${task.status === "concluida" ? "Conclu\u00edda" : task.status === "em_andamento" ? "Em andamento" : "Come\u00e7ar"}
      </button>
      <button class="wz-btn-ghost" id="taskRegisterBtn"><i class="fa-solid fa-clipboard-list"></i> Registro</button>
    </div>
  `

  wireTaskModalInteractions()

  if (KB.currentTaskTab === "attachments") {
    if (!task.attachmentsLoaded && !KB.attachmentsLoading) {
      void loadTaskAttachments(task)
    } else {
      console.log("[ATT] skip - loaded:", task.attachmentsLoaded, "loading:", KB.attachmentsLoading)
    }
  }
}

function renderTaskTabButton(tab, icon, label) {
  return `
    <button type="button" class="task-tab ${KB.currentTaskTab === tab ? "active" : ""}" data-tab="${esc(tab)}">
      <i class="${esc(icon)}"></i>${esc(label)}
    </button>
  `
}

function renderTaskPanel(task) {
  if (KB.currentTaskTab === "subtasks") return renderTaskSubtasksPanel(task)
  if (KB.currentTaskTab === "resources") return renderTaskResourcesPanel(task)
  if (KB.currentTaskTab === "attachments") return renderTaskAttachmentsPanel(task)
  return renderTaskGeneralPanel(task)
}

function renderTaskGeneralPanel(task) {
  const criticality = formatCriticality(task.criticality)
  return `
    <section class="task-section">
      <div class="task-section-head">
        <div class="task-section-title">Geral</div>
      </div>
      <div class="task-task-name">${esc(task.name || "Sem nome")}</div>
      <div class="task-grid">
        <div class="task-field">
          <label>Tipo de tarefa</label>
          <div class="task-readonly">${esc(task.task_type || "---")}</div>
        </div>
        <div class="task-field">
          <label>Data programada</label>
          <div class="task-readonly">${esc(task.scheduled_date ? fmtDateLong(task.scheduled_date) : "---")}</div>
        </div>
        <div class="task-field">
          <label>Criticidade</label>
          <div class="task-readonly task-readonly--critical"><span class="task-criticality-dot ${esc(criticality.key)}"></span>${esc(criticality.label)}</div>
        </div>
        <div class="task-field">
          <label>Classifica\u00e7\u00e3o 1</label>
          <div class="task-readonly">${esc(task.classification_1 || "---")}</div>
        </div>
        <div class="task-field">
          <label>Classifica\u00e7\u00e3o 2</label>
          <div class="task-readonly">${esc(task.classification_2 || "---")}</div>
        </div>
        <div class="task-field">
          <label>Status</label>
          <div class="task-readonly">${esc(TASK_STATUS_LABELS[normalizeTaskStatus(task.status)] || normalizeTaskStatus(task.status))}</div>
        </div>
      </div>
    </section>

    <section class="task-section">
      <div class="task-section-title">Tempo</div>
      <div class="task-grid">
        <div class="task-field">
          <label>Duracao estimada</label>
          <div class="task-readonly">${esc(formatDurationLong(task.estimated_duration))}</div>
        </div>
        <div class="task-field">
          <label>Tempo de execucao</label>
          <div class="task-readonly">${esc(computeExecutionTime(task))}</div>
        </div>
        <div class="task-field">
          <label for="taskStartDate">Data inicial</label>
          <input id="taskStartDate" class="task-input" type="datetime-local" value="${esc(toDatetimeLocalInputValue(task.start_date))}" />
        </div>
        <div class="task-field">
          <label for="taskEndDate">Data final</label>
          <input id="taskEndDate" class="task-input" type="datetime-local" value="${esc(toDatetimeLocalInputValue(task.end_date))}" />
        </div>
      </div>
    </section>
  `
}

function renderTaskSubtasksPanel(task) {
  const subtasks = task.subtasks || []
  return `
    <section class="task-section">
      <div class="task-section-title">Procedimento</div>
      <textarea id="taskProcedureInput" class="task-textarea" placeholder="Descreva o que foi realizado em campo...">${esc(task.procedure || "")}</textarea>
    </section>
    <section class="task-section">
      <div class="task-section-title">Subtarefas</div>
      <div class="task-subtask-list">
        ${
          subtasks.length
            ? subtasks
                .map(
                  (item, index) => `
              <label class="task-subtask-item">
                <span class="task-subtask-check">
                  <input type="checkbox" data-subtask-index="${index}" ${item.done ? "checked" : ""} />
                    <span class="task-subtask-copy">
                      <span class="task-subtask-title">${esc(item.title || item.name || `Subtarefa ${index + 1}`)}</span>
                      <span class="task-subtask-status">${item.done ? "Conclu\u00edda" : "Pendente"}</span>
                    </span>
                  </span>
                </label>
            `
                )
                .join("")
            : '<div class="task-empty">Nenhuma subtarefa cadastrada.</div>'
        }
      </div>
    </section>
  `
}

function renderTaskResourcesPanel(task) {
  const resources = task.resources || []
  return `
    <section class="task-section">
      <div class="task-section-title">Recursos</div>
      <div class="task-resource-list">
        ${
          resources.length
            ? resources
                .map(
                  (resource) => `
              <div class="task-resource-item">
                <div class="task-resource-copy">
                  <div class="task-resource-name">${esc(resource.name || "Recurso")}</div>
                  <div class="task-resource-meta">Quantidade: ${esc(String(resource.quantity || 1))} | Status: ${esc(resource.status || "Planejado")}</div>
                </div>
              </div>
            `
                )
                .join("")
            : '<div class="task-empty">Nenhum recurso cadastrado.</div>'
        }
      </div>
    </section>
  `
}

function renderTaskAttachmentsPanel(task) {
  const pending = KB.pendingAttachment
  const attachments = task.attachments || []

  return `
    <section class="task-section">
      <div class="task-attachment-toolbar">
        <div class="task-section-title">Anexos</div>
        <button type="button" class="wz-btn-primary" id="taskAddAttachmentBtn"><i class="fa-solid fa-plus"></i> Adicionar anexo</button>
      </div>

      ${
        pending
          ? `
            <div class="task-attachment-pending">
              <div class="task-attachment-preview">
                ${
                  pending.preview && pending.isImage
                    ? `<img src="${pending.preview}" alt="Preview do anexo" />`
                    : `<i class="fa-solid ${pending.isImage ? "fa-image" : "fa-file-pdf"}"></i>`
                }
              </div>
              <div class="task-attachment-copy">
                <div class="task-attachment-name">${esc(pending.file.name)}</div>
                <div class="task-attachment-meta">${esc(formatBytes(pending.file.size))}</div>
                <textarea id="taskAttachmentNote" class="task-textarea" placeholder="Nota sobre este anexo">${esc(pending.note || "")}</textarea>
                ${
                  pending.uploading
                    ? `
                      <div class="upload-progress"><div class="upload-progress-fill" id="taskUploadProgressFill" style="width:${pending.progress || 0}%"></div></div>
                      <div class="task-attachment-meta" id="taskUploadProgressText">${pending.progress || 0}% enviado</div>
                    `
                    : ""
                }
                <div class="task-attachment-actions">
                  <button type="button" class="wz-btn-primary" id="taskConfirmAttachmentBtn" ${pending.uploading ? "disabled" : ""}>Confirmar upload</button>
                  <button type="button" class="wz-btn-ghost" id="taskCancelAttachmentBtn" ${pending.uploading ? "disabled" : ""}>Cancelar</button>
                </div>
              </div>
            </div>
          `
          : ""
      }

      <div class="task-attachment-list">
        ${
          KB.attachmentsLoading
            ? '<div class="task-empty"><span class="kb-spinner"></span><span>Carregando anexos...</span></div>'
            : attachments.length
            ? attachments.map((attachment) => renderAttachmentItem(attachment)).join("")
            : '<div class="task-empty">Nenhum anexo enviado ainda.</div>'
        }
      </div>
    </section>
  `
}

function renderAttachmentItem(attachment) {
  const isImg = isImageAttachment(attachment)
  const iconClass = isImg ? "fa-image" : "fa-file-pdf"
  const thumb = attachment.thumbnail_url || (isImg ? attachment.download_url : "")
  const createdAt = attachment.created_at ? fmtDatetime(attachment.created_at) : "---"
  const available = attachment.is_available && attachment.download_url
  const thumbHtml = thumb
    ? `<img src="${escAttr(thumb)}" alt="${escAttr(attachment.name)}" style="width:100%;height:100%;object-fit:cover;border-radius:8px;" />`
    : `<i class="fa-solid ${iconClass}" style="font-size:22px;color:var(--text-muted);"></i>`

  return `
    <div class="task-attachment-item">
      <div class="task-attachment-preview" style="cursor:${isImg && available ? "zoom-in" : "default"};"
        ${isImg && available ? `data-lightbox="${escAttr(attachment.download_url)}" data-lightbox-alt="${escAttr(attachment.name)}"` : ""}>
        ${thumbHtml}
      </div>
      <div class="task-attachment-copy">
        <div class="task-attachment-name">${esc(attachment.name || "Arquivo")}</div>
        ${attachment.note ? `<div class="task-attachment-meta">${esc(attachment.note)}</div>` : ""}
        <div class="task-attachment-meta">${esc(createdAt)}</div>
        <div class="task-attachment-actions">
          ${available
            ? `<a href="${escAttr(attachment.download_url)}" target="_blank" rel="noopener noreferrer" class="wz-btn-ghost" style="padding:4px 10px;font-size:11px;"><i class="fa-solid fa-download"></i> Download</a>`
            : `<span style="font-size:11px;color:#ef4444;"><i class="fa-solid fa-triangle-exclamation"></i> Arquivo indisponÃ­vel</span>`
          }
        </div>
      </div>
      <button type="button" class="icon-btn task-attachment-delete" data-attachment-id="${esc(attachment.id)}" title="Excluir anexo"><i class="fa-solid fa-trash"></i></button>
    </div>
  `
}

function openLightbox(src, alt) {
  const existing = document.getElementById("kbLightbox")
  if (existing) existing.remove()

  const overlay = document.createElement("div")
  overlay.id = "kbLightbox"
  overlay.style.cssText = "position:fixed;inset:0;background:rgba(0,0,0,.88);z-index:9999;display:flex;align-items:center;justify-content:center;cursor:zoom-out;backdrop-filter:blur(4px);"
  overlay.innerHTML = `<img src="${escAttr(src)}" alt="${escAttr(alt || "")}" style="max-width:90vw;max-height:90vh;object-fit:contain;border-radius:10px;box-shadow:0 8px 48px rgba(0,0,0,.6);" />`
  overlay.addEventListener("click", () => overlay.remove())
  document.body.appendChild(overlay)
}

function renderTaskRegisterPanel() {
  return `
    <div class="task-register-panel">
      <div class="task-field">
        <label for="taskRegisterDuration">Horas</label>
        <input id="taskRegisterDuration" class="task-input" type="text" placeholder="HH:MM" value="${esc(KB.taskRegisterDraft.duration || "")}" />
      </div>
      <div class="task-field">
        <label for="taskRegisterNote">Apontamento</label>
        <textarea id="taskRegisterNote" class="task-textarea" placeholder="Registre o apontamento de horas...">${esc(KB.taskRegisterDraft.note || "")}</textarea>
      </div>
      <div class="task-field">
        <label>&nbsp;</label>
        <button class="wz-btn-primary" id="taskRegisterSaveBtn">Salvar registro</button>
      </div>
    </div>
  `
}

function wireTaskModalInteractions() {
  document.getElementById("taskModalBackBtn")?.addEventListener("click", closeTaskModal)
  document.getElementById("taskModalSaveBtn")?.addEventListener("click", saveCurrentTask)
  document.getElementById("taskStartBtn")?.addEventListener("click", beginTaskExecution)
  document.getElementById("taskRegisterBtn")?.addEventListener("click", () => {
    KB.taskRegisterOpen = !KB.taskRegisterOpen
    renderTaskModal()
  })

  document.querySelectorAll(".task-tab[data-tab]").forEach((button) => {
    button.addEventListener("click", () => {
      KB.currentTaskTab = button.dataset.tab
      if (button.dataset.tab === "attachments" && KB.currentTask) {
        KB.currentTask.attachmentsLoaded = false  // forÃ§ar reload sempre
      }
      renderTaskModal()
    })
  })

  document.getElementById("taskStartDate")?.addEventListener("change", (event) => {
    if (KB.currentTask) KB.currentTask.start_date = event.target.value || null
  })

  document.getElementById("taskEndDate")?.addEventListener("change", (event) => {
    if (KB.currentTask) KB.currentTask.end_date = event.target.value || null
  })

  document.getElementById("taskProcedureInput")?.addEventListener("input", (event) => {
    if (KB.currentTask) KB.currentTask.procedure = event.target.value
  })

  document.querySelectorAll("[data-subtask-index]").forEach((checkbox) => {
    checkbox.addEventListener("change", (event) => {
      if (!KB.currentTask) return
      const index = Number(event.target.dataset.subtaskIndex)
      const subtask = KB.currentTask.subtasks[index]
      if (!subtask) return
      subtask.done = !!event.target.checked
      KB.currentTask.progress = getProgressValue(KB.currentTask)
      syncPrimaryTaskToDetail(KB.currentOsDetail)
      renderTaskModal()
      renderOsDetail(KB.currentOsDetail)
    })
  })

  document.getElementById("taskAddAttachmentBtn")?.addEventListener("click", () => {
    document.getElementById("taskAttachmentInput")?.click()
  })

  document.getElementById("taskAttachmentNote")?.addEventListener("input", (event) => {
    if (KB.pendingAttachment) KB.pendingAttachment.note = event.target.value
  })

  document.getElementById("taskConfirmAttachmentBtn")?.addEventListener("click", confirmAttachmentUpload)
  document.getElementById("taskCancelAttachmentBtn")?.addEventListener("click", () => {
    KB.pendingAttachment = null
    renderTaskModal()
  })

  document.getElementById("taskRegisterDuration")?.addEventListener("input", (event) => {
    KB.taskRegisterDraft.duration = event.target.value
  })

  document.getElementById("taskRegisterNote")?.addEventListener("input", (event) => {
    KB.taskRegisterDraft.note = event.target.value
  })

  document.getElementById("taskRegisterSaveBtn")?.addEventListener("click", saveTaskRegister)

  document.querySelectorAll("[data-attachment-id]").forEach((button) => {
    button.addEventListener("click", () => deleteAttachment(button.dataset.attachmentId))
  })

  document.querySelectorAll("[data-lightbox]").forEach((el) => {
    el.addEventListener("click", () => openLightbox(el.dataset.lightbox, el.dataset.lightboxAlt))
  })
}

function syncCurrentTaskDraftFromForm() {
  if (!KB.currentTask) return

  const startDate = document.getElementById("taskStartDate")
  const endDate = document.getElementById("taskEndDate")
  const procedure = document.getElementById("taskProcedureInput")

  if (startDate) KB.currentTask.start_date = startDate.value || null
  if (endDate) KB.currentTask.end_date = endDate.value || null
  if (procedure) KB.currentTask.procedure = procedure.value || ""
}

async function saveCurrentTask() {
  if (!KB.currentTask || !KB.currentOsDetail) return

  syncCurrentTaskDraftFromForm()
  syncPrimaryTaskToDetail(KB.currentOsDetail)
  await persistWorkOrderDetail(KB.currentOsDetail, "Tarefa atualizada com sucesso")
}

async function beginTaskExecution() {
  if (!KB.currentTask || !KB.currentOsDetail) return

  syncCurrentTaskDraftFromForm()
  if (!KB.currentTask.start_date) KB.currentTask.start_date = new Date().toISOString()
  KB.currentTask.status = "em_andamento"
  if (KB.currentOsDetail.status === "pendente") KB.currentOsDetail.status = "em_processo"
  syncPrimaryTaskToDetail(KB.currentOsDetail)

  await persistWorkOrderDetail(KB.currentOsDetail, "Tarefa iniciada")
}

async function saveTaskRegister() {
  if (!KB.currentTask || !KB.currentOsDetail) return

  const duration = (KB.taskRegisterDraft.duration || "").trim()
  if (!duration) {
    showToast("Informe a quantidade de horas do registro", "error")
    return
  }

  const logEntry = {
    id: Date.now(),
    duration,
    note: (KB.taskRegisterDraft.note || "").trim(),
    created_at: new Date().toISOString()
  }

  if (!Array.isArray(KB.currentTask.execution_logs)) KB.currentTask.execution_logs = []
  KB.currentTask.execution_logs.push(logEntry)

  if (!KB.currentTask.start_date) KB.currentTask.start_date = new Date().toISOString()
  if (KB.currentTask.status === "nao_iniciada") KB.currentTask.status = "em_andamento"
  if (KB.currentOsDetail.status === "pendente") KB.currentOsDetail.status = "em_processo"

  KB.taskRegisterOpen = false
  KB.taskRegisterDraft = { duration: "", note: "" }
  syncPrimaryTaskToDetail(KB.currentOsDetail)

  await persistWorkOrderDetail(KB.currentOsDetail, "Registro salvo com sucesso")
}

function handleAttachmentFileSelected(event) {
  const file = event.target.files?.[0]
  event.target.value = ""
  if (!file) return

  KB.pendingAttachment = {
    file,
    note: "",
    preview: "",
    isImage: file.type.startsWith("image/"),
    uploading: false,
    progress: 0
  }

  if (KB.pendingAttachment.isImage) {
    const reader = new FileReader()
    reader.onload = () => {
      if (KB.pendingAttachment && KB.pendingAttachment.file === file) {
        KB.pendingAttachment.preview = typeof reader.result === "string" ? reader.result : ""
        renderTaskModal()
      }
    }
    reader.readAsDataURL(file)
  }

  renderTaskModal()
}

async function confirmAttachmentUpload() {
  if (!KB.pendingAttachment || !KB.currentOsDetail || !KB.currentTask) return

  KB.pendingAttachment.uploading = true
  KB.pendingAttachment.progress = 0
  renderTaskModal()

  try {
    const workOrderId = getWorkOrderId(KB.currentOsDetail)
    await uploadWorkOrderAttachment(workOrderId, KB.pendingAttachment, (progress) => {
      KB.pendingAttachment.progress = progress
      const fill = document.getElementById("taskUploadProgressFill")
      const text = document.getElementById("taskUploadProgressText")
      if (fill) fill.style.width = `${progress}%`
      if (text) text.textContent = `${progress}% enviado`
    })

    showToast("Anexo enviado com sucesso", "success")
    KB.pendingAttachment = null
    KB.currentTask.attachmentsLoaded = false
    KB.currentTaskTab = "attachments"
    await refreshCurrentWorkOrderDetail({ preserveTaskId: KB.currentTask.id })
  } catch (error) {
    console.error("[OS] upload attachment", error)
    KB.pendingAttachment.uploading = false
    showToast(`Erro ao enviar anexo: ${error.message}`, "error")
    renderTaskModal()
  }
}

async function uploadWorkOrderAttachment(workOrderId, pendingAttachment, onProgress) {
  const file = pendingAttachment.file
  const fileName = file.name
  const contentType = file.type || "application/octet-stream"
  console.log("[UPLOAD] iniciando", { workOrderId, fileName, contentType, size: file.size })

  // Passo 1: presign
  const presignData = await apiJson(`/work-orders/${workOrderId}/attachments/presign`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ file_name: fileName, content_type: contentType })
  })
  console.log("[UPLOAD] presign ok", presignData)

  const uploadUrl = presignData.upload_url
  const s3Key = presignData.s3_key
  if (!uploadUrl || !s3Key) throw new Error("Presign retornou sem upload_url ou s3_key")

  // Passo 2: PUT S3
  await new Promise((resolve, reject) => {
    const xhr = new XMLHttpRequest()
    xhr.open("PUT", uploadUrl)
    xhr.setRequestHeader("Content-Type", contentType)
    xhr.upload.addEventListener("progress", (e) => {
      if (e.lengthComputable) onProgress(Math.round((e.loaded / e.total) * 90))
    })
    xhr.onload = () => {
      console.log("[UPLOAD] S3 PUT status:", xhr.status)
      xhr.status >= 200 && xhr.status < 300 ? resolve() : reject(new Error(`S3 upload falhou: ${xhr.status} ${xhr.responseText}`))
    }
    xhr.onerror = () => reject(new Error("Falha de rede no upload S3"))
    xhr.send(file)
  })
  console.log("[UPLOAD] S3 ok, registrando no banco...")

  // Passo 3: registrar no banco
  const regResult = await apiJson(`/work-orders/${workOrderId}/attachments`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      file_url: s3Key,
      file_name: fileName,
      content_type: contentType,
      file_size_bytes: file.size,
      note: pendingAttachment.note || ""
    })
  })
  console.log("[UPLOAD] registro ok", regResult)
  onProgress(100)
}

async function loadTaskAttachments(task) {
  if (!task || !KB.currentOsDetail) return

  KB.attachmentsLoading = true
  renderTaskModal()

  try {
    const woId = getWorkOrderId(KB.currentOsDetail)
    console.log("[ATT] carregando anexos para woId:", woId)
    const data = await apiJson(`/work-orders/${woId}/attachments`)
    console.log("[ATT] resposta bruta:", data)
    const items = Array.isArray(data?.items)
      ? data.items
      : Array.isArray(data?.attachments)
      ? data.attachments
      : Array.isArray(data)
      ? data
      : []
    console.log("[ATT] items normalizados:", items.length, items)

    task.attachments = normalizeAttachments(items)
    console.log("[ATT] apÃ³s normalize:", task.attachments)
    task.attachmentsLoaded = true
    syncPrimaryTaskToDetail(KB.currentOsDetail)
  } catch (error) {
    console.error("[ATT] ERRO ao carregar:", error.message, error.status, error)
    task.attachmentsLoaded = true
  } finally {
    KB.attachmentsLoading = false
    renderTaskModal()
  }
}

async function deleteAttachment(attachmentId) {
  if (!attachmentId || !KB.currentOsDetail || !KB.currentTask) return
  if (!window.confirm("Deseja excluir este anexo?")) return

  try {
    await apiJson(`/work-orders/${getWorkOrderId(KB.currentOsDetail)}/attachments/${attachmentId}`, {
      method: "DELETE"
    })

    KB.currentTask.attachments = (KB.currentTask.attachments || []).filter(
      (attachment) => String(attachment.id) !== String(attachmentId)
    )
    syncPrimaryTaskToDetail(KB.currentOsDetail)
    renderTaskModal()
    renderOsDetail(KB.currentOsDetail)
    showToast("Anexo removido com sucesso", "success")
  } catch (error) {
    console.error("[OS] delete attachment", error)
    showToast(`Erro ao excluir anexo: ${error.message}`, "error")
  }
}

// =============================================================================
// FAB / wizard
// =============================================================================

function bindFab() {
  const currentUser = getCurrentUser()
  const canCreate = currentUser?.is_superuser === true || currentUser?.permissions?.os_open === true
  const fab = document.getElementById("kbFab")
  if (!fab) return
  if (!canCreate) {
    fab.style.display = "none"
    return
  }
  fab.addEventListener("click", openWizard)
}

function renderWizardAttachmentList() {
  const list = document.getElementById("wzAttachmentList")
  if (!list) return
  const attachments = KB.wizardPendingAttachments
  if (!attachments.length) {
    list.innerHTML = ""
    return
  }
  list.innerHTML = attachments
    .map(
      (att, i) => `
      <div class="wz-attachment-item">
        <i class="fa-solid fa-paperclip"></i>
        <span class="wz-att-name">${esc(att.name)}</span>
        <span class="wz-att-size">${formatFileSize(att.size)}</span>
        <button type="button" class="wz-att-remove" data-idx="${i}" title="Remover"><i class="fa-solid fa-xmark"></i></button>
      </div>`
    )
    .join("")
  list.querySelectorAll(".wz-att-remove").forEach((btn) => {
    btn.addEventListener("click", () => {
      KB.wizardPendingAttachments.splice(Number(btn.dataset.idx), 1)
      renderWizardAttachmentList()
    })
  })
}

function formatFileSize(bytes) {
  if (bytes < 1024) return `${bytes} B`
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`
  return `${(bytes / (1024 * 1024)).toFixed(1)} MB`
}

function bindWizard() {
  document.getElementById("wzCloseBtn")?.addEventListener("click", closeWizard)
  document.getElementById("wzCancelBtn")?.addEventListener("click", closeWizard)
  document.getElementById("wzBackBtn")?.addEventListener("click", () => goToStep(KB.currentStep - 1))
  document.getElementById("wzPrevBtn")?.addEventListener("click", () => goToStep(KB.currentStep - 1))
  document.getElementById("wzNextBtn")?.addEventListener("click", () => goToStep(KB.currentStep + 1))
  document.getElementById("wzSubmitBtn")?.addEventListener("click", submitWizard)

  document.getElementById("failedCheckLabel")?.addEventListener("click", () => {
    KB.failedChecked = !KB.failedChecked
    document.getElementById("failedCheck")?.classList.toggle("checked", KB.failedChecked)
    document.getElementById("failureFields")?.classList.toggle("hidden", !KB.failedChecked)
  })

  document.getElementById("serviceCheckLabel")?.addEventListener("click", () => {
    KB.serviceChecked = !KB.serviceChecked
    document.getElementById("serviceCheck")?.classList.toggle("checked", KB.serviceChecked)
  })

  document.getElementById("alreadyDoneLabel")?.addEventListener("click", () => {
    KB.alreadyDoneChecked = !KB.alreadyDoneChecked
    document.getElementById("alreadyDoneCheck")?.classList.toggle("checked", KB.alreadyDoneChecked)
    document.getElementById("radioGroupNormal")?.classList.toggle("hidden", KB.alreadyDoneChecked)
    document.getElementById("radioGroupDone")?.classList.toggle("hidden", !KB.alreadyDoneChecked)
    document.getElementById("responsavelGroup")?.classList.toggle("hidden", !KB.alreadyDoneChecked)
  })

  document.getElementById("f-plant")?.addEventListener("change", (e) => {
    KB.selectedPlantId = e.target.value || null
  })

  document.getElementById("addSubtaskBtn")?.addEventListener("click", addSubtask)
  document.getElementById("addResourceBtn")?.addEventListener("click", addResource)

  document.getElementById("wzAttachmentAddBtn")?.addEventListener("click", () => {
    document.getElementById("wzAttachmentsInput")?.click()
  })
  document.getElementById("wzAttachmentsInput")?.addEventListener("change", (e) => {
    const files = Array.from(e.target.files || [])
    e.target.value = ""
    files.forEach((file) => {
      KB.wizardPendingAttachments.push({ file, name: file.name, size: file.size })
    })
    renderWizardAttachmentList()
  })
}

function openWizard() {
  resetWizard()
  loadPlants()
  document.getElementById("wzOverlay")?.classList.remove("hidden")

  const radioGroupNormal = document.getElementById("radioGroupNormal")
  const radioGroupDone = document.getElementById("radioGroupDone")
  if (radioGroupNormal) radioGroupNormal.style.display = ""
  if (radioGroupDone) radioGroupDone.style.display = ""

  const now = new Date()
  const local = new Date(now.getTime() - now.getTimezoneOffset() * 60000).toISOString().slice(0, 16)
  document.getElementById("f-incident-date").value = local
  document.getElementById("f-scheduled-date").value = local
  document.getElementById("f-start-date").value = local
}

function closeWizard() {
  document.getElementById("wzOverlay")?.classList.add("hidden")
}

async function loadPlants() {
  try {
    const data = await apiJson("/os-plants")
    const plants = data?.items || []
    const opts = plants.map(p => `<option value="${esc(String(p.id))}">${esc(p.name)}</option>`).join("")
    const wzSelect = document.getElementById("f-plant")
    const fltSelect = document.getElementById("flt-location")
    if (wzSelect) wzSelect.innerHTML = `<option value="">Todas as usinas</option>${opts}`
    if (fltSelect) fltSelect.innerHTML = `<option value="">Todas</option>${opts}`
    if (!plants.length) console.warn("[OS] loadPlants: nenhuma usina retornada")
  } catch (e) {
    console.error("[OS] loadPlants error:", e)
    showToast("Erro ao carregar usinas", "error")
  }
}

function resetWizard() {
  KB.currentStep = 1
  KB.selectedPlantId = null
  KB.selectedAsset = null
  KB.selectedResponsavel = null
  KB.subtasks = []
  KB.resources = []
  KB.wizardPendingAttachments = []
  KB.failedChecked = false
  KB.serviceChecked = false
  KB.alreadyDoneChecked = false

  ;["f-task-desc", "f-observation", "f-request-num"].forEach((id) => {
    const element = document.getElementById(id)
    if (element) element.value = ""
  })

  document.getElementById("f-duration").value = "000:10"
  document.getElementById("f-interruption-duration").value = "000:00"

  ;[
    "f-plant",
    "f-failure-type",
    "f-failure-cause",
    "f-detection-method",
    "f-failure-severity",
    "f-task-type",
    "f-class1",
    "f-class2"
  ].forEach((id) => {
    const element = document.getElementById(id)
    if (element) element.value = ""
  })

  document.getElementById("f-criticality").value = ""
  document.getElementById("f-damage-type").value = "Nenhum"

  ;["failedCheck", "serviceCheck", "alreadyDoneCheck"].forEach((id) => {
    document.getElementById(id)?.classList.remove("checked")
  })

  document.getElementById("failureFields")?.classList.add("hidden")
  document.getElementById("radioGroupNormal")?.classList.remove("hidden")
  document.getElementById("radioGroupDone")?.classList.add("hidden")
  document.getElementById("responsavelGroup")?.classList.add("hidden")
  document.getElementById("assetSelected")?.classList.add("hidden")
  document.getElementById("assetSearchBtn")?.classList.remove("hidden")

  const respLabel = document.getElementById("responsavelBtnLabel")
  if (respLabel) respLabel.textContent = "Selecionar respons\u00e1vel..."

  document.getElementById("sendToPending").checked = true
  document.getElementById("subtasksList").innerHTML =
    '<div class="wz-subtasks-empty"><i class="fa-regular fa-circle-check"></i><span>Nenhuma subtarefa adicionada</span></div>'
  document.getElementById("resourcesList").innerHTML =
    '<div class="wz-subtasks-empty"><i class="fa-regular fa-toolbox"></i><span>Nenhum recurso adicionado</span></div>'
  document.querySelectorAll(".wz-error").forEach((element) => element.classList.add("hidden"))

  goToStep(1, true)
}

function goToStep(step, force = false) {
  if (step < 1 || step > 4) return
  if (!force && step > KB.currentStep && !validateStep(KB.currentStep)) return

  KB.currentStep = step

  for (let index = 1; index <= 4; index += 1) {
    document.getElementById(`wz-step-${index}`)?.classList.toggle("hidden", index !== step)
  }

  document.querySelectorAll(".wz-step[data-step]").forEach((element) => {
    const current = Number(element.dataset.step)
    element.classList.toggle("active", current === step)
    element.classList.toggle("done", current < step)
  })

  document.getElementById("wzBackBtn")?.classList.toggle("hidden", step <= 1)
  document.getElementById("wzPrevBtn")?.classList.toggle("hidden", step <= 1)
  document.getElementById("wzNextBtn")?.classList.toggle("hidden", step >= 4)
  document.getElementById("wzSubmitBtn")?.classList.toggle("hidden", step < 4)

  if (step === 4) buildSummary()
}

function validateStep(step) {
  let ok = true

  if (step === 1) {
    if (!KB.selectedAsset) {
      document.getElementById("err-asset")?.classList.remove("hidden")
      document.getElementById("assetSearchBtn")?.classList.add("has-error")
      ok = false
    }

    if (KB.failedChecked) {
      ;["f-failure-type", "f-failure-cause", "f-detection-method"].forEach((id) => {
        const element = document.getElementById(id)
        if (!element?.value) {
          document.getElementById(id.replace("f-", "err-"))?.classList.remove("hidden")
          ok = false
        }
      })
    }
  }

  if (step === 2) {
    const description = document.getElementById("f-task-desc")?.value?.trim()
    if (!description) {
      document.getElementById("err-task-desc")?.classList.remove("hidden")
      ok = false
    }

    const taskType = document.getElementById("f-task-type")?.value
    if (!taskType) {
      document.getElementById("err-task-type")?.classList.remove("hidden")
      ok = false
    }

    if (KB.alreadyDoneChecked && !KB.selectedResponsavel) {
      document.getElementById("err-responsavel")?.classList.remove("hidden")
      ok = false
    }
  }

  return ok
}

function buildSummary() {
  const grid = document.getElementById("wzSummaryGrid")
  if (!grid) return

  const sendTo = KB.alreadyDoneChecked
    ? document.getElementById("sendToVerif")?.checked
      ? "Verificacao"
      : "Finalizados"
    : document.getElementById("sendToPending")?.checked
    ? "Tarefas pendentes"
    : "OSs em Processo"

  const items = [
    ["Ativo", KB.selectedAsset?.name || "---"],
    ["Codigo", KB.selectedAsset?.code || "---"],
    ["Incidente", document.getElementById("f-incident-date")?.value || "---"],
    ["Ativo falhou?", KB.failedChecked ? "Sim" : "Nao"],
    ["Tipo tarefa", document.getElementById("f-task-type")?.value || "---"],
    ["Classifica\u00e7\u00e3o 1", document.getElementById("f-class1")?.value || "---"],
    ["Classifica\u00e7\u00e3o 2", document.getElementById("f-class2")?.value || "---"],
    ["Responsavel", KB.selectedResponsavel?.name || "---"],
    ["Enviar para", sendTo],
    ["Subtarefas", String(KB.subtasks.filter(Boolean).length)]
  ]

  grid.innerHTML = items
    .map(
      ([key, value]) =>
        `<div class="wz-summary-item"><span class="wz-summary-key">${esc(key)}</span><span class="wz-summary-val">${esc(value)}</span></div>`
    )
    .join("")
}

async function submitWizard() {
  if (!validateStep(1) || !validateStep(2)) {
    showToast("Preencha os campos obrigatorios", "error")
    return
  }

  console.log("[WZ] wizardPendingAttachments:", KB.wizardPendingAttachments.length, KB.wizardPendingAttachments)

  const submitBtn = document.getElementById("wzSubmitBtn")
  submitBtn.disabled = true
  submitBtn.innerHTML = '<i class="fa-solid fa-spinner fa-spin"></i> Gerando...'

  let sendStatus
  if (KB.alreadyDoneChecked) {
    sendStatus = document.getElementById("sendToVerif")?.checked ? "em_verificacao" : "concluida"
  } else {
    sendStatus = document.getElementById("sendToPending")?.checked ? "pendente" : "em_processo"
  }

  const body = {
    asset_id: KB.selectedAsset?.id || null,
    asset_name: KB.selectedAsset?.name || null,
    asset_code: KB.selectedAsset?.code || null,
    asset_location: KB.selectedAsset?.location || null,
    plant_id: KB.selectedAsset?.plant_id || null,
    incident_date: document.getElementById("f-incident-date")?.value || new Date().toISOString(),
    requested_by: document.getElementById("f-requested-by")?.value || null,
    asset_failed: KB.failedChecked,
    failure_type: KB.failedChecked ? document.getElementById("f-failure-type")?.value : null,
    failure_cause: KB.failedChecked ? document.getElementById("f-failure-cause")?.value : null,
    failure_detection_method: KB.failedChecked ? document.getElementById("f-detection-method")?.value : null,
    failure_severity: KB.failedChecked ? document.getElementById("f-failure-severity")?.value : null,
    damage_type: KB.failedChecked ? document.getElementById("f-damage-type")?.value : "Nenhum",
    caused_interruption_duration: KB.failedChecked ? document.getElementById("f-interruption-duration")?.value : "000:00",
    back_to_service: KB.serviceChecked,
    task_description: document.getElementById("f-task-desc")?.value?.trim(),
    observations: document.getElementById("f-observation")?.value?.trim() || null,
    task_type: document.getElementById("f-task-type")?.value,
    classification_1: document.getElementById("f-class1")?.value || null,
    classification_2: document.getElementById("f-class2")?.value || null,
    criticality: document.getElementById("f-criticality")?.value || "media",
    estimated_duration: document.getElementById("f-duration")?.value || "000:10",
    request_number: document.getElementById("f-request-num")?.value?.trim() || null,
    already_performed: KB.alreadyDoneChecked,
    responsavel_id: KB.selectedResponsavel?.id || null,
    responsavel_name: KB.selectedResponsavel?.name || null,
    responsavel_email: KB.selectedResponsavel?.email || null,
    status: sendStatus,
    scheduled_date: document.getElementById("f-scheduled-date")?.value || null,
    start_date: document.getElementById("f-start-date")?.value || null,
    subtasks: KB.subtasks.filter(Boolean),
    resources: KB.resources.filter(Boolean)
  }

  try {
    const created = await apiJson("/work-orders", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body)
    })

    const newWoId = created?.item?.id || created?.work_order?.id || created?.id
    if (!newWoId) {
      console.error("[WZ] newWoId nÃ£o encontrado na resposta:", created)
    } else if (KB.wizardPendingAttachments.length) {
      for (const att of KB.wizardPendingAttachments) {
        try {
          await uploadWorkOrderAttachment(newWoId, { file: att.file, note: "" }, () => {})
        } catch (err) {
          console.error("[WZ] anexo falhou:", att.name, err.message, err)
          showToast(`Anexo "${att.name}" falhou: ${err.message}`, "error")
        }
      }
    }

    closeWizard()
    showToast(STATUS_CREATE_TOAST[sendStatus] || "Uma nova OS foi gerada", "success")
    await refreshColumns([sendStatus])
  } catch (error) {
    console.error("[WZ] submit", error)
    showToast(`Erro ao criar OS: ${error.message}`, "error")
  } finally {
    submitBtn.disabled = false
    submitBtn.innerHTML =
      '<svg viewBox="0 0 20 20" fill="none" stroke="currentColor" stroke-width="2.2" stroke-linecap="round" stroke-linejoin="round" style="width:15px;height:15px"><polyline points="4 10 8 14 16 6"/></svg> Gerar OS'
  }
}

function addSubtask() {
  const list = document.getElementById("subtasksList")
  list.querySelector(".wz-subtasks-empty")?.remove()

  const index = KB.subtasks.length
  KB.subtasks.push("")

  const item = document.createElement("div")
  item.className = "wz-subtask-item"
  item.innerHTML =
    '<i class="fa-regular fa-circle-check" style="color:var(--text-muted);font-size:14px;"></i><input type="text" placeholder="Descricao da subtarefa..." /><button class="wz-subtask-remove"><i class="fa-solid fa-xmark"></i></button>'

  const input = item.querySelector("input")
  input.addEventListener("input", () => {
    KB.subtasks[index] = input.value
  })

  item.querySelector(".wz-subtask-remove").addEventListener("click", () => {
    KB.subtasks[index] = null
    item.remove()
    if (!list.querySelector(".wz-subtask-item")) {
      list.innerHTML =
        '<div class="wz-subtasks-empty"><i class="fa-regular fa-circle-check"></i><span>Nenhuma subtarefa adicionada</span></div>'
    }
  })

  list.appendChild(item)
  input.focus()
}

function addResource() {
  const list = document.getElementById("resourcesList")
  list.querySelector(".wz-subtasks-empty")?.remove()

  const index = KB.resources.length
  KB.resources.push("")

  const item = document.createElement("div")
  item.className = "wz-resource-item"
  item.innerHTML =
    '<i class="fa-solid fa-wrench" style="color:var(--text-muted);font-size:13px;"></i><input type="text" placeholder="Nome do recurso..." /><button class="wz-subtask-remove"><i class="fa-solid fa-xmark"></i></button>'

  const input = item.querySelector("input")
  input.addEventListener("input", () => {
    KB.resources[index] = input.value
  })

  item.querySelector(".wz-subtask-remove").addEventListener("click", () => {
    KB.resources[index] = null
    item.remove()
    if (!list.querySelector(".wz-resource-item")) {
      list.innerHTML =
        '<div class="wz-subtasks-empty"><i class="fa-regular fa-toolbox"></i><span>Nenhum recurso adicionado</span></div>'
    }
  })

  list.appendChild(item)
  input.focus()
}

// =============================================================================
// Asset drawer
// =============================================================================

function bindAssetDrawer() {
  document.getElementById("assetSearchBtn")?.addEventListener("click", openAssetDrawer)
  document.getElementById("assetDrawerBack")?.addEventListener("click", closeAssetDrawer)
  document.getElementById("assetDrawerOverlay")?.addEventListener("click", closeAssetDrawer)
  document.getElementById("assetClearBtn")?.addEventListener("click", clearAsset)
  document.getElementById("assetFilterToggle")?.addEventListener("click", () => {
    document.getElementById("assetFilterPanel")?.classList.remove("hidden")
  })
  document.getElementById("assetFilterBack")?.addEventListener("click", () => {
    document.getElementById("assetFilterPanel")?.classList.add("hidden")
  })
  document.getElementById("afpApplyBtn")?.addEventListener("click", () => {
    KB.assetFilters = {
      plant_id: document.getElementById("flt-location")?.value || "",
      asset_type: document.getElementById("flt-asset-type")?.value || "",
      code: document.getElementById("flt-code")?.value || "",
      criticality: document.getElementById("flt-criticality")?.value || ""
    }
    document.getElementById("assetFilterPanel")?.classList.add("hidden")
    searchAssets()
  })

  document.getElementById("afpClearBtn")?.addEventListener("click", () => {
    KB.assetFilters = {}
    ;["flt-location", "flt-asset-type", "flt-desc", "flt-code", "flt-unit", "flt-barcode", "flt-criticality", "flt-type"].forEach((id) => {
      const element = document.getElementById(id)
      if (element) element.value = ""
    })
    document.getElementById("assetFilterPanel")?.classList.add("hidden")
    searchAssets()
  })

  document.getElementById("assetSearchInput")?.addEventListener("input", (event) => {
    KB.assetSearch = event.target.value
    clearTimeout(KB._assetTimer)
    KB._assetTimer = setTimeout(searchAssets, 350)
  })
}

function openAssetDrawer() {
  document.getElementById("assetDrawerOverlay")?.classList.remove("hidden")
  document.getElementById("assetDrawer")?.classList.remove("hidden")
  document.getElementById("assetFilterPanel")?.classList.add("hidden")
  KB.assetSearch = ""
  KB.assetFilters = {}
  if (KB.selectedPlantId) {
    KB.assetFilters.plant_id = KB.selectedPlantId
    const fltSelect = document.getElementById("flt-location")
    if (fltSelect) fltSelect.value = KB.selectedPlantId
  }
  const input = document.getElementById("assetSearchInput")
  if (input) input.value = ""
  searchAssets()
}

function closeAssetDrawer() {
  document.getElementById("assetDrawerOverlay")?.classList.add("hidden")
  document.getElementById("assetDrawer")?.classList.add("hidden")
}

function clearAsset() {
  KB.selectedAsset = null
  document.getElementById("assetSelected")?.classList.add("hidden")
  document.getElementById("assetSearchBtn")?.classList.remove("hidden")
}

async function searchAssets() {
  const loading = document.getElementById("assetLoading")
  const empty = document.getElementById("assetEmpty")
  const list = document.getElementById("assetResultsList")
  const footer = document.getElementById("assetResultsFooter")

  loading?.classList.remove("hidden")
  empty?.classList.add("hidden")
  if (list) list.innerHTML = ""
  if (footer) footer.textContent = ""

  const params = new URLSearchParams({ page: "1", page_size: "50" })
  if (KB.assetSearch) params.set("q", KB.assetSearch)
  Object.entries(KB.assetFilters).forEach(([key, value]) => {
    if (value) params.set(key, value)
  })

  try {
    const data = await apiJson(`/assets?${params.toString()}`)
    const items = Array.isArray(data?.items) ? data.items : Array.isArray(data?.assets) ? data.assets : []
    loading?.classList.add("hidden")

    if (!items.length) {
      empty?.classList.remove("hidden")
      return
    }

    items.forEach((asset) => {
      const element = document.createElement("div")
      element.className = "asset-item"
      const imgUrl = asset.image_url || ""
      element.innerHTML = `
        <div class="asset-item-icon" style="${imgUrl ? "padding:0;overflow:hidden;" : ""}">
          ${imgUrl
            ? `<img src="${esc(imgUrl)}" alt="" style="width:32px;height:32px;object-fit:cover;border-radius:7px;"
                onerror="this.parentElement.innerHTML='<i class=\\'fa-solid fa-microchip\\'></i>'" />`
            : `<i class="fa-solid fa-microchip"></i>`}
        </div>
        <div class="asset-item-body">
          <div class="asset-item-name">${esc(asset.name || asset.description)}</div>
          <div class="asset-item-code">${esc(asset.code || asset.asset_code || "")}</div>
          <div class="asset-item-meta">
            <div class="asset-meta-row"><span>Tipo:</span><span>${esc(asset.asset_type || "Instalacoes")}</span></div>
            <div class="asset-meta-row"><span>Local:</span><span>${esc(asset.location || asset.plant_name || "---")}</span></div>
            ${asset.criticality ? `<div class="asset-meta-row"><span>Criticidade:</span><span>${esc(asset.criticality)}</span></div>` : ""}
          </div>
        </div>
      `
      element.addEventListener("click", () => selectAsset(asset))
      list?.appendChild(element)
    })

    if (footer) footer.textContent = `Mostrando ${items.length} de ${data?.total || items.length}`
  } catch (error) {
    console.error("[OS] search assets", error)
    loading?.classList.add("hidden")
    if (list) {
      list.innerHTML =
        '<div class="asset-empty"><i class="fa-solid fa-triangle-exclamation"></i><span>Erro ao buscar ativos</span></div>'
    }
  }
}

function selectAsset(asset) {
  KB.selectedAsset = {
    id: asset.id || asset.device_id,
    name: asset.name || asset.description,
    code: asset.code || asset.asset_code,
    location: asset.location || asset.plant_name,
    plant_id: asset.plant_id
  }

  document.getElementById("assetSelected")?.classList.remove("hidden")
  document.getElementById("assetSearchBtn")?.classList.add("hidden")
  document.getElementById("assetSelectedName").textContent = `${KB.selectedAsset.name}${KB.selectedAsset.code ? ` { ${KB.selectedAsset.code} }` : ""}`
  document.getElementById("err-asset")?.classList.add("hidden")
  document.getElementById("assetSearchBtn")?.classList.remove("has-error")
  closeAssetDrawer()
}

// =============================================================================
// Responsavel drawer
// =============================================================================

function bindRespDrawer() {
  document.getElementById("responsavelBtn")?.addEventListener("click", openRespDrawer)
  document.getElementById("respDrawerBack")?.addEventListener("click", closeRespDrawer)
  document.getElementById("respDrawerOverlay")?.addEventListener("click", closeRespDrawer)

  document.getElementById("respSearchInput")?.addEventListener("input", (event) => {
    clearTimeout(KB._respTimer)
    KB._respTimer = setTimeout(() => filterRespTable(event.target.value), 250)
  })

  document.getElementById("respSearchClear")?.addEventListener("click", () => {
    const input = document.getElementById("respSearchInput")
    if (input) input.value = ""
    filterRespTable("")
  })

  document.getElementById("respDatePicker")?.addEventListener("change", loadRespUsers)
}

function openRespDrawer() {
  document.getElementById("respDrawerOverlay")?.classList.remove("hidden")
  document.getElementById("respDrawer")?.classList.remove("hidden")

  const today = new Date().toISOString().split("T")[0]
  const picker = document.getElementById("respDatePicker")
  if (picker && !picker.value) picker.value = today

  if (!KB.respUsers.length) loadRespUsers()
  else filterRespTable(document.getElementById("respSearchInput")?.value || "")
}

function closeRespDrawer() {
  document.getElementById("respDrawerOverlay")?.classList.add("hidden")
  document.getElementById("respDrawer")?.classList.add("hidden")
}

async function loadRespUsers() {
  const tbody = document.getElementById("respTableBody")
  if (tbody) {
    tbody.innerHTML =
      '<tr><td colspan="9" style="text-align:center;padding:24px;color:var(--text-muted);"><span class="kb-spinner" style="display:inline-block;vertical-align:middle;margin-right:8px;"></span>Carregando...</td></tr>'
  }

  try {
    const data = await apiJson(`/workers`)
    const users = Array.isArray(data?.items) ? data.items : Array.isArray(data?.users) ? data.users : Array.isArray(data) ? data : []
    KB.respUsers = users
    filterRespTable(document.getElementById("respSearchInput")?.value || "")
  } catch (error) {
    console.warn("[RESP] load", error)
    KB.respUsers = []
    renderRespTable([], 0)
  }
}

function renderRespTable(users, totalCount = users.length) {
  const tbody = document.getElementById("respTableBody")
  const meta = document.getElementById("respResultsMeta")
  if (!tbody) return

  if (!users.length) {
    tbody.innerHTML =
      '<tr><td colspan="9" style="text-align:center;padding:24px;color:var(--text-muted);">Nenhum respons\u00e1vel encontrado</td></tr>'
    if (meta) meta.textContent = `Mostrando 0 de ${totalCount}`
    return
  }

  tbody.innerHTML = users
    .map((user) => {
      const userId = user.id || user.user_id || ""
      const isSelected = String(KB.selectedResponsavel?.id || "") === String(userId)
      const dayCells = WEEK_DAY_COLUMNS.map((dayColumn) => {
        const value = resolveUserWeekHoursValue(user, dayColumn)
        const hasHours = hasHoursBadgeValue(value)
        const className = hasHours ? "resp-hours-cell has-hours" : "resp-hours-cell"
        return `<td><span class="${className}">${esc(hasHours ? value : "SEM HORAS")}</span></td>`
      }).join("")

      return `
        <tr class="${isSelected ? "selected" : ""}" data-user-id="${esc(userId)}" data-user-name="${escAttr(user.name || user.username || "")}" data-user-email="${escAttr(user.email || "")}" data-user-code="${escAttr(user.code || userId || "")}">
          <td style="font-family:'JetBrains Mono',monospace;font-size:11px;color:var(--text-muted);">${esc(user.code || user.id || "---")}</td>
          <td>${esc(user.name || user.username || "---")}</td>
          <td style="font-size:11px;color:var(--text-muted);">${esc(user.email || "---")}</td>
          ${dayCells}
        </tr>
      `
    })
    .join("")

  if (meta) meta.textContent = `Mostrando ${users.length} de ${totalCount}`

  tbody.querySelectorAll("tr[data-user-id]").forEach((row) => {
    row.addEventListener("click", () => {
      tbody.querySelectorAll("tr").forEach((item) => item.classList.remove("selected"))
      row.classList.add("selected")
      selectResponsavel({
        id: row.dataset.userId,
        name: row.dataset.userName,
        email: row.dataset.userEmail,
        code: row.dataset.userCode
      })
    })
  })
}

function filterRespTable(query) {
  const normalizedQuery = (query || "").trim().toLowerCase()
  const filtered = !normalizedQuery
    ? KB.respUsers
    : KB.respUsers.filter((user) => {
        const search = `${user.name || user.username || ""} ${user.email || ""} ${user.code || ""}`.toLowerCase()
        return search.includes(normalizedQuery)
      })

  renderRespTable(filtered, KB.respUsers.length)
}

function selectResponsavel(user) {
  KB.selectedResponsavel = user
  const label = document.getElementById("responsavelBtnLabel")
  if (label) label.textContent = user.name
  document.getElementById("err-responsavel")?.classList.add("hidden")
  closeRespDrawer()
}

// =============================================================================
// Normalization and helpers
// =============================================================================

function normalizeWorkOrderList(data) {
  const items = Array.isArray(data?.items) ? data.items : Array.isArray(data) ? data : []
  return items.map((item) => normalizeWorkOrderDetail(item))
}

function normalizeWorkOrderDetail(raw) {
  const detail = { ...(raw || {}) }
  const rawAttachments = raw?.attachments || raw?.files
  detail.id = getWorkOrderId(detail)
  detail.os_number = detail.os_number || detail.order_number || detail.id
  detail.status = detail.status || "pendente"
  detail.total_cost = toNumber(detail.total_cost ?? detail.cost_total ?? detail.cost ?? 0) || 0
  detail.responsavel_name = detail.responsavel_name || detail.assignee_name || detail.requested_by || ""
  detail.task_type = raw.task_type || raw.task_type_name || ""
  detail.classification_1 = raw.classification_1 || raw.classification_1_name || ""
  detail.classification_2 = raw.classification_2 || raw.classification_2_name || ""
  detail.criticality = raw.criticality || raw.criticality_name || "media"
  detail.task_description = raw.task_description || raw.title || ""
  detail.incident_date = raw.incident_date || raw.incident_at || null
  detail.scheduled_date = raw.scheduled_date || raw.scheduled_start_at || null
  detail.start_date = raw.start_date || raw.started_at || null
  detail.end_date = raw.end_date || raw.finished_at || raw.closed_at || null
  detail.closed_at = raw.closed_at || null
  detail.observations = raw.observations || raw.observation || ""
  detail.process_started_at = raw.process_started_at || detail.start_date || null
  detail.elapsed_execution_seconds = toNumber(raw.elapsed_execution_seconds) ?? 0
  detail._time_progress_synced_at = Date.now()
  detail.requested_by = raw.requested_by || ""
  detail.assignee_name = raw.assignee_name || raw.assigned_worker_name || raw.responsavel_name || ""
  detail.estimated_duration_minutes = toNumber(raw.estimated_duration_minutes) ?? toNumber(raw.effective_estimated_duration_minutes)
  detail.estimated_duration = raw.estimated_duration || formatMinutesToDuration(detail.estimated_duration_minutes) || ""
  detail.progress_percent = raw.progress_percent ?? raw.progress ?? detail.progress ?? 0
  detail.time_progress_percent = toNumber(raw.time_progress_percent) ?? 0
  detail.asset_failed = !!(raw.asset_failed ?? raw.is_asset_failed)
  detail.caused_interruption = !!raw.caused_interruption
  detail.caused_interruption_duration = raw.caused_interruption_duration || formatMinutesToDuration(raw.interruption_duration_minutes) || ""
  detail.failure_detection_method = raw.failure_detection_method || raw.detection_method || ""
  detail.subtasks = normalizeSubtasks(detail.subtasks)
  detail.resources = normalizeResources(detail.resources)
  detail.attachments = normalizeAttachments(rawAttachments)
  detail.attachmentsLoaded = Array.isArray(rawAttachments)
  detail.tasks = normalizeTasks(detail)
  syncPrimaryTaskToDetail(detail)
  detail.progress = getProgressValue(detail)
  return detail
}

function normalizeTasks(detail) {
  const rawTasks =
    pickArray(detail.tasks) ||
    pickArray(detail.task_list) ||
    pickArray(detail.items) ||
    pickArray(detail.work_order_tasks) ||
    null

  const source = rawTasks || [detail]
  return source.map((item, index) => normalizeTask(item, detail, index)).filter(Boolean)
}

function normalizeTask(rawTask, detail, index) {
  const task = { ...(rawTask || {}) }
  const rawTaskAttachments = task.attachments
  const subtasks = normalizeSubtasks(task.subtasks ?? detail.subtasks)
  const resources = normalizeResources(task.resources ?? detail.resources)
  const attachments = normalizeAttachments(rawTaskAttachments ?? detail.attachments)
  const status = normalizeTaskStatus(task.status || task.task_status || detail.task_status || detail.status)

  return {
    id: task.id || task.task_id || `${detail.id || "task"}-${index + 1}`,
    work_order_id: detail.id,
    asset_name: task.asset_name || detail.asset_name || "Ativo sem nome",
    asset_code: task.asset_code || detail.asset_code || detail.asset_location || "",
    asset_location: task.asset_location || detail.asset_location || detail.plant_name || "",
    name: task.name || task.title || task.task_description || detail.task_description || `Tarefa ${index + 1}`,
    description: task.description || task.task_description || detail.task_description || "",
    task_type: task.task_type || task.task_type_name || detail.task_type || detail.task_type_name || "",
    criticality: task.criticality || task.criticality_name || detail.criticality || detail.criticality_name || "media",
    classification_1: task.classification_1 || task.classification_1_name || detail.classification_1 || detail.classification_1_name || "",
    classification_2: task.classification_2 || task.classification_2_name || detail.classification_2 || detail.classification_2_name || "",
    request_number: task.request_number || detail.request_number || "",
    estimated_duration: task.estimated_duration || detail.estimated_duration || "",
    scheduled_date: task.scheduled_date || detail.scheduled_date || detail.incident_date || "",
    start_date: task.start_date || detail.start_date || "",
    end_date: task.end_date || detail.end_date || "",
    procedure: task.procedure || detail.procedure || "",
    execution_logs: Array.isArray(task.execution_logs) ? task.execution_logs : Array.isArray(detail.execution_logs) ? detail.execution_logs : [],
    status,
    subtasks,
    resources,
    attachments,
    attachmentsLoaded: Array.isArray(rawTaskAttachments) || !!detail.attachmentsLoaded,
    progress: getProgressValue({ progress_percent: task.progress_percent ?? task.progress, status, subtasks })
  }
}

function normalizeSubtasks(value) {
  const items = Array.isArray(value) ? value : []
  return items
    .map((item, index) => {
      if (item == null) return null
      if (typeof item === "string") {
        return { id: `subtask-${index + 1}`, title: item, done: false }
      }

      return {
        id: item.id || item.subtask_id || `subtask-${index + 1}`,
        title: item.title || item.description || item.name || `Subtarefa ${index + 1}`,
        done: !!(item.done ?? item.completed ?? item.is_done)
      }
    })
    .filter(Boolean)
}

function normalizeResources(value) {
  const items = Array.isArray(value) ? value : []
  return items
    .map((item, index) => {
      if (item == null) return null
      if (typeof item === "string") {
        return { id: `resource-${index + 1}`, name: item, quantity: 1, status: "Planejado" }
      }

      return {
        id: item.id || item.resource_id || `resource-${index + 1}`,
        name: item.name || item.resource_name || `Recurso ${index + 1}`,
        quantity: item.quantity || item.qty || 1,
        status: item.status || "Planejado"
      }
    })
    .filter(Boolean)
}

function normalizeAttachments(value) {
  const items = Array.isArray(value) ? value : []
  return items
    .map((item, index) => {
      if (!item) return null
      const fileName = item.name || item.filename || item.file_name || `Anexo ${index + 1}`
      return {
        id: item.id || item.attachment_id || `attachment-${index + 1}`,
        name: fileName,
        note: item.note || item.description || "",
        url: item.download_url || item.url || item.file_url || "",
        download_url: item.download_url || item.url || "",
        is_available: item.is_available !== false && !!(item.download_url || item.url),
        has_file: !!(item.file_url || item.download_url || item.url),
        thumbnail_url: item.thumbnail_url || item.preview_url || (item.download_url && isImageMime(item.content_type || item.file_mime_type || "") ? item.download_url : ""),
        content_type: item.content_type || item.file_mime_type || item.mime_type || guessMimeType(fileName),
        created_at: item.created_at || item.uploaded_at || item.date || ""
      }
    })
    .filter(Boolean)
}

function cacheWorkOrder(workOrder) {
  const id = getWorkOrderId(workOrder)
  if (!id) return
  KB.workOrderCache.set(String(id), workOrder)
}

function getWorkOrderId(workOrder) {
  const value = workOrder?.id ?? workOrder?.work_order_id ?? workOrder?.os_id
  return value == null || value === "" ? null : Number(value)
}

function getFilteredTasks(detail) {
  const tasks = Array.isArray(detail?.tasks) ? detail.tasks : []
  if (KB.currentTaskFilter === "open") return tasks.filter((task) => normalizeTaskStatus(task.status) === "nao_iniciada")
  if (KB.currentTaskFilter === "running") return tasks.filter((task) => normalizeTaskStatus(task.status) === "em_andamento")
  if (KB.currentTaskFilter === "done") return tasks.filter((task) => normalizeTaskStatus(task.status) === "concluida")
  return tasks
}

function findTaskById(detail, taskId) {
  return (detail?.tasks || []).find((task) => String(task.id) === String(taskId))
}

function syncPrimaryTaskStatus(detail, status) {
  if (!detail?.tasks?.length) return
  detail.tasks[0].status = normalizeTaskStatus(status)
  detail.progress = getProgressValue(detail)
}

function syncPrimaryTaskToDetail(detail) {
  if (!detail || !Array.isArray(detail.tasks) || !detail.tasks.length) return detail

  const primaryTask = detail.tasks[0]
  detail.task_description = primaryTask.name
  detail.asset_name = primaryTask.asset_name
  detail.asset_code = primaryTask.asset_code
  detail.asset_location = primaryTask.asset_location
  detail.task_type = primaryTask.task_type
  detail.classification_1 = primaryTask.classification_1
  detail.classification_2 = primaryTask.classification_2
  detail.request_number = primaryTask.request_number
  detail.criticality = primaryTask.criticality
  detail.estimated_duration = primaryTask.estimated_duration
  detail.scheduled_date = primaryTask.scheduled_date
  detail.start_date = primaryTask.start_date
  detail.end_date = primaryTask.end_date
  detail.procedure = primaryTask.procedure
  detail.subtasks = primaryTask.subtasks
  detail.resources = primaryTask.resources
  detail.attachments = primaryTask.attachments
  detail.attachmentsLoaded = !!primaryTask.attachmentsLoaded

  const taskStatus = normalizeTaskStatus(primaryTask.status)
  if (taskStatus === "em_andamento" && detail.status === "pendente") detail.status = "em_processo"
  if (taskStatus === "concluida") detail.status = "concluida"

  detail.progress = getProgressValue({
    progress: detail.progress_percent ?? detail.progress,
    status: detail.status,
    subtasks: primaryTask.subtasks
  })
  detail.progress_percent = detail.progress
  return detail
}

function cloneWorkOrderDetail(detail) {
  return normalizeWorkOrderDetail(JSON.parse(JSON.stringify(detail || {})))
}

function buildWorkOrderPatchPayload(detail) {
  const primaryTask = detail.tasks?.[0] || {}

  return {
    asset_id: detail.asset_id || null,
    asset_name: primaryTask.asset_name || detail.asset_name || null,
    asset_code: primaryTask.asset_code || detail.asset_code || null,
    asset_type: detail.asset_type || null,
    asset_location: primaryTask.asset_location || detail.asset_location || null,
    plant_id: detail.plant_id || null,
    incident_date: detail.incident_date || null,
    requested_by: detail.requested_by || null,
    asset_failed: !!detail.asset_failed,
    is_asset_failed: !!detail.asset_failed,
    failure_type: detail.failure_type || null,
    failure_cause: detail.failure_cause || null,
    detection_method: detail.failure_detection_method || detail.detection_method || null,
    failure_detection_method: detail.failure_detection_method || null,
    failure_severity: detail.failure_severity || null,
    damage_type: detail.damage_type || null,
    caused_interruption: !!detail.caused_interruption || durationToMinutes(detail.caused_interruption_duration) > 0,
    interruption_duration_minutes: durationToMinutes(detail.caused_interruption_duration) || 0,
    caused_interruption_duration: detail.caused_interruption_duration || null,
    back_to_service: !!detail.back_to_service,
    ...(primaryTask.name || detail.task_description
        ? { task_description: primaryTask.name || detail.task_description }
        : {}),
    observations: detail.observations || "",
    task_type: primaryTask.task_type || detail.task_type || null,
    classification_1: primaryTask.classification_1 || detail.classification_1 || null,
    classification_2: primaryTask.classification_2 || detail.classification_2 || null,
    criticality: primaryTask.criticality || detail.criticality || "media",
    estimated_duration: primaryTask.estimated_duration || detail.estimated_duration || null,
    request_number: primaryTask.request_number || detail.request_number || null,
    responsavel_id: detail.responsavel_id || detail.assignee_id || null,
    responsavel_name: detail.responsavel_name || null,
    responsavel_email: detail.responsavel_email || null,
    status: detail.status || "pendente",
    scheduled_date: detail.scheduled_date || null,
    start_date: detail.start_date || null,
    end_date: detail.end_date || null,
    total_cost: detail.total_cost ?? null,
    subtasks: serializeSubtasks(primaryTask.subtasks || detail.subtasks),
    resources: serializeResources(primaryTask.resources || detail.resources),
    tasks: serializeTasks(detail.tasks || [])
  }
}

function serializeTasks(tasks) {
  return (tasks || []).map((task) => ({
    id: task.id,
    asset_name: task.asset_name || null,
    asset_code: task.asset_code || null,
    asset_location: task.asset_location || null,
    name: task.name || null,
    task_description: task.name || null,
    task_type: task.task_type || null,
    criticality: task.criticality || null,
    classification_1: task.classification_1 || null,
    classification_2: task.classification_2 || null,
    request_number: task.request_number || null,
    estimated_duration: task.estimated_duration || null,
    scheduled_date: task.scheduled_date || null,
    start_date: task.start_date || null,
    end_date: task.end_date || null,
    procedure: task.procedure || "",
    status: normalizeTaskStatus(task.status),
    subtasks: serializeSubtasks(task.subtasks),
    resources: serializeResources(task.resources),
    execution_logs: Array.isArray(task.execution_logs) ? task.execution_logs : []
  }))
}

function serializeSubtasks(subtasks) {
  return (subtasks || []).map((item) => ({ id: item.id, title: item.title, done: !!item.done }))
}

function serializeResources(resources) {
  return (resources || []).map((item) => ({
    id: item.id,
    name: item.name,
    quantity: item.quantity || 1,
    status: item.status || "Planejado"
  }))
}

function getProcessStartAt(wo) {
  return wo.process_started_at || wo.started_at || wo.start_date || null
}

function getElapsedSeconds(wo) {
  const rawElapsed = toNumber(wo?.elapsed_execution_seconds)
  if (rawElapsed != null && ["em_processo", "em_verificacao"].includes(wo.status)) {
    const syncedAt = toNumber(wo?._time_progress_synced_at) || Date.now()
    const drift = Math.max(0, Math.round((Date.now() - syncedAt) / 1000))
    return Math.max(0, Math.round(rawElapsed + drift))
  }

  const start = getProcessStartAt(wo)
  if (!start || !["em_processo", "em_verificacao"].includes(wo.status)) return 0
  const startTime = new Date(start).getTime()
  if (Number.isNaN(startTime)) return 0
  return Math.max(0, Math.round((Date.now() - startTime) / 1000))
}

function getTimeProgressPercent(wo) {
  const raw = toNumber(wo?.time_progress_percent)
  const mins = toNumber(wo?.estimated_duration_minutes) ?? toNumber(wo?.effective_estimated_duration_minutes) ?? durationToMinutes(wo?.estimated_duration)
  if (!mins || mins <= 0) return raw != null ? clamp(Math.round(raw), 0, 100) : 0
  const elapsed = getElapsedSeconds(wo) / 60
  if (elapsed <= 0 && raw != null) return clamp(Math.round(raw), 0, 100)
  return clamp(Math.round((elapsed / mins) * 100), 0, 100)
}

function getCardProgressValue(source) {
  const status = source?.status || "pendente"
  if (status === "concluida") return 100
  if (status === "em_processo" || status === "em_verificacao") {
    return getTimeProgressPercent(source)
  }
  return getProgressValue(source)
}

function formatElapsed(seconds) {
  const h = Math.floor(seconds / 3600)
  const m = Math.floor((seconds % 3600) / 60)
  const s = seconds % 60
  if (h > 0) return `${String(h).padStart(2, "0")}:${String(m).padStart(2, "0")}:${String(s).padStart(2, "0")}`
  return `${String(m).padStart(2, "0")}:${String(s).padStart(2, "0")}`
}

function getProgressValue(source) {
  const status = normalizeTaskStatus(source?.status)

  if (status === "nao_iniciada") return 0
  if (status === "concluida") return 100

  const raw = toNumber(source?.progress_percent ?? source?.progress)
  if (raw != null) return clamp(Math.round(raw), 0, 100)

  const subtasks = Array.isArray(source?.subtasks) ? source.subtasks : []
  if (subtasks.length) {
    const completed = subtasks.filter((item) => item.done).length
    return clamp(Math.round((completed / subtasks.length) * 100), 0, 100)
  }

  return 0
}

function normalizeTaskStatus(status) {
  const value = safeLower(status)
  if (value === "em_processo" || value === "em processo" || value === "em andamento" || value === "em_andamento") return "em_andamento"
  if (value === "em_verificacao" || value === "em verificacao") return "em_verificacao"
  if (value === "concluida" || value === "concluido") return "concluida"
  if (value === "cancelada" || value === "cancelado") return "cancelada"
  if (value === "nao iniciada" || value === "nao_iniciada" || value === "pendente") return "nao_iniciada"
  return "nao_iniciada"
}

function getStatusColor(status) {
  if (status === "pendente") return "#f59e0b"
  if (status === "em_processo") return "#3b82f6"
  if (status === "em_verificacao") return "#a855f7"
  if (status === "concluida") return "#2aff7b"
  if (status === "cancelada") return "#ef4444"
  return "transparent"
}

function isOverdue(workOrder) {
  if (!workOrder?.scheduled_date || workOrder?.status === "concluida" || workOrder?.status === "cancelada") return false
  const target = new Date(workOrder.scheduled_date)
  if (Number.isNaN(target.getTime())) return false
  return target.getTime() < Date.now()
}

function computeExecutionTime(task) {
  if (!task) return "---"

  if (task.start_date && task.end_date) {
    const start = new Date(task.start_date)
    const end = new Date(task.end_date)
    const diffMinutes = Math.max(0, Math.round((end.getTime() - start.getTime()) / 60000))
    return minutesToHourLabel(diffMinutes)
  }

  const totalLoggedMinutes = (task.execution_logs || []).reduce(
    (total, item) => total + durationToMinutes(item.duration),
    0
  )
  return totalLoggedMinutes > 0 ? minutesToHourLabel(totalLoggedMinutes) : "---"
}

function formatMinutesToDuration(minutes) {
  if (!minutes && minutes !== 0) return ""
  const h = Math.floor(minutes / 60)
  const m = minutes % 60
  return `${String(h).padStart(3, "0")}:${String(m).padStart(2, "0")}`
}

function formatDurationCompact(value) {
  const minutes = durationToMinutes(value)
  if (!minutes && minutes !== 0) return "00:10"
  return minutesToHourLabel(minutes, false)
}

function formatDurationLong(value) {
  const minutes = durationToMinutes(value)
  if (!minutes && minutes !== 0) return "00:10:00"
  const hours = Math.floor(minutes / 60)
  const mins = minutes % 60
  return `${String(hours).padStart(2, "0")}:${String(mins).padStart(2, "0")}:00`
}

function durationToMinutes(value) {
  if (value == null || value === "") return null
  if (typeof value === "number" && Number.isFinite(value)) return value

  const raw = String(value).trim()
  if (!raw) return null
  const parts = raw.split(":").map((part) => Number(part))
  if (parts.some((part) => Number.isNaN(part))) return null

  if (parts.length === 2) {
    const [hours, minutes] = parts
    return hours * 60 + minutes
  }

  if (parts.length >= 3) {
    const [hours, minutes, seconds] = parts
    return hours * 60 + minutes + Math.round((seconds || 0) / 60)
  }

  return null
}

function minutesToHourLabel(minutes) {
  const safeMinutes = Math.max(0, Number(minutes) || 0)
  const hours = Math.floor(safeMinutes / 60)
  const mins = safeMinutes % 60
  return `${String(hours).padStart(2, "0")}:${String(mins).padStart(2, "0")}`
}

function formatCriticality(value) {
  const normalized = safeLower(value)
  if (normalized === "muito alta" || normalized === "muito_alta" || normalized === "muito alto" || normalized === "critica" || normalized === "critico") {
    return { key: "muito_alta", label: "Muito alto" }
  }
  if (normalized === "alta") return { key: "alta", label: "Alto" }
  if (normalized === "baixa") return { key: "baixa", label: "Baixo" }
  return { key: "media", label: "M\u00e9dio" }
}

function resolveUserWeekHoursValue(user, dayColumn) {
  const sources = [
    user?.week_hours,
    user?.weekHours,
    user?.hours_by_day,
    user?.hoursByDay,
    user?.availability,
    user?.schedule,
    user?.working_hours,
    user?.workingHours
  ].filter(Boolean)

  for (const source of sources) {
    const resolved = resolveWeekHoursFromSource(source, dayColumn.aliases)
    if (resolved != null) return resolved
  }

  return null
}

function resolveWeekHoursFromSource(source, aliases) {
  if (Array.isArray(source)) {
    for (const item of source) {
      const dayKey = safeLower(
        item?.day || item?.weekday || item?.week_day || item?.label || item?.name || item?.title || item?.key
      )
      if (!aliases.includes(dayKey)) continue
      return normalizeWeekHoursValue(
        item?.hours ?? item?.value ?? item?.duration ?? item?.total ?? item?.label_value ?? item?.time ?? null
      )
    }
    return null
  }

  if (source && typeof source === "object") {
    for (const [key, value] of Object.entries(source)) {
      if (!aliases.includes(safeLower(key))) continue
      return normalizeWeekHoursValue(value)
    }
  }

  return null
}

function normalizeWeekHoursValue(value) {
  if (value == null) return null

  if (Array.isArray(value)) {
    const joined = value.map((item) => normalizeWeekHoursValue(item)).filter(Boolean).join(" | ")
    return joined || null
  }

  if (typeof value === "object") {
    return normalizeWeekHoursValue(value.hours ?? value.value ?? value.duration ?? value.total ?? value.label ?? value.time ?? null)
  }

  const text = String(value).trim()
  return text || null
}

function hasHoursBadgeValue(value) {
  const normalized = safeLower(value)
  if (!normalized) return false
  return !["0", "0h", "00:00", "00:00:00", "sem horas", "sem hora", "null", "-", "---"].includes(normalized)
}

function fmtDate(value) {
  if (!value) return "---"
  try {
    return new Date(value).toLocaleDateString("pt-BR", { day: "2-digit", month: "2-digit", year: "2-digit" })
  } catch (_error) {
    return "---"
  }
}

function fmtDateLong(value) {
  if (!value) return "---"
  try {
    return new Date(value).toLocaleDateString("pt-BR", { year: "numeric", month: "2-digit", day: "2-digit" })
  } catch (_error) {
    return "---"
  }
}

function fmtDatetime(value) {
  if (!value) return "---"
  try {
    return new Date(value).toLocaleString("pt-BR", {
      day: "2-digit",
      month: "2-digit",
      year: "numeric",
      hour: "2-digit",
      minute: "2-digit"
    })
  } catch (_error) {
    return "---"
  }
}

function toDatetimeLocalInputValue(value) {
  if (!value) return ""
  const date = new Date(value)
  if (Number.isNaN(date.getTime())) return String(value).slice(0, 16)
  const tzOffset = date.getTimezoneOffset() * 60000
  return new Date(date.getTime() - tzOffset).toISOString().slice(0, 16)
}

function readDatetimeLocalValue(id) {
  const value = document.getElementById(id)?.value || ""
  return value || null
}

function formatCurrencyBRL(value) {
  const amount = toNumber(value) || 0
  return new Intl.NumberFormat("pt-BR", { style: "currency", currency: "BRL" }).format(amount)
}

function avatarInitials(name) {
  return (name || "?")
    .split(" ")
    .filter(Boolean)
    .map((part) => part[0])
    .join("")
    .slice(0, 2)
    .toUpperCase()
}

function formatBytes(value) {
  const size = Number(value) || 0
  if (size >= 1024 * 1024) return `${(size / (1024 * 1024)).toFixed(1)} MB`
  if (size >= 1024) return `${Math.round(size / 1024)} KB`
  return `${size} B`
}

function toNumber(value) {
  if (value == null || value === "") return null
  const parsed = Number(value)
  return Number.isFinite(parsed) ? parsed : null
}

function clamp(value, min, max) {
  return Math.min(max, Math.max(min, value))
}

function getBoardSearch() {
  return document.getElementById("kbSearchInput")?.value?.trim() || ""
}

function safeLower(value) {
  return String(value || "")
    .trim()
    .toLowerCase()
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
}

function pickArray(value) {
  return Array.isArray(value) && value.length ? value : null
}

function isImageMime(mime) {
  return String(mime || "").toLowerCase().startsWith("image/")
}

function isImageAttachment(attachment) {
  const contentType = String(attachment?.content_type || "").toLowerCase()
  const name = String(attachment?.name || "").toLowerCase()
  return contentType.startsWith("image/") || /\.(png|jpg|jpeg|gif|webp|bmp|svg)$/.test(name)
}

function guessMimeType(name) {
  const lower = String(name || "").toLowerCase()
  if (/\.(png|jpg|jpeg|gif|webp|bmp|svg)$/.test(lower)) return "image/*"
  if (/\.pdf$/.test(lower)) return "application/pdf"
  return "application/octet-stream"
}

function esc(value) {
  if (value == null) return ""
  return String(value)
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
}

function escAttr(value) {
  return esc(value).replace(/'/g, "&#39;")
}

let toastTimer = null
function showToast(message, type = "success") {
  const toast = document.getElementById("osToast")
  const icon = document.getElementById("osToastIcon")
  const text = document.getElementById("osToastText")
  if (!toast || !icon || !text) return

  const iconByType = {
    success: "fa-solid fa-circle-check",
    error: "fa-solid fa-circle-xmark",
    info: "fa-solid fa-circle-info"
  }

  icon.innerHTML = `<i class="${iconByType[type] || iconByType.success}"></i>`
  text.textContent = message
  toast.className = `os-toast os-toast--${type}`

  clearTimeout(toastTimer)
  toastTimer = setTimeout(() => toast.classList.add("hidden"), 3500)
}

// =============================================================================
// AUTOMACAO DE OS â€” corretivas automaticas + preventivas recorrentes
// (modal do botao robo na topbar; backend: GET/POST /os-automation;
//  quem cria as OSs e o os_automator.py no cron da EC2)
// =============================================================================

const AUTO_OS = { config: null, plans: [], plantsLoaded: false }

function autoOsShow(show) {
  const ov = document.getElementById("autoOsOverlay")
  if (ov) ov.style.display = show ? "flex" : "none"
}

function autoOsFeedback(msg, ok = true) {
  const el = document.getElementById("autoOsFeedback")
  if (!el) return
  el.style.display = "block"
  el.style.background = ok ? "rgba(57,229,140,.12)" : "rgba(248,113,113,.12)"
  el.style.color = ok ? "#39e58c" : "#f87171"
  el.textContent = msg
  setTimeout(() => { el.style.display = "none" }, 4000)
}

async function autoOsLoadPlants() {
  if (AUTO_OS.plantsLoaded) return
  try {
    const data = await apiJson("/os-plants")
    const sel = document.getElementById("autoPlanPlant")
    if (sel && Array.isArray(data?.items)) {
      const opts = data.items.map(p => `<option value="${esc(String(p.id))}">${esc(p.name)}</option>`).join("")
      sel.innerHTML = `<option value="">Todas as usinas</option>` + opts
      AUTO_OS.plantsLoaded = true
    }
  } catch (e) { console.warn("[autoOs] os-plants:", e?.message || e) }
}

async function autoOsLoad() {
  const data = await apiJson("/os-automation")
  AUTO_OS.config = data?.config || {}
  AUTO_OS.plans  = data?.plans || []

  const c = AUTO_OS.config
  const set = (id, v) => { const el = document.getElementById(id); if (el) el.checked = !!v }
  set("autoCfgCorrective",   c.auto_corrective)
  set("autoCfgTrigShutdown", c.trigger_plant_shutdown)
  set("autoCfgTrigNoComm",   c.trigger_no_comm)
  set("autoCfgTrigRelay",    c.trigger_relay_flags)
  set("autoCfgPreventive",   c.auto_preventive)
  autoOsRenderPlans()
}

function autoOsRenderPlans() {
  const box = document.getElementById("autoPlansList")
  if (!box) return
  if (!AUTO_OS.plans.length) {
    box.innerHTML = `<div style="color:rgba(255,255,255,.35);font-size:12px;font-style:italic;">Nenhum plano preventivo cadastrado.</div>`
    return
  }
  box.innerHTML = AUTO_OS.plans.map(p => `
    <div style="display:flex;align-items:center;gap:10px;background:rgba(255,255,255,.03);border:1px solid rgba(255,255,255,.06);border-radius:9px;padding:8px 12px;">
      <div style="flex:1;min-width:0;">
        <div style="color:#eafff3;font-size:12.5px;font-weight:600;">${esc(p.title)}</div>
        <div style="color:rgba(255,255,255,.45);font-size:11px;">
          ${esc(p.plant_name || "Todas as usinas")} Â· a cada ${esc(String(p.frequency_days))}d Â·
          prÃ³x.: ${esc(String(p.next_due_date).slice(0, 10))} Â· cria ${esc(String(p.advance_days))}d antes
        </div>
      </div>
      <button data-auto-edit="${esc(String(p.id))}" title="Editar" style="background:none;border:none;color:#9adbb8;cursor:pointer;font-size:13px;"><i class="fa-solid fa-pen"></i></button>
      <button data-auto-del="${esc(String(p.id))}" title="Excluir" style="background:none;border:none;color:#f87171;cursor:pointer;font-size:13px;"><i class="fa-solid fa-trash"></i></button>
    </div>
  `).join("")

  box.querySelectorAll("[data-auto-edit]").forEach(btn => btn.addEventListener("click", () => {
    const plan = AUTO_OS.plans.find(p => String(p.id) === btn.dataset.autoEdit)
    if (!plan) return
    document.getElementById("autoPlanId").value      = plan.id
    document.getElementById("autoPlanTitle").value   = plan.title || ""
    document.getElementById("autoPlanPlant").value   = plan.power_plant_id != null ? String(plan.power_plant_id) : ""
    document.getElementById("autoPlanDesc").value    = plan.description || ""
    document.getElementById("autoPlanFreq").value    = plan.frequency_days
    document.getElementById("autoPlanDue").value     = String(plan.next_due_date).slice(0, 10)
    document.getElementById("autoPlanAdvance").value = plan.advance_days
    document.getElementById("autoPlanFormTitle").textContent = `Editando plano #${plan.id}`
    document.getElementById("autoPlanCancelEdit").style.display = ""
  }))

  box.querySelectorAll("[data-auto-del]").forEach(btn => btn.addEventListener("click", async () => {
    if (!confirm("Excluir este plano preventivo? (as OSs jÃ¡ criadas nÃ£o sÃ£o apagadas)")) return
    try {
      await apiJson("/os-automation", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ action: "delete_plan", plan_id: Number(btn.dataset.autoDel) }),
      })
      await autoOsLoad()
      autoOsFeedback("Plano excluÃ­do.")
    } catch (e) { autoOsFeedback("Erro ao excluir: " + (e?.message || e), false) }
  }))
}

function autoOsResetPlanForm() {
  ;["autoPlanId", "autoPlanTitle", "autoPlanDesc"].forEach(id => { const el = document.getElementById(id); if (el) el.value = "" })
  document.getElementById("autoPlanPlant").value   = ""
  document.getElementById("autoPlanFreq").value    = "90"
  document.getElementById("autoPlanAdvance").value = "7"
  document.getElementById("autoPlanDue").value     = ""
  document.getElementById("autoPlanFormTitle").textContent = "Novo plano"
  document.getElementById("autoPlanCancelEdit").style.display = "none"
}

function bindAutomationModal() {
  const openBtn = document.getElementById("kbAutomationBtn")
  if (!openBtn) return

  openBtn.addEventListener("click", async () => {
    autoOsShow(true)
    try {
      await Promise.all([autoOsLoad(), autoOsLoadPlants()])
    } catch (e) {
      autoOsFeedback("Erro ao carregar automaÃ§Ã£o: " + (e?.message || e), false)
    }
  })

  document.getElementById("autoOsClose")?.addEventListener("click", () => autoOsShow(false))
  document.getElementById("autoOsCancel")?.addEventListener("click", () => autoOsShow(false))
  document.getElementById("autoOsOverlay")?.addEventListener("click", (e) => {
    if (e.target === document.getElementById("autoOsOverlay")) autoOsShow(false)
  })
  document.getElementById("autoPlanCancelEdit")?.addEventListener("click", autoOsResetPlanForm)

  document.getElementById("autoOsSaveConfig")?.addEventListener("click", async () => {
    const val = id => !!document.getElementById(id)?.checked
    try {
      await apiJson("/os-automation", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          action: "save_config",
          user_id: getCurrentUser().id || null,
          config: {
            auto_corrective:        val("autoCfgCorrective"),
            trigger_plant_shutdown: val("autoCfgTrigShutdown"),
            trigger_no_comm:        val("autoCfgTrigNoComm"),
            trigger_relay_flags:    val("autoCfgTrigRelay"),
            auto_preventive:        val("autoCfgPreventive"),
          },
        }),
      })
      showToast("AutomaÃ§Ã£o salva com sucesso")
      autoOsShow(false)
    } catch (e) { autoOsFeedback("Erro ao salvar: " + (e?.message || e), false) }
  })

  document.getElementById("autoPlanSave")?.addEventListener("click", async () => {
    const g = id => document.getElementById(id)?.value?.trim() || ""
    const plan = {
      id: g("autoPlanId") || null,
      title: g("autoPlanTitle"),
      power_plant_id: g("autoPlanPlant") ? Number(g("autoPlanPlant")) : null,
      description: g("autoPlanDesc") || null,
      frequency_days: Number(g("autoPlanFreq")),
      advance_days: Number(g("autoPlanAdvance") || 7),
      next_due_date: g("autoPlanDue"),
    }
    if (!plan.title)         return autoOsFeedback("Informe o tÃ­tulo do plano.", false)
    if (!plan.frequency_days || plan.frequency_days < 1) return autoOsFeedback("FrequÃªncia invÃ¡lida (dias >= 1).", false)
    if (!plan.next_due_date) return autoOsFeedback("Informe o prÃ³ximo vencimento.", false)
    try {
      await apiJson("/os-automation", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ action: "save_plan", user_id: getCurrentUser().id || null, plan }),
      })
      autoOsResetPlanForm()
      await autoOsLoad()
      autoOsFeedback("Plano salvo.")
    } catch (e) { autoOsFeedback("Erro ao salvar plano: " + (e?.message || e), false) }
  })
}

if (document.readyState === "loading") {
  document.addEventListener("DOMContentLoaded", bindAutomationModal)
} else {
  bindAutomationModal()
}
