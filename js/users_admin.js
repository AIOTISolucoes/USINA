// =============================================================================
// GESTÃO DE USUÁRIOS POR EMPRESA (autocontido, padrão branding.js)
// - Botão na sidebar visível só para admin_customer / superuser
// - Usa GET/POST/PATCH /users do api2.py (exigem token de sessão do login)
// - admin_customer só enxerga/gerencia usuários do próprio cliente
// =============================================================================
(function usersAdmin() {
  "use strict";

  const API = (typeof API_BASE !== "undefined")
    ? API_BASE
    : "https://jgeg9i0js1.execute-api.us-east-1.amazonaws.com";

  const ROLE_LABELS = {
    admin_customer: "Administrador",
    operator: "Operador",
    viewer: "Visualizador"
  };
  const PERM_FIELDS = [
    ["plant_edit", "Editar usinas (cadastro, capacidade, manutenção)"],
    ["device_edit", "Editar dispositivos/equipamentos"],
    ["remote_command", "Enviar comandos remotos"],
    ["string_config_edit", "Configurar strings"]
  ];

  function getUser() {
    try { return JSON.parse(localStorage.getItem("user") || "{}"); } catch { return {}; }
  }
  function isAdmin(u) {
    return u.is_superuser === true || u.role_key === "admin_customer" ||
      (u.permissions && u.permissions.admin_customer === true);
  }

  async function api(path, options = {}) {
    const u = getUser();
    const headers = { "Content-Type": "application/json", ...(options.headers || {}) };
    if (u.customer_id) headers["X-Customer-Id"] = u.customer_id;
    if (u.is_superuser === true) headers["X-Is-Superuser"] = "true";
    if (u.username) headers["X-Username"] = u.username;
    if (u.id) headers["X-User-Id"] = u.id;
    if (u.token) headers["Authorization"] = `Bearer ${u.token}`;
    const res = await fetch(`${API}${path}`, { ...options, headers, cache: "no-store" });
    let data = {};
    try { data = await res.json(); } catch { /* corpo vazio */ }
    if (!res.ok) {
      const err = new Error(data.error || `HTTP ${res.status}`);
      err.code = data.code || null;
      throw err;
    }
    return data;
  }

  // ---------------------------------------------------------------------------
  // estado
  // ---------------------------------------------------------------------------
  const S = {
    users: [],
    customers: null,       // só superuser
    customerFilter: "",    // só superuser
    plants: [],            // p/ allowed_plant_ids
    editingId: null        // null = criando
  };

  // ---------------------------------------------------------------------------
  // estilos + esqueleto do modal
  // ---------------------------------------------------------------------------
  function injectStyles() {
    if (document.getElementById("uaStyles")) return;
    const st = document.createElement("style");
    st.id = "uaStyles";
    st.textContent = `
      .ua-overlay{position:fixed;inset:0;z-index:9990;background:rgba(4,14,10,.72);
        backdrop-filter:blur(3px);display:flex;align-items:center;justify-content:center;padding:18px;}
      .ua-modal{width:min(880px,100%);max-height:92vh;display:flex;flex-direction:column;
        background:var(--bg-card,#0d1f17);border:1px solid rgba(57,229,140,.22);border-radius:14px;
        color:var(--text-main,#e6f2ea);font-family:inherit;box-shadow:0 18px 60px rgba(0,0,0,.5);}
      .ua-head{display:flex;align-items:center;gap:10px;padding:14px 18px;
        border-bottom:1px solid rgba(57,229,140,.14);}
      .ua-head i{color:var(--accent,#39e58c);}
      .ua-head h2{margin:0;font-size:1rem;flex:1;}
      .ua-close{background:none;border:none;color:inherit;font-size:1.1rem;cursor:pointer;opacity:.75;}
      .ua-close:hover{opacity:1;}
      .ua-body{padding:14px 18px;overflow-y:auto;}
      .ua-toolbar{display:flex;gap:10px;align-items:center;margin-bottom:12px;flex-wrap:wrap;}
      .ua-toolbar select{background:rgba(255,255,255,.05);border:1px solid rgba(57,229,140,.25);
        color:inherit;border-radius:8px;padding:6px 10px;font-size:.8rem;}
      .ua-toolbar select option,.ua-field select option{background:#0d1f17;color:#e6f2ea;}
      .ua-btn{border:1px solid rgba(57,229,140,.35);background:rgba(57,229,140,.12);
        color:var(--accent,#39e58c);border-radius:8px;padding:7px 14px;font-size:.8rem;
        cursor:pointer;font-weight:600;}
      .ua-btn:hover{background:rgba(57,229,140,.2);}
      .ua-btn.ua-ghost{background:none;color:inherit;border-color:rgba(255,255,255,.18);}
      .ua-table{width:100%;border-collapse:collapse;font-size:.8rem;}
      .ua-table th{text-align:left;padding:7px 8px;color:var(--text-muted,#7fa892);
        font-weight:600;font-size:.7rem;text-transform:uppercase;letter-spacing:.04em;
        border-bottom:1px solid rgba(57,229,140,.14);}
      .ua-table td{padding:8px;border-bottom:1px solid rgba(255,255,255,.05);vertical-align:middle;}
      .ua-muted{color:var(--text-muted,#7fa892);font-size:.72rem;}
      .ua-pill{display:inline-block;padding:2px 9px;border-radius:99px;font-size:.68rem;font-weight:600;}
      .ua-pill.admin{background:rgba(255,196,77,.14);color:#ffc44d;}
      .ua-pill.operator{background:rgba(77,163,255,.14);color:#4da3ff;}
      .ua-pill.viewer{background:rgba(255,255,255,.08);color:var(--text-muted,#9fb8ab);}
      .ua-pill.su{background:rgba(240,98,146,.16);color:#f06292;}
      .ua-pill.off{background:rgba(255,107,107,.14);color:#ff6b6b;}
      .ua-pill.on{background:rgba(57,229,140,.14);color:var(--accent,#39e58c);}
      .ua-form{display:grid;grid-template-columns:1fr 1fr;gap:12px;}
      .ua-form .ua-full{grid-column:1/-1;}
      .ua-field label{display:block;font-size:.7rem;color:var(--text-muted,#7fa892);
        margin-bottom:4px;font-weight:600;}
      .ua-field input[type=text],.ua-field input[type=email],.ua-field input[type=password],
      .ua-field select{width:100%;box-sizing:border-box;background:rgba(255,255,255,.05);
        border:1px solid rgba(57,229,140,.25);color:inherit;border-radius:8px;
        padding:8px 10px;font-size:.82rem;}
      .ua-field input:disabled{opacity:.55;}
      .ua-checks{display:flex;flex-direction:column;gap:6px;font-size:.8rem;}
      .ua-checks label{display:flex;align-items:center;gap:8px;cursor:pointer;font-weight:normal;}
      .ua-plants{max-height:150px;overflow-y:auto;border:1px solid rgba(57,229,140,.18);
        border-radius:8px;padding:8px 10px;margin-top:6px;}
      .ua-error{background:rgba(255,107,107,.12);border:1px solid rgba(255,107,107,.3);
        color:#ff9b9b;border-radius:8px;padding:9px 12px;font-size:.78rem;margin-bottom:10px;}
      .ua-actions{display:flex;gap:10px;justify-content:flex-end;margin-top:14px;}
      .ua-selfnote{font-size:.72rem;color:var(--text-muted,#7fa892);font-style:italic;}
      @media (max-width:640px){.ua-form{grid-template-columns:1fr;}}
    `;
    document.head.appendChild(st);
  }

  function buildModal() {
    if (document.getElementById("uaOverlay")) return;
    const ov = document.createElement("div");
    ov.id = "uaOverlay";
    ov.className = "ua-overlay";
    ov.style.display = "none";
    ov.innerHTML = `
      <div class="ua-modal" role="dialog" aria-label="Gestão de usuários">
        <div class="ua-head">
          <i class="fa-solid fa-users-gear"></i>
          <h2 id="uaTitle">Usuários da empresa</h2>
          <button class="ua-close" id="uaClose" aria-label="Fechar">&times;</button>
        </div>
        <div class="ua-body" id="uaBody"></div>
      </div>`;
    document.body.appendChild(ov);
    ov.addEventListener("click", (e) => { if (e.target === ov) closeModal(); });
    ov.querySelector("#uaClose").addEventListener("click", closeModal);
  }

  function openModal() {
    injectStyles();
    buildModal();
    document.getElementById("uaOverlay").style.display = "flex";
    loadAndRenderList();
  }
  function closeModal() {
    const ov = document.getElementById("uaOverlay");
    if (ov) ov.style.display = "none";
  }

  // ---------------------------------------------------------------------------
  // LISTA
  // ---------------------------------------------------------------------------
  async function loadAndRenderList() {
    const body = document.getElementById("uaBody");
    body.innerHTML = `<div class="ua-muted">Carregando usuários…</div>`;
    try {
      const qs = S.customerFilter ? `?customer_id=${S.customerFilter}` : "";
      const data = await api(`/users${qs}`);
      S.users = data.users || [];
      if (data.customers) S.customers = data.customers;
      renderList();
    } catch (err) {
      renderLoadError(err);
    }
  }

  function renderLoadError(err) {
    const body = document.getElementById("uaBody");
    let msg = err.message || "erro desconhecido";
    if (err.code === "token_required" || err.code === "token_invalid") {
      msg = "Sua sessão é de antes desta atualização e não tem o token de segurança. " +
        "Saia e faça login novamente para usar a gestão de usuários.";
    }
    body.innerHTML = `<div class="ua-error">${msg}</div>
      <button class="ua-btn ua-ghost" id="uaRetry">Tentar de novo</button>`;
    body.querySelector("#uaRetry").addEventListener("click", loadAndRenderList);
  }

  function renderList() {
    const me = getUser();
    const body = document.getElementById("uaBody");
    document.getElementById("uaTitle").textContent = "Usuários da empresa";

    let custFilter = "";
    if (me.is_superuser && S.customers) {
      const opts = S.customers.map(c =>
        `<option value="${c.id}" ${String(S.customerFilter) === String(c.id) ? "selected" : ""}>${c.name}</option>`).join("");
      custFilter = `<select id="uaCustFilter" title="Filtrar por cliente">
        <option value="">Todos os clientes</option>${opts}</select>`;
    }

    const rows = S.users.map(u => {
      const role = u.is_superuser
        ? `<span class="ua-pill su">Superuser</span>`
        : `<span class="ua-pill ${u.role_key}">${ROLE_LABELS[u.role_key] || u.role_key}</span>`;
      const status = u.is_active
        ? `<span class="ua-pill on">Ativo</span>`
        : `<span class="ua-pill off">Inativo</span>`;
      const plantScope = (u.permissions && Array.isArray(u.permissions.allowed_plant_ids) &&
        u.permissions.allowed_plant_ids.length)
        ? `${u.permissions.allowed_plant_ids.length} usina(s)` : "Todas";
      const custCol = me.is_superuser ? `<td>${u.customer_name || u.customer_id || "—"}</td>` : "";
      return `<tr>
        <td><strong>${u.full_name || u.username}</strong>
          <div class="ua-muted">${u.username} · ${u.email || ""}</div></td>
        ${custCol}
        <td>${role}</td>
        <td>${plantScope}</td>
        <td>${status}</td>
        <td style="text-align:right">
          <button class="ua-btn ua-ghost" data-edit="${u.id}" title="Editar">
            <i class="fa-solid fa-pen"></i></button>
        </td>
      </tr>`;
    }).join("");

    body.innerHTML = `
      <div class="ua-toolbar">
        ${custFilter}
        <span style="flex:1"></span>
        <button class="ua-btn" id="uaNew"><i class="fa-solid fa-user-plus"></i> Novo usuário</button>
      </div>
      <table class="ua-table">
        <thead><tr>
          <th>Usuário</th>${me.is_superuser ? "<th>Cliente</th>" : ""}
          <th>Papel</th><th>Usinas</th><th>Status</th><th></th>
        </tr></thead>
        <tbody>${rows || `<tr><td colspan="6" class="ua-muted">Nenhum usuário encontrado.</td></tr>`}</tbody>
      </table>`;

    body.querySelector("#uaNew").addEventListener("click", () => renderForm(null));
    body.querySelectorAll("[data-edit]").forEach(btn =>
      btn.addEventListener("click", () => {
        const u = S.users.find(x => String(x.id) === btn.getAttribute("data-edit"));
        if (u) renderForm(u);
      }));
    const cf = body.querySelector("#uaCustFilter");
    if (cf) cf.addEventListener("change", () => {
      S.customerFilter = cf.value;
      loadAndRenderList();
    });
  }

  // ---------------------------------------------------------------------------
  // FORM (criar / editar)
  // ---------------------------------------------------------------------------
  async function ensurePlants() {
    if (S.plants.length) return;
    try {
      const res = await api("/plants");
      const arr = Array.isArray(res) ? res : (res.plants || res.items || []);
      S.plants = arr.map(p => {
        const id = p.power_plant_id ?? p.id;
        return {
          id,
          name: p.display_name || p.power_plant_name || p.name || p.plant_name || `Usina ${id}`,
          customer_id: p.customer_id
        };
      }).filter(p => p.id != null);
    } catch { S.plants = []; }
  }

  async function renderForm(user) {
    const me = getUser();
    S.editingId = user ? user.id : null;
    const editingSelf = user && String(user.id) === String(me.id);
    const body = document.getElementById("uaBody");
    document.getElementById("uaTitle").textContent = user
      ? `Editar: ${user.full_name || user.username}` : "Novo usuário";
    body.innerHTML = `<div class="ua-muted">Carregando…</div>`;
    await ensurePlants();

    const perms = (user && user.permissions) || {};
    const allowed = Array.isArray(perms.allowed_plant_ids) ? perms.allowed_plant_ids : null;
    const roleSel = ["viewer", "operator", "admin_customer"].map(r =>
      `<option value="${r}" ${user && user.role_key === r ? "selected" : ""}>${ROLE_LABELS[r]}</option>`).join("");

    let custSel = "";
    if (me.is_superuser && !user && S.customers) {
      custSel = `<div class="ua-field"><label>Cliente</label>
        <select id="uaFCustomer">${S.customers.map(c =>
          `<option value="${c.id}">${c.name}</option>`).join("")}</select></div>`;
    }

    const permChecks = PERM_FIELDS.map(([key, label]) =>
      `<label><input type="checkbox" data-perm="${key}" ${perms[key] === true ? "checked" : ""}> ${label}</label>`).join("");

    const restricted = editingSelf
      ? `<div class="ua-full ua-selfnote">Você está editando o próprio usuário: papel, permissões e
         status só podem ser alterados por outro administrador (trava de segurança).</div>`
      : `
      <div class="ua-field"><label>Papel</label>
        <select id="uaFRole">${roleSel}</select></div>
      <div class="ua-field"><label>Status</label>
        <select id="uaFActive">
          <option value="true" ${!user || user.is_active ? "selected" : ""}>Ativo</option>
          <option value="false" ${user && !user.is_active ? "selected" : ""}>Inativo</option>
        </select></div>
      <div class="ua-field ua-full"><label>Permissões extras</label>
        <div class="ua-checks">${permChecks}</div></div>
      <div class="ua-field ua-full">
        <label><input type="checkbox" id="uaFAllPlants" ${allowed ? "" : "checked"}>
          Acesso a todas as usinas da empresa</label>
        <div class="ua-plants" id="uaFPlants" style="display:${allowed ? "block" : "none"}"></div>
      </div>`;

    body.innerHTML = `
      <div class="ua-error" id="uaFormError" style="display:none"></div>
      <div class="ua-form">
        <div class="ua-field"><label>Nome completo</label>
          <input type="text" id="uaFName" value="${user ? (user.full_name || "") : ""}" maxlength="120"></div>
        <div class="ua-field"><label>Usuário (login)</label>
          <input type="text" id="uaFUsername" value="${user ? user.username : ""}"
            ${user ? "disabled" : ""} maxlength="40" autocomplete="off"></div>
        <div class="ua-field"><label>E-mail</label>
          <input type="email" id="uaFEmail" value="${user ? (user.email || "") : ""}" autocomplete="off"></div>
        <div class="ua-field"><label>${user ? "Nova senha (em branco = manter)" : "Senha (mín. 8)"}</label>
          <input type="password" id="uaFPassword" autocomplete="new-password"></div>
        ${custSel}
        ${restricted}
      </div>
      <div class="ua-actions">
        <button class="ua-btn ua-ghost" id="uaCancel">Voltar</button>
        <button class="ua-btn" id="uaSave">${user ? "Salvar alterações" : "Criar usuário"}</button>
      </div>`;

    // usinas do cliente-alvo (admin: o próprio; superuser: o do form/do usuário)
    const plantsBox = body.querySelector("#uaFPlants");
    const allChk = body.querySelector("#uaFAllPlants");
    function fillPlants() {
      if (!plantsBox) return;
      const custSelEl = body.querySelector("#uaFCustomer");
      const targetCid = user ? user.customer_id
        : (custSelEl ? Number(custSelEl.value) : me.customer_id);
      const list = S.plants.filter(p => p.customer_id == null || !me.is_superuser ||
        String(p.customer_id) === String(targetCid));
      plantsBox.innerHTML = list.length
        ? `<div class="ua-checks">${list.map(p =>
            `<label><input type="checkbox" data-plant="${p.id}"
              ${allowed && allowed.includes(p.id) ? "checked" : ""}> ${p.name}</label>`).join("")}</div>`
        : `<span class="ua-muted">Nenhuma usina encontrada.</span>`;
    }
    if (allChk) {
      fillPlants();
      allChk.addEventListener("change", () => {
        plantsBox.style.display = allChk.checked ? "none" : "block";
      });
      const custSelEl = body.querySelector("#uaFCustomer");
      if (custSelEl) custSelEl.addEventListener("change", fillPlants);
    }

    body.querySelector("#uaCancel").addEventListener("click", loadAndRenderList);
    body.querySelector("#uaSave").addEventListener("click", () => saveForm(user, editingSelf));
  }

  async function saveForm(user, editingSelf) {
    const body = document.getElementById("uaBody");
    const errBox = body.querySelector("#uaFormError");
    const showErr = (m) => { errBox.textContent = m; errBox.style.display = "block"; };
    errBox.style.display = "none";

    const payload = {
      full_name: body.querySelector("#uaFName").value.trim(),
      email: body.querySelector("#uaFEmail").value.trim()
    };
    const pw = body.querySelector("#uaFPassword").value;
    if (pw) payload.password = pw;

    if (!editingSelf) {
      const roleEl = body.querySelector("#uaFRole");
      if (roleEl) {
        payload.role_key = roleEl.value;
        const perms = {};
        body.querySelectorAll("[data-perm]").forEach(c => { perms[c.getAttribute("data-perm")] = c.checked; });
        payload.permissions = perms;
        const allChk = body.querySelector("#uaFAllPlants");
        if (allChk && !allChk.checked) {
          payload.allowed_plant_ids = [...body.querySelectorAll("[data-plant]:checked")]
            .map(c => Number(c.getAttribute("data-plant")));
          if (!payload.allowed_plant_ids.length) {
            return showErr("Marque ao menos uma usina, ou volte para \"todas as usinas\".");
          }
        }
        const activeEl = body.querySelector("#uaFActive");
        if (activeEl) payload.is_active = activeEl.value === "true";
      }
    }

    const saveBtn = body.querySelector("#uaSave");
    saveBtn.disabled = true;
    try {
      if (user) {
        await api(`/users/${user.id}`, { method: "PATCH", body: JSON.stringify(payload) });
      } else {
        payload.username = body.querySelector("#uaFUsername").value.trim();
        if (!pw) return showErr("Informe a senha do novo usuário (mínimo 8 caracteres).");
        const custSelEl = body.querySelector("#uaFCustomer");
        if (custSelEl) payload.customer_id = Number(custSelEl.value);
        await api("/users", { method: "POST", body: JSON.stringify(payload) });
      }
      loadAndRenderList();
    } catch (err) {
      let msg = err.message || "erro ao salvar";
      if (err.code === "token_required" || err.code === "token_invalid") {
        msg = "Sessão sem token de segurança — saia e faça login novamente.";
      }
      showErr(msg);
    } finally {
      saveBtn.disabled = false;
    }
  }

  // ---------------------------------------------------------------------------
  // botão na sidebar (mesmo slot do branding)
  // ---------------------------------------------------------------------------
  function injectSidebarButton() {
    const u = getUser();
    if (!u.username || !isAdmin(u)) return;
    const slot = document.querySelector(".sidebar-bottom");
    if (!slot || document.getElementById("btnUsersAdmin")) return;
    const btn = document.createElement("button");
    btn.id = "btnUsersAdmin";
    btn.className = "sidebar-btn";
    btn.title = "Usuários da empresa";
    btn.setAttribute("aria-label", "Usuários da empresa");
    btn.innerHTML = '<i class="fa-solid fa-users-gear"></i>';
    btn.addEventListener("click", openModal);
    slot.insertBefore(btn, slot.firstChild);
  }

  window.__openUsersAdmin = openModal; // acesso via console

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", injectSidebarButton);
  } else {
    injectSidebarButton();
  }
})();
