// =============================================================================
// CATÁLOGO DE ALARMES — edição de severidade e descrição (padrão users_admin.js)
//
// - Botão na sidebar visível só para admin_customer / superuser
// - GET  /alarms/catalog  → uma linha por código, já resolvida para o escopo
// - PUT  /alarms/catalog  → grava E corrige os alarmes já abertos
//
// Dois escopos:
//   global   → muda para TODOS os clientes. Só superusuário.
//   cliente  → grava exceção em tb_event_alarm_catalog_override e vale só
//              para as usinas daquele cliente.
//
// O que a tela mostra é sempre o valor RESOLVIDO (exceção por cima do global).
// Quando há exceção, o valor global aparece ao lado para a pessoa saber do que
// ela partiu — sem isso não dá para julgar se a exceção ainda faz sentido.
// =============================================================================
(function alarmesAdmin() {
  "use strict";

  const API = (typeof API_BASE !== "undefined")
    ? API_BASE
    : "https://jgeg9i0js1.execute-api.us-east-1.amazonaws.com";

  // A ordem aqui é a ordem de gravidade, usada no <select> e na legenda.
  const SEVERIDADES = [
    ["critical", "Crítica"],
    ["high", "Alta"],
    ["medium", "Média"],
    ["low", "Baixa"]
  ];
  const SEV_LABEL = Object.fromEntries(SEVERIDADES);
  const TIPOS = [
    ["inverter", "Inversor", 1],
    ["relay", "Relé", 2]
  ];

  function getUser() {
    try { return JSON.parse(localStorage.getItem("user") || "{}"); } catch { return {}; }
  }
  function isAdmin(u) {
    return u.is_superuser === true || u.role_key === "admin_customer" ||
      (u.permissions && u.permissions.admin_customer === true);
  }
  function esc(s) {
    return String(s == null ? "" : s)
      .replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;");
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
      err.hint = data.hint || null;
      throw err;
    }
    return data;
  }

  // ---------------------------------------------------------------------------
  // estado
  // ---------------------------------------------------------------------------
  const S = {
    tipo: "inverter",
    escopo: "",          // "" = global; "<id>" = cliente
    itens: [],
    customers: null,     // só superuser
    busca: "",
    editando: null,      // code em edição (um por vez)
    overridesOk: true,
    aviso: null          // resultado do último salvamento
  };

  // ---------------------------------------------------------------------------
  // estilos + esqueleto do modal
  // ---------------------------------------------------------------------------
  function injectStyles() {
    if (document.getElementById("acStyles")) return;
    const st = document.createElement("style");
    st.id = "acStyles";
    st.textContent = `
      .ac-overlay{position:fixed;inset:0;z-index:9990;background:rgba(4,14,10,.72);
        backdrop-filter:blur(3px);display:flex;align-items:center;justify-content:center;padding:18px;}
      .ac-modal{width:min(1020px,100%);max-height:92vh;display:flex;flex-direction:column;
        background:var(--bg-card,#0d1f17);border:1px solid rgba(57,229,140,.22);border-radius:14px;
        color:var(--text-main,#e6f2ea);font-family:inherit;box-shadow:0 18px 60px rgba(0,0,0,.5);}
      .ac-head{display:flex;align-items:center;gap:10px;padding:14px 18px;
        border-bottom:1px solid rgba(57,229,140,.14);}
      .ac-head i{color:var(--accent,#39e58c);}
      .ac-head h2{margin:0;font-size:1rem;flex:1;}
      .ac-close{background:none;border:none;color:inherit;font-size:1.1rem;cursor:pointer;opacity:.75;}
      .ac-close:hover{opacity:1;}
      .ac-body{padding:14px 18px;overflow-y:auto;}
      .ac-toolbar{display:flex;gap:10px;align-items:center;margin-bottom:6px;flex-wrap:wrap;}
      .ac-toolbar select,.ac-toolbar input{background:rgba(255,255,255,.05);
        border:1px solid rgba(57,229,140,.25);color:inherit;border-radius:8px;
        padding:6px 10px;font-size:.8rem;}
      .ac-toolbar input{min-width:180px;}
      .ac-toolbar select option{background:#0d1f17;color:#e6f2ea;}
      .ac-scope-note{font-size:.72rem;margin:0 0 12px;padding:8px 11px;border-radius:8px;
        border:1px solid rgba(255,196,77,.28);background:rgba(255,196,77,.07);color:#ffc44d;}
      .ac-scope-note.cliente{border-color:rgba(57,229,140,.25);
        background:rgba(57,229,140,.06);color:var(--accent,#39e58c);}
      .ac-btn{border:1px solid rgba(57,229,140,.35);background:rgba(57,229,140,.12);
        color:var(--accent,#39e58c);border-radius:8px;padding:6px 13px;font-size:.78rem;
        cursor:pointer;font-weight:600;}
      .ac-btn:hover{background:rgba(57,229,140,.2);}
      .ac-btn:disabled{opacity:.5;cursor:default;}
      .ac-btn.ac-ghost{background:none;color:inherit;border-color:rgba(255,255,255,.18);}
      .ac-table{width:100%;border-collapse:collapse;font-size:.8rem;table-layout:fixed;}
      .ac-table th{text-align:left;padding:7px 8px;color:var(--text-muted,#7fa892);
        font-weight:600;font-size:.68rem;text-transform:uppercase;letter-spacing:.04em;
        border-bottom:1px solid rgba(57,229,140,.14);}
      .ac-table td{padding:8px;border-bottom:1px solid rgba(255,255,255,.05);
        vertical-align:middle;word-wrap:break-word;}
      .ac-table tr.ac-editando{background:rgba(57,229,140,.05);}
      .ac-code{font-family:ui-monospace,Menlo,Consolas,monospace;font-size:.76rem;}
      .ac-muted{color:var(--text-muted,#7fa892);font-size:.72rem;}
      .ac-pill{display:inline-block;padding:2px 9px;border-radius:99px;
        font-size:.68rem;font-weight:600;white-space:nowrap;}
      .ac-pill.critical{background:rgba(213,0,0,.2);color:#ff6b6b;}
      .ac-pill.high{background:rgba(244,67,54,.16);color:#f44336;}
      .ac-pill.medium{background:rgba(255,152,0,.16);color:#ff9800;}
      .ac-pill.low{background:rgba(76,175,80,.16);color:#4caf50;}
      .ac-pill.off{background:rgba(255,255,255,.08);color:var(--text-muted,#9fb8ab);}
      .ac-pill.exc{background:rgba(77,163,255,.16);color:#4da3ff;margin-left:6px;}
      .ac-edit input[type=text],.ac-edit select{width:100%;box-sizing:border-box;
        background:rgba(255,255,255,.05);border:1px solid rgba(57,229,140,.25);
        color:inherit;border-radius:7px;padding:6px 8px;font-size:.78rem;}
      .ac-edit select option{background:#0d1f17;color:#e6f2ea;}
      .ac-acoes{display:flex;gap:6px;justify-content:flex-end;}
      .ac-aviso{margin-bottom:12px;padding:9px 12px;border-radius:9px;font-size:.76rem;
        border:1px solid rgba(57,229,140,.4);background:rgba(57,229,140,.08);}
      .ac-aviso.erro{border-color:rgba(255,107,107,.45);background:rgba(255,107,107,.09);color:#ff9b9b;}
      .ac-legenda{margin-top:14px;font-size:.7rem;color:var(--text-muted,#7fa892);line-height:1.6;}
      /* botão no canto da tela de alertas: margin-left:auto encosta na direita
         do flex das abas sem precisar mexer no layout existente */
      .ac-open-btn{margin-left:auto;align-self:center;display:inline-flex;align-items:center;
        gap:8px;border:1px solid rgba(57,229,140,.3);background:rgba(57,229,140,.1);
        color:var(--accent,#39e58c);border-radius:8px;padding:9px 16px;font-size:.78rem;
        font-weight:600;cursor:pointer;font-family:inherit;
        transition:background .18s ease,border-color .18s ease;}
      .ac-open-btn:hover{background:rgba(57,229,140,.2);border-color:rgba(57,229,140,.5);}
      @media (max-width:640px){ .ac-open-btn span{display:none;} .ac-open-btn{padding:9px 12px;} }
    `;
    document.head.appendChild(st);
  }

  function ensureModal() {
    injectStyles();
    let ov = document.getElementById("acOverlay");
    if (ov) return ov;
    ov = document.createElement("div");
    ov.id = "acOverlay";
    ov.className = "ac-overlay";
    ov.style.display = "none";
    ov.innerHTML = `
      <div class="ac-modal" role="dialog" aria-modal="true" aria-labelledby="acTitle">
        <div class="ac-head">
          <i class="fa-solid fa-bell"></i>
          <h2 id="acTitle">Catálogo de alarmes</h2>
          <button class="ac-close" id="acClose" aria-label="Fechar">
            <i class="fa-solid fa-xmark"></i></button>
        </div>
        <div class="ac-body" id="acBody"></div>
      </div>`;
    document.body.appendChild(ov);
    ov.querySelector("#acClose").addEventListener("click", closeModal);
    ov.addEventListener("click", (e) => { if (e.target === ov) closeModal(); });
    return ov;
  }

  function closeModal() {
    const ov = document.getElementById("acOverlay");
    if (ov) ov.style.display = "none";
    document.removeEventListener("keydown", onEsc);
  }
  function onEsc(e) { if (e.key === "Escape") closeModal(); }

  async function openModal() {
    const ov = ensureModal();
    ov.style.display = "flex";
    document.addEventListener("keydown", onEsc);
    const u = getUser();
    // admin_customer só mexe no próprio cliente; superuser começa no global
    if (!u.is_superuser) S.escopo = String(u.customer_id || "");
    await carregar();
  }

  // ---------------------------------------------------------------------------
  // carga
  // ---------------------------------------------------------------------------
  async function carregar() {
    const body = document.getElementById("acBody");
    body.innerHTML = `<div class="ac-muted">Carregando catálogo…</div>`;
    S.editando = null;
    try {
      const u = getUser();
      // a lista de clientes só existe no /users; não há rota /customers
      if (u.is_superuser && S.customers === null) {
        try {
          const d = await api("/users");
          S.customers = d.customers || [];
        } catch { S.customers = []; }
      }
      const qs = new URLSearchParams({ device_type: S.tipo });
      if (S.escopo) qs.set("customer_id", S.escopo);
      const data = await api(`/alarms/catalog?${qs.toString()}`);
      S.itens = data.items || [];
      S.overridesOk = data.overrides_disponiveis !== false;
      render();
    } catch (err) {
      renderErro(err);
    }
  }

  function renderErro(err) {
    const body = document.getElementById("acBody");
    let msg = err.message || "erro desconhecido";
    if (err.code === "token_required" || err.code === "token_invalid") {
      msg = "Sua sessão é de antes desta atualização e não tem o token de segurança. " +
        "Saia e entre de novo para editar o catálogo.";
    }
    body.innerHTML = `
      <div class="ac-aviso erro">${esc(msg)}</div>
      <button class="ac-btn" id="acRetry">Tentar de novo</button>`;
    body.querySelector("#acRetry").addEventListener("click", carregar);
  }

  // ---------------------------------------------------------------------------
  // render
  // ---------------------------------------------------------------------------
  function render() {
    const body = document.getElementById("acBody");
    const u = getUser();

    const tipoOpts = TIPOS.map(([v, label]) =>
      `<option value="${v}" ${S.tipo === v ? "selected" : ""}>${label}</option>`).join("");

    let escopoSel = "";
    if (u.is_superuser) {
      const opts = (S.customers || []).map(c =>
        `<option value="${c.id}" ${String(S.escopo) === String(c.id) ? "selected" : ""}>${esc(c.name)}</option>`).join("");
      escopoSel = `<select id="acEscopo" title="Escopo da edição">
        <option value="">Global (todos os clientes)</option>${opts}</select>`;
    }

    const nomeCliente = (S.customers || []).find(c => String(c.id) === String(S.escopo));
    const nota = S.escopo
      ? `<p class="ac-scope-note cliente"><i class="fa-solid fa-user-shield"></i>
           Editando como exceção de <strong>${esc(nomeCliente ? nomeCliente.name : "seu cliente")}</strong>.
           Não afeta os outros clientes.</p>`
      : `<p class="ac-scope-note"><i class="fa-solid fa-triangle-exclamation"></i>
           Editando o catálogo <strong>global</strong>: vale para todas as usinas de
           todos os clientes, menos onde já existir exceção.</p>`;

    const busca = S.busca.trim().toLowerCase();
    const visiveis = S.itens.filter(it => !busca ||
      String(it.code).toLowerCase().includes(busca) ||
      String(it.description_pt || "").toLowerCase().includes(busca));

    const linhas = visiveis.map(it => linhaHTML(it)).join("") ||
      `<tr><td colspan="5" class="ac-muted">Nenhum código encontrado.</td></tr>`;

    const aviso = S.aviso
      ? `<div class="ac-aviso ${S.aviso.erro ? "erro" : ""}">${esc(S.aviso.texto)}</div>`
      : "";

    body.innerHTML = `
      <div class="ac-toolbar">
        <select id="acTipo" title="Tipo de equipamento">${tipoOpts}</select>
        ${escopoSel}
        <input type="text" id="acBusca" placeholder="Buscar código ou descrição"
               value="${esc(S.busca)}">
        <span class="ac-muted" style="margin-left:auto;">${visiveis.length} de ${S.itens.length} códigos</span>
      </div>
      ${nota}
      ${aviso}
      <table class="ac-table">
        <colgroup><col style="width:15%"><col style="width:36%"><col style="width:19%">
                  <col style="width:12%"><col style="width:18%"></colgroup>
        <thead><tr>
          <th>Código</th><th>Descrição</th><th>Severidade</th><th>Estado</th><th></th>
        </tr></thead>
        <tbody>${linhas}</tbody>
      </table>
      <div class="ac-legenda">
        Mudar a severidade também corrige os alarmes que já estão abertos.
        Desativar um código fecha os abertos dele e para de gerar novos.
      </div>`;

    body.querySelector("#acTipo").addEventListener("change", (e) => {
      S.tipo = e.target.value; S.aviso = null; carregar();
    });
    const es = body.querySelector("#acEscopo");
    if (es) es.addEventListener("change", (e) => {
      S.escopo = e.target.value; S.aviso = null; carregar();
    });
    const bs = body.querySelector("#acBusca");
    bs.addEventListener("input", (e) => {
      S.busca = e.target.value;
      // re-render perderia o foco do campo; guarda e devolve
      const pos = e.target.selectionStart;
      render();
      const novo = document.getElementById("acBusca");
      novo.focus(); novo.setSelectionRange(pos, pos);
    });

    body.querySelectorAll("[data-edit]").forEach(b =>
      b.addEventListener("click", () => { S.editando = b.getAttribute("data-edit"); S.aviso = null; render(); }));
    body.querySelectorAll("[data-cancel]").forEach(b =>
      b.addEventListener("click", () => { S.editando = null; render(); }));
    body.querySelectorAll("[data-save]").forEach(b =>
      b.addEventListener("click", () => salvar(b.getAttribute("data-save"))));
  }

  function linhaHTML(it) {
    const editando = S.editando === it.code;
    const sev = it.severity || "high";
    const ativo = it.is_active !== false;

    if (!editando) {
      const excecao = it.tem_excecao
        ? `<span class="ac-pill exc" title="Global: ${esc(SEV_LABEL[it.severity_global] || it.severity_global)}${it.excecao_por ? " · por " + esc(it.excecao_por) : ""}">exceção</span>`
        : "";
      const descGlobal = (it.tem_excecao && it.description_global &&
                          it.description_global !== it.description_pt)
        ? `<div class="ac-muted">global: ${esc(it.description_global)}</div>` : "";
      return `
        <tr>
          <td class="ac-code">${esc(it.code)}</td>
          <td>${esc(it.description_pt || "")}${descGlobal}</td>
          <td><span class="ac-pill ${esc(sev)}">${esc(SEV_LABEL[sev] || sev)}</span>${excecao}</td>
          <td>${ativo ? `<span class="ac-pill low">Ativo</span>`
                      : `<span class="ac-pill off">Desativado</span>`}</td>
          <td class="ac-acoes">
            <button class="ac-btn ac-ghost" data-edit="${esc(it.code)}">Editar</button>
          </td>
        </tr>`;
    }

    const sevOpts = SEVERIDADES.map(([v, label]) =>
      `<option value="${v}" ${sev === v ? "selected" : ""}>${label}</option>`).join("");
    return `
      <tr class="ac-editando ac-edit">
        <td class="ac-code">${esc(it.code)}</td>
        <td><input type="text" id="acFDesc" value="${esc(it.description_pt || "")}"></td>
        <td><select id="acFSev">${sevOpts}</select></td>
        <td><select id="acFAtivo">
              <option value="1" ${ativo ? "selected" : ""}>Ativo</option>
              <option value="0" ${!ativo ? "selected" : ""}>Desativado</option>
            </select></td>
        <td class="ac-acoes">
          <button class="ac-btn ac-ghost" data-cancel="1">Cancelar</button>
          <button class="ac-btn" data-save="${esc(it.code)}">Salvar</button>
        </td>
      </tr>`;
  }

  // ---------------------------------------------------------------------------
  // salvar
  // ---------------------------------------------------------------------------
  async function salvar(code) {
    const it = S.itens.find(i => i.code === code);
    if (!it) return;

    const desc = document.getElementById("acFDesc").value.trim();
    const sev = document.getElementById("acFSev").value;
    const ativo = document.getElementById("acFAtivo").value === "1";

    if (!desc) {
      S.aviso = { erro: true, texto: "A descrição não pode ficar vazia." };
      return render();
    }

    // manda só o que MUDOU. Campo não enviado fica NULL na exceção e continua
    // caindo no valor global, que é o ponto da tabela de exceção existir.
    const payload = {
      code, device_type_id: it.device_type_id,
      scope: S.escopo ? "customer" : "global"
    };
    if (S.escopo) payload.customer_id = Number(S.escopo);
    if (sev !== (it.severity || "high")) payload.severity = sev;
    if (desc !== (it.description_pt || "")) payload.description_pt = desc;
    if (ativo !== (it.is_active !== false)) payload.is_active = ativo;

    if (payload.severity === undefined && payload.description_pt === undefined &&
        payload.is_active === undefined) {
      S.editando = null;
      return render();
    }

    if (payload.is_active === false) {
      const onde = S.escopo ? "deste cliente" : "de TODOS os clientes";
      if (!confirm(`Desativar "${code}" fecha os alarmes abertos ${onde} e para de gerar novos. Confirma?`)) return;
    }
    if (!S.escopo && !confirm(`Esta alteração vale para TODOS os clientes. Confirma?`)) return;

    const btn = document.querySelector(`[data-save="${CSS.escape(code)}"]`);
    if (btn) { btn.disabled = true; btn.textContent = "Salvando…"; }

    try {
      const r = await api("/alarms/catalog", {
        method: "PUT", body: JSON.stringify(payload)
      });
      const partes = [`Código ${code} salvo.`];
      if (r.alarmes_abertos_corrigidos) {
        partes.push(`${r.alarmes_abertos_corrigidos} alarme(s) já aberto(s) corrigido(s).`);
      }
      if (r.alarmes_fechados) partes.push(`${r.alarmes_fechados} fechado(s).`);
      S.aviso = { erro: false, texto: partes.join(" ") };
      await carregar();
    } catch (err) {
      let msg = err.message || "erro desconhecido";
      if (err.code === "token_required" || err.code === "token_invalid") {
        msg = "Sua sessão não tem o token de segurança. Saia e entre de novo.";
      } else if (err.hint) {
        msg += ` (${err.hint})`;
      }
      S.aviso = { erro: true, texto: msg };
      render();
    }
  }

  // ---------------------------------------------------------------------------
  // botão no canto da TELA DE ALERTAS (não na sidebar)
  //
  // Fica ao lado das abas do #alarmsView, empurrado para a direita pelo
  // margin-left:auto do flex. O lugar certo é aqui e não na barra lateral:
  // quem vai reclassificar um código chegou nisso olhando a lista de
  // alarmes, não procurando um ícone de configuração.
  // ---------------------------------------------------------------------------
  function injectOpenButton() {
    const u = getUser();
    if (!u.username || !isAdmin(u)) return;
    if (document.getElementById("btnAlarmesAdmin")) return;
    const barra = document.querySelector("#alarmsView .alarms-tabs");
    if (!barra) return;
    injectStyles();
    const btn = document.createElement("button");
    btn.id = "btnAlarmesAdmin";
    btn.className = "ac-open-btn";
    btn.title = "Editar severidade e descrição dos códigos de alarme";
    btn.innerHTML = '<i class="fa-solid fa-sliders"></i><span>Editar catálogo</span>';
    btn.addEventListener("click", openModal);
    barra.appendChild(btn);
  }

  window.__openAlarmesAdmin = openModal; // acesso via console

  // A tela de alertas já existe no DOM desde o load (só fica com .hidden),
  // então não precisa esperar troca de aba. Mas o login pode terminar depois
  // do DOMContentLoaded, e aí getUser() ainda estaria vazio: por isso a
  // segunda tentativa.
  function agenda() {
    injectOpenButton();
    setTimeout(injectOpenButton, 1500);
  }
  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", agenda);
  } else {
    agenda();
  }
})();
