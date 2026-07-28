// =============================================================================
// gateway_editor.js — Configurador remoto do Gateway V16 (tela cheia).
//
// Equivalente web do config_editor.py do publicador local do Igor: mesmas abas,
// mesma lógica de tabela + formulário. O que muda de propósito:
//   • a configuração vive no BANCO da plataforma (app.gateway_config), não em
//     arquivo solto — foi o pedido dele ("esse tem que ser um dos que vc pode
//     armazenar no banco");
//   • quem compila e publica é o BACKEND, com o mqtt_entities.py dele, um put
//     por vez esperando ACK. O navegador não tem credencial de broker.
//
// Estado e renderização ficam aqui; catálogo, validação, banco e publicação
// ficam em gateway_actions.js.
// =============================================================================

const GW_API_BASE = "https://jgeg9i0js1.execute-api.us-east-1.amazonaws.com";

const GW = {
  plantId: null,        // id da usina na plataforma (número)
  plantName: "",        // nome no cadastro = segmento do tópico
  cfg: null,            // a configuração inteira (é ela que vai para o gateway)
  salvaNoBanco: null,   // cópia do que está gravado, para "Restaurar"
  historico: [],
  podeEditar: false,
  aba: "geral",
  sel: {},              // id selecionado por aba
  filtro: {},           // texto do filtro por aba
  sujo: false,
  catalogo: null,       // índice de gateway_templates/index.json
  leuDoBanco: false,    // a leitura inicial do banco deu certo?
};

// ---------------------------------------------------------------------------
// Utilidades
// ---------------------------------------------------------------------------

function gwEsc(v) {
  return String(v == null ? "" : v).replace(/[&<>"']/g, (c) => (
    { "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c]
  ));
}

function gwUser() {
  try { return JSON.parse(localStorage.getItem("user") || "{}"); } catch (e) { return {}; }
}

function gwFetch(path, options = {}) {
  const u = gwUser();
  const headers = { ...(options.headers || {}) };
  if (u.customer_id) headers["X-Customer-Id"] = u.customer_id;
  if (u.is_superuser === true) headers["X-Is-Superuser"] = "true";
  if (u.username) headers["X-Username"] = u.username;
  if (u.token) headers["Authorization"] = `Bearer ${u.token}`;
  return fetch(`${GW_API_BASE}${path}`, { ...options, headers, cache: "no-store" });
}

// caminho com ponto: "metadata.inter_device_ms"
function gwGet(obj, caminho, pad = "") {
  let v = obj;
  for (const parte of String(caminho).split(".")) {
    if (!v || typeof v !== "object") return pad;
    v = v[parte];
  }
  return v === undefined ? pad : v;
}

function gwSet(obj, caminho, valor) {
  const partes = String(caminho).split(".");
  let alvo = obj;
  for (const parte of partes.slice(0, -1)) {
    if (!alvo[parte] || typeof alvo[parte] !== "object" || Array.isArray(alvo[parte])) {
      alvo[parte] = {};
    }
    alvo = alvo[parte];
  }
  alvo[partes[partes.length - 1]] = valor;
}

function gwParse(texto, tipo) {
  const t = String(texto == null ? "" : texto).trim();
  if (tipo === "bool") return ["1", "true", "sim", "yes", "on"].includes(t.toLowerCase());
  if (tipo === "int") return parseInt(t || "0", 10) || 0;
  if (tipo === "float") return parseFloat((t || "0").replace(",", ".")) || 0;
  if (tipo === "json" || tipo === "lista") return JSON.parse(t || (tipo === "lista" ? "[]" : "{}"));
  return t;
}

function gwMostra(valor, tipo) {
  if (tipo === "bool") return valor ? "true" : "false";
  if (tipo === "json" || tipo === "lista") {
    try { return JSON.stringify(valor === undefined ? (tipo === "lista" ? [] : {}) : valor); }
    catch (e) { return ""; }
  }
  return valor == null ? "" : String(valor);
}

function gwSecao(key) {
  return GW_SECOES.find((s) => s.key === key) || null;
}

function gwLista(key) {
  if (!GW.cfg) return [];
  if (!Array.isArray(GW.cfg[key])) GW.cfg[key] = [];
  return GW.cfg[key];
}

function gwMarcaSujo() {
  GW.sujo = true;
  gwPintaStatus();
}

function gwStatus(msg, tipo = "info") {
  const el = document.getElementById("gwMsg");
  if (!el) return;
  const icone = { ok: "fa-circle-check", erro: "fa-triangle-exclamation",
                  wait: "fa-circle-notch fa-spin", info: "fa-circle-info" }[tipo] || "fa-circle-info";
  el.className = `gw-msg gw-msg--${tipo}`;
  el.innerHTML = `<i class="fa-solid ${icone}"></i> ${msg}`;
}

function gwPintaStatus() {
  const el = document.getElementById("gwDirty");
  if (!el) return;
  el.className = GW.sujo ? "gw-dirty gw-dirty--on" : "gw-dirty";
  el.innerHTML = GW.sujo
    ? '<i class="fa-solid fa-pen"></i> alterações não salvas'
    : '<i class="fa-solid fa-check"></i> sem alterações pendentes';
}

// ---------------------------------------------------------------------------
// Modal genérico
// ---------------------------------------------------------------------------

function gwModal(titulo, conteudoHtml, botoes = [], largura = 560) {
  const antigo = document.getElementById("gwModal");
  if (antigo) antigo.remove();

  const wrap = document.createElement("div");
  wrap.id = "gwModal";
  wrap.className = "gw-modal";
  wrap.innerHTML = `
    <div class="gw-modal-box" style="max-width:${largura}px">
      <div class="gw-modal-head">
        <h3>${gwEsc(titulo)}</h3>
        <button class="gw-modal-x" type="button" aria-label="Fechar">&times;</button>
      </div>
      <div class="gw-modal-body">${conteudoHtml}</div>
      <div class="gw-modal-foot"></div>
    </div>`;
  document.body.appendChild(wrap);

  const fecha = () => wrap.remove();
  wrap.querySelector(".gw-modal-x").onclick = fecha;
  wrap.onclick = (e) => { if (e.target === wrap) fecha(); };
  document.addEventListener("keydown", function esc(e) {
    if (e.key === "Escape") { fecha(); document.removeEventListener("keydown", esc); }
  });

  const rodape = wrap.querySelector(".gw-modal-foot");
  botoes.forEach((b) => {
    const btn = document.createElement("button");
    btn.type = "button";
    btn.className = `gw-btn ${b.classe || "gw-btn--ghost"}`;
    btn.innerHTML = b.rotulo;
    btn.onclick = () => { if (b.acao && b.acao(wrap) === false) return; if (b.fecha !== false) fecha(); };
    rodape.appendChild(btn);
  });
  return wrap;
}

function gwPergunta(titulo, texto) {
  return new Promise((resolve) => {
    gwModal(titulo, `<p class="gw-modal-txt">${texto}</p>`, [
      { rotulo: "Cancelar", acao: () => resolve(false) },
      { rotulo: "Confirmar", classe: "gw-btn--accent", acao: () => resolve(true) },
    ], 460);
  });
}

// ---------------------------------------------------------------------------
// Render — abas
// ---------------------------------------------------------------------------

function gwRenderAbas() {
  const nav = document.getElementById("gwTabs");
  if (!nav) return;
  const abas = [{ key: "geral", titulo: "Usina e gateway", icone: "fa-industry" }]
    .concat(GW_SECOES.map((s) => ({
      key: s.key, titulo: s.titulo, icone: s.icone, n: gwLista(s.key).length,
    })));
  nav.innerHTML = abas.map((a) => `
    <button type="button" class="gw-tab ${a.key === GW.aba ? "is-on" : ""}"
            data-aba="${a.key}">
      <i class="fa-solid ${a.icone}"></i>
      <span>${gwEsc(a.titulo)}</span>
      ${a.n != null ? `<em class="gw-tab-n">${a.n}</em>` : ""}
    </button>`).join("");
  nav.querySelectorAll(".gw-tab").forEach((b) => {
    b.onclick = () => { GW.aba = b.dataset.aba; gwRender(); };
  });
}

function gwRender() {
  gwRenderAbas();
  const corpo = document.getElementById("gwBody");
  if (!corpo) return;
  corpo.innerHTML = GW.aba === "geral" ? gwHtmlGeral() : gwHtmlSecao(gwSecao(GW.aba));
  if (GW.aba === "geral") gwLigaGeral();
  else gwLigaSecao(gwSecao(GW.aba));
  gwPintaStatus();
}

// ---------------------------------------------------------------------------
// Aba "Usina e gateway"
// ---------------------------------------------------------------------------

function gwHtmlGeral() {
  const campos = GW_CAMPOS_GERAIS.map((c) => gwHtmlCampo(c, gwGet(GW.cfg, c.k, c.pad), "geral")).join("");
  return `
    <div class="gw-geral">
      <div class="gw-card">
        <div class="gw-card-head">
          <h2><i class="fa-solid fa-industry"></i> Usina e gateway</h2>
          <p>Identificação, qualidade 192/28, servidor Modbus TCP, padrões de tópicos
             e os três tópicos de configuração.</p>
        </div>
        <div class="gw-grid-2">${campos}</div>
        <div class="gw-card-foot">
          <button type="button" class="gw-btn gw-btn--ghost" id="gwGeralPadroes">
            <i class="fa-solid fa-wand-magic-sparkles"></i> Preencher tópicos pela usina
          </button>
          <button type="button" class="gw-btn gw-btn--accent" id="gwGeralSalvar">
            <i class="fa-solid fa-check"></i> Aplicar dados gerais
          </button>
        </div>
      </div>
    </div>`;
}

function gwLigaGeral() {
  const btn = document.getElementById("gwGeralSalvar");
  if (btn) btn.onclick = () => { gwGeralAplica(true); };
  const pad = document.getElementById("gwGeralPadroes");
  if (pad) pad.onclick = gwGeralPreencheTopicos;
}

function gwGeralAplica(avisa) {
  try {
    GW_CAMPOS_GERAIS.forEach((c) => {
      const el = document.querySelector(`[data-gw-campo="${CSS.escape(c.k)}"]`);
      if (!el) return;
      gwSet(GW.cfg, c.k, gwParse(el.value, c.tipo));
    });
    // o compilador lê general.plant_id; manter espelhado evita tópico errado
    GW.cfg.general = GW.cfg.general || {};
    GW.cfg.general.plant_id = String(gwGet(GW.cfg, "plant.id", ""));
    gwMarcaSujo();
    if (avisa) gwStatus("Dados gerais aplicados na configuração.", "ok");
    return true;
  } catch (e) {
    gwStatus(`Dados gerais inválidos: ${gwEsc(e.message || e)}`, "erro");
    return false;
  }
}

// Preenche os 4 tópicos + o filtro de comandos a partir do id da usina.
// Erra aqui e o gateway simplesmente não escuta — vale ter o botão.
function gwGeralPreencheTopicos() {
  const el = document.querySelector('[data-gw-campo="plant.id"]');
  const usina = String((el && el.value) || gwGet(GW.cfg, "plant.id", "")).trim();
  if (!usina) { gwStatus("Preencha o ID da usina primeiro.", "erro"); return; }
  const mapa = {
    "general.command_subscribe_filter": `dev/write/UFV/${usina}/+/+`,
    "general.command_feedback_topic": `dev/write/UFV/${usina}/feedback`,
    "general.config_command_topic": `dev/write/UFV/${usina}/gateway/configuration`,
    "general.config_feedback_topic": `dev/write/UFV/${usina}/gateway/configuration/feedback`,
    "general.metadata.telemetry_pattern": "dev/read/UFV/{plant}/{type}/{id}",
    "general.metadata.command_pattern": "dev/write/UFV/{plant}/{type}/{id}",
    "plant.metadata.topic_slug": usina,
  };
  Object.entries(mapa).forEach(([k, v]) => {
    const campo = document.querySelector(`[data-gw-campo="${CSS.escape(k)}"]`);
    if (campo) campo.value = v;
  });
  gwStatus("Tópicos preenchidos pelo padrão. Confira e clique em Aplicar.", "ok");
}

// ---------------------------------------------------------------------------
// Abas de registro (tabela + formulário)
// ---------------------------------------------------------------------------

function gwHtmlSecao(spec) {
  if (!spec) return "";
  const registros = gwLista(spec.key);
  const extras = gwBotoesExtras(spec.key);
  return `
    <div class="gw-split">
      <section class="gw-list">
        <div class="gw-list-head">
          <div class="gw-filtro">
            <i class="fa-solid fa-magnifying-glass"></i>
            <input type="text" id="gwFiltro" placeholder="Filtrar ${gwEsc(spec.titulo.toLowerCase())}..."
                   value="${gwEsc(GW.filtro[spec.key] || "")}">
          </div>
          <span class="gw-list-n" id="gwListN"></span>
          ${extras}
        </div>
        <p class="gw-list-ajuda">${gwEsc(spec.ajuda || "")}</p>
        <div class="gw-table-wrap"><table class="gw-table">
          <thead><tr>${spec.colunas.map((c) => `<th>${gwEsc(c)}</th>`).join("")}</tr></thead>
          <tbody id="gwTbody"></tbody>
        </table></div>
      </section>
      <aside class="gw-form" id="gwForm"></aside>
    </div>`;
}

function gwBotoesExtras(key) {
  if (key === "devices") {
    return `<button type="button" class="gw-btn gw-btn--accent gw-btn--sm" id="gwAddCatalogo">
              <i class="fa-solid fa-boxes-stacked"></i> Adicionar do catálogo
            </button>`;
  }
  if (key === "templates") {
    return `<button type="button" class="gw-btn gw-btn--accent gw-btn--sm" id="gwImportCatalogo">
              <i class="fa-solid fa-download"></i> Templates padrão
            </button>
            <button type="button" class="gw-btn gw-btn--ghost gw-btn--sm" id="gwExportTemplate">
              <i class="fa-solid fa-file-export"></i> Exportar
            </button>`;
  }
  if (key === "topics") {
    return `<button type="button" class="gw-btn gw-btn--accent gw-btn--sm" id="gwGeraTopicos">
              <i class="fa-solid fa-wand-magic-sparkles"></i> Gerar tópicos
            </button>`;
  }
  return "";
}

function gwLigaSecao(spec) {
  if (!spec) return;
  const filtro = document.getElementById("gwFiltro");
  if (filtro) {
    filtro.oninput = () => {
      GW.filtro[spec.key] = filtro.value;
      gwPintaTabela(spec);
    };
  }
  const liga = (id, fn) => { const b = document.getElementById(id); if (b) b.onclick = fn; };
  liga("gwAddCatalogo", gwAbreWizardDevices);
  liga("gwImportCatalogo", gwAbreCatalogo);
  liga("gwExportTemplate", gwExportaTemplate);
  liga("gwGeraTopicos", gwGeraTopicosFaltantes);

  gwPintaTabela(spec);
  gwPintaForm(spec);
}

function gwValorColuna(spec, registro, coluna) {
  if (spec.colunasCalc && spec.colunasCalc[coluna]) {
    try { return spec.colunasCalc[coluna](registro); } catch (e) { return ""; }
  }
  const v = gwGet(registro, coluna, "");
  if (v && typeof v === "object") { try { return JSON.stringify(v); } catch (e) { return "[objeto]"; } }
  if (typeof v === "boolean") return v ? "sim" : "não";
  return v;
}

function gwPintaTabela(spec) {
  const tbody = document.getElementById("gwTbody");
  if (!tbody) return;
  const registros = gwLista(spec.key);
  const termo = String(GW.filtro[spec.key] || "").trim().toLowerCase();

  const linhas = registros
    .map((r, i) => ({ r, i }))
    .filter(({ r }) => {
      if (!termo) return true;
      try { return JSON.stringify(r).toLowerCase().includes(termo); } catch (e) { return true; }
    });

  tbody.innerHTML = linhas.map(({ r, i }) => {
    const id = String(r && r.id != null ? r.id : "");
    const on = GW.sel[spec.key] === id ? "is-on" : "";
    const off = r && r.enabled === false ? "is-off" : "";
    const celulas = spec.colunas
      .map((c) => `<td title="${gwEsc(gwValorColuna(spec, r, c))}">${gwEsc(gwValorColuna(spec, r, c))}</td>`)
      .join("");
    return `<tr class="${on} ${off}" data-i="${i}" data-id="${gwEsc(id)}">${celulas}</tr>`;
  }).join("") || `<tr class="gw-vazio"><td colspan="${spec.colunas.length}">
      ${termo ? "Nada casa com o filtro." : "Nenhum item. Use NOVO ao lado."}</td></tr>`;

  tbody.querySelectorAll("tr[data-i]").forEach((tr) => {
    tr.onclick = () => {
      GW.sel[spec.key] = tr.dataset.id;
      gwPintaTabela(spec);
      gwPintaForm(spec);
    };
  });

  const n = document.getElementById("gwListN");
  if (n) {
    n.textContent = termo
      ? `${linhas.length} de ${registros.length}`
      : `${registros.length} ${registros.length === 1 ? "item" : "itens"}`;
  }
}

function gwIndiceSel(spec) {
  const id = GW.sel[spec.key];
  if (id == null) return -1;
  return gwLista(spec.key).findIndex((r) => String(r && r.id) === String(id));
}

function gwPintaForm(spec) {
  const box = document.getElementById("gwForm");
  if (!box) return;
  const idx = gwIndiceSel(spec);
  const registro = idx >= 0 ? gwLista(spec.key)[idx] : null;

  const campos = spec.campos.map((c) => {
    const valor = registro ? gwGet(registro, c.k, c.pad) : c.pad;
    return gwHtmlCampo(c, valor, spec.key);
  }).join("");

  box.innerHTML = `
    <div class="gw-form-head">
      <h3>${registro ? `<i class="fa-solid fa-pen"></i> Editando` : `<i class="fa-solid fa-plus"></i> Novo`}
        <span>${gwEsc(spec.titulo)}</span></h3>
      ${registro ? `<code>${gwEsc(registro.id)}</code>` : ""}
    </div>
    <div class="gw-form-campos">${campos}</div>
    <div class="gw-form-botoes">
      <button type="button" class="gw-btn gw-btn--ghost" id="gwNovo"><i class="fa-solid fa-plus"></i> Novo</button>
      <button type="button" class="gw-btn gw-btn--ghost" id="gwDuplicar" ${registro ? "" : "disabled"}>
        <i class="fa-solid fa-copy"></i> Duplicar</button>
      <button type="button" class="gw-btn gw-btn--accent" id="gwSalvarItem">
        <i class="fa-solid fa-floppy-disk"></i> Salvar item</button>
      <button type="button" class="gw-btn gw-btn--danger" id="gwExcluir" ${registro ? "" : "disabled"}>
        <i class="fa-solid fa-trash"></i> Excluir</button>
    </div>`;

  document.getElementById("gwNovo").onclick = () => {
    GW.sel[spec.key] = null;
    gwPintaTabela(spec);
    gwPintaForm(spec);
  };
  document.getElementById("gwSalvarItem").onclick = () => gwSalvaItem(spec);
  const dup = document.getElementById("gwDuplicar");
  if (dup) dup.onclick = () => gwDuplicaItem(spec);
  const del = document.getElementById("gwExcluir");
  if (del) del.onclick = () => gwExcluiItem(spec);

  box.querySelectorAll("[data-gw-lista]").forEach((b) => {
    b.onclick = () => gwAbreSubLista(spec, b.dataset.gwLista);
  });
}

// ---------------------------------------------------------------------------
// Campo do formulário
// ---------------------------------------------------------------------------

function gwHtmlCampo(campo, valor, escopo) {
  const id = `gwc-${escopo}-${campo.k.replace(/[^\w]/g, "_")}`;
  const dica = campo.dica ? `<small class="gw-dica">${gwEsc(campo.dica)}</small>` : "";

  if (campo.tipo === "lista") {
    const n = Array.isArray(valor) ? valor.length : 0;
    return `
      <label class="gw-campo gw-campo--largo" for="${id}">
        <span class="gw-rot">${gwEsc(campo.rot)}</span>
        <button type="button" class="gw-btn gw-btn--ghost gw-btn--sm" data-gw-lista="${gwEsc(campo.k)}">
          <i class="fa-solid fa-list-ol"></i> Editar ${gwEsc(campo.sub.rotulo)}s (${n})
        </button>
        <input type="hidden" id="${id}" data-gw-campo="${gwEsc(campo.k)}"
               data-tipo="lista" value="${gwEsc(gwMostra(valor, "lista"))}">
        ${dica}
      </label>`;
  }

  if (campo.tipo === "json") {
    return `
      <label class="gw-campo gw-campo--largo" for="${id}">
        <span class="gw-rot">${gwEsc(campo.rot)}</span>
        <textarea id="${id}" class="gw-input gw-input--json" rows="2" spellcheck="false"
          data-gw-campo="${gwEsc(campo.k)}" data-tipo="json">${gwEsc(gwMostra(valor, "json"))}</textarea>
        ${dica}
      </label>`;
  }

  const sugestoes = campo.tipo === "bool"
    ? ["true", "false"]
    : (campo.ref ? gwLista(campo.ref).map((r) => String(r && r.id != null ? r.id : "")).filter(Boolean)
                 : (campo.vals || []));
  const listaId = sugestoes.length ? `${id}-opts` : "";
  const datalist = sugestoes.length
    ? `<datalist id="${listaId}">${sugestoes.map((o) => `<option value="${gwEsc(o)}"></option>`).join("")}</datalist>`
    : "";

  return `
    <label class="gw-campo" for="${id}">
      <span class="gw-rot">${gwEsc(campo.rot)}</span>
      <input id="${id}" class="gw-input" type="text" autocomplete="off"
             ${listaId ? `list="${listaId}"` : ""}
             data-gw-campo="${gwEsc(campo.k)}" data-tipo="${campo.tipo || "str"}"
             value="${gwEsc(gwMostra(valor, campo.tipo))}">
      ${datalist}${dica}
    </label>`;
}

function gwLeFormulario(spec, base) {
  // Parte de uma cópia do registro atual: chave que a tela não conhece
  // (o JSON do Igor tem várias) sobrevive à edição.
  const registro = base ? JSON.parse(JSON.stringify(base)) : {};
  spec.campos.forEach((c) => {
    const el = document.querySelector(`#gwForm [data-gw-campo="${CSS.escape(c.k)}"]`);
    if (!el) return;
    gwSet(registro, c.k, gwParse(el.value, c.tipo));
  });
  const id = String(registro.id == null ? "" : registro.id).trim();
  if (!id) throw new Error("O campo ID é obrigatório.");
  if (!/^[A-Za-z0-9._-]{1,63}$/.test(id)) {
    throw new Error(`ID "${id}" inválido: use até 63 caracteres entre letras, números, ponto, hífen e sublinhado.`);
  }
  return registro;
}

function gwSalvaItem(spec) {
  try {
    const idx = gwIndiceSel(spec);
    const lista = gwLista(spec.key);
    const registro = gwLeFormulario(spec, idx >= 0 ? lista[idx] : null);
    const repetido = lista.findIndex((r, i) => i !== idx && String(r && r.id) === String(registro.id));
    if (repetido >= 0) throw new Error(`Já existe um item com ID ${registro.id}.`);

    if (idx >= 0) lista[idx] = registro;
    else lista.push(registro);

    GW.sel[spec.key] = String(registro.id);
    gwMarcaSujo();
    gwRenderAbas();
    gwPintaTabela(spec);
    gwPintaForm(spec);
    gwStatus(`${spec.titulo}: item ${gwEsc(registro.id)} salvo na configuração.`, "ok");
  } catch (e) {
    gwStatus(`Item inválido — ${gwEsc(e.message || e)}`, "erro");
  }
}

function gwDuplicaItem(spec) {
  const idx = gwIndiceSel(spec);
  if (idx < 0) return;
  const lista = gwLista(spec.key);
  const clone = JSON.parse(JSON.stringify(lista[idx]));
  const ids = new Set(lista.map((r) => String(r && r.id)));
  const original = String(clone.id || "item");
  let candidato = `${original}-copy`;
  let n = 2;
  while (ids.has(candidato)) { candidato = `${original}-copy-${n}`; n += 1; }
  clone.id = candidato;
  if (clone.name) clone.name = `${clone.name} cópia`;
  lista.push(clone);
  GW.sel[spec.key] = candidato;
  gwMarcaSujo();
  gwRenderAbas();
  gwPintaTabela(spec);
  gwPintaForm(spec);
  gwStatus(`Duplicado como ${gwEsc(candidato)}.`, "ok");
}

async function gwExcluiItem(spec) {
  const idx = gwIndiceSel(spec);
  if (idx < 0) return;
  const lista = gwLista(spec.key);
  const id = String(lista[idx].id);

  // Quem depende deste id? Apagar um template e deixar 111 campos órfãos é o
  // tipo de coisa que só aparece lá na frente, como recusa no validate.
  const dependentes = gwDependentes(spec.key, id);
  const aviso = dependentes.length
    ? `<p class="gw-modal-txt gw-modal-txt--alerta"><i class="fa-solid fa-triangle-exclamation"></i>
       Ficam órfãos: ${gwEsc(dependentes.join(", "))}.</p>`
    : "";
  const ok = await new Promise((resolve) => {
    gwModal("Excluir item",
      `<p class="gw-modal-txt">Excluir <code>${gwEsc(id)}</code> de ${gwEsc(spec.titulo)}?</p>${aviso}`,
      [{ rotulo: "Cancelar", acao: () => resolve(false) },
       { rotulo: "Excluir", classe: "gw-btn--danger", acao: () => resolve(true) }], 480);
  });
  if (!ok) return;

  lista.splice(idx, 1);
  GW.sel[spec.key] = null;
  gwMarcaSujo();
  gwRenderAbas();
  gwPintaTabela(spec);
  gwPintaForm(spec);
  gwStatus(`${gwEsc(id)} excluído.`, "ok");
}

// Conta quem aponta para este id (usado no aviso da exclusão).
function gwDependentes(secao, id) {
  const alvo = String(id);
  const conta = [];
  const olha = (key, campo, rotulo) => {
    const n = gwLista(key).filter((r) => String(gwGet(r, campo, "")) === alvo).length;
    if (n) conta.push(`${n} ${rotulo}`);
  };
  if (secao === "templates") {
    olha("requests", "template_id", "requests");
    olha("fields", "template_id", "campos");
    olha("devices", "template_id", "devices");
  } else if (secao === "channels") {
    olha("devices", "channel_id", "devices");
  } else if (secao === "devices") {
    olha("topics", "device_id", "tópicos");
    olha("commands", "device_id", "comandos");
    olha("alarms", "device_id", "alarmes");
    olha("events", "device_id", "eventos");
  } else if (secao === "requests") {
    olha("fields", "request_id", "campos");
  }
  return conta;
}

// ---------------------------------------------------------------------------
// Sub-listas (passos da sequência, alvos do PID)
// ---------------------------------------------------------------------------

function gwAbreSubLista(spec, chaveCampo) {
  const campo = spec.campos.find((c) => c.k === chaveCampo);
  if (!campo || !campo.sub) return;
  const escondido = document.querySelector(`#gwForm [data-gw-campo="${CSS.escape(chaveCampo)}"]`);
  let itens;
  try { itens = JSON.parse(escondido.value || "[]"); } catch (e) { itens = []; }
  if (!Array.isArray(itens)) itens = [];

  let sel = itens.length ? 0 : -1;
  const modal = gwModal(
    `${campo.rot} — ${spec.titulo}`,
    `<div class="gw-sub">
       <div class="gw-sub-lista">
         <table class="gw-table gw-table--sub">
           <thead><tr><th>#</th>${campo.sub.colunas.map((c) => `<th>${gwEsc(c)}</th>`).join("")}</tr></thead>
           <tbody id="gwSubBody"></tbody>
         </table>
         <button type="button" class="gw-btn gw-btn--ghost gw-btn--sm" id="gwSubNovo">
           <i class="fa-solid fa-plus"></i> Novo ${gwEsc(campo.sub.rotulo)}
         </button>
         <span class="gw-sub-limite">máximo ${campo.max}</span>
       </div>
       <div class="gw-sub-form" id="gwSubForm"></div>
     </div>`,
    [{ rotulo: "Fechar" },
     { rotulo: '<i class="fa-solid fa-check"></i> Aplicar', classe: "gw-btn--accent",
       acao: () => {
         escondido.value = JSON.stringify(itens);
         gwStatus(`${itens.length} ${campo.sub.rotulo}(s) no formulário — clique em Salvar item.`, "ok");
       } }],
    980
  );

  const pintaLista = () => {
    const tb = modal.querySelector("#gwSubBody");
    tb.innerHTML = itens.map((it, i) => `
      <tr class="${i === sel ? "is-on" : ""}" data-i="${i}">
        <td>${i + 1}</td>
        ${campo.sub.colunas.map((c) => {
          const v = gwGet(it, c, "");
          const txt = v && typeof v === "object" ? JSON.stringify(v) : String(v == null ? "" : v);
          return `<td title="${gwEsc(txt)}">${gwEsc(txt)}</td>`;
        }).join("")}
      </tr>`).join("") ||
      `<tr class="gw-vazio"><td colspan="${campo.sub.colunas.length + 1}">Nenhum ${gwEsc(campo.sub.rotulo)}.</td></tr>`;
    tb.querySelectorAll("tr[data-i]").forEach((tr) => {
      tr.onclick = () => { sel = Number(tr.dataset.i); pintaLista(); pintaForm(); };
    });
  };

  const pintaForm = () => {
    const box = modal.querySelector("#gwSubForm");
    const atual = sel >= 0 ? itens[sel] : null;
    box.innerHTML = campo.sub.campos
      .map((c) => gwHtmlCampo(c, atual ? gwGet(atual, c.k, c.pad) : c.pad, "sub")).join("") +
      `<div class="gw-form-botoes">
         <button type="button" class="gw-btn gw-btn--accent gw-btn--sm" id="gwSubSalvar">
           <i class="fa-solid fa-check"></i> ${atual ? "Atualizar" : "Adicionar"}</button>
         <button type="button" class="gw-btn gw-btn--danger gw-btn--sm" id="gwSubExcluir" ${atual ? "" : "disabled"}>
           <i class="fa-solid fa-trash"></i> Excluir</button>
       </div>`;

    box.querySelector("#gwSubSalvar").onclick = () => {
      try {
        const novo = atual ? JSON.parse(JSON.stringify(atual)) : {};
        campo.sub.campos.forEach((c) => {
          const el = box.querySelector(`[data-gw-campo="${CSS.escape(c.k)}"]`);
          if (el) gwSet(novo, c.k, gwParse(el.value, c.tipo));
        });
        if (sel >= 0) itens[sel] = novo;
        else {
          if (itens.length >= campo.max) {
            gwStatus(`Limite de ${campo.max} ${campo.sub.rotulo}s atingido — o gateway recusaria.`, "erro");
            return;
          }
          itens.push(novo);
          sel = itens.length - 1;
        }
        pintaLista(); pintaForm();
      } catch (e) {
        gwStatus(`${campo.sub.rotulo} inválido: ${gwEsc(e.message || e)}`, "erro");
      }
    };
    const del = box.querySelector("#gwSubExcluir");
    if (del) del.onclick = () => { itens.splice(sel, 1); sel = -1; pintaLista(); pintaForm(); };
  };

  modal.querySelector("#gwSubNovo").onclick = () => { sel = -1; pintaForm(); pintaLista(); };
  pintaLista();
  pintaForm();
}
