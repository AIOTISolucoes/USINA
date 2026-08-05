// =============================================================================
// gateway_actions.js — catálogo de modelos, geração de tópicos, validação,
// banco e publicação do configurador do Gateway V16.
//
// Regras que não se negociam (custaram madrugada em 25-27/07):
//   • quem compila é o BACKEND com o mqtt_entities.py do Igor — a tela nunca
//     recalcula CRC nem monta envelope;
//   • publicar é begin → put+ACK ×N → validate → commit, no worker assíncrono:
//     aqui só se dispara e acompanha;
//   • request_id tem no máximo 16 caracteres.
// =============================================================================

// ---------------------------------------------------------------------------
// Catálogo de modelos (gateway_templates/)
// ---------------------------------------------------------------------------

async function gwCarregaCatalogo() {
  if (GW.catalogo) return GW.catalogo;
  const r = await fetch("gateway_templates/index.json", { cache: "no-store" });
  if (!r.ok) throw new Error("catálogo de modelos não encontrado no site");
  const d = await r.json();
  GW.catalogo = Array.isArray(d.templates) ? d.templates : [];
  return GW.catalogo;
}

async function gwCarregaPacote(arquivo) {
  const r = await fetch(`gateway_templates/${arquivo}`, { cache: "no-store" });
  if (!r.ok) throw new Error(`não consegui abrir ${arquivo}`);
  return r.json();
}

// Insere template + requests + fields (pulando o que já existe pelo id).
function gwInserePacote(pacote) {
  let novos = 0;
  ["templates", "requests", "fields"].forEach((secao) => {
    const lista = gwLista(secao);
    const ids = new Set(lista.map((r) => String(r && r.id)));
    (pacote[secao] || []).forEach((item) => {
      if (!ids.has(String(item.id))) {
        lista.push(JSON.parse(JSON.stringify(item)));
        ids.add(String(item.id));
        novos += 1;
      }
    });
  });
  return novos;
}

async function gwAbreCatalogo() {
  let itens;
  try { itens = await gwCarregaCatalogo(); }
  catch (e) { gwStatus(`Catálogo indisponível: ${gwEsc(e.message || e)}`, "erro"); return; }

  const jaTem = new Set(gwLista("templates").map((t) => String(t && t.id)));
  const cards = itens.map((t) => `
    <label class="gw-cat-item ${jaTem.has(t.id) ? "is-in" : ""}">
      <input type="checkbox" value="${gwEsc(t.file)}" ${jaTem.has(t.id) ? "disabled" : ""}>
      <div>
        <strong>${gwEsc(t.title)}</strong>
        <small>${gwEsc(t.manufacturer)} · ${gwEsc(t.device_type)} ·
          ${t.requests} request(s), ${t.fields} campos${t.commands ? `, ${t.commands} comandos` : ""}</small>
        <small class="gw-cat-src">${gwEsc(t.summary)}</small>
        ${jaTem.has(t.id) ? '<em class="gw-cat-tag">já está nesta configuração</em>' : ""}
      </div>
    </label>`).join("");

  gwModal("Templates padrão",
    `<p class="gw-modal-txt">Mapas Modbus já validados em campo. Entram como template +
      requests + campos, prontos para associar a um device.</p>
     <div class="gw-cat">${cards || "<p>Catálogo vazio.</p>"}</div>`,
    [{ rotulo: "Cancelar" },
     { rotulo: '<i class="fa-solid fa-download"></i> Importar', classe: "gw-btn--accent",
       acao: async (wrap) => {
         const arquivos = [...wrap.querySelectorAll("input:checked")].map((i) => i.value);
         if (!arquivos.length) { gwStatus("Nenhum modelo marcado.", "erro"); return; }
         let total = 0;
         for (const a of arquivos) {
           try { total += gwInserePacote(await gwCarregaPacote(a)); }
           catch (e) { gwStatus(`Falha em ${gwEsc(a)}: ${gwEsc(e.message || e)}`, "erro"); return; }
         }
         gwMarcaSujo();
         gwRender();
         gwStatus(`${total} registro(s) importados do catálogo.`, "ok");
       } }],
    620);
}

// ---------------------------------------------------------------------------
// Wizard "adicionar equipamento do catálogo"
//
// É o que faltava para não redigitar 60 devices na mão: escolhe o modelo,
// o canal e a faixa de Unit IDs, e sai com device + tópicos + comandos.
// ---------------------------------------------------------------------------

async function gwAbreWizardDevices() {
  let itens = [];
  try { itens = await gwCarregaCatalogo(); } catch (e) { /* segue só com template local */ }

  const opcoesCatalogo = itens.map((t) =>
    `<option value="cat:${gwEsc(t.file)}">${gwEsc(t.title)} (catálogo)</option>`).join("");
  const opcoesLocal = gwLista("templates").map((t) =>
    `<option value="tpl:${gwEsc(t.id)}">${gwEsc(t.name || t.id)} (já na config)</option>`).join("");
  const canais = gwLista("channels").map((c) =>
    `<option value="${gwEsc(c.id)}">${gwEsc(c.name || c.id)}${c.ip ? ` — ${gwEsc(c.ip)}` : ""}</option>`).join("");

  if (!canais) {
    gwStatus("Cadastre um canal na aba Comunicação antes de adicionar equipamentos.", "erro");
    return;
  }

  gwModal("Adicionar equipamentos",
    `<div class="gw-wiz">
      <label class="gw-campo"><span class="gw-rot">Modelo</span>
        <select class="gw-input" id="gwWzModelo">${opcoesCatalogo}${opcoesLocal}</select></label>
      <label class="gw-campo"><span class="gw-rot">Canal</span>
        <select class="gw-input" id="gwWzCanal">${canais}</select></label>
      <label class="gw-campo"><span class="gw-rot">Quantidade</span>
        <input class="gw-input" id="gwWzQtd" type="number" min="1" max="255" value="1"></label>
      <label class="gw-campo"><span class="gw-rot">Unit ID inicial</span>
        <input class="gw-input" id="gwWzUnit" type="number" min="1" max="255" value="1"></label>
      <label class="gw-campo"><span class="gw-rot">Prefixo do ID</span>
        <input class="gw-input" id="gwWzPrefixo" type="text" value="inv"></label>
      <label class="gw-campo"><span class="gw-rot">Nome base</span>
        <input class="gw-input" id="gwWzNome" type="text" value="Inversor"></label>
      <label class="gw-check"><input type="checkbox" id="gwWzTopicos" checked>
        <span>Gerar os tópicos (telemetry, command, feedback)</span></label>
      <label class="gw-check"><input type="checkbox" id="gwWzComandos" checked>
        <span>Criar os comandos padrão do modelo</span></label>
      <label class="gw-check"><input type="checkbox" id="gwWzMapa" checked>
        <span>Aceitar mapa de template ainda não validado em campo</span></label>
      <p class="gw-modal-txt gw-modal-txt--dica">
        O Unit ID cresce de 1 em 1. Os tópicos seguem o padrão da aba Usina e gateway,
        numerados por tipo de equipamento (inverter/1, inverter/2...).</p>
    </div>`,
    [{ rotulo: "Cancelar" },
     { rotulo: '<i class="fa-solid fa-plus"></i> Adicionar', classe: "gw-btn--accent",
       acao: async (wrap) => {
         const pega = (id) => wrap.querySelector("#" + id);
         const escolha = pega("gwWzModelo").value;
         const qtd = Math.max(1, Math.min(255, parseInt(pega("gwWzQtd").value, 10) || 1));
         const unit0 = Math.max(0, parseInt(pega("gwWzUnit").value, 10) || 1);
         const prefixo = (pega("gwWzPrefixo").value || "dev").trim();
         const nomeBase = (pega("gwWzNome").value || prefixo).trim();
         const canal = pega("gwWzCanal").value;
         const comTopicos = pega("gwWzTopicos").checked;
         const comComandos = pega("gwWzComandos").checked;
         const mapaLivre = pega("gwWzMapa").checked;

         try {
           let pacote = null;
           let templateId;
           if (escolha.startsWith("cat:")) {
             pacote = await gwCarregaPacote(escolha.slice(4));
             gwInserePacote(pacote);
             templateId = pacote.templates[0].id;
           } else {
             templateId = escolha.slice(4);
           }
           const criados = gwCriaDevices({
             templateId, pacote, qtd, unit0, prefixo, nomeBase, canal,
             comTopicos, comComandos, mapaLivre,
           });
           gwMarcaSujo();
           gwRender();
           gwStatus(`${criados.devices} equipamento(s), ${criados.topicos} tópico(s) e ` +
                    `${criados.comandos} comando(s) criados.`, "ok");
         } catch (e) {
           gwStatus(`Não deu para adicionar: ${gwEsc(e.message || e)}`, "erro");
           return false;
         }
       } }],
    620);
}

function gwCriaDevices(op) {
  const devices = gwLista("devices");
  const ids = new Set(devices.map((d) => String(d && d.id)));
  const padroes = (op.pacote && op.pacote.device_defaults) || {};
  const blueprints = (op.pacote && op.pacote.command_defaults) || [];
  const template = gwLista("templates").find((t) => String(t.id) === String(op.templateId)) || {};
  const usina = String(gwGet(GW.cfg, "plant.metadata.topic_slug", "") ||
                       gwGet(GW.cfg, "plant.id", "")).trim();

  let nDev = 0, nTop = 0, nCmd = 0;
  for (let i = 0; i < op.qtd; i += 1) {
    const seq = String(i + 1).padStart(2, "0");
    let id = `${op.prefixo}-${seq}`;
    let n = i + 1;
    while (ids.has(id)) { n += 1; id = `${op.prefixo}-${String(n).padStart(2, "0")}`; }
    ids.add(id);

    const device = {
      id,
      name: `${op.nomeBase} ${seq}`,
      description: `${op.nomeBase}${n}`,
      enabled: true,
      manufacturer: padroes.manufacturer || template.manufacturer || "",
      model: padroes.model || template.model || "",
      device_type: padroes.device_type || template.device_type || "custom",
      channel_id: op.canal,
      unit_id: op.unit0 + i,
      template_id: op.templateId,
      poll_interval_ms: padroes.poll_interval_ms != null ? padroes.poll_interval_ms : 1000,
      publish_interval_ms: padroes.publish_interval_ms != null ? padroes.publish_interval_ms : 10000,
      stale_timeout_ms: padroes.stale_timeout_ms != null ? padroes.stale_timeout_ms : 30000,
      commands_enabled: !!padroes.commands_enabled,
      metadata: {
        ...(padroes.metadata || {}),
        domain: gwGet(GW.cfg, "plant.company", "") || gwGet(padroes, "metadata.domain", ""),
        complex: gwGet(GW.cfg, "plant.metadata.complex", "UFV"),
        power_plant: usina,
        allow_unverified_map: op.mapaLivre ? true : !!gwGet(padroes, "metadata.allow_unverified_map", false),
      },
    };
    devices.push(device);
    nDev += 1;

    if (op.comTopicos) nTop += gwCriaTopicos(device).length;
    if (op.comComandos && blueprints.length) nCmd += gwCriaComandos(device, blueprints, op.templateId);
  }
  return { devices: nDev, topicos: nTop, comandos: nCmd };
}

// Índice do device dentro do seu TIPO — é assim que a Naturágua numera os
// tópicos (inverter/1..10, stringbox/1..50), não pelo id do device.
function gwIndiceNoTipo(device) {
  const tipo = String(device.device_type || "custom");
  const mesmos = gwLista("devices").filter((d) => String(d.device_type || "custom") === tipo);
  const pos = mesmos.findIndex((d) => String(d.id) === String(device.id));
  return (pos < 0 ? mesmos.length : pos) + 1;
}

function gwFormataTopico(padrao, device, indice) {
  const usina = String(gwGet(GW.cfg, "plant.metadata.topic_slug", "") ||
                       gwGet(GW.cfg, "plant.id", "")).trim();
  return String(padrao || "")
    .replace(/\{plant\}/g, usina)
    .replace(/\{type\}/g, String(device.device_type || "custom"))
    .replace(/\{id\}/g, String(indice))
    .replace(/\{device\}/g, String(device.id));
}

function gwCriaTopicos(device) {
  const topicos = gwLista("topics");
  const existentes = new Set(topicos.map((t) => `${t.device_id}|${t.purpose}`));
  const indice = gwIndiceNoTipo(device);
  const geral = GW.cfg.general || {};
  const meta = geral.metadata || {};
  const criar = [
    { purpose: "telemetry", sufixo: "telemetry", shared: false,
      topico: gwFormataTopico(meta.telemetry_pattern || "dev/read/UFV/{plant}/{type}/{id}", device, indice) },
    { purpose: "command", sufixo: "command", shared: false,
      topico: gwFormataTopico(meta.command_pattern || "dev/write/UFV/{plant}/{type}/{id}", device, indice) },
    { purpose: "feedback", sufixo: "feedback", shared: true,
      topico: String(geral.command_feedback_topic || "") },
  ];
  const novos = [];
  criar.forEach((t) => {
    if (!t.topico) return;
    if (existentes.has(`${device.id}|${t.purpose}`)) return;
    const registro = {
      id: `${device.id}-${t.sufixo}`, device_id: device.id, purpose: t.purpose,
      topic: t.topico, qos: Number(geral.default_qos != null ? geral.default_qos : 1),
      retain: !!geral.default_retain, shared: t.shared, editable: true,
    };
    topicos.push(registro);
    novos.push(registro);
  });
  return novos;
}

function gwCriaComandos(device, blueprints, templateId) {
  const comandos = gwLista("commands");
  const ids = new Set(comandos.map((c) => String(c && c.id)));
  const feedback = String(gwGet(GW.cfg, "general.command_feedback_topic", ""));
  let n = 0;
  blueprints.slice(0, GW_LIMITES.commands_por_device).forEach((bp) => {
    const cmd = { ...bp };
    delete cmd.key;
    cmd.id = `${device.id}-${bp.key || String(bp.name || "cmd").toLowerCase()}`;
    if (ids.has(cmd.id)) return;
    cmd.device_id = device.id;
    cmd.template_id = templateId;
    cmd.feedback_topic = feedback;
    comandos.push(cmd);
    ids.add(cmd.id);
    n += 1;
  });
  return n;
}

// Gera os tópicos que faltam para TODOS os devices (botão da aba Tópicos).
function gwGeraTopicosFaltantes() {
  let n = 0;
  gwLista("devices").forEach((d) => { n += gwCriaTopicos(d).length; });
  if (!n) { gwStatus("Todos os equipamentos já têm tópicos.", "info"); return; }
  gwMarcaSujo();
  gwRender();
  gwStatus(`${n} tópico(s) gerados pelo padrão.`, "ok");
}

// ---------------------------------------------------------------------------
// Exportar template (mesmo formato do publicador do Igor)
// ---------------------------------------------------------------------------

function gwExportaTemplate() {
  const id = GW.sel.templates;
  if (!id) { gwStatus("Selecione um template na tabela primeiro.", "erro"); return; }
  const template = gwLista("templates").find((t) => String(t.id) === String(id));
  if (!template) return;
  const pacote = {
    format: "aioti.gateway.template",
    schema_version: "1.0.0",
    templates: [template],
    requests: gwLista("requests").filter((r) => String(r.template_id) === String(id)),
    fields: gwLista("fields").filter((f) => String(f.template_id) === String(id)),
  };
  gwBaixaJson(pacote, `${id}.json`);
  gwStatus(`Template ${gwEsc(id)} exportado (abre no publicador local também).`, "ok");
}

function gwBaixaJson(objeto, nome) {
  const blob = new Blob([JSON.stringify(objeto, null, 2) + "\n"], { type: "application/json" });
  const a = document.createElement("a");
  a.href = URL.createObjectURL(blob);
  a.download = nome;
  a.click();
  setTimeout(() => URL.revokeObjectURL(a.href), 1500);
}

function gwAbreArquivo(aoLer) {
  const input = document.createElement("input");
  input.type = "file";
  input.accept = "application/json,.json";
  input.onchange = () => {
    const arq = input.files && input.files[0];
    if (!arq) return;
    const leitor = new FileReader();
    leitor.onload = () => {
      try { aoLer(JSON.parse(String(leitor.result).replace(/^﻿/, ""))); }
      catch (e) { gwStatus(`Arquivo inválido: ${gwEsc(e.message || e)}`, "erro"); }
    };
    leitor.readAsText(arq, "utf-8");
  };
  input.click();
}

// ---------------------------------------------------------------------------
// Validação local
//
// Não substitui o compilador (é ele quem manda), mas pega no editor o que
// senão viraria recusa silenciosa no validate do gateway.
// ---------------------------------------------------------------------------

function gwValida() {
  const p = [];
  const cfg = GW.cfg || {};
  const ids = (k) => new Set(gwLista(k).map((r) => String(r && r.id)));

  if (!String(gwGet(cfg, "plant.id", "")).trim()) {
    p.push('Aba Usina e gateway: "ID da usina" está vazio — é o segmento do tópico.');
  }
  if (GW.plantName && String(gwGet(cfg, "plant.id", "")).trim() !== GW.plantName) {
    p.push(`O ID da usina ("${gwGet(cfg, "plant.id", "")}") é diferente do nome no cadastro ` +
           `("${GW.plantName}"). A ingestão casa EXATO — confira antes de publicar.`);
  }

  // ids duplicados / vazios
  GW_SECOES.forEach((s) => {
    const vistos = new Set();
    gwLista(s.key).forEach((r, i) => {
      const id = String(r && r.id != null ? r.id : "").trim();
      if (!id) { p.push(`${s.titulo}: item ${i + 1} sem ID.`); return; }
      if (vistos.has(id)) p.push(`${s.titulo}: ID repetido "${id}".`);
      vistos.add(id);
    });
  });

  // referências
  const canais = ids("channels"), templates = ids("templates"),
        devices = ids("devices"), requests = ids("requests"), sequencias = ids("sequences");
  const ref = (secao, campo, universo, rotulo, obrigatorio = true) => {
    gwLista(secao).forEach((r) => {
      const v = String(gwGet(r, campo, "") || "").trim();
      if (!v) { if (obrigatorio) p.push(`${secao}[${r.id}]: ${rotulo} não preenchido.`); return; }
      if (!universo.has(v)) p.push(`${secao}[${r.id}]: ${rotulo} "${v}" não existe.`);
    });
  };
  ref("devices", "channel_id", canais, "canal");
  ref("devices", "template_id", templates, "template");
  ref("requests", "template_id", templates, "template");
  ref("fields", "template_id", templates, "template");
  ref("topics", "device_id", devices, "device");
  ref("commands", "device_id", devices, "device");
  ref("alarms", "device_id", devices, "device");
  ref("events", "device_id", devices, "device");
  ref("sequences", "device_id", devices, "device", false);

  // campo modbus tem que apontar para uma request
  gwLista("fields").forEach((f) => {
    const origem = String(f.source_type || "modbus");
    const req = String(f.request_id || "").trim();
    if (origem === "modbus" && !req) p.push(`fields[${f.id}]: origem modbus sem request.`);
    if (req && !requests.has(req)) p.push(`fields[${f.id}]: request "${req}" não existe.`);
  });

  // alvos do PID
  gwLista("pid").forEach((pid) => {
    const alvos = gwGet(pid, "metadata.targets", []);
    (Array.isArray(alvos) ? alvos : []).forEach((t, i) => {
      const d = String(t.device_id || "").trim();
      if (!d || !devices.has(d)) p.push(`pid[${pid.id}] alvo ${i + 1}: device "${d}" não existe.`);
      const s = String(t.sequence_id || "").trim();
      if (s && !sequencias.has(s)) p.push(`pid[${pid.id}] alvo ${i + 1}: sequência "${s}" não existe.`);
    });
  });

  // device sem telemetria não publica nada
  const comTelemetria = new Set(gwLista("topics")
    .filter((t) => String(t.purpose) === "telemetry").map((t) => String(t.device_id)));
  const semTopico = gwLista("devices").filter((d) => !comTelemetria.has(String(d.id)));
  if (semTopico.length) {
    p.push(`${semTopico.length} equipamento(s) sem tópico de telemetria ` +
           `(${semTopico.slice(0, 4).map((d) => d.id).join(", ")}${semTopico.length > 4 ? "..." : ""}). ` +
           `Use GERAR TÓPICOS na aba Tópicos MQTT.`);
  }

  // armadilha nº 4 de 25/07: mapa de template não validado derruba a transação inteira
  const naoVerificado = new Set();
  gwLista("requests").concat(gwLista("fields")).forEach((r) => {
    if (String(r.validation_status || "verified") !== "verified") naoVerificado.add(String(r.template_id));
  });
  const arriscados = gwLista("devices").filter((d) =>
    naoVerificado.has(String(d.template_id)) && !gwGet(d, "metadata.allow_unverified_map", false));
  if (arriscados.length) {
    p.push(`${arriscados.length} equipamento(s) usam template com mapa NÃO validado e estão sem ` +
           `"Permitir mapa não verificado". O gateway recusaria a configuração inteira ` +
           `(${arriscados.slice(0, 4).map((d) => d.id).join(", ")}).`);
  }

  // capacidades do staging
  const conta = (lista, campo) => {
    const c = {};
    lista.forEach((x) => {
      const k = String(gwGet(x, campo, "") || "");
      if (k) c[k] = (c[k] || 0) + 1;
    });
    return c;
  };
  [["channels", "canais"], ["templates", "templates"], ["devices", "equipamentos"],
   ["sequences", "sequências"], ["pid", "controladores PID"]].forEach(([k, rot]) => {
    const n = gwLista(k).length;
    if (n > GW_LIMITES[k]) p.push(`${n} ${rot} — o gateway aceita ${GW_LIMITES[k]}.`);
  });
  Object.entries(conta(gwLista("requests"), "template_id")).forEach(([t, n]) => {
    if (n > GW_LIMITES.requests_por_template) p.push(`template ${t}: ${n} requests (máximo ${GW_LIMITES.requests_por_template}).`);
  });
  Object.entries(conta(gwLista("fields"), "template_id")).forEach(([t, n]) => {
    if (n > GW_LIMITES.fields_por_template) p.push(`template ${t}: ${n} campos (máximo ${GW_LIMITES.fields_por_template}).`);
  });
  Object.entries(conta(gwLista("commands"), "device_id")).forEach(([d, n]) => {
    if (n > GW_LIMITES.commands_por_device) p.push(`device ${d}: ${n} comandos (máximo ${GW_LIMITES.commands_por_device}).`);
  });
  gwLista("sequences").forEach((s) => {
    const n = Array.isArray(s.steps) ? s.steps.length : 0;
    if (n > GW_LIMITES.passos_por_sequencia) p.push(`sequência ${s.id}: ${n} passos (máximo ${GW_LIMITES.passos_por_sequencia}).`);
  });
  // status_map: 16 por TEMPLATE, somando todos os campos dele
  const statusPorTemplate = {};
  gwLista("fields").forEach((f) => {
    const m = f.status_mapping;
    if (m && typeof m === "object") {
      const t = String(f.template_id || "");
      statusPorTemplate[t] = (statusPorTemplate[t] || 0) + Object.keys(m).length;
    }
  });
  Object.entries(statusPorTemplate).forEach(([t, n]) => {
    if (n > GW_LIMITES.status_map_por_template) {
      p.push(`template ${t}: ${n} correlações de status (máximo ${GW_LIMITES.status_map_por_template}).`);
    }
  });

  return p;
}

function gwMostraValidacao(problemas, extra = "") {
  const corpo = problemas.length
    ? `<p class="gw-modal-txt gw-modal-txt--alerta">
         <i class="fa-solid fa-triangle-exclamation"></i>
         ${problemas.length} ponto(s) para revisar antes de publicar:</p>
       <ul class="gw-lista-erros">${problemas.map((x) => `<li>${gwEsc(x)}</li>`).join("")}</ul>`
    : `<p class="gw-modal-txt gw-modal-txt--ok">
         <i class="fa-solid fa-circle-check"></i>
         Nenhum problema encontrado na conferência da tela.</p>`;
  gwModal("Validação", corpo + extra, [{ rotulo: "Fechar", classe: "gw-btn--accent" }], 680);
}

// "Validar tudo": confere aqui e, se o backend já tiver o modo de conferência,
// compila de verdade com o mqtt_entities.py (sem publicar nada).
async function gwValidaTudo() {
  if (GW.aba === "geral") gwGeralAplica(false);
  const problemas = gwValida();
  let extra = "";
  try {
    const r = await gwFetch(`/plants/${GW.plantId}/clp/config`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ action: "compile", configuration: GW.cfg }),
    });
    const d = await r.json().catch(() => ({}));
    if (r.ok && d.ok && d.entity_count != null) {
      extra = `<p class="gw-modal-txt gw-modal-txt--ok"><i class="fa-solid fa-microchip"></i>
        O compilador do gateway aceitou: <strong>${d.entity_count}</strong> entidades em
        <strong>${d.messages}</strong> mensagens (CRC ${gwEsc(d.crc32 || "—")}).</p>`;
    } else if (r.status === 400 && /^configuração inválida/i.test(String(d.error || ""))) {
      // só é recusa do compilador quando a mensagem vem DELE. A Lambda antiga,
      // que não conhece action="compile", cai no caminho legado e responde
      // "username e password são obrigatórios" — exibir isso como recusa da
      // configuração seria mentira.
      extra = `<p class="gw-modal-txt gw-modal-txt--alerta"><i class="fa-solid fa-microchip"></i>
        O compilador do gateway recusou: ${gwEsc(d.error)}</p>`;
    }
    // qualquer outra resposta = Lambda ainda sem o modo de conferência:
    // fica só a conferência local, sem inventar veredito
  } catch (e) { /* offline: conferência local basta */ }
  gwMostraValidacao(problemas, extra);
  return problemas.length === 0;
}

// ---------------------------------------------------------------------------
// Banco: carregar e salvar
// ---------------------------------------------------------------------------

async function gwCarregaDoBanco(silencioso = false) {
  if (!silencioso) gwStatus("Carregando a configuração da usina...", "wait");
  const r = await gwFetch(`/plants/${GW.plantId}/clp/diagnostics`);
  if (!r.ok) throw new Error(r.status === 401 || r.status === 403
    ? `o servidor recusou o acesso (HTTP ${r.status}) — talvez seja preciso entrar de novo`
    : `o servidor respondeu HTTP ${r.status}`);
  const d = await r.json();

  GW.podeEditar = !!d.is_admin;
  GW.historico = Array.isArray(d.gateway_config_history) ? d.gateway_config_history : [];
  const gc = d.gateway_config && typeof d.gateway_config.config === "object"
    ? d.gateway_config.config : null;
  GW.salvaNoBanco = gc ? JSON.parse(JSON.stringify(gc)) : null;

  const info = document.getElementById("gwSalvaInfo");
  if (info) {
    info.innerHTML = d.gateway_config
      ? `salva em ${gwEsc(new Date(d.gateway_config.updated_at).toLocaleString("pt-BR"))}` +
        (d.gateway_config.updated_by ? ` por ${gwEsc(d.gateway_config.updated_by)}` : "")
      : "nenhuma configuração salva ainda";
  }
  return gc;
}

async function gwSalvaNoBanco() {
  // Se a leitura inicial falhou, a tela não sabe o que está gravado: salvar
  // aqui pode enterrar uma configuração boa. Perguntar antes.
  if (!GW.leuDoBanco && !(await gwPergunta("Salvar sem ter lido a atual",
      "Não consegui ler a configuração que está gravada nesta usina. Salvar agora " +
      "substitui o que estiver lá — inclusive uma configuração que esteja funcionando. Continuar?"))) {
    return;
  }
  if (GW.aba === "geral") gwGeralAplica(false);
  gwStatus("Salvando no banco...", "wait");
  try {
    const r = await gwFetch(`/plants/${GW.plantId}/clp/config`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ action: "save", config: GW.cfg }),
    });
    const d = await r.json().catch(() => ({}));
    if (!r.ok || d.ok === false) throw new Error(d.error || `HTTP ${r.status}`);
    GW.salvaNoBanco = JSON.parse(JSON.stringify(GW.cfg));
    GW.sujo = false;
    gwPintaStatus();
    await gwCarregaDoBanco(true);
    gwStatus("Configuração salva no banco desta usina.", "ok");
  } catch (e) {
    gwStatus(`Não deu para salvar: ${gwEsc(e.message || e)}`, "erro");
  }
}

async function gwRestauraDoBanco() {
  if (!GW.salvaNoBanco) { gwStatus("Não há configuração salva no banco.", "erro"); return; }
  if (GW.sujo && !(await gwPergunta("Restaurar",
      "Descartar as alterações da tela e voltar para a configuração salva no banco?"))) return;
  GW.cfg = JSON.parse(JSON.stringify(GW.salvaNoBanco));
  GW.sujo = false;
  GW.sel = {};
  gwRender();
  gwStatus("Configuração do banco recarregada.", "ok");
}

// ---------------------------------------------------------------------------
// Publicação (begin → put+ACK ×N → validate → commit, no worker do backend)
// ---------------------------------------------------------------------------

async function gwPublica() {
  if (GW.aba === "geral") gwGeralAplica(false);
  const problemas = gwValida();
  if (problemas.length) {
    gwMostraValidacao(problemas,
      `<p class="gw-modal-txt">Resolva os pontos acima antes de publicar — o gateway
        recusa a transação inteira e a configuração antiga continua valendo.</p>`);
    return;
  }
  if (GW.sujo) {
    const salvar = await gwPergunta("Alterações não salvas",
      "Há alterações que ainda não foram salvas no banco. Salvar antes de publicar?");
    if (salvar) await gwSalvaNoBanco();
  }

  const nDev = gwLista("devices").length;
  const nCampos = gwLista("fields").length;
  gwModal("Publicar no gateway",
    `<p class="gw-modal-txt">
       Vão para o gateway <strong>${nDev}</strong> equipamento(s) e <strong>${nCampos}</strong> campo(s).
       O servidor compila, abre a transação e envia uma mensagem por vez, esperando o gateway
       confirmar cada uma — leva alguns minutos. Nada muda no equipamento antes do commit.</p>
     <p class="gw-modal-txt gw-modal-txt--dica">Confirme com o seu usuário e senha
       (mesma exigência do comando remoto, porque isto escreve em hardware de campo).</p>
     <label class="gw-campo"><span class="gw-rot">Usuário</span>
       <input class="gw-input" id="gwPubUser" type="text" autocomplete="off"
              value="${gwEsc(gwUser().username || "")}"></label>
     <label class="gw-campo"><span class="gw-rot">Senha</span>
       <input class="gw-input" id="gwPubPass" type="password" autocomplete="new-password"></label>`,
    [{ rotulo: "Cancelar" },
     { rotulo: '<i class="fa-solid fa-satellite-dish"></i> Publicar e aplicar', classe: "gw-btn--accent",
       acao: (wrap) => {
         const u = wrap.querySelector("#gwPubUser").value.trim();
         const s = wrap.querySelector("#gwPubPass").value;
         if (!u || !s) { gwStatus("Informe usuário e senha.", "erro"); return false; }
         gwEnvia(u, s);
       } }],
    560);
}

async function gwEnvia(username, password) {
  const painel = document.getElementById("gwProgresso");
  const pinta = (html) => { if (painel) painel.innerHTML = html; };
  const espera = (ms) => new Promise((r) => setTimeout(r, ms));

  try {
    painel.classList.add("is-on");
    pinta(`<div class="gw-prog-msg"><i class="fa-solid fa-circle-notch fa-spin"></i>
             Compilando a configuração no servidor...</div>`);

    const r = await gwFetch(`/plants/${GW.plantId}/clp/config`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ action: "apply_v2", configuration: GW.cfg, username, password }),
    });
    const d = await r.json().catch(() => ({}));
    if (!r.ok || d.ok === false) throw new Error(d.error || `HTTP ${r.status}`);

    const rid = d.request_id;
    const total = Number(d.messages) || 0;

    for (;;) {
      await espera(1500);
      const jr = await gwFetch(`/plants/${GW.plantId}/clp/config`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ action: "job", request_id: rid }),
      });
      const j = await jr.json().catch(() => ({}));
      if (!jr.ok) throw new Error(j.error || `HTTP ${jr.status}`);

      const enviadas = Number(j.entities_sent) || 0;
      const pct = total ? Math.min(100, Math.round((enviadas / total) * 100)) : 0;
      const etapa = {
        begin: "abrindo a transação", put: "enviando entidades",
        validate: "o gateway está conferindo CRC e SHA-256",
        commit: "aplicando no equipamento", finished: "concluído",
      }[j.stage] || (j.stage || "");

      if (j.status === "success") {
        pinta(`<div class="gw-prog-msg gw-prog-msg--ok"><i class="fa-solid fa-circle-check"></i>
                 Configuração aplicada e confirmada pelo gateway (${enviadas}/${total} mensagens).</div>`);
        gwStatus("Gateway confirmou o commit.", "ok");
        gwCarregaDoBanco(true).catch(() => {});
        return;
      }
      if (j.status === "failed") {
        pinta(`<div class="gw-prog-msg gw-prog-msg--erro"><i class="fa-solid fa-triangle-exclamation"></i>
                 O gateway recusou: ${gwEsc(j.message || "sem detalhe")}
                 <small>Parou em ${enviadas} de ${total}. A configuração anterior continua valendo
                 (o servidor publicou abort).</small></div>`);
        gwStatus("O gateway recusou a configuração.", "erro");
        return;
      }

      pinta(`<div class="gw-prog-msg"><i class="fa-solid fa-circle-notch fa-spin"></i>
               ${pct}% · ${enviadas} de ${total} mensagens · ${gwEsc(etapa)}</div>
             <div class="gw-prog-bar"><span style="width:${pct}%"></span></div>
             <small class="gw-prog-hint">Pode fechar a tela — o envio continua no servidor.</small>`);
    }
  } catch (e) {
    pinta(`<div class="gw-prog-msg gw-prog-msg--erro"><i class="fa-solid fa-triangle-exclamation"></i>
             ${gwEsc(e.message || e)}</div>`);
    gwStatus(`Falha no envio: ${gwEsc(e.message || e)}`, "erro");
  }
}

// ---------------------------------------------------------------------------
// Boot
// ---------------------------------------------------------------------------

async function gwInicia() {
  const params = new URLSearchParams(location.search);
  GW.plantId = params.get("plant_id");
  GW.plantName = (params.get("plant") || "").trim();

  const u = gwUser();
  if (!u || !u.username) { location.href = "index.html"; return; }
  if (!GW.plantId) {
    gwStatus("Abra o configurador pelo botão do gateway na tela da usina.", "erro");
    return;
  }

  const titulo = document.getElementById("gwPlanta");
  if (titulo) titulo.textContent = GW.plantName || `usina #${GW.plantId}`;

  // Só cai em modo leitura quando o servidor DISSE que não é administrador.
  // Falha de rede não é falta de permissão: a tela continua editável e quem
  // barra de verdade é o backend, no salvar/publicar.
  let doBanco = null;
  let respondeu = false;
  GW.podeEditar = true;
  try {
    doBanco = await gwCarregaDoBanco();
    respondeu = true;
  } catch (e) {
    gwStatus(`Não consegui ler a usina (${gwEsc(e.message || e)}). ` +
             `Dá para editar e baixar o JSON, mas salvar no banco vai depender da conexão.`, "erro");
  }

  if (respondeu && !GW.podeEditar) {
    document.getElementById("gwShell").classList.add("is-readonly");
    gwStatus("Você está vendo a configuração em modo leitura (é preciso ser administrador).", "info");
  }

  GW.cfg = doBanco || gwNovaConfiguracao(GW.plantName);
  GW.leuDoBanco = respondeu;
  GW.sujo = false;
  gwRender();
  // "não consegui ler" e "ainda não tem configuração" produzem a mesma tela em
  // branco, e são coisas MUITO diferentes: no primeiro caso pode haver uma
  // configuração boa gravada. Só dá a mensagem de vazio quando a leitura deu
  // certo; senão o aviso de erro fica de pé (e o salvar pede confirmação).
  if (respondeu) {
    gwStatus(doBanco
      ? "Configuração carregada do banco desta usina."
      // quem não é administrador não enxerga a aba "Usina e gateway", então
      // mandar começar por ela seria mandar procurar o que não existe
      : GW.podeEditar
        ? "Usina ainda sem configuração — começamos de uma em branco. " +
          "Comece pela aba Usina e gateway."
        : "Esta usina ainda não tem configuração de gateway salva.",
      doBanco ? "ok" : "info");
  }

  // ações da barra
  const liga = (id, fn) => { const b = document.getElementById(id); if (b) b.onclick = fn; };
  liga("gwBtnSalvar", gwSalvaNoBanco);
  liga("gwBtnRestaurar", gwRestauraDoBanco);
  liga("gwBtnValidar", gwValidaTudo);
  liga("gwBtnPublicar", gwPublica);
  liga("gwBtnBaixar", () => {
    if (GW.aba === "geral") gwGeralAplica(false);
    const nome = String(gwGet(GW.cfg, "plant.id", "config") || "config");
    gwBaixaJson(GW.cfg, `${nome}_gateway.json`);
  });
  liga("gwBtnAbrir", () => gwAbreArquivo(async (json) => {
    if (!json || typeof json !== "object") { gwStatus("JSON não é um objeto.", "erro"); return; }
    if (Array.isArray(json.templates) && !json.plant && !json.devices) {
      const n = gwInserePacote(json);   // é um pacote de template, não uma planta
      gwMarcaSujo(); gwRender();
      gwStatus(`${n} registro(s) importados do pacote de template.`, "ok");
      return;
    }
    if (GW.sujo && !(await gwPergunta("Abrir arquivo", "Descartar as alterações da tela?"))) return;
    GW.cfg = json;
    GW.sel = {};
    gwMarcaSujo();
    gwRender();
    gwStatus("Configuração do arquivo carregada na tela (ainda não foi salva no banco).", "ok");
  }));
  liga("gwBtnNova", async () => {
    if (GW.sujo && !(await gwPergunta("Nova configuração", "Descartar as alterações da tela?"))) return;
    GW.cfg = gwNovaConfiguracao(GW.plantName);
    GW.sel = {};
    gwMarcaSujo();
    gwRender();
    gwStatus("Configuração em branco. Comece pela aba Usina e gateway.", "info");
  });

  window.addEventListener("beforeunload", (e) => {
    if (!GW.sujo) return;
    e.preventDefault();
    e.returnValue = "";
  });
}

document.addEventListener("DOMContentLoaded", gwInicia);
