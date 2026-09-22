/* ======================================================================
   HELP TOUR — manual de ajuda interativo da plataforma AIOTI
   Botão "Ajuda ?" em cada aba → tour guiado com "holofote" que destaca
   cada elemento da tela e explica ao lado, avançando no clique.
   Auto-contido: injeta os próprios estilos e o botão de ajuda.
   ====================================================================== */
(function () {
  "use strict";

  // ─────────────────────────────────────────────────────────────
  // TOURS — passos por aba (seletores + título + descrição)
  // Passos cujo elemento não existir/estiver oculto são pulados.
  // ─────────────────────────────────────────────────────────────
  const TOURS = {
    overview: [
      { sel: "#topSummary", title: "Resumo geral", text: "Visão consolidada de todas as suas usinas em tempo real: contadores de status, capacidade total e potência ativa somada." },
      { sel: "#countGen", title: "Em geração", text: "Quantos inversores estão gerando energia agora, em todas as usinas." },
      { sel: "#countNoComm", title: "No comm", text: "Inversores sem comunicação — a leitura não chega há mais de 25 minutos. Pode ser internet do local, equipamento desligado ou manutenção." },
      { sel: "#countOff", title: "Off", text: "Inversores desligados (comunicando, porém sem gerar)." },
      { sel: "#psfCapacityPct", title: "Capacidade total", text: "Percentual da potência ativa atual sobre a Capacity (AC) somada das usinas — o quanto do potencial de entrega está sendo usado agora." },
      { sel: "#psfActivePower", title: "Potência ativa", text: "Soma da potência de todas as usinas neste instante, em kW." },
      { sel: "#portfolioSearchInput", title: "Busca", text: "Filtre as usinas pelo nome." },
      { sel: "#btnViewCard", title: "Cards ou lista", text: "Alterne entre a visão em cards (com mini-gráficos) e a visão em tabela." },
      { sel: ".plant-card", title: "Card da usina", text: "Cada card mostra potência ativa, energia do dia, PR (Performance Ratio — eficiência real vs. teórica), irradiância e disponibilidade. Clique para abrir a página completa da usina." },
      { sel: ".plant-card__issue-badge", title: "Acontecimentos", text: "Este sinal aparece quando a usina tem algum acontecimento: vermelho = crítico, âmbar = aviso. Passe o mouse para ver o resumo e clique para abrir o relatório." },
      { sel: "#miniChartPowerToggleWrap", title: "Fonte dos mini-gráficos", text: "Escolha se os mini-gráficos dos cards mostram a potência dos inversores ou do multimedidor." },
      { sel: "#robotAssistant", title: "Assistente AIOTI", text: "O robô monitora as usinas e avisa sobre anomalias: usina desligada, temperatura alta, PR caindo e mais. Clique nele para ver o relatório e a ronda diária." },
      { sel: "#notifBellBtn", title: "Notificações", text: "Central de avisos da plataforma — novidades de atualização e retornos de tickets aparecem aqui." },
      { sel: "#btnDataStudio", title: "Data Studio", text: "Monte gráficos personalizados com qualquer medida de qualquer equipamento — vamos lá? Abra a aba e clique em Ajuda de novo." }
    ],

    alarms: [
      { sel: "#alarmsView", title: "Alertas", text: "Todos os alarmes ativos das suas usinas, com severidade, equipamento e horário." },
      { sel: "#alarmsTbody", title: "Lista de alarmes", text: "Cada linha é um alarme ativo. A cor indica a severidade: vermelho = alto, laranja = médio." }
    ],

    events: [
      { sel: "#eventsView", title: "Eventos", text: "Histórico de tudo que aconteceu: alarmes que abriram e fecharam, mudanças de estado dos equipamentos." },
      { sel: "#eventsStartDateTimeInput", title: "Período", text: "Defina o intervalo de datas da consulta." },
      { sel: "#eventsPlantSelect", title: "Filtros", text: "Filtre por usina, equipamento, tipo de evento, status (ativo/normalizado) e severidade." },
      { sel: "#eventsApplyBtn", title: "Aplicar", text: "Roda a consulta com os filtros escolhidos." },
      { sel: "#eventsTbody", title: "Resultado", text: "Os eventos aparecem aqui, do mais recente para o mais antigo." },
      { sel: "#eventsPageLabel", title: "Paginação", text: "Navegue entre as páginas do resultado." }
    ],

    datastudio: [
      { sel: "#dsPlantSelect", title: "Escolha a usina", text: "Selecione uma ou mais usinas para analisar (o seletor múltiplo permite comparar usinas)." },
      { sel: "#dsStartDateInput", title: "Período", text: "Datas de início e fim da análise. Períodos curtos trazem mais detalhe (até 5 em 5 minutos); longos são agregados por hora/dia." },
      { sel: "#dsModeSelect", title: "Modo", text: "Histórico = série temporal contínua; Consolidado = valores diários/mensais fechados (energia, PR)." },
      { sel: "#dsAggregationSelect", title: "Agregação", text: "Como resumir cada intervalo: média, máximo ou soma. Ex.: potência usa média; energia usa soma." },
      { sel: "#dsApplyTopbarBtn", title: "Aplicar", text: "Carrega as medidas disponíveis para o período e usinas escolhidas." },
      { sel: "#dsPlantBlocks", title: "Medidas (tags)", text: "Aqui ficam todas as medidas de cada equipamento: potências, correntes, tensões, temperaturas, strings, alarmes... Marque as que quiser plotar. Use a busca para achar rápido (ex.: \"temperatura\")." },
      { sel: "#dsLoadSeriesBtn", title: "Gerar gráfico", text: "Plota as medidas selecionadas em um gráfico interativo (zoom com a roda do mouse)." },
      { sel: "#dsFavoriteToggleBtn", title: "Favoritos", text: "Salve conjuntos de medidas que você usa sempre, para recarregar com um clique." },
      { sel: "#dsExportAllBtn", title: "Exportar", text: "Baixa os dados do gráfico em CSV para usar no Excel." },
      { sel: "#dsSaveSelectionBtn", title: "Salvar seleção", text: "Guarda a seleção atual de medidas para continuar depois." }
    ],

    explorer: [
      { sel: "#explorerPlantSelect", title: "Explorador de dados", text: "Consulta direta aos dados brutos que chegam dos equipamentos — útil para conferir leituras exatas." },
      { sel: "#explorerDataType", title: "Tipo de dado", text: "Escolha a origem: inversor, relé, multimedidor, estação solarimétrica..." },
      { sel: "#explorerStartDate", title: "Período", text: "Intervalo da consulta." },
      { sel: "#explorerLoadBtn", title: "Buscar", text: "Executa a consulta." },
      { sel: "#explorerTableContainer", title: "Resultado", text: "Dados crus em tabela, como chegaram do equipamento." },
      { sel: "#explorerExportCsv", title: "Exportar", text: "Baixa o resultado em CSV." }
    ],

    tickets: [
      { sel: "#tkBtnNew", title: "Novo chamado", text: "Abra um chamado para o suporte AIOTI: dúvidas, problemas ou solicitações." },
      { sel: "#tkList", title: "Seus chamados", text: "Acompanhe o status de cada chamado. Clique em um para ver a conversa completa e responder." }
    ],

    plant: [
      { sel: "#plantName", title: "Página da usina", text: "Tudo desta usina em um só lugar: produção, equipamentos, alarmes e diagrama unifilar." },
      { sel: "#headerActivePower", title: "Potência ativa", text: "Quanto a usina está gerando neste instante." },
      { sel: "#headerCapacity", title: "Capacidade", text: "Percentual da potência atual sobre a Capacity (AC) — o quanto do potencial de entrega está em uso." },
      { sel: "#plantAlarmMenuButton", title: "Alarmes", text: "Alarmes ativos desta usina. O contador vermelho indica quantos." },
      { sel: "#weatherExpandBtn", title: "Estação meteorológica", text: "Irradiância, temperaturas, vento e chuva medidos na usina. Clique para expandir mais sensores." },
      { sel: "#plantMainChart", title: "Produção diária", text: "A curva do dia: potência gerada (verde), irradiância (amarelo) e a expectativa (tracejado — curva PVSyst ou linha do Capacity AC)." },
      { sel: "#dailyPowerToggleWrap", title: "Fonte da curva", text: "Alterne entre a potência somada dos inversores e a medição do multimedidor." },
      { sel: "#dailyExpToggleWrap", title: "Expectativa", text: "Escolha a referência: curva PVSyst (simulação) ou linha reta no Capacity AC da usina." },
      { sel: "#dailyZoomInBtn", title: "Zoom", text: "Aproxime, afaste ou resete o zoom do gráfico. A roda do mouse também funciona." },
      { sel: "#plantMonthlyChart", title: "Geração mensal", text: "Energia real de cada dia do mês (barras) contra a expectativa (linha). Os KPIs acima mostram o acumulado." },
      { sel: "#invertersContainer", title: "Inversores", text: "Cada inversor com potência, eficiência, temperatura e status. Clique em um para ver detalhes e as correntes de string." },
      { sel: "#invViewToggle", title: "Lista ou mapa", text: "Alterne entre a lista de inversores e o diagrama unifilar da usina (mapa elétrico por cabine)." },
      { sel: "#multimeterSection", title: "Multimedidor", text: "Medição oficial no ponto de conexão: potências, tensões, correntes e energia importada/exportada." },
      { sel: "#relaySection", title: "Relé de proteção", text: "Estado do relé e suas grandezas elétricas. As flags de proteção (sobrecorrente, sub/sobretensão...) viram alarmes quando atuam." }
    ]
  };

  // ─────────────────────────────────────────────────────────────
  // ENGINE
  // ─────────────────────────────────────────────────────────────
  const state = { steps: [], idx: 0, active: false, raf: null };
  let elSpot = null, elCatch = null, elTip = null;

  function injectStyles() {
    if (document.getElementById("htourStyles")) return;
    const css = `
      /* visual espelha o .status-pill do layout.css (EM OPERAÇÃO): letras cinza, ícone verde */
      .htour-help-btn{display:inline-flex;align-items:center;gap:7px;padding:5px 12px;border-radius:99px;
        background:rgba(57,229,140,.06);border:1px solid rgba(57,229,140,.22);
        color:var(--text-muted,#4dab7a);font-size:10px;font-weight:400;letter-spacing:.06em;
        text-transform:uppercase;font-family:'JetBrains Mono',monospace;
        cursor:pointer;transition:background .2s,box-shadow .2s;white-space:nowrap;}
      .htour-help-btn i{color:#39e58c;font-size:12px;}
      .htour-help-btn:hover{background:rgba(57,229,140,.14);box-shadow:0 0 12px rgba(57,229,140,.18);}
      .htour-help-btn--fixed{position:fixed;bottom:18px;left:18px;z-index:2500;}
      @media (max-width: 900px){
        .htour-help-btn{font-size:9px;padding:4px 8px;gap:5px;}
        .htour-help-btn i{font-size:11px;}
      }
      @media (max-width: 640px){
        .htour-help-btn{font-size:0;gap:0;padding:7px 9px;border-radius:50%;}
        .htour-help-btn i{font-size:14px;}
        .htour-help-btn--fixed{font-size:0;padding:9px 11px;}
        .htour-help-btn--fixed i{font-size:15px;}
      }
      .htour-catch{position:fixed;inset:0;z-index:9990;cursor:pointer;background:transparent;}
      .htour-spot{position:fixed;z-index:9991;pointer-events:none;border-radius:12px;
        border:1.5px solid rgba(57,229,140,.9);
        box-shadow:0 0 0 200vmax rgba(2,8,5,.78),0 0 22px rgba(57,229,140,.35);
        transition:top .35s cubic-bezier(.4,0,.2,1),left .35s cubic-bezier(.4,0,.2,1),
                   width .35s cubic-bezier(.4,0,.2,1),height .35s cubic-bezier(.4,0,.2,1);}
      .htour-spot::after{content:"";position:absolute;inset:-7px;border-radius:16px;
        border:1px solid rgba(57,229,140,.35);animation:htourPulse 1.6s ease-in-out infinite;}
      @keyframes htourPulse{0%,100%{opacity:.35;transform:scale(1);}50%{opacity:.9;transform:scale(1.02);}}
      .htour-tip{position:fixed;z-index:9992;width:300px;max-width:calc(100vw - 24px);
        background:linear-gradient(160deg,rgba(8,20,12,.99) 0%,rgba(4,12,7,.99) 100%);
        border:1px solid rgba(57,229,140,.35);border-radius:14px;padding:14px 16px 12px;
        box-shadow:0 14px 40px rgba(0,0,0,.65),0 0 20px rgba(57,229,140,.08);
        font-family:inherit;animation:htourTipIn .22s ease-out;transition:top .35s ease,left .35s ease;}
      @keyframes htourTipIn{from{opacity:0;transform:translateY(6px);}to{opacity:1;transform:none;}}
      .htour-tip__step{font-size:10px;letter-spacing:.8px;text-transform:uppercase;color:rgba(57,229,140,.75);
        font-weight:700;margin-bottom:6px;display:flex;justify-content:space-between;align-items:center;}
      .htour-tip__close{background:none;border:none;color:rgba(255,255,255,.45);cursor:pointer;font-size:14px;
        padding:0 2px;line-height:1;}
      .htour-tip__close:hover{color:#fff;}
      .htour-tip__title{font-size:14px;font-weight:700;color:#eafff3;margin-bottom:5px;}
      .htour-tip__text{font-size:12.5px;line-height:1.5;color:rgba(255,255,255,.78);}
      .htour-tip__nav{display:flex;gap:8px;margin-top:12px;align-items:center;}
      .htour-tip__btn{flex:1;padding:7px 0;border-radius:9px;font-size:12px;font-weight:600;cursor:pointer;
        border:1px solid rgba(57,229,140,.35);background:rgba(57,229,140,.10);color:#39e58c;transition:background .18s;}
      .htour-tip__btn:hover{background:rgba(57,229,140,.20);}
      .htour-tip__btn--ghost{flex:0 0 auto;padding:7px 12px;background:transparent;
        border-color:rgba(255,255,255,.14);color:rgba(255,255,255,.55);}
      .htour-tip__btn--ghost:hover{background:rgba(255,255,255,.06);}
      .htour-tip__hint{font-size:10px;color:rgba(255,255,255,.30);text-align:center;margin-top:8px;font-style:italic;}
    `;
    const st = document.createElement("style");
    st.id = "htourStyles";
    st.textContent = css;
    document.head.appendChild(st);
  }

  function isVisible(el) {
    if (!el) return false;
    const rects = el.getClientRects();
    if (!rects.length) return false;
    const cs = getComputedStyle(el);
    return cs.visibility !== "hidden" && cs.display !== "none";
  }

  function currentTourKey() {
    // plant.html → tour da usina; resumo → tour da aba ativa
    if (document.getElementById("plantMainChart")) return "plant";
    const v = localStorage.getItem("currentView") || "overview";
    return TOURS[v] ? v : "overview";
  }

  function start(key) {
    const defs = TOURS[key || currentTourKey()] || [];
    state.steps = defs
      .map(d => ({ ...d, el: document.querySelector(d.sel) }))
      .filter(d => isVisible(d.el));
    if (!state.steps.length) return;

    injectStyles();
    state.idx = 0;
    state.active = true;

    elCatch = document.createElement("div");
    elCatch.className = "htour-catch";
    elCatch.addEventListener("click", next);
    document.body.appendChild(elCatch);

    elSpot = document.createElement("div");
    elSpot.className = "htour-spot";
    document.body.appendChild(elSpot);

    elTip = document.createElement("div");
    elTip.className = "htour-tip";
    document.body.appendChild(elTip);

    document.addEventListener("keydown", onKey);
    window.addEventListener("resize", reposition);
    window.addEventListener("scroll", reposition, true);
    showStep();
  }

  function end() {
    state.active = false;
    document.removeEventListener("keydown", onKey);
    window.removeEventListener("resize", reposition);
    window.removeEventListener("scroll", reposition, true);
    [elCatch, elSpot, elTip].forEach(e => { try { e?.remove(); } catch (_) {} });
    elCatch = elSpot = elTip = null;
  }

  function next() { state.idx < state.steps.length - 1 ? (state.idx++, showStep()) : end(); }
  function prev() { if (state.idx > 0) { state.idx--; showStep(); } }

  function onKey(e) {
    if (e.key === "Escape") end();
    else if (e.key === "ArrowRight" || e.key === " " || e.key === "Enter") { e.preventDefault(); next(); }
    else if (e.key === "ArrowLeft") prev();
  }

  function reposition() {
    if (!state.active) return;
    if (state.raf) cancelAnimationFrame(state.raf);
    state.raf = requestAnimationFrame(() => positionAt(state.steps[state.idx], false));
  }

  function showStep() {
    const step = state.steps[state.idx];
    if (!step || !isVisible(step.el)) { next(); return; }
    step.el.scrollIntoView({ block: "center", behavior: "smooth" });
    // espera o smooth scroll assentar antes de medir
    setTimeout(() => positionAt(step, true), 320);
    renderTip(step);
  }

  function positionAt(step, _initial) {
    if (!step || !elSpot) return;
    const r = step.el.getBoundingClientRect();
    const pad = 8;
    const top = r.top - pad, left = r.left - pad;
    const w = r.width + pad * 2, h = r.height + pad * 2;
    elSpot.style.top = top + "px";
    elSpot.style.left = left + "px";
    elSpot.style.width = w + "px";
    elSpot.style.height = h + "px";

    // tooltip: embaixo do alvo; se não couber, em cima; clampa na tela
    if (!elTip) return;
    const tw = elTip.offsetWidth || 300;
    const th = elTip.offsetHeight || 160;
    let tipTop = top + h + 14;
    if (tipTop + th > window.innerHeight - 10) tipTop = Math.max(10, top - th - 14);
    let tipLeft = left + w / 2 - tw / 2;
    tipLeft = Math.max(10, Math.min(tipLeft, window.innerWidth - tw - 10));
    elTip.style.top = tipTop + "px";
    elTip.style.left = tipLeft + "px";
  }

  function renderTip(step) {
    if (!elTip) return;
    const last = state.idx === state.steps.length - 1;
    elTip.innerHTML = `
      <div class="htour-tip__step">
        <span>Passo ${state.idx + 1} de ${state.steps.length}</span>
        <button class="htour-tip__close" title="Sair do tour" aria-label="Sair">✕</button>
      </div>
      <div class="htour-tip__title">${step.title}</div>
      <div class="htour-tip__text">${step.text}</div>
      <div class="htour-tip__nav">
        ${state.idx > 0 ? '<button class="htour-tip__btn htour-tip__btn--ghost" data-act="prev">Voltar</button>' : ""}
        <button class="htour-tip__btn" data-act="next">${last ? "Concluir ✓" : "Próximo →"}</button>
      </div>
      <div class="htour-tip__hint">clique em qualquer lugar para avançar · Esc para sair</div>
    `;
    elTip.querySelector(".htour-tip__close")?.addEventListener("click", (e) => { e.stopPropagation(); end(); });
    elTip.querySelector('[data-act="next"]')?.addEventListener("click", (e) => { e.stopPropagation(); next(); });
    elTip.querySelector('[data-act="prev"]')?.addEventListener("click", (e) => { e.stopPropagation(); prev(); });
  }

  // ─────────────────────────────────────────────────────────────
  // BOTÃO "Ajuda ?" — injetado na topbar (resumo) ou fixo (usina)
  // ─────────────────────────────────────────────────────────────
  function injectButton() {
    injectStyles();
    if (document.getElementById("htourHelpBtn")) return;
    const btn = document.createElement("button");
    btn.id = "htourHelpBtn";
    btn.type = "button";
    btn.className = "htour-help-btn";
    btn.innerHTML = '<i class="fa-solid fa-circle-question"></i> Ajuda ?';
    btn.title = "Tour guiado desta aba";
    btn.addEventListener("click", () => start());

    const bellWrap = document.getElementById("notifBellWrap");
    if (bellWrap && bellWrap.parentElement) {
      bellWrap.parentElement.insertBefore(btn, bellWrap);
    } else {
      btn.classList.add("htour-help-btn--fixed");
      document.body.appendChild(btn);
    }
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", injectButton);
  } else {
    injectButton();
  }

  window.HelpTour = { start, end };
})();

/* AIOTI-DEMOTOUR-2026-09-21-INICIO
   ==========================================================================
   TOUR DE APRESENTACAO — SOMENTE NA DEMONSTRACAO          21/09/2026
   ==========================================================================
   Vai no FIM de `js/help_tour.js`. Esse arquivo ja e carregado pelo
   resumo.html, entao NENHUM HTML precisa ser alterado -- nem em producao.

   🔑 COMO ISTO NAO VAZA PARA A PRODUCAO
   O `demo.html` grava `localStorage.demo_tour = "1"` antes de redirecionar.
   So quem passou pela landing do evento tem essa marca. Usuario de producao
   nunca a tem, entao este bloco inteiro e um `return` silencioso para ele.
   ⚠️ localStorage e POR ORIGEM -- e por isso que a landing e o app moram no
      MESMO host. Se um dia a landing mudar de dominio, o tour para de
      aparecer e nada no console vai dizer por que.

   🔑 POR QUE APPEND NUM JS EXISTENTE, E NAO UM <script> NOVO
   Acrescentar `<script>` no resumo.html obrigaria a editar um arquivo que
   serve a producao. Append no fim de um arquivo que a pagina JA carrega e
   aditivo: so pode somar comportamento, e desfazer e apagar o bloco.

   ↩️ DESFAZER: apagar deste comentario ate o fim do arquivo.
   ========================================================================== */
(function () {
  "use strict";

  // ---- PORTAO -------------------------------------------------------------
  // Duas condicoes, as duas necessarias:
  //  1. veio da landing do evento (a marca);
  //  2. esta na tela de portfolio -- o tour fala do que se ve AQUI.
  try {
    if (localStorage.getItem("demo_tour") !== "1") return;
  } catch (e) { return; }                       // navegador sem localStorage
  if (!/resumo\.html|\/$/.test(window.location.pathname)) return;

  // ---- O ROTEIRO ----------------------------------------------------------
  // `alvo` null = cartao central, sem holofote. Passo cujo alvo NAO EXISTE e
  // PULADO -- a tela muda conforme o cliente e o horario, e um tour que
  // aponta para o vazio e pior que nenhum tour.
  var PASSOS = [
    { alvo: null,
      titulo: "Bem-vindo à AIOTI",
      texto: "Esta é a plataforma que monitora usinas solares em tempo real. Em um minuto eu mostro o essencial." },

    { alvo: ".psf-inner",
      titulo: "A frota inteira, num relance",
      texto: "Potência gerada agora, capacidade instalada e o quanto disso está em uso. Atualiza sozinho, a cada poucos minutos." },

    { alvo: ".psf-status-icons",
      titulo: "Estado dos equipamentos",
      texto: "Quantos inversores estão gerando, quantos pararam de comunicar e quantos estão desligados. É o primeiro lugar onde um problema aparece." },

    { alvo: ".plant-card__top",
      titulo: "Cada usina, um cartão",
      texto: "Uma linha por usina. A cor da borda e o ícone já dizem se há alarme ativo." },

    { alvo: ".plant-card__stats",
      titulo: "Os números que importam",
      texto: "Potência, energia do dia, irradiância e Performance Ratio — o indicador que diz se a usina entrega o que o sol ofereceu." },

    { alvo: ".plant-card canvas",
      titulo: "A curva do dia",
      texto: "Geração e irradiância lado a lado. Quando a linha verde descola da laranja, algo está segurando a usina." },

    { alvo: ".plant-card__status",
      titulo: "Alarmes com contexto",
      texto: "A plataforma cruza a medição elétrica antes de alarmar, para não acordar ninguém por causa de um sensor mudo." },

    { alvo: null,
      titulo: "Sua vez",
      texto: "Toque em uma usina para abrir os detalhes: inversores, strings, trackers e o histórico completo." }
  ];

  var VEL_DIGITACAO = 18;   // ms por caractere

  // ---- ESTILO (injetado: nao depende de nenhum .css) ----------------------
  var css = document.createElement("style");
  css.textContent = [
    // 🔑 O FUNDO NAO ESCURECE. Ele so apanha o toque.
    //    A v1 tinha fundo em rgba(0,0,0,.55) cobrindo a tela INTEIRA **e** a
    //    sombra do holofote escurecendo o entorno. Resultado: a area em
    //    destaque levava a camada do fundo por cima e saia escura tambem --
    //    o holofote iluminava e o fundo apagava de novo.
    //    Agora quem escurece e SO a sombra de `.dtour-foco`, que tem o buraco
    //    no lugar certo por construcao.
    ".dtour-fundo{position:fixed;inset:0;z-index:100000;background:transparent;",
    "  opacity:0;transition:opacity .25s,background .25s;cursor:pointer;}",
    ".dtour-fundo.on{opacity:1;}",
    // Passo SEM alvo nao tem holofote, logo nao tem sombra para escurecer.
    // So nesse caso o fundo assume o escurecimento.
    ".dtour-fundo--cheio{background:rgba(0,0,0,.72);}",
    ".dtour-foco{position:fixed;z-index:100001;border-radius:14px;pointer-events:none;",
    "  box-shadow:0 0 0 9999px rgba(0,0,0,.72),0 0 0 2px #39e58c,0 0 22px rgba(57,229,140,.55);",
    "  transition:all .35s cubic-bezier(.4,0,.2,1);}",
    ".dtour-cartao{position:fixed;z-index:100002;left:12px;right:12px;",
    "  background:rgba(4,14,7,.985);border:1px solid rgba(57,229,140,.30);border-radius:16px;",
    "  padding:18px 18px 14px;color:#d4f5e4;box-shadow:0 12px 40px rgba(0,0,0,.6);",
    "  font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,system-ui,sans-serif;",
    "  max-width:520px;margin:0 auto;transition:top .35s,bottom .35s;}",
    ".dtour-passo{font-size:.7rem;letter-spacing:.14em;color:#39e58c;opacity:.85;",
    "  text-transform:uppercase;margin-bottom:7px;}",
    ".dtour-titulo{font-size:1.12rem;font-weight:700;margin-bottom:8px;line-height:1.25;}",
    ".dtour-texto{font-size:.96rem;line-height:1.55;color:#a9cdb9;min-height:4.6em;}",
    ".dtour-cursor{display:inline-block;width:8px;background:#39e58c;margin-left:2px;",
    "  animation:dtourPisca .7s steps(1) infinite;}",
    "@keyframes dtourPisca{50%{opacity:0}}",
    ".dtour-rodape{display:flex;align-items:center;justify-content:space-between;",
    "  margin-top:14px;gap:12px;}",
    ".dtour-pontos{display:flex;gap:5px;}",
    ".dtour-ponto{width:6px;height:6px;border-radius:50%;background:rgba(57,229,140,.25);}",
    ".dtour-ponto.on{background:#39e58c;box-shadow:0 0 8px rgba(57,229,140,.8);}",
    ".dtour-btn{background:#39e58c;color:#04180d;border:0;border-radius:11px;",
    "  padding:11px 18px;font-size:.95rem;font-weight:700;font-family:inherit;cursor:pointer;}",
    "@media (prefers-reduced-motion: reduce){.dtour-foco{transition:none}}",
    // ---- convite: o que o cliente ve primeiro ----------------------------
    ".dtour-convite{position:fixed;z-index:99990;left:10px;right:10px;",
    "  bottom:calc(74px + env(safe-area-inset-bottom,0px));max-width:520px;margin:0 auto;",
    "  background:rgba(4,14,7,.985);border:1px solid rgba(57,229,140,.45);border-radius:16px;",
    "  padding:14px 16px;color:#d4f5e4;display:flex;align-items:center;gap:12px;",
    "  font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,system-ui,sans-serif;",
    "  animation:dtourPulsa 2s ease-in-out infinite;}",
    "@keyframes dtourPulsa{",
    "  0%,100%{box-shadow:0 0 0 0 rgba(57,229,140,.42),0 8px 26px rgba(0,0,0,.5);}",
    "  50%    {box-shadow:0 0 0 12px rgba(57,229,140,0),0 8px 26px rgba(0,0,0,.5);}}",
    ".dtour-convite-txt{flex:1;min-width:0;font-size:.92rem;line-height:1.35;}",
    ".dtour-convite-txt b{display:block;font-size:1rem;color:#d4f5e4;margin-bottom:2px;}",
    ".dtour-convite-txt span{color:#8fb9a3;font-size:.82rem;}",
    ".dtour-sim{background:#39e58c;color:#04180d;border:0;border-radius:11px;",
    "  padding:11px 16px;font-size:.93rem;font-weight:700;font-family:inherit;",
    "  cursor:pointer;flex:0 0 auto;}",
    ".dtour-nao{background:transparent;border:0;color:#6f8e7e;font-size:1.4rem;",
    "  line-height:1;padding:4px 6px;cursor:pointer;font-family:inherit;flex:0 0 auto;}",
    "@media (prefers-reduced-motion: reduce){.dtour-convite{animation:none}}",
    // ---- modal de novidades fora do caminho -------------------------------
    // 🔑 CLICAR NO "Entendido" NAO BASTA: medido em 21/09, o modal renderiza
    //    DEPOIS do convite e volta a cobrir tudo -- a dispensa por clique
    //    corre antes de ele existir. Regra de CSS e imune a isso, porque vale
    //    para qualquer instancia que apareca, agora ou daqui a 5 segundos.
    // ⚠️ Escopo `body.dtour-ativo`: essa classe so existe na demo, entao o
    //    modal continua normal para o cliente de producao.
    "body.dtour-ativo .notif-modal-overlay{display:none !important;}"
  ].join("");
  document.head.appendChild(css);

  // ---- ELEMENTOS ----------------------------------------------------------
  var fundo, foco, cartao, elPasso, elTitulo, elTexto, elPontos, elBtn;
  var i = 0, digitando = false, timerDigit = null;

  function montar() {
    fundo = document.createElement("div");
    fundo.className = "dtour-fundo";

    foco = document.createElement("div");
    foco.className = "dtour-foco";

    cartao = document.createElement("div");
    cartao.className = "dtour-cartao";
    cartao.innerHTML =
      '<div class="dtour-passo"></div>' +
      '<div class="dtour-titulo"></div>' +
      '<div class="dtour-texto"></div>' +
      '<div class="dtour-rodape"><div class="dtour-pontos"></div>' +
      '<button class="dtour-btn" type="button">Continuar</button></div>';

    document.body.appendChild(fundo);
    document.body.appendChild(foco);
    document.body.appendChild(cartao);

    elPasso  = cartao.querySelector(".dtour-passo");
    elTitulo = cartao.querySelector(".dtour-titulo");
    elTexto  = cartao.querySelector(".dtour-texto");
    elPontos = cartao.querySelector(".dtour-pontos");
    elBtn    = cartao.querySelector(".dtour-btn");

    PASSOS.forEach(function () {
      var p = document.createElement("div");
      p.className = "dtour-ponto";
      elPontos.appendChild(p);
    });

    // Um toque em qualquer lugar avanca. Se ainda esta digitando, o primeiro
    // toque COMPLETA o texto -- quem le rapido nao deve esperar a animacao.
    fundo.addEventListener("click", avancar);
    elBtn.addEventListener("click", function (ev) { ev.stopPropagation(); avancar(); });

    requestAnimationFrame(function () { fundo.classList.add("on"); });
  }

  // ---- DIGITACAO ----------------------------------------------------------
  function digitar(texto) {
    clearInterval(timerDigit);
    digitando = true;
    elTexto.textContent = "";
    var cursor = document.createElement("span");
    cursor.className = "dtour-cursor";
    cursor.innerHTML = "&nbsp;";
    elTexto.appendChild(cursor);

    var n = 0;
    timerDigit = setInterval(function () {
      n++;
      cursor.remove();
      elTexto.textContent = texto.slice(0, n);
      elTexto.appendChild(cursor);
      if (n >= texto.length) {
        clearInterval(timerDigit);
        digitando = false;
        cursor.remove();
      }
    }, VEL_DIGITACAO);
  }

  function completarTexto() {
    clearInterval(timerDigit);
    digitando = false;
    elTexto.textContent = PASSOS[i].texto;
  }

  // ---- POSICIONAMENTO -----------------------------------------------------
  function posicionar(alvo) {
    if (!alvo) {
      // sem holofote -> quem escurece e o fundo (ver comentario no CSS)
      fundo.classList.add("dtour-fundo--cheio");
      foco.style.opacity = "0";
      foco.style.width = "0px";
      foco.style.height = "0px";
      cartao.style.top = "50%";
      cartao.style.bottom = "auto";
      cartao.style.transform = "translateY(-50%)";
      return;
    }
    // com holofote -> o fundo precisa ficar LIMPO, senao escurece o destaque
    fundo.classList.remove("dtour-fundo--cheio");
    var r = alvo.getBoundingClientRect();
    foco.style.opacity = "1";
    foco.style.left   = (r.left - 6) + "px";
    foco.style.top    = (r.top - 6) + "px";
    foco.style.width  = (r.width + 12) + "px";
    foco.style.height = (r.height + 12) + "px";

    // o cartao vai para o lado OPOSTO do alvo, para nao cobrir o que aponta
    cartao.style.transform = "none";
    var meio = window.innerHeight / 2;
    if (r.top + r.height / 2 < meio) {
      cartao.style.top = "auto";
      cartao.style.bottom = "18px";
    } else {
      cartao.style.top = "18px";
      cartao.style.bottom = "auto";
    }
  }

  // ---- FLUXO --------------------------------------------------------------
  function mostrar() {
    // pula passo cujo alvo nao existe nesta tela
    while (i < PASSOS.length && PASSOS[i].alvo && !document.querySelector(PASSOS[i].alvo)) i++;
    if (i >= PASSOS.length) return encerrar();

    var p = PASSOS[i];
    var alvo = p.alvo ? document.querySelector(p.alvo) : null;

    elPasso.textContent = (i + 1) + " de " + PASSOS.length;
    elTitulo.textContent = p.titulo;
    elBtn.textContent = (i === PASSOS.length - 1) ? "Explorar a plataforma" : "Continuar";
    [].forEach.call(elPontos.children, function (d, k) {
      d.classList.toggle("on", k <= i);
    });

    if (alvo) {
      alvo.scrollIntoView({ behavior: "smooth", block: "center" });
      // espera a rolagem assentar antes de medir: posicionar antes daria o
      // retangulo ANTIGO e o holofote cairia no lugar errado.
      setTimeout(function () { posicionar(alvo); }, 380);
    } else {
      posicionar(null);
    }
    digitar(p.texto);
  }

  function avancar() {
    if (digitando) { completarTexto(); return; }
    i++;
    if (i >= PASSOS.length) return encerrar();
    mostrar();
  }

  function encerrar() {
    clearInterval(timerDigit);
    if (convite && convite.parentNode) convite.remove();
    // "feito" em vez de apagar: recarregar a pagina nao reinicia o tour, e
    // ainda fica o registro de que este aparelho ja viu.
    try { localStorage.setItem("demo_tour", "feito"); } catch (e) {}
    [fundo, foco, cartao].forEach(function (el) { if (el && el.parentNode) el.remove(); });
  }

  // ---- CONVITE ------------------------------------------------------------
  // 🔑 DECISAO REVISTA (21/09): a 1a versao ABRIA o tour sozinho, sem escapatoria.
  //    Virou CONVITE por pedido do dono do produto: um botao bem visivel,
  //    pulsando, perguntando se a pessoa quer a apresentacao. Quem so quer
  //    mexer na plataforma nao fica preso em 8 passos.
  //    O pulso e `box-shadow` expandindo -- nao mexe no layout, entao nao
  //    empurra conteudo nem dispara reflow a cada 2 s.
  var convite;

  function convidar() {
    // 🔑 A classe fica no body pelo RESTO DA SESSAO, de proposito. Devolver o
    //    modal de novidades ao visitante depois do tour nao serve a ninguem:
    //    ele e changelog interno, nao material de demonstracao.
    document.body.classList.add("dtour-ativo");

    // O clique continua, para o caso de o modal JA estar aberto -- a regra de
    // CSS esconde, mas o clique tambem marca como lido do lado da aplicacao.
    var fechar = [].filter.call(document.querySelectorAll("button"), function (b) {
      return /entendido/i.test(b.textContent || "");
    })[0];
    if (fechar) { try { fechar.click(); } catch (e) {} }

    convite = document.createElement("div");
    convite.className = "dtour-convite";
    convite.innerHTML =
      '<div class="dtour-convite-txt"><b>Quer uma apresentação guiada?</b>' +
      '<span>1 minuto, mostrando o essencial da tela.</span></div>' +
      '<button class="dtour-sim" type="button">Começar</button>' +
      '<button class="dtour-nao" type="button" aria-label="Agora não">&times;</button>';
    document.body.appendChild(convite);

    convite.querySelector(".dtour-sim").addEventListener("click", function () {
      convite.remove();
      montar();
      mostrar();
    });
    convite.querySelector(".dtour-nao").addEventListener("click", function () {
      convite.remove();
      // "recusado", nao "feito": fica registrado que a pessoa viu o convite e
      // disse nao -- diferente de ter percorrido o tour.
      try { localStorage.setItem("demo_tour", "recusado"); } catch (e) {}
    });
  }

  // Espera o primeiro cartao de usina existir: o tour fala de dados, e comecar
  // com a tela vazia mostraria holofote em cima de nada. Teto de 12s para nao
  // ficar refem de uma tela que nunca carrega.
  var esperou = 0;
  var aguarde = setInterval(function () {
    esperou += 250;
    if (document.querySelector(".plant-card") || esperou >= 12000) {
      clearInterval(aguarde);
      setTimeout(convidar, 450);
    }
  }, 250);
})();
