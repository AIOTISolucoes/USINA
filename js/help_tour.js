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
