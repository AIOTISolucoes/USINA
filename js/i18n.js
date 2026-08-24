/* =====================================================================
 * i18n da plataforma AIOTI
 * ---------------------------------------------------------------------
 * DESENHO, e por que e assim:
 *
 * 1) A CHAVE E O PROPRIO TEXTO EM PORTUGUES.
 *    Nao ha "btn.save". A plataforma tem ~31.700 linhas de front ja
 *    escritas em portugues; inventar 900 chaves semanticas exigiria
 *    tocar em todas elas de uma vez, e uma migracao pela metade
 *    mostraria "btn.save" cru na tela. Com o texto como chave, o que
 *    ainda nao foi traduzido simplesmente aparece em portugues.
 *
 * 2) TRADUCAO SO POR IGUALDADE EXATA.
 *    O motor nunca traduz "parecido". Se o texto nao esta no dicionario,
 *    ele passa intacto. Isso e o que protege o DADO: "Inversor 1",
 *    "Naturagua", "1.234,5 kW" nunca estao no dicionario curado, entao
 *    nunca sao tocados. A seguranca vem da forma, nao de lista de
 *    excecao.
 *
 * 3) EM PORTUGUES O MOTOR NAO LIGA NADA.
 *    Sem observer, sem varredura, sem custo. O caminho de producao de
 *    hoje continua byte a byte o mesmo.
 *
 * LIMITE CONHECIDO (nao finge estar resolvido): formato de numero e
 * data continua pt-BR no codigo (ex.: toFixed(1).replace(".", ",")).
 * Trocar isso e trabalho separado, em ~190 pontos de interpolacao.
 * ===================================================================== */

(function (global) {
  "use strict";

  var PADRAO = "pt";

  // O rotulo vai no PROPRIO idioma, de proposito: quem abre o menu com a
  // tela em alemao precisa reconhecer "Português". Nao ha bandeira —
  // bandeira e pais, nao idioma, e no Windows o emoji nem renderiza.
  var IDIOMAS = [
    { code: "pt", rotulo: "Português", htmlLang: "pt-BR" },
    { code: "en", rotulo: "English",   htmlLang: "en" },
    { code: "es", rotulo: "Español",   htmlLang: "es" },
    { code: "de", rotulo: "Deutsch",   htmlLang: "de" },
    { code: "zh", rotulo: "中文",       htmlLang: "zh-CN" }
  ];

  var CHAVE_LS = "aioti_lang";

  // Atributos cujo valor o usuario le.
  var ATTRS = ["placeholder", "title", "alt", "aria-label"];

  // Nao entrar aqui de jeito nenhum.
  var TAGS_PROIBIDAS = { SCRIPT: 1, STYLE: 1, NOSCRIPT: 1, TEXTAREA: 1, CODE: 1, PRE: 1 };

  var dicionarios = {};      // code -> { textoPt: traducao }
  var idioma = PADRAO;
  var dicAtual = null;
  var observer = null;
  var ligado = false;

  // -------------------------------------------------------------------
  // normalizacao
  // -------------------------------------------------------------------
  // O extrator gravou as chaves com espaco colapsado. O DOM traz o mesmo
  // texto com quebra de linha e indentacao do HTML. Sem normalizar dos
  // dois lados, quase nada casaria.
  function normalizar(txt) {
    return String(txt).replace(/\s+/g, " ").trim();
  }

  function traduzir(txt) {
    if (!dicAtual) return txt;
    var alvo = dicAtual[normalizar(txt)];
    return (typeof alvo === "string" && alvo) ? alvo : txt;
  }

  // -------------------------------------------------------------------
  // aplicacao no DOM
  // -------------------------------------------------------------------

  function traduzirNoTexto(node) {
    var bruto = node.nodeValue;
    if (!bruto) return;
    var chave = normalizar(bruto);
    if (chave.length < 2) return;

    var alvo = dicAtual[chave];
    if (typeof alvo !== "string" || !alvo || alvo === chave) return;

    // Preserva o espaco em volta para nao colar palavra em elemento vizinho
    // (ex.: "<b>Total</b> de 5" perderia o espaco).
    var antes = bruto.match(/^\s*/)[0];
    var depois = bruto.match(/\s*$/)[0];
    node.nodeValue = antes + alvo + depois;
  }

  function traduzirAtributos(el) {
    for (var i = 0; i < ATTRS.length; i++) {
      var nome = ATTRS[i];
      if (!el.hasAttribute || !el.hasAttribute(nome)) continue;
      var valor = el.getAttribute(nome);
      var alvo = dicAtual[normalizar(valor)];
      if (typeof alvo === "string" && alvo) el.setAttribute(nome, alvo);
    }
  }

  function proibido(el) {
    for (var n = el; n && n.nodeType === 1; n = n.parentNode) {
      if (TAGS_PROIBIDAS[n.tagName]) return true;
      if (n.hasAttribute && n.hasAttribute("data-i18n-skip")) return true;
    }
    return false;
  }

  function varrer(raiz) {
    if (!dicAtual || !raiz) return;

    if (raiz.nodeType === 3) {                 // veio um no de texto solto
      if (raiz.parentNode && !proibido(raiz.parentNode)) traduzirNoTexto(raiz);
      return;
    }
    if (raiz.nodeType !== 1 && raiz.nodeType !== 9 && raiz.nodeType !== 11) return;
    if (raiz.nodeType === 1 && proibido(raiz)) return;

    var doc = raiz.ownerDocument || document;
    var caminhador = doc.createTreeWalker(
      raiz,
      NodeFilter.SHOW_TEXT,
      {
        acceptNode: function (no) {
          if (!no.nodeValue || !no.nodeValue.trim()) return NodeFilter.FILTER_REJECT;
          var pai = no.parentNode;
          if (!pai || TAGS_PROIBIDAS[pai.tagName]) return NodeFilter.FILTER_REJECT;
          return NodeFilter.FILTER_ACCEPT;
        }
      }
    );

    var pendentes = [];
    var no;
    while ((no = caminhador.nextNode())) {
      if (!proibido(no.parentNode)) pendentes.push(no);
    }
    for (var i = 0; i < pendentes.length; i++) traduzirNoTexto(pendentes[i]);

    // atributos
    if (raiz.nodeType === 1) traduzirAtributos(raiz);
    var seletor = ATTRS.map(function (a) { return "[" + a + "]"; }).join(",");
    var comAttr = raiz.querySelectorAll ? raiz.querySelectorAll(seletor) : [];
    for (var j = 0; j < comAttr.length; j++) {
      if (!proibido(comAttr[j])) traduzirAtributos(comAttr[j]);
    }
  }

  // -------------------------------------------------------------------
  // observador do DOM
  // -------------------------------------------------------------------
  // A plataforma redesenha por innerHTML em dezenas de pontos (app.js e
  // plant.js somam ~20.700 linhas) e ainda tem refresh de 30 s. Enganchar
  // cada render seria invasivo e sempre incompleto; o observer pega tudo.
  //
  // O disconnect/reconnect dentro do callback e proposital: sem ele, a
  // propria escrita da traducao geraria novas mutacoes e o observer
  // entraria em laco consigo mesmo.
  function ligarObserver() {
    if (observer || !global.MutationObserver) return;

    observer = new MutationObserver(function (mutacoes) {
      observer.disconnect();
      try {
        for (var i = 0; i < mutacoes.length; i++) {
          var m = mutacoes[i];
          if (m.type === "childList") {
            for (var j = 0; j < m.addedNodes.length; j++) varrer(m.addedNodes[j]);
          } else if (m.type === "characterData") {
            varrer(m.target);
          } else if (m.type === "attributes" && m.target) {
            if (!proibido(m.target)) traduzirAtributos(m.target);
          }
        }
      } finally {
        observar();
      }
    });
    observar();
  }

  function observar() {
    if (!observer) return;
    observer.observe(document.documentElement, {
      childList: true,
      subtree: true,
      characterData: true,
      attributes: true,
      attributeFilter: ATTRS
    });
  }

  function desligarObserver() {
    if (observer) { observer.disconnect(); observer = null; }
  }

  // -------------------------------------------------------------------
  // carga de dicionario
  // -------------------------------------------------------------------

  function carregar(code) {
    if (code === PADRAO) return Promise.resolve(null);
    if (dicionarios[code]) return Promise.resolve(dicionarios[code]);

    return new Promise(function (resolve) {
      var s = document.createElement("script");
      // BASE, nao base(): document.currentScript so vale durante a execucao
      // inicial do arquivo; aqui dentro ja e null e o caminho sairia errado.
      s.src = BASE + "js/i18n/" + code + ".js";
      s.async = true;
      s.onload = function () { resolve(dicionarios[code] || null); };
      s.onerror = function () {
        // Idioma sem arquivo nao pode derrubar a tela: cai para portugues.
        console.warn("[i18n] dicionario nao carregou: " + code + " — seguindo em pt");
        resolve(null);
      };
      document.head.appendChild(s);
    });
  }

  // Descobre o prefixo para funcionar tanto na raiz quanto em subpasta.
  function base() {
    var atual = document.currentScript && document.currentScript.src;
    if (atual) {
      var m = String(atual).match(/^(.*\/)js\/i18n\.js(?:\?.*)?$/);
      if (m) return m[1];
    }
    return "";
  }
  var BASE = base();

  // -------------------------------------------------------------------
  // escolha do idioma
  // -------------------------------------------------------------------

  function suportado(code) {
    if (!code) return null;
    var c = String(code).toLowerCase().replace("_", "-");
    for (var i = 0; i < IDIOMAS.length; i++) {
      if (IDIOMAS[i].code === c) return IDIOMAS[i].code;
    }
    var curto = c.split("-")[0];
    for (var j = 0; j < IDIOMAS.length; j++) {
      if (IDIOMAS[j].code === curto) return IDIOMAS[j].code;
    }
    return null;
  }

  // Ordem: conta do usuario > escolha local > navegador > portugues.
  // A conta ganha do navegador porque foi escolha explicita da pessoa.
  function idiomaInicial() {
    try {
      var bruto = localStorage.getItem("user");
      if (bruto) {
        var u = JSON.parse(bruto);
        var daConta = suportado(u && (u.language || u.idioma || u.lang));
        if (daConta) return daConta;
      }
    } catch (e) { /* localStorage cheio, JSON velho: nao e motivo de quebrar */ }

    var local = suportado(localStorage.getItem(CHAVE_LS));
    if (local) return local;

    var nav = (navigator.languages && navigator.languages[0]) || navigator.language;
    return suportado(nav) || PADRAO;
  }

  function marcarHtmlLang(code) {
    for (var i = 0; i < IDIOMAS.length; i++) {
      if (IDIOMAS[i].code === code) {
        document.documentElement.setAttribute("lang", IDIOMAS[i].htmlLang);
        return;
      }
    }
  }

  // -------------------------------------------------------------------
  // API
  // -------------------------------------------------------------------

  var API = {
    IDIOMAS: IDIOMAS,
    PADRAO: PADRAO,

    getLang: function () { return idioma; },

    /** Registra um dicionario. Chamado pelos arquivos js/i18n/<code>.js. */
    register: function (code, mapa) {
      dicionarios[code] = mapa || {};
      if (code === idioma) dicAtual = dicionarios[code];
    },

    /** Traduz uma string avulsa. Sem entrada no dicionario, devolve igual. */
    t: function (txt) {
      if (txt == null) return txt;
      return traduzir(txt);
    },

    /** Varre um pedaco do DOM. Util depois de render manual. */
    apply: function (raiz) {
      if (idioma !== PADRAO && dicAtual) varrer(raiz || document.body);
    },

    /**
     * Troca o idioma.
     * persist=true grava local; salvarNaConta manda para a API tambem.
     */
    setLang: function (code, opcoes) {
      opcoes = opcoes || {};
      var alvo = suportado(code) || PADRAO;
      if (alvo === idioma && ligado) return Promise.resolve(alvo);

      // A CHAVE do dicionario e o texto em PORTUGUES. Depois que a tela ja
      // foi traduzida, o DOM nao tem mais portugues nenhum — entao trocar
      // en -> es no ar nao acharia chave alguma e a tela ficaria em ingles,
      // parecendo que a troca falhou. Guardar o original de cada no custaria
      // memoria em telas que redesenham a cada 30 s. Recarregar e o caminho
      // honesto: a pagina volta em portugues e traduz do zero.
      var precisaRecarregar = ligado && dicAtual !== null;

      idioma = alvo;
      if (opcoes.persist !== false) {
        try { localStorage.setItem(CHAVE_LS, alvo); } catch (e) {}
      }
      marcarHtmlLang(alvo);

      if (precisaRecarregar && opcoes.recarregar !== false) {
        location.reload();
        return Promise.resolve(alvo);
      }

      if (alvo === PADRAO) {
        dicAtual = null;
        desligarObserver();
        return Promise.resolve(alvo);
      }

      return carregar(alvo).then(function (dic) {
        if (!dic) { idioma = PADRAO; dicAtual = null; return PADRAO; }
        dicAtual = dic;
        varrer(document.body);
        ligarObserver();
        ligado = true;
        return alvo;
      });
    },

    /** Reaplica a partir do usuario salvo (chamar depois do login). */
    sincronizarComConta: function () {
      return API.setLang(idiomaInicial(), { persist: false, recarregar: false });
    }
  };

  global.AIOTI_I18N = API;

  // -------------------------------------------------------------------
  // arranque
  // -------------------------------------------------------------------

  idioma = idiomaInicial();
  marcarHtmlLang(idioma);

  if (idioma !== PADRAO) {
    // Carrega o dicionario ja, e aplica assim que houver corpo.
    var pronto = carregar(idioma).then(function (dic) {
      dicAtual = dic;
      if (!dic) return;
      if (document.body) { varrer(document.body); ligarObserver(); ligado = true; }
      else {
        document.addEventListener("DOMContentLoaded", function () {
          varrer(document.body); ligarObserver(); ligado = true;
        });
      }
    });
    API.pronto = pronto;
  } else {
    API.pronto = Promise.resolve(null);
  }

})(window);
