/* =====================================================================
 * Seletor de idioma
 * ---------------------------------------------------------------------
 * Se injeta sozinho: acha um lugar no topo da tela e se pendura la. Foi
 * feito assim para nao precisar editar a marcacao dos 7 HTML — cada um
 * tem um cabecalho diferente, e mexer nos 7 e 7 chances de quebrar
 * layout.
 *
 * ONDE O IDIOMA FICA GUARDADO, e por que:
 *   1) localStorage  -> resposta imediata, sem piscar no carregamento
 *   2) notif_prefs.language na conta -> segue a pessoa para outro aparelho
 *
 * O item 2 reusa POST/GET /users/notif-prefs, que JA EXISTE e ja faz
 * merge de jsonb. Criar rota nova custaria: coluna nova no banco, rota
 * nova no api2.py, deploy da Lambda e — o passo que costuma ser
 * esquecido e da CORS no navegador — criar a rota no API Gateway, que e
 * por method+path. Reusar nao paga nada disso.
 * ===================================================================== */

(function () {
  "use strict";

  if (!window.AIOTI_I18N) return;

  var I18N = window.AIOTI_I18N;
  var CHAVE_PREF = "language";
  var widget = null;            // a caixa montada, para poder atualizar depois

  // -------------------------------------------------------------- estilo
  function injetarCSS() {
    if (document.getElementById("aioti-i18n-css")) return;
    var css = document.createElement("style");
    css.id = "aioti-i18n-css";
    // Cores vem das variaveis da plataforma, com fallback escuro. O fundo
    // e configuravel desde 13/08 (claro/escuro): cravar #12161d deixaria o
    // menu errado no tema claro — "cor nao se decide sem saber a
    // superficie".
    css.textContent = [
      ".aioti-lang{position:relative;display:inline-flex;align-items:center}",
      ".aioti-lang__btn{display:inline-flex;align-items:center;gap:6px;",
      "  background:transparent;",
      "  border:1px solid var(--border, rgba(128,128,128,.35));",
      "  color:inherit;font:inherit;font-size:12px;line-height:1;",
      "  padding:6px 9px;border-radius:8px;cursor:pointer;opacity:.85}",
      ".aioti-lang__btn:hover{opacity:1;",
      "  border-color:var(--primary, rgba(128,128,128,.6))}",
      ".aioti-lang__code{font-weight:700;letter-spacing:.06em}",
      ".aioti-lang__globo{width:14px;height:14px;flex:0 0 auto;",
      "  display:block;opacity:.9}",
      ".aioti-lang__menu{position:absolute;top:calc(100% + 6px);right:0;",
      "  min-width:172px;",
      "  background:var(--bg-card, #12161d);",
      "  color:var(--text-main, #e8eef6);",
      "  border:1px solid var(--border, rgba(128,128,128,.3));",
      "  border-radius:10px;box-shadow:0 12px 30px rgba(0,0,0,.45);",
      "  padding:5px;z-index:99999}",
      ".aioti-lang__menu[hidden]{display:none}",
      ".aioti-lang__item{display:block;width:100%;",
      "  background:transparent;border:0;color:inherit;font:inherit;",
      "  font-size:13px;text-align:left;padding:9px 11px;border-radius:7px;",
      "  cursor:pointer}",
      ".aioti-lang__item:hover{background:rgba(127,208,85,.10)}",
      ".aioti-lang__item[aria-current='true']{",
      "  background:rgba(127,208,85,.18);font-weight:600}",
      // Fora de um cabecalho conhecido (ex.: tela de login) ele flutua.
      ".aioti-lang--solto{position:fixed;top:14px;right:14px;z-index:99999}",
      ".aioti-lang--solto .aioti-lang__btn{background:rgba(10,14,20,.55);",
      "  color:#e8eef6;backdrop-filter:blur(6px)}"
    ].join("");
    document.head.appendChild(css);
  }

  // ------------------------------------------------------------- ancora
  // Ordem do mais especifico para o mais generico.
  //
  // modo "antes": entra IMEDIATAMENTE ANTES do elemento — usado para
  // encostar no sininho. Sem isto o seletor cairia no fim da
  // .topbar-right, ou seja DEPOIS do nome do usuario, longe do sino.
  // A ordem no topo fica: [idioma] [🔔] [usuario].
  var ANCORAS = [
    { sel: "#notifBellWrap",         modo: "antes"  },  // resumo.html e plant.html
    { sel: ".notif-bell-wrap",       modo: "antes"  },  // idem, sem id
    { sel: ".topbar-right",          modo: "dentro" },
    { sel: ".ds-v2-topbar-controls", modo: "dentro" },
    { sel: ".os-topbar-right",       modo: "dentro" },
    { sel: ".header-actions",        modo: "dentro" }
  ];

  function acharAncora() {
    for (var i = 0; i < ANCORAS.length; i++) {
      var a = ANCORAS[i];
      var el = document.querySelector(a.sel);
      // "antes" exige um pai para o insertBefore; sem pai, nao serve.
      if (el && (a.modo !== "antes" || el.parentNode)) {
        return { el: el, modo: a.modo, solto: false };
      }
    }
    return { el: document.body, modo: "dentro", solto: true };
  }

  // ------------------------------------------------------------ widget
  function montar() {
    if (document.querySelector(".aioti-lang")) return;

    var destino = acharAncora();
    if (!destino.el) return;

    injetarCSS();

    var atual = I18N.getLang();
    var info = null;
    for (var i = 0; i < I18N.IDIOMAS.length; i++) {
      if (I18N.IDIOMAS[i].code === atual) info = I18N.IDIOMAS[i];
    }
    info = info || I18N.IDIOMAS[0];

    var caixa = document.createElement("div");
    caixa.className = "aioti-lang" + (destino.solto ? " aioti-lang--solto" : "");
    // O proprio seletor nao pode ser traduzido: "Português" tem que
    // aparecer como "Português" mesmo com a tela em alemao.
    caixa.setAttribute("data-i18n-skip", "");

    var btn = document.createElement("button");
    btn.type = "button";
    btn.className = "aioti-lang__btn";
    btn.setAttribute("aria-haspopup", "true");
    btn.setAttribute("aria-expanded", "false");
    btn.title = "Idioma / Language";
    // Globo em SVG, nao bandeira. Duas razoes:
    // 1) No Windows o emoji 🇧🇷 nao vira bandeira — vira as letras "BR",
    //    e o botao ficava "BR PT", redundante (visto em teste real).
    // 2) Bandeira representa PAIS, nao idioma: espanhol nao e so da
    //    Espanha, nem portugues so do Brasil.
    btn.innerHTML =
      '<svg class="aioti-lang__globo" viewBox="0 0 24 24" fill="none" ' +
      'stroke="currentColor" stroke-width="1.8" stroke-linecap="round" ' +
      'aria-hidden="true">' +
      '<circle cx="12" cy="12" r="9"/>' +
      '<path d="M3 12h18"/>' +
      '<path d="M12 3c2.6 2.7 3.9 5.7 3.9 9s-1.3 6.3-3.9 9"/>' +
      '<path d="M12 3c-2.6 2.7-3.9 5.7-3.9 9s1.3 6.3 3.9 9"/>' +
      "</svg>" +
      '<span class="aioti-lang__code">' + info.code.toUpperCase() + "</span>";

    var menu = document.createElement("div");
    menu.className = "aioti-lang__menu";
    menu.hidden = true;
    menu.setAttribute("role", "menu");

    I18N.IDIOMAS.forEach(function (idi) {
      var item = document.createElement("button");
      item.type = "button";
      item.className = "aioti-lang__item";
      item.setAttribute("role", "menuitem");
      if (idi.code === atual) item.setAttribute("aria-current", "true");
      // Só o nome, no próprio idioma. "Português", "Deutsch", "中文" se
      // explicam sozinhos e não têm o problema de bandeira≠idioma.
      item.textContent = idi.rotulo;
      item.addEventListener("click", function () {
        menu.hidden = true;
        btn.setAttribute("aria-expanded", "false");
        escolher(idi.code);
      });
      menu.appendChild(item);
    });

    btn.addEventListener("click", function (ev) {
      ev.stopPropagation();
      var aberto = !menu.hidden;
      menu.hidden = aberto;
      btn.setAttribute("aria-expanded", String(!aberto));
    });

    document.addEventListener("click", function () {
      if (!menu.hidden) {
        menu.hidden = true;
        btn.setAttribute("aria-expanded", "false");
      }
    });
    menu.addEventListener("click", function (ev) { ev.stopPropagation(); });

    caixa.appendChild(btn);
    caixa.appendChild(menu);

    if (destino.solto) {
      document.body.appendChild(caixa);
    } else if (destino.modo === "antes") {
      destino.el.parentNode.insertBefore(caixa, destino.el);
    } else {
      destino.el.appendChild(caixa);
    }

    widget = caixa;
  }

  // ----------------------------------------------------- troca de idioma
  function escolher(code) {
    salvarNaConta(code);          // dispara antes: setLang pode recarregar
    Promise.resolve(I18N.setLang(code, { persist: true }))
      .then(refletirEstado);
  }

  /**
   * Poe o botao e o menu de acordo com o idioma que esta valendo AGORA.
   *
   * Precisa existir porque a primeira troca (pt -> outro) NAO recarrega a
   * pagina: a traducao acontece no ar. Sem isto, a tela virava ingles e o
   * botao continuava escrito "PT" — pego em teste. Nas trocas seguintes
   * (en -> es) a pagina recarrega e o widget nasce ja correto, mas nao da
   * para depender disso.
   */
  function refletirEstado() {
    if (!widget) return;
    var agora = I18N.getLang();

    var span = widget.querySelector(".aioti-lang__code");
    if (span) span.textContent = agora.toUpperCase();

    var itens = widget.querySelectorAll(".aioti-lang__item");
    for (var i = 0; i < itens.length; i++) {
      if (I18N.IDIOMAS[i] && I18N.IDIOMAS[i].code === agora) {
        itens[i].setAttribute("aria-current", "true");
      } else {
        itens[i].removeAttribute("aria-current");
      }
    }
  }

  function baseApi() {
    if (typeof window.API_BASE === "string" && window.API_BASE) return window.API_BASE;
    return "https://jgeg9i0js1.execute-api.us-east-1.amazonaws.com";
  }

  function token() {
    try {
      var u = JSON.parse(localStorage.getItem("user") || "null");
      return (u && (u.token || u.access_token)) || null;
    } catch (e) { return null; }
  }

  function salvarNaConta(code) {
    var t = token();
    if (!t) return;                       // deslogado: local ja basta
    var prefs = {};
    prefs[CHAVE_PREF] = code;
    try {
      // keepalive: a pagina vai recarregar logo em seguida e sem isso o
      // navegador cancelaria a requisicao no meio.
      fetch(baseApi() + "/users/notif-prefs", {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          "Authorization": "Bearer " + t
        },
        body: JSON.stringify({ prefs: prefs, merge: true }),
        keepalive: true
      }).catch(function () { /* preferencia local ja valeu */ });
    } catch (e) { /* idem */ }
  }

  // Traz o idioma da conta quando a pessoa entra de outro aparelho.
  function puxarDaConta() {
    var t = token();
    if (!t) return;
    fetch(baseApi() + "/users/notif-prefs", {
      headers: { "Authorization": "Bearer " + t }
    })
      .then(function (r) { return r.ok ? r.json() : null; })
      .then(function (j) {
        var prefs = (j && (j.prefs || j)) || {};
        var daConta = prefs[CHAVE_PREF];
        if (!daConta || daConta === I18N.getLang()) return;
        // persist:true para que a proxima carga ja saia certa sem esperar
        // esta ida ao servidor.
        return Promise.resolve(I18N.setLang(daConta, { persist: true }))
          .then(refletirEstado);
      })
      .catch(function () { /* offline: segue com o local */ });
  }

  function iniciar() {
    montar();
    puxarDaConta();
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", iniciar);
  } else {
    iniciar();
  }
})();
