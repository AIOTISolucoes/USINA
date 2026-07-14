// ── PWA: Service Worker + Install + Push ──
(function () {
  if (!("serviceWorker" in navigator)) return;

  // ── Injeta CSS do banner (funciona em qualquer página) ──
  const style = document.createElement("style");
  style.textContent =
    "#pwa-install-banner,#pwa-push-banner{position:fixed;bottom:0;left:0;right:0;z-index:9999;" +
    "display:flex;align-items:center;justify-content:center;gap:12px;padding:14px 20px;" +
    "background:linear-gradient(135deg,#0a1410,#081210);border-top:1px solid rgba(42,255,123,.25);" +
    "box-shadow:0 -4px 24px rgba(0,0,0,.5);font-size:14px;color:#e6f5ec;font-family:'Roboto',sans-serif;" +
    "animation:pwa-slide-up .35s ease-out}" +
    "#pwa-install-banner span,#pwa-push-banner span{flex:1}" +
    "#pwa-install-btn,#pwa-push-btn{padding:8px 20px;border:none;border-radius:8px;background:#2aff7b;" +
    "color:#050a07;font-weight:700;font-size:13px;cursor:pointer;white-space:nowrap}" +
    "#pwa-dismiss-btn,#pwa-push-dismiss{background:none;border:none;color:#8aaea0;font-size:22px;" +
    "cursor:pointer;padding:0 4px;line-height:1}" +
    "@keyframes pwa-slide-up{from{transform:translateY(100%)}to{transform:translateY(0)}}" +
    // botão fixo de instalar (chip na topbar do PC; no mobile vira .sidebar-btn e herda o estilo da barra)
    // visual espelha o .status-pill do layout.css (EM OPERAÇÃO): letras cinza, ícone verde
    ".pwa-install-chip{display:inline-flex;align-items:center;gap:7px;padding:5px 12px;margin-right:10px;" +
    "border:1px solid rgba(57,229,140,.22);border-radius:99px;background:rgba(57,229,140,.06);" +
    "color:var(--text-muted,#4dab7a);font-size:10px;font-weight:400;letter-spacing:.06em;" +
    "text-transform:uppercase;font-family:'JetBrains Mono',monospace;cursor:pointer;white-space:nowrap;" +
    "transition:background .2s ease}" +
    ".pwa-install-chip:hover{background:rgba(57,229,140,.14)}" +
    ".pwa-install-chip i{font-size:12px;color:#2aff7b}" +
    "@media (max-width:900px){.pwa-install-chip{font-size:9px;padding:4px 8px;gap:5px}" +
    ".pwa-install-chip i{font-size:11px}}" +
    "#pwa-install-fixed.sidebar-btn{color:#2aff7b}" +
    "#pwa-install-help{position:fixed;inset:0;z-index:10000;display:flex;align-items:center;" +
    "justify-content:center;background:rgba(0,0,0,.65);padding:20px}" +
    ".pwa-help-card{max-width:420px;width:100%;background:linear-gradient(135deg,#0a1410,#081210);" +
    "border:1px solid rgba(42,255,123,.25);border-radius:14px;padding:22px;color:#e6f5ec;" +
    "font-family:'Roboto',sans-serif;box-shadow:0 8px 40px rgba(0,0,0,.6)}" +
    ".pwa-help-card h3{margin:0 0 12px;font-size:16px;color:#2aff7b;display:flex;align-items:center;gap:8px}" +
    ".pwa-help-card ol{margin:0;padding-left:20px;font-size:14px;line-height:1.7}" +
    ".pwa-help-note{margin-top:12px;font-size:12px;color:#8aaea0}" +
    ".pwa-help-close{margin-top:16px;width:100%;padding:10px;border:none;border-radius:8px;" +
    "background:#2aff7b;color:#050a07;font-weight:700;font-size:13px;cursor:pointer}";
  document.head.appendChild(style);

  const API_BASE = "https://jgeg9i0js1.execute-api.us-east-1.amazonaws.com";
  let deferredInstallPrompt = null;

  // ── Register SW ──
  // caminho relativo: funciona na raiz (produção) e em subpath (pipeline de teste github.io/USINA/)
  navigator.serviceWorker.register("sw.js").then((reg) => {
    console.log("[PWA] SW registrado", reg.scope);
    // iOS Safari fora do app instalado NÃO tem window.Notification —
    // sem este guard a linha abaixo estoura e mata o fluxo em silêncio.
    if (!("Notification" in window)) return;
    if (Notification.permission === "granted") {
      subscribePush(reg);
    } else if (Notification.permission === "default") {
      const isIOS = /iphone|ipad|ipod/i.test(navigator.userAgent);
      if (isIOS) {
        // Apple só aceita requestPermission() disparado por um TAP explícito
        // em elemento interativo (botão) — gesto solto no documento não vale.
        showPushBanner(reg);
      } else {
        // Android/desktop: primeiro toque/clique já vale como gesto.
        const onFirstGesture = () => {
          document.removeEventListener("pointerdown", onFirstGesture);
          askNotificationPermission(reg);
        };
        document.addEventListener("pointerdown", onFirstGesture, { once: true });
      }
    }
  });

  // ── Install prompt (Android / desktop Chrome) ──
  window.addEventListener("beforeinstallprompt", (e) => {
    e.preventDefault();
    deferredInstallPrompt = e;
    showInstallBanner();
  });

  // ── iOS: não há prompt automático; mostra instrução manual ──
  (function iosInstallHint() {
    const isIOS = /iphone|ipad|ipod/i.test(navigator.userAgent);
    const isStandalone =
      window.navigator.standalone === true ||
      window.matchMedia("(display-mode: standalone)").matches;
    if (!isIOS || isStandalone) return;
    if (localStorage.getItem("pwa_ios_hint_dismissed")) return;
    if (window.location.pathname.includes("index.html")) return;

    window.addEventListener("load", () => {
      const banner = document.createElement("div");
      banner.id = "pwa-install-banner";
      banner.innerHTML =
        '<span>Para instalar: toque em <b>Compartilhar</b> e depois em <b>Adicionar à Tela de Início</b></span>' +
        '<button id="pwa-dismiss-btn" aria-label="Fechar">&times;</button>';
      document.body.appendChild(banner);
      document.getElementById("pwa-dismiss-btn").onclick = () => {
        localStorage.setItem("pwa_ios_hint_dismissed", "1");
        banner.remove();
      };
    });
  })();

  // ── Botão FIXO "Instalar app" (não some ao dispensar, ao contrário dos banners) ──
  // PC: chip no canto superior esquerdo da topbar. Mobile: ícone no menuzinho de baixo
  // (a .sidebar vira bottom bar ≤640px). iOS não permite disparar instalação por JS,
  // então o clique abre um passo a passo por plataforma.
  (function fixedInstallButton() {
    const isStandalone =
      window.navigator.standalone === true ||
      window.matchMedia("(display-mode: standalone)").matches;
    if (isStandalone) return; // já instalado / rodando como app

    const mount = () => {
      if (document.getElementById("pwa-install-fixed")) return;
      const mobile = window.matchMedia("(max-width: 640px)").matches;
      const nav = document.querySelector(".sidebar-nav");
      const topLeft = document.querySelector(".topbar-left");
      let btn = null;

      if (mobile && nav) {
        btn = document.createElement("button");
        btn.className = "sidebar-btn";
        btn.innerHTML = '<i class="fa-solid fa-circle-down"></i>';
      } else if (topLeft) {
        btn = document.createElement("button");
        btn.className = "pwa-install-chip";
        btn.innerHTML =
          '<i class="fa-solid fa-circle-down"></i><span>Instalar app</span>';
      }
      if (!btn) return; // página sem topbar/menuzinho (ex.: plant.html)

      btn.id = "pwa-install-fixed";
      btn.type = "button";
      btn.title = "Instalar aplicativo";
      btn.setAttribute("aria-label", "Instalar aplicativo");
      btn.addEventListener("click", onInstallClick);
      if (mobile && nav) nav.appendChild(btn);
      else topLeft.insertBefore(btn, topLeft.firstChild);
    };

    window.addEventListener("appinstalled", () => {
      const b = document.getElementById("pwa-install-fixed");
      if (b) b.remove();
    });

    if (document.readyState === "loading")
      document.addEventListener("DOMContentLoaded", mount);
    else mount();
  })();

  function onInstallClick() {
    if (deferredInstallPrompt) {
      // Chrome/Edge (Android + desktop): dispara o prompt nativo
      deferredInstallPrompt.prompt();
      deferredInstallPrompt.userChoice.then((choice) => {
        deferredInstallPrompt = null;
        if (choice && choice.outcome === "accepted") {
          const b = document.getElementById("pwa-install-fixed");
          if (b) b.remove();
        }
      });
      return;
    }
    showInstallHelpModal();
  }

  function showInstallHelpModal() {
    if (document.getElementById("pwa-install-help")) return;
    const isIOS = /iphone|ipad|ipod/i.test(navigator.userAgent);
    const isAndroid = /android/i.test(navigator.userAgent);

    let steps, note;
    if (isIOS) {
      steps =
        "<li>Abra este site no <b>Safari</b> (no iPhone/iPad só funciona por ele).</li>" +
        '<li>Toque em <b>Compartilhar</b> <i class="fa-solid fa-arrow-up-from-bracket"></i> na barra do navegador.</li>' +
        "<li>Role a lista e toque em <b>Adicionar à Tela de Início</b>.</li>" +
        "<li>Confirme em <b>Adicionar</b> — o ícone AIOTI aparece na tela inicial.</li>";
      note = "Requer iOS 16.4 ou superior para receber notificações.";
    } else if (isAndroid) {
      steps =
        "<li>Toque no menu <b>⋮</b> do navegador (canto superior direito).</li>" +
        "<li>Toque em <b>Instalar aplicativo</b> (ou <b>Adicionar à tela inicial</b>).</li>" +
        "<li>Confirme — o ícone AIOTI aparece na tela inicial.</li>";
      note = "No Brave, ative antes: Configurações → Privacidade → \"Usar serviços do Google para mensagens push\".";
    } else {
      steps =
        "<li>Use <b>Chrome</b> ou <b>Edge</b>.</li>" +
        "<li>Clique no ícone de <b>instalação</b> no fim da barra de endereço (um monitor com seta).</li>" +
        "<li>Ou abra o menu <b>⋮</b> → <b>Transmitir, salvar e compartilhar</b> → <b>Instalar app</b>.</li>";
      note = "Se o app já estiver instalado, abra pelo atalho AIOTI no seu sistema.";
    }

    const overlay = document.createElement("div");
    overlay.id = "pwa-install-help";
    overlay.innerHTML =
      '<div class="pwa-help-card" role="dialog" aria-label="Como instalar o aplicativo">' +
      '<h3><i class="fa-solid fa-circle-down"></i> Instalar o app AIOTI</h3>' +
      "<ol>" + steps + "</ol>" +
      '<div class="pwa-help-note">' + note + "</div>" +
      '<button class="pwa-help-close" type="button">Entendi</button>' +
      "</div>";
    document.body.appendChild(overlay);
    overlay.querySelector(".pwa-help-close").onclick = () => overlay.remove();
    overlay.addEventListener("click", (e) => {
      if (e.target === overlay) overlay.remove();
    });
  }

  function showInstallBanner() {
    if (document.getElementById("pwa-install-banner")) return;

    const banner = document.createElement("div");
    banner.id = "pwa-install-banner";
    banner.innerHTML =
      '<span>Instalar o app AIOTI no seu dispositivo</span>' +
      '<button id="pwa-install-btn">Instalar</button>' +
      '<button id="pwa-dismiss-btn" aria-label="Fechar">&times;</button>';
    document.body.appendChild(banner);

    document.getElementById("pwa-install-btn").onclick = () => {
      deferredInstallPrompt.prompt();
      deferredInstallPrompt.userChoice.then(() => {
        deferredInstallPrompt = null;
        banner.remove();
      });
    };
    document.getElementById("pwa-dismiss-btn").onclick = () => banner.remove();
  }

  // ── iOS: banner com botão p/ ativar push (permissão exige tap em botão) ──
  function showPushBanner(reg) {
    if (window.location.pathname.includes("index.html")) return; // não na tela de login
    const user = JSON.parse(localStorage.getItem("user") || "{}");
    if (!user.username) return; // só logado
    if (localStorage.getItem("pwa_push_hint_dismissed")) return;

    const mount = () => {
      if (document.getElementById("pwa-push-banner")) return;
      const banner = document.createElement("div");
      banner.id = "pwa-push-banner";
      banner.innerHTML =
        '<span>🔔 Receber alertas das usinas neste aparelho?</span>' +
        '<button id="pwa-push-btn">Ativar</button>' +
        '<button id="pwa-push-dismiss" aria-label="Fechar">&times;</button>';
      document.body.appendChild(banner);
      document.getElementById("pwa-push-btn").onclick = () => {
        // requestPermission() PRECISA ser chamado direto no handler do clique,
        // sem await/fetch antes — senão o Safari descarta o gesto.
        Notification.requestPermission().then((perm) => {
          if (perm !== "granted") return;
          // iOS: garante SW ATIVO antes de assinar (pode ainda estar instalando)
          navigator.serviceWorker.ready
            .then((r) => subscribePush(r))
            .catch(() => subscribePush(reg));
        });
        banner.remove();
      };
      document.getElementById("pwa-push-dismiss").onclick = () => {
        localStorage.setItem("pwa_push_hint_dismissed", "1");
        banner.remove();
      };
    };
    if (document.body) mount();
    else window.addEventListener("load", mount);
  }

  // ── Push Notification ──
  function askNotificationPermission(reg) {
    if (window.location.pathname.includes("index.html")) return;
    Notification.requestPermission().then((perm) => {
      if (perm !== "granted") return;
      navigator.serviceWorker.ready
        .then((r) => subscribePush(r))
        .catch(() => subscribePush(reg));
    });
  }

  function subscribePush(reg) {
    reg.pushManager.getSubscription().then((sub) => {
      if (sub) return sendSubToServer(sub);
      const vapidKey = localStorage.getItem("vapid_public_key");
      if (!vapidKey) {
        fetchVapidKey().then((key) => {
          if (key) doSubscribe(reg, key);
        });
      } else {
        doSubscribe(reg, vapidKey);
      }
    });
  }

  function fetchVapidKey() {
    return fetch(API_BASE + "/push/vapid-key")
      .then((r) => r.json())
      .then((d) => {
        if (d.public_key) {
          localStorage.setItem("vapid_public_key", d.public_key);
          return d.public_key;
        }
        return null;
      })
      .catch(() => null);
  }

  function doSubscribe(reg, vapidKey) {
    const appServerKey = urlBase64ToUint8Array(vapidKey);
    reg.pushManager
      .subscribe({ userVisibleOnly: true, applicationServerKey: appServerKey })
      .then((sub) => sendSubToServer(sub))
      .catch((err) => console.warn("[PWA] push subscribe failed:", err));
  }

  function sendSubToServer(sub) {
    const user = JSON.parse(localStorage.getItem("user") || "{}");
    fetch(API_BASE + "/push/subscribe", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "X-Customer-Id": user.customer_id || "",
        "X-Is-Superuser": user.is_superuser ? "true" : "false",
        "X-User-Id": user.id || "",
        "X-Username": user.username || "",
      },
      body: JSON.stringify({ subscription: sub.toJSON(), username: user.username }),
    }).catch(() => {});
  }

  function urlBase64ToUint8Array(base64String) {
    const padding = "=".repeat((4 - (base64String.length % 4)) % 4);
    const base64 = (base64String + padding).replace(/-/g, "+").replace(/_/g, "/");
    const raw = atob(base64);
    const arr = new Uint8Array(raw.length);
    for (let i = 0; i < raw.length; i++) arr[i] = raw.charCodeAt(i);
    return arr;
  }
})();
