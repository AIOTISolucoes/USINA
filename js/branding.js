/* =========================================================
   BRANDING — personalização white-label por cliente (v1)
   - Carrega GET /branding (cache em localStorage p/ não piscar)
   - Aplica cores (deriva glows/borders da cor primária), logo,
     nome da plataforma, textos e skin do robô
   - Injeta o painel "Personalização" na sidebar (só admin)
   Auto-contido: não depende de app.js/plant.js.
   Sem linha no banco = tema AIOTI padrão (não mexe em nada).
========================================================= */
(function () {
  "use strict";

  const API_BASE = "https://jgeg9i0js1.execute-api.us-east-1.amazonaws.com";
  const CACHE_KEY = "branding_v1";
  const DEFAULT_PDF_FOOTER = "Gerado automaticamente pela plataforma AIOTI Solar SCADA";

  function getUser() {
    try { return JSON.parse(localStorage.getItem("user") || "{}"); } catch { return {}; }
  }

  function authHeaders(extra) {
    const u = getUser();
    const h = Object.assign({}, extra || {});
    if (u.customer_id) h["X-Customer-Id"] = u.customer_id;
    if (u.is_superuser === true) h["X-Is-Superuser"] = "true";
    if (u.username) h["X-Username"] = u.username;
    return h;
  }

  /* ---------- cor: helpers ---------- */
  function hexToRgb(hex) {
    const m = /^#([0-9a-f]{6})$/i.exec((hex || "").trim());
    if (!m) return null;
    const n = parseInt(m[1], 16);
    return [(n >> 16) & 255, (n >> 8) & 255, n & 255];
  }
  function rgbToHex(r, g, b) {
    const c = (v) => Math.max(0, Math.min(255, Math.round(v))).toString(16).padStart(2, "0");
    return "#" + c(r) + c(g) + c(b);
  }
  // mistura cor com alvo (t=0 -> cor, t=1 -> alvo)
  function mix(hex, target, t) {
    const a = hexToRgb(hex), b = hexToRgb(target);
    if (!a || !b) return hex;
    return rgbToHex(a[0] + (b[0] - a[0]) * t, a[1] + (b[1] - a[1]) * t, a[2] + (b[2] - a[2]) * t);
  }
  const lighten = (hex, t) => mix(hex, "#ffffff", t);
  const darken = (hex, t) => mix(hex, "#000000", t);
  function rgba(hex, a) {
    const c = hexToRgb(hex);
    return c ? `rgba(${c[0]},${c[1]},${c[2]},${a})` : hex;
  }

  /* ---------- aplicação do tema ---------- */
  const setP = (name, val) => document.documentElement.style.setProperty(name, val);

  function applyColors(colors) {
    const P = colors.primary;
    if (P && hexToRgb(P)) {
      const rgb = hexToRgb(P).join(", ");
      const accent = (colors.accent && hexToRgb(colors.accent)) ? colors.accent : lighten(P, 0.20);
      // layout.css / style.css (resumo, OS)
      setP("--accent-green", P);
      setP("--green-bright", lighten(P, 0.22));
      setP("--green-mid", P);
      setP("--green-deep", darken(P, 0.35));
      setP("--green-dark", darken(P, 0.78));
      setP("--border", rgba(P, 0.14));
      setP("--border-soft", rgba(P, 0.14));
      setP("--border-focus", rgba(P, 0.42));
      setP("--accent-soft", rgba(P, 0.08));
      setP("--glow", rgba(P, 0.16));
      setP("--text-primary", mix(P, "#ffffff", 0.84));
      setP("--text-main", mix(P, "#ffffff", 0.84));
      setP("--text-muted", mix(P, "#888888", 0.45));
      // plant.css / plant2.css (página da usina)
      setP("--neon", P);
      setP("--neon-rgb", rgb);
      setP("--neon-soft", rgba(P, 0.25));
      setP("--neon-strong", rgba(P, 0.55));
      setP("--neon-glow-1", rgba(P, 0.45));
      setP("--neon-glow-2", rgba(P, 0.22));
      setP("--neon-glow-3", rgba(P, 0.10));
      setP("--border-neon", rgba(P, 0.22));
      setP("--lime", accent);
      setP("--lime-soft", rgba(accent, 0.18));
      // os.html define os tokens no <body> (inline vence a regra body{})
      const setBody = (name, val) => {
        if (document.body) document.body.style.setProperty(name, val);
        else document.addEventListener("DOMContentLoaded", () => document.body.style.setProperty(name, val));
      };
      setBody("--accent", P);
      setBody("--accent-dim", rgba(P, 0.15));
      setBody("--accent-glow", rgba(P, 0.35));
      // theme-color (barra do browser/PWA aberto)
      document.querySelectorAll('meta[name="theme-color"]').forEach((m) => m.setAttribute("content", P));
    }
    const B = colors.bg;
    if (B && hexToRgb(B)) {
      setP("--bg-darkest", B);
      setP("--bg-dark", lighten(B, 0.03));
      setP("--bg-0", B);
      setP("--bg-1", lighten(B, 0.04));
      setP("--bg-main", B);
      setP("--bg-card", rgba(lighten(B, 0.05), 0.98));
      setP("--bg-card-alt", rgba(lighten(B, 0.10), 0.35));
      setP("--card-0", rgba(lighten(B, 0.06), 0.97));
      setP("--card-1", rgba(lighten(B, 0.03), 0.97));
    }
    const A = colors.alarm;
    if (A && hexToRgb(A)) {
      setP("--error", A);
      setP("--alarm-red", A);
      setP("--alarm-red-2", lighten(A, 0.15));
      setP("--alarm-red-glow-1", rgba(A, 0.35));
      setP("--alarm-red-glow-2", rgba(A, 0.18));
      setP("--alarm-red-glow-3", rgba(A, 0.12));
    }
  }

  const LOGO_SELECTOR = 'img[src*="logo-aioti"], .sidebar-logo-img, .notif-modal-logo, .tk-modal-logo';
  let _logoObserver = null;

  function applyLogo(logoData) {
    if (!logoData) return;
    const swap = (root) => {
      (root.querySelectorAll ? root.querySelectorAll(LOGO_SELECTOR) : []).forEach((img) => {
        if (img.src !== logoData) {
          if (!img.dataset.brandingOriginal) img.dataset.brandingOriginal = img.src;
          img.src = logoData;
        }
      });
    };
    const run = () => swap(document);
    if (document.readyState === "loading") document.addEventListener("DOMContentLoaded", run);
    else run();
    // modais criados dinamicamente (notificações, tickets) também trocam
    if (!_logoObserver && document.body) {
      _logoObserver = new MutationObserver(() => swap(document));
      _logoObserver.observe(document.body, { childList: true, subtree: true });
    } else if (!_logoObserver) {
      document.addEventListener("DOMContentLoaded", () => {
        _logoObserver = new MutationObserver(() => swap(document));
        _logoObserver.observe(document.body, { childList: true, subtree: true });
      });
    }
  }

  function applyName(appName) {
    if (!appName) return;
    let t = document.title;
    t = t.split("AIOTI Solar SCADA").join(appName);
    t = t.split("AIOTI").join(appName);
    document.title = t;
  }

  function applyRobotSkin(skin) {
    let st = document.getElementById("brandingRobotSkin");
    if (skin === "neutral") {
      if (!st) {
        st = document.createElement("style");
        st.id = "brandingRobotSkin";
        st.textContent = 'img[src*="roboaioti"]{filter:grayscale(1) brightness(1.05);}';
        document.head.appendChild(st);
      }
    } else if (st) {
      st.remove();
    }
  }

  function applyBranding(b) {
    if (!b) return;
    applyColors(b.colors || {});
    applyLogo(b.logo_data);
    applyName(b.app_name);
    applyRobotSkin(b.robot_skin);
    // usado pelo rodapé dos relatórios PDF (app.js/plant.js)
    window.__BRANDING = b;
    window.__BRANDING_PDF_FOOTER =
      (b.texts && b.texts.pdf_footer) ||
      (b.app_name ? "Gerado automaticamente pela plataforma " + b.app_name : DEFAULT_PDF_FOOTER);
  }

  /* ---------- carga: cache primeiro (sem flash), depois rede ---------- */
  function cacheKey() {
    const u = getUser();
    return CACHE_KEY + ":" + (u.customer_id || "anon");
  }

  function boot() {
    const u = getUser();
    if (!u.username) return; // não logado (login fica com tema padrão)
    try {
      const cached = JSON.parse(localStorage.getItem(cacheKey()) || "null");
      if (cached) applyBranding(cached);
    } catch { /* cache corrompido: ignora */ }

    fetch(API_BASE + "/branding", { headers: authHeaders(), cache: "no-store" })
      .then((r) => (r.ok ? r.json() : null))
      .then((data) => {
        if (!data) return;
        const b = data.branding;
        if (b) {
          localStorage.setItem(cacheKey(), JSON.stringify(b));
          applyBranding(b);
        } else {
          // cliente sem branding: limpa cache (se tinha, um F5 volta ao padrão)
          localStorage.removeItem(cacheKey());
        }
      })
      .catch(() => { /* offline: fica no cache */ });
  }

  /* =========================================================
     PAINEL DE PERSONALIZAÇÃO (só admin_customer / superuser)
  ========================================================= */
  function isAdmin() {
    const u = getUser();
    return u.is_superuser === true || u.role_key === "admin_customer";
  }

  const PANEL_CSS = `
  .brd-overlay{position:fixed;inset:0;background:rgba(0,0,0,.62);z-index:12000;
    display:flex;align-items:center;justify-content:center;padding:16px;}
  .brd-modal{background:var(--bg-card,#08120c);border:1px solid var(--border-soft,rgba(57,229,140,.2));
    border-radius:14px;width:min(460px,96vw);max-height:92vh;overflow:auto;
    padding:20px 22px;color:var(--text-main,#d4f5e4);font-family:'Exo 2','Segoe UI',sans-serif;}
  .brd-modal h3{margin:0 0 4px;font-size:1.02rem;display:flex;align-items:center;gap:8px;}
  .brd-modal .brd-sub{font-size:.74rem;opacity:.65;margin-bottom:14px;}
  .brd-field{margin-bottom:12px;}
  .brd-field label{display:block;font-size:.72rem;letter-spacing:.04em;opacity:.8;margin-bottom:4px;}
  .brd-field input[type=text],.brd-field input[type=number]{width:100%;box-sizing:border-box;
    background:rgba(0,0,0,.3);border:1px solid var(--border-soft,rgba(57,229,140,.2));
    border-radius:8px;color:inherit;padding:8px 10px;font-size:.82rem;}
  .brd-colors{display:flex;gap:10px;flex-wrap:wrap;}
  .brd-color{display:flex;flex-direction:column;align-items:center;gap:4px;font-size:.66rem;opacity:.85;}
  .brd-color input[type=color]{width:46px;height:34px;border:1px solid var(--border-soft,rgba(255,255,255,.15));
    border-radius:8px;background:transparent;cursor:pointer;padding:2px;}
  .brd-logo-row{display:flex;align-items:center;gap:12px;}
  .brd-logo-prev{width:56px;height:56px;object-fit:contain;border:1px dashed var(--border-soft,rgba(255,255,255,.2));
    border-radius:10px;background:rgba(0,0,0,.25);padding:4px;}
  .brd-check{display:flex;align-items:center;gap:8px;font-size:.78rem;}
  .brd-actions{display:flex;gap:8px;margin-top:16px;flex-wrap:wrap;}
  .brd-btn{flex:1;min-width:110px;padding:9px 10px;border-radius:9px;cursor:pointer;font-size:.78rem;
    font-family:inherit;border:1px solid var(--border-soft,rgba(57,229,140,.25));
    background:rgba(0,0,0,.25);color:inherit;}
  .brd-btn.brd-save{background:var(--accent-green,#39e58c);color:#03140a;font-weight:700;border:none;}
  .brd-btn.brd-danger{border-color:rgba(255,80,80,.4);color:#ff8080;}
  .brd-note{font-size:.68rem;opacity:.55;margin-top:10px;line-height:1.4;}
  @media (max-width:640px){.brd-modal{padding:16px;}.brd-actions{flex-direction:column;}
    .brd-btn{min-width:0;}}
  `;

  function ensurePanelCss() {
    if (document.getElementById("brandingPanelCss")) return;
    const st = document.createElement("style");
    st.id = "brandingPanelCss";
    st.textContent = PANEL_CSS;
    document.head.appendChild(st);
  }

  function fileToDataUrl(file, cb) {
    // SVG pequeno: usa direto; raster: redimensiona p/ 512px via canvas
    if (file.type === "image/svg+xml" && file.size < 300000) {
      const fr = new FileReader();
      fr.onload = () => cb(fr.result);
      fr.readAsDataURL(file);
      return;
    }
    const img = new Image();
    const url = URL.createObjectURL(file);
    img.onload = () => {
      const MAX = 512;
      const scale = Math.min(1, MAX / Math.max(img.width, img.height));
      const cv = document.createElement("canvas");
      cv.width = Math.max(1, Math.round(img.width * scale));
      cv.height = Math.max(1, Math.round(img.height * scale));
      cv.getContext("2d").drawImage(img, 0, 0, cv.width, cv.height);
      URL.revokeObjectURL(url);
      cb(cv.toDataURL("image/png"));
    };
    img.onerror = () => { URL.revokeObjectURL(url); alert("Não consegui ler essa imagem."); };
    img.src = url;
  }

  function openPanel() {
    ensurePanelCss();
    const u = getUser();
    const saved = window.__BRANDING || (() => {
      try { return JSON.parse(localStorage.getItem(cacheKey()) || "null") || {}; } catch { return {}; }
    })();
    const colors = saved.colors || {};
    let draftLogo = saved.logo_data || null;

    const ov = document.createElement("div");
    ov.className = "brd-overlay";
    ov.innerHTML = `
      <div class="brd-modal" role="dialog" aria-label="Personalização">
        <h3><i class="fa-solid fa-palette"></i> Personalização da plataforma</h3>
        <div class="brd-sub">Vale para todos os usuários ${u.is_superuser ? "do cliente escolhido" : "da sua empresa"}. Pré-visualização ao vivo — nada é salvo até clicar em Salvar.</div>
        ${u.is_superuser ? `
        <div class="brd-field"><label>Cliente (ID) — superusuário</label>
          <input type="number" id="brdCustomer" value="${u.customer_id || 1}" min="1"></div>` : ""}
        <div class="brd-field"><label>Nome da plataforma</label>
          <input type="text" id="brdName" maxlength="60" placeholder="AIOTI Solar SCADA"
                 value="${(saved.app_name || "").replace(/"/g, "&quot;")}"></div>
        <div class="brd-field"><label>Logo (PNG/JPG/SVG — fundo transparente fica melhor)</label>
          <div class="brd-logo-row">
            <img id="brdLogoPrev" class="brd-logo-prev" src="${draftLogo || "assets/logo-aioti.png"}" alt="logo">
            <input type="file" id="brdLogoFile" accept="image/png,image/jpeg,image/svg+xml">
          </div></div>
        <div class="brd-field"><label>Cores</label>
          <div class="brd-colors">
            <span class="brd-color"><input type="color" id="brdPrimary" value="${colors.primary || "#7FD055"}">Primária</span>
            <span class="brd-color"><input type="color" id="brdAccent" value="${colors.accent || "#a8f060"}">Destaque</span>
            <span class="brd-color"><input type="color" id="brdBg" value="${colors.bg || "#02050a"}">Fundo</span>
            <span class="brd-color"><input type="color" id="brdAlarm" value="${colors.alarm || "#ff3b3b"}">Alarme</span>
          </div></div>
        <div class="brd-field"><label>Rodapé dos relatórios PDF</label>
          <input type="text" id="brdPdf" maxlength="300" placeholder="${DEFAULT_PDF_FOOTER}"
                 value="${((saved.texts || {}).pdf_footer || "").replace(/"/g, "&quot;")}"></div>
        <div class="brd-field brd-check">
          <input type="checkbox" id="brdRobot" ${saved.robot_skin === "neutral" ? "checked" : ""}>
          <label for="brdRobot" style="margin:0;">Robô assistente neutro (sem cores AIOTI)</label></div>
        <div class="brd-actions">
          <button class="brd-btn brd-save" id="brdSave"><i class="fa-solid fa-floppy-disk"></i> Salvar</button>
          <button class="brd-btn" id="brdCancel">Cancelar</button>
          <button class="brd-btn brd-danger" id="brdReset">Restaurar padrão AIOTI</button>
        </div>
        <div class="brd-note">A tela de login e o ícone do app instalado (PWA) continuam AIOTI nesta versão.
        Cores dos gráficos entram numa próxima etapa.</div>
      </div>`;
    document.body.appendChild(ov);

    const $ = (id) => ov.querySelector("#" + id);

    function draft() {
      return {
        app_name: $("brdName").value.trim() || null,
        logo_data: draftLogo,
        colors: {
          primary: $("brdPrimary").value,
          accent: $("brdAccent").value,
          bg: $("brdBg").value,
          alarm: $("brdAlarm").value,
        },
        texts: $("brdPdf").value.trim() ? { pdf_footer: $("brdPdf").value.trim() } : {},
        robot_skin: $("brdRobot").checked ? "neutral" : "default",
      };
    }
    const preview = () => applyBranding(draft());

    ["brdPrimary", "brdAccent", "brdBg", "brdAlarm"].forEach((id) =>
      $(id).addEventListener("input", preview));
    $("brdRobot").addEventListener("change", preview);
    $("brdName").addEventListener("input", preview);
    $("brdLogoFile").addEventListener("change", (e) => {
      const f = e.target.files && e.target.files[0];
      if (!f) return;
      fileToDataUrl(f, (dataUrl) => {
        if (dataUrl.length > 500000) { alert("Logo ficou grande demais mesmo comprimida — use uma imagem menor."); return; }
        draftLogo = dataUrl;
        $("brdLogoPrev").src = dataUrl;
        preview();
      });
    });

    function close(reloadPage) {
      ov.remove();
      if (reloadPage) location.reload();
    }
    $("brdCancel").addEventListener("click", () => close(true)); // reverte o preview
    ov.addEventListener("click", (e) => { if (e.target === ov) close(true); });

    $("brdSave").addEventListener("click", () => {
      const body = draft();
      if (u.is_superuser && $("brdCustomer")) body.customer_id = parseInt($("brdCustomer").value, 10);
      $("brdSave").disabled = true;
      fetch(API_BASE + "/branding", {
        method: "POST",
        headers: authHeaders({ "Content-Type": "application/json" }),
        body: JSON.stringify(body),
      })
        .then((r) => r.json().then((j) => ({ ok: r.ok, j })))
        .then(({ ok, j }) => {
          if (!ok) throw new Error((j && j.error) || "erro ao salvar");
          const mine = !body.customer_id || body.customer_id === u.customer_id;
          if (mine) localStorage.setItem(cacheKey(), JSON.stringify(body));
          close(!mine); // salvou p/ outro cliente: volta meu tema
        })
        .catch((err) => { alert("Não salvou: " + err.message); $("brdSave").disabled = false; });
    });

    $("brdReset").addEventListener("click", () => {
      if (!confirm("Restaurar o tema padrão AIOTI para todos os usuários?")) return;
      const body = { reset: true };
      if (u.is_superuser && $("brdCustomer")) body.customer_id = parseInt($("brdCustomer").value, 10);
      fetch(API_BASE + "/branding", {
        method: "POST",
        headers: authHeaders({ "Content-Type": "application/json" }),
        body: JSON.stringify(body),
      })
        .then((r) => { if (!r.ok) throw new Error("erro"); })
        .then(() => { localStorage.removeItem(cacheKey()); close(true); })
        .catch(() => alert("Não consegui restaurar — tente de novo."));
    });
  }

  function injectSidebarButton() {
    if (!isAdmin()) return;
    const slot = document.querySelector(".sidebar-bottom");
    if (!slot || document.getElementById("btnBranding")) return;
    const btn = document.createElement("button");
    btn.id = "btnBranding";
    btn.className = "sidebar-btn";
    btn.title = "Personalização";
    btn.setAttribute("aria-label", "Personalização");
    btn.innerHTML = '<i class="fa-solid fa-palette"></i>';
    btn.addEventListener("click", openPanel);
    slot.insertBefore(btn, slot.firstChild);
  }

  /* ---------- boot ---------- */
  boot();
  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", injectSidebarButton);
  } else {
    injectSidebarButton();
  }
})();
