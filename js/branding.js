/* =========================================================
   BRANDING v2 — personalização white-label por cliente
   ---------------------------------------------------------
   Como funciona:
   - MOTOR DE REMAP: varre as stylesheets da página e reescreve
     TODA a família de cor da marca (verdes, hue 70-170) para a
     matiz escolhida, preservando luminosidade e alpha. Cobre
     menu, cards, fundos, gradientes, sombras — sem tocar no CSS.
   - PLUGIN Chart.js global: remapeia as cores de todos os
     datasets/opções na criação de cada gráfico.
   - Escopos: empresa (banco, via /branding — admin) e pessoal
     (este aparelho, localStorage — qualquer usuário).
   - Painel dock com preview ao vivo: cada ajuste aplica na hora
     e dá um flash nas áreas afetadas. Nada é salvo até Salvar.
   Sem branding = tema AIOTI padrão (não mexe em nada).
========================================================= */
(function () {
  "use strict";

  const API_BASE = "https://jgeg9i0js1.execute-api.us-east-1.amazonaws.com";
  const CACHE_COMPANY = "branding_v1";       // espelho do banco (por customer)
  const CACHE_USER = "branding_user_v1";     // personalização pessoal (por username)
  const CACHE_REMAP = "branding_remap_v1";   // css remapeado (evita flash/reprocesso)
  const DEFAULT_PDF_FOOTER = "Gerado automaticamente pela plataforma AIOTI Solar SCADA";
  const BRAND_HUE_MIN = 70, BRAND_HUE_MAX = 170, BRAND_SAT_MIN = 0.16;

  function getUser() {
    try { return JSON.parse(localStorage.getItem("user") || "{}"); } catch { return {}; }
  }
  function authHeaders(extra) {
    const u = getUser();
    const h = Object.assign({}, extra || {});
    if (u.customer_id) h["X-Customer-Id"] = u.customer_id;
    if (u.is_superuser === true) h["X-Is-Superuser"] = "true";
    if (u.username) h["X-Username"] = u.username;
    if (u.id) h["X-User-Id"] = u.id;
    return h;
  }
  const keyCompany = () => CACHE_COMPANY + ":" + (getUser().customer_id || "anon");
  const keyUser = () => CACHE_USER + ":" + (getUser().username || "anon");

  /* =========================================================
     COR — parse/convert/remap
  ========================================================= */
  function rgbToHsl(r, g, b) {
    r /= 255; g /= 255; b /= 255;
    const mx = Math.max(r, g, b), mn = Math.min(r, g, b), d = mx - mn;
    let h = 0;
    if (d) {
      if (mx === r) h = ((g - b) / d) % 6;
      else if (mx === g) h = (b - r) / d + 2;
      else h = (r - g) / d + 4;
      h *= 60; if (h < 0) h += 360;
    }
    const l = (mx + mn) / 2;
    const s = d === 0 ? 0 : d / (1 - Math.abs(2 * l - 1));
    return [h, s, l];
  }
  function hslToRgb(h, s, l) {
    const c = (1 - Math.abs(2 * l - 1)) * s, x = c * (1 - Math.abs(((h / 60) % 2) - 1)), m = l - c / 2;
    let r = 0, g = 0, b = 0;
    if (h < 60) { r = c; g = x; } else if (h < 120) { r = x; g = c; }
    else if (h < 180) { g = c; b = x; } else if (h < 240) { g = x; b = c; }
    else if (h < 300) { r = x; b = c; } else { r = c; b = x; }
    return [Math.round((r + m) * 255), Math.round((g + m) * 255), Math.round((b + m) * 255)];
  }
  function parseHex(hex) {
    let m = /^#([0-9a-f]{6})([0-9a-f]{2})?$/i.exec(hex);
    if (m) {
      const n = parseInt(m[1], 16);
      return [(n >> 16) & 255, (n >> 8) & 255, n & 255, m[2] ? parseInt(m[2], 16) / 255 : 1];
    }
    m = /^#([0-9a-f]{3})$/i.exec(hex);
    if (m) {
      const s = m[1];
      return [parseInt(s[0] + s[0], 16), parseInt(s[1] + s[1], 16), parseInt(s[2] + s[2], 16), 1];
    }
    return null;
  }
  const toHex = (r, g, b) =>
    "#" + [r, g, b].map((v) => Math.max(0, Math.min(255, Math.round(v))).toString(16).padStart(2, "0")).join("");

  // alvos do remap (recalculados a cada applyBranding)
  let TGT = null; // {accH,accS, bgH,bgS} ou null (sem remap)

  function computeTargets(colors) {
    const p = parseHex(colors && colors.primary || "");
    if (!p) { TGT = null; return; }
    const [ph, ps] = rgbToHsl(p[0], p[1], p[2]);
    let bh = ph, bs = ps;
    const bg = parseHex(colors.bg || "");
    if (bg) { const hs = rgbToHsl(bg[0], bg[1], bg[2]); bh = hs[0]; bs = Math.max(hs[1], 0.10); }
    // destaque = SEGUNDA matiz de verdade (verdes claros/brilhos vão pra ela,
    // evitando tema monocromático)
    let sh = ph, ss = ps;
    const ac = parseHex(colors.accent || "");
    if (ac) { const hs = rgbToHsl(ac[0], ac[1], ac[2]); sh = hs[0]; ss = hs[1]; }
    TGT = { accH: ph, accS: ps, bgH: bh, bgS: bs, sacH: sh, sacS: ss };
  }

  // remapeia UMA cor (r,g,b,a) se pertencer à família da marca:
  // escuras -> matiz do fundo | médias -> cor principal | claras -> destaque
  function remapRgb(r, g, b, a) {
    if (!TGT) return null;
    const [h, s, l] = rgbToHsl(r, g, b);
    if (h < BRAND_HUE_MIN || h > BRAND_HUE_MAX || s < BRAND_SAT_MIN) return null;
    let nh, ns;
    if (l < 0.30) { nh = TGT.bgH; ns = Math.min(1, s * 0.55 + TGT.bgS * 0.45); }
    else if (l >= 0.62) { nh = TGT.sacH; ns = Math.min(1, s * 0.35 + TGT.sacS * 0.65); }
    else { nh = TGT.accH; ns = Math.min(1, s * 0.35 + TGT.accS * 0.65); }
    const [nr, ng, nb] = hslToRgb(nh, ns, l);
    return [nr, ng, nb, a];
  }

  function rgbaOf(hex, a) {
    const p = parseHex(hex);
    return p ? `rgba(${p[0]},${p[1]},${p[2]},${a})` : hex;
  }
  function mixHex(h1, h2, t) {
    const a = parseHex(h1), b = parseHex(h2);
    if (!a || !b) return h1;
    return toHex(a[0] + (b[0] - a[0]) * t, a[1] + (b[1] - a[1]) * t, a[2] + (b[2] - a[2]) * t);
  }

  // fundo em DEGRADÊ do tema (bg -> bg2) + auroras sutis nas matizes da marca
  function applyGradient(colors) {
    let st = document.getElementById("brandingGradient");
    if (!st) {
      st = document.createElement("style");
      st.id = "brandingGradient";
      document.head.appendChild(st);
    }
    const ok = colors && parseHex(colors.bg || "") && parseHex(colors.bg2 || "") && parseHex(colors.primary || "");
    if (!ok) { st.textContent = ""; return; }
    const { bg, bg2, primary } = colors;
    const accent = (colors.accent && parseHex(colors.accent)) ? colors.accent : primary;
    st.textContent =
      `body{background:` +
      `radial-gradient(1100px 700px at 10% -10%, ${rgbaOf(primary, 0.10)}, transparent 60%),` +
      `radial-gradient(900px 620px at 100% 0%, ${rgbaOf(accent, 0.07)}, transparent 55%),` +
      `linear-gradient(168deg, ${bg} 0%, ${mixHex(bg, bg2, 0.5)} 48%, ${bg2} 100%) fixed !important;` +
      `background-color:${bg} !important;}`;
  }

  // paleta de séries p/ gráficos multi-série (Data Studio etc.)
  function hueRot(hex, deg) {
    const p = parseHex(hex);
    if (!p) return hex;
    const [h, s, l] = rgbToHsl(p[0], p[1], p[2]);
    const [r, g, b] = hslToRgb((h + deg + 360) % 360, s, l);
    return toHex(r, g, b);
  }
  function buildSeriesPalette(colors) {
    if (!colors || !parseHex(colors.primary || "")) return null;
    const P = colors.primary;
    const A = (colors.accent && parseHex(colors.accent)) ? colors.accent : hueRot(P, 40);
    return [
      P, A, hueRot(P, 35), hueRot(A, -35), hueRot(P, 180), hueRot(A, 150),
      "#ffd84d", "#ff8a65", "#b39ddb", "#80cbc4", "#f06292", "#ffb74d",
    ];
  }

  const HEX_RE = /#[0-9a-fA-F]{3,8}\b/g;
  const RGB_RE = /rgba?\(\s*(\d{1,3})\s*,\s*(\d{1,3})\s*,\s*(\d{1,3})\s*(?:,\s*([\d.]+)\s*)?\)/g;

  // remapeia todas as cores da marca dentro de um texto CSS/valor
  function remapText(text) {
    if (!TGT || !text) return text;
    let changed = false;
    let out = text.replace(RGB_RE, (mm, r, g, b, a) => {
      const nv = remapRgb(+r, +g, +b, a === undefined ? 1 : +a);
      if (!nv) return mm;
      changed = true;
      return a === undefined ? `rgb(${nv[0]},${nv[1]},${nv[2]})`
                             : `rgba(${nv[0]},${nv[1]},${nv[2]},${a})`;
    });
    out = out.replace(HEX_RE, (mm) => {
      const p = parseHex(mm);
      if (!p) return mm;
      const nv = remapRgb(p[0], p[1], p[2], p[3]);
      if (!nv) return mm;
      changed = true;
      const base = toHex(nv[0], nv[1], nv[2]);
      return mm.length === 9 ? base + mm.slice(7) : base;
    });
    return changed ? out : text;
  }
  // exposto p/ app.js/plant.js (paleta de séries, gradientes de canvas)
  window.__brandRemap = (c) => (typeof c === "string" ? remapText(c) : c);

  /* =========================================================
     MOTOR DE REMAP DAS STYLESHEETS
  ========================================================= */
  function collectRules(rules, out) {
    for (const rule of rules) {
      try {
        if (rule.cssRules && rule.cssText && (rule.conditionText !== undefined || rule.type === 4 || rule.type === 12)) {
          // @media / @supports: recursão mantendo o wrapper
          const inner = [];
          collectRules(rule.cssRules, inner);
          if (inner.length) {
            const head = rule.cssText.slice(0, rule.cssText.indexOf("{"));
            out.push(head + "{" + inner.join("\n") + "}");
          }
        } else if (rule.cssText) {
          const mapped = remapText(rule.cssText);
          if (mapped !== rule.cssText) out.push(mapped);
        }
      } catch { /* regra exótica: ignora */ }
    }
  }

  function buildRemapCss() {
    const out = [];
    for (const sheet of document.styleSheets) {
      // NUNCA incluir os styles injetados pelo próprio branding: o
      // #brandingRemap dentro da varredura dobrava o CSS a cada aplicação
      const id = sheet.ownerNode && sheet.ownerNode.id || "";
      if (id.startsWith("branding")) continue;
      if (sheet.ownerNode && sheet.ownerNode.closest && sheet.ownerNode.closest(".brd-dock")) continue;
      let rules = null;
      try { rules = sheet.cssRules; } catch { continue; } // cross-origin (CDN): pula
      if (rules) collectRules(rules, out);
    }
    return out.join("\n");
  }

  // estilos inline criados em runtime (ex.: notify_sound, badges do JS)
  let _inlineScheduled = false;
  function remapInlineStyles() {
    if (!TGT || _inlineScheduled || !document.body) return;
    _inlineScheduled = true;
    requestAnimationFrame(() => {
      _inlineScheduled = false;
      document.querySelectorAll("[style]").forEach((el) => {
        if (el.closest(".brd-dock")) return;
        // remapeia sempre a partir do estilo ORIGINAL (troca de tema re-remapeia certo)
        const css = el.dataset.brdStyle0 || el.getAttribute("style");
        if (!css || (css.indexOf("rgb") < 0 && css.indexOf("#") < 0)) return;
        const mapped = remapText(css);
        if (mapped !== css) {
          if (!el.dataset.brdStyle0) el.dataset.brdStyle0 = css;
          if (el.getAttribute("style") !== mapped) el.setAttribute("style", mapped);
        }
      });
      // SVGs inline com cor em ATRIBUTO (fill/stroke/stop-color) — ícones de
      // device do plant.html etc. CSS não reescreve atributo, então remapeia aqui.
      ["fill", "stroke", "stop-color"].forEach((attr) => {
        const dk = "brd" + attr.replace("-", "") + "0";
        document.querySelectorAll(`svg [${attr}], svg[${attr}]`).forEach((el) => {
          const orig = el.dataset[dk] || el.getAttribute(attr);
          if (!orig || orig === "none" || orig === "currentColor" || orig.startsWith("url(")) return;
          const mapped = remapText(orig);
          if (mapped !== orig) {
            if (!el.dataset[dk]) el.dataset[dk] = orig;
            if (el.getAttribute(attr) !== mapped) el.setAttribute(attr, mapped);
          }
        });
      });
    });
  }

  let _remapScheduled = false;
  function applyRemap(persist) {
    const el = document.getElementById("brandingRemap") || (() => {
      const s = document.createElement("style");
      s.id = "brandingRemap";
      document.head.appendChild(s);
      return s;
    })();
    if (!TGT) {
      el.textContent = "";
      try { localStorage.removeItem(CACHE_REMAP); } catch { }
      // devolve estilos inline e atributos SVG originais (volta ao padrão)
      document.querySelectorAll("[data-brd-style0]").forEach((n) => {
        n.setAttribute("style", n.dataset.brdStyle0);
        delete n.dataset.brdStyle0;
      });
      [["fill", "brdFill0"], ["stroke", "brdStroke0"], ["stop-color", "brdStopcolor0"]].forEach(([attr, dk]) => {
        document.querySelectorAll(`[data-${attr === "stop-color" ? "brd-stopcolor0" : "brd-" + attr + "0"}]`).forEach((n) => {
          n.setAttribute(attr, n.dataset[dk]);
          delete n.dataset[dk];
        });
      });
      return;
    }
    let css = buildRemapCss();
    // SVGs de status com verde embutido no arquivo (gen.svg, logo-plant.svg):
    // rotaciona a matiz do verde AIOTI (~149°) para a matiz da marca
    const rot = Math.round(TGT.accH - 149);
    css += `\nimg[src*="gen.svg"], img[src*="logo-plant"]{filter:hue-rotate(${rot}deg);}`;
    el.textContent = css;
    remapInlineStyles();
    // garante que fica DEPOIS de qualquer stylesheet (cascata vence por ordem)
    if (el !== document.head.lastElementChild) document.head.appendChild(el);
    if (persist) {
      try { localStorage.setItem(CACHE_REMAP, JSON.stringify({ k: remapCacheKey(), css })); } catch { /* quota */ }
    }
  }
  function scheduleRemap(persist) {
    if (_remapScheduled) return;
    _remapScheduled = true;
    requestAnimationFrame(() => { _remapScheduled = false; applyRemap(persist); });
  }
  function remapCacheKey() {
    const b = _effective || {};
    let n = 0;
    try { n = document.styleSheets.length; } catch { }
    return JSON.stringify(b.colors || {}) + "|" + n + "|" + location.pathname.split("/").pop();
  }
  function applyCachedRemap() {
    try {
      const c = JSON.parse(localStorage.getItem(CACHE_REMAP) || "null");
      if (c && c.k === remapCacheKey() && c.css) {
        const s = document.createElement("style");
        s.id = "brandingRemap";
        s.textContent = c.css;
        document.head.appendChild(s);
        return true;
      }
    } catch { }
    return false;
  }

  /* =========================================================
     PLUGIN Chart.js — remapeia cores de todos os gráficos
  ========================================================= */
  const COLOR_FULL_RE = /^(#[0-9a-fA-F]{3,8}|rgba?\([\d.,\s]+\))$/;
  // Mapa reverso GLOBAL: toda cor que o remap produziu lembra a original.
  // Necessário porque o Chart.js v4 troca a identidade dos objetos de options
  // no update() — o registro por referência ($brd) fica órfão e a cor
  // remapeada (matiz fora da faixa 70-170) nunca mais seria recuperada
  // (bug: ticks roxos após "Restaurar padrão").
  const REV = new Map();
  function revRemember(nv, orig) {
    if (nv === orig) return;
    if (REV.size > 4000) REV.clear(); // nunca deixa crescer sem limite
    REV.set(nv, REV.get(orig) || orig);
  }
  // rec: registro [obj, chave, valorOriginal] p/ restaurar na troca de tema
  function remapColorValue(v) {
    const v0 = REV.get(v) || v;     // recupera a original se v já é cor remapeada
    const nv = remapText(v0);       // TGT null => nv = v0 (volta ao padrão)
    return [v0, nv];
  }
  function deepRemap(obj, depth, rec) {
    if (!obj || depth > 8) return;
    if (Array.isArray(obj)) {
      for (let i = 0; i < obj.length; i++) {
        const v = obj[i];
        if (typeof v === "string" && COLOR_FULL_RE.test(v)) {
          const [v0, nv] = remapColorValue(v);
          if (nv !== v) { if (rec) rec.push([obj, i, v0]); obj[i] = nv; revRemember(nv, v0); }
        } else if (v && typeof v === "object") deepRemap(v, depth + 1, rec);
      }
      return;
    }
    if (typeof obj !== "object") return;
    for (const k of Object.keys(obj)) {
      const v = obj[k];
      if (typeof v === "string" && COLOR_FULL_RE.test(v)) {
        const [v0, nv] = remapColorValue(v);
        if (nv !== v) { if (rec) rec.push([obj, k, v0]); obj[k] = nv; revRemember(nv, v0); }
      } else if (v && typeof v === "object" && k !== "data") deepRemap(v, depth + 1, rec);
    }
  }

  const _charts = new Set();
  function remapChart(chart) {
    chart.$brd = chart.$brd || [];
    // sem tema (TGT null) o deepRemap ainda roda: via REV ele CURA valores
    // remapeados que ficaram órfãos do $brd (devolve a cor original)
    try { deepRemap(chart.config.data, 0, chart.$brd); } catch { }
    try { deepRemap(chart.config.options, 0, chart.$brd); } catch { }
  }
  // troca de tema AO VIVO: devolve as cores originais e remapeia de novo
  function recolorCharts() {
    _charts.forEach((chart) => {
      try {
        (chart.$brd || []).forEach(([o, k, orig]) => { o[k] = orig; });
        chart.$brd = [];
        remapChart(chart);
        chart.update("none");
      } catch { }
    });
  }
  function hookCharts() {
    if (!window.Chart || window.Chart.__brandingHooked) return;
    try {
      window.Chart.register({
        id: "aiotiBranding",
        beforeInit(chart) { remapChart(chart); },
        afterInit(chart) { _charts.add(chart); },
        afterDestroy(chart) { _charts.delete(chart); },
      });
      window.Chart.__brandingHooked = true;
    } catch { }
  }
  // Chart.js entra com defer — fica de olho até aparecer
  (function waitChart(tries) {
    if (window.Chart) { hookCharts(); return; }
    if (tries > 100) return;
    setTimeout(() => waitChart(tries + 1), 100);
  })(0);

  /* =========================================================
     APLICAÇÃO — logo, nome, robô, pdf, tokens fora de CSS
  ========================================================= */
  const LOGO_SELECTOR = 'img[src*="logo-aioti"], .sidebar-logo-img, .notif-modal-logo, .tk-modal-logo';
  let _logoObserver = null, _currentLogo = null;

  function applyLogo(logoData) {
    _currentLogo = logoData || null;
    const swap = () => {
      document.querySelectorAll(LOGO_SELECTOR).forEach((img) => {
        // nunca registra data: como "original" — um nó criado já com a logo
        // custom contaminaria o restore (logo custom presa após restaurar)
        if (!img.dataset.brandingOriginal) {
          const s = img.getAttribute("src");
          if (s && !s.startsWith("data:")) img.dataset.brandingOriginal = s;
        }
        const want = _currentLogo || img.dataset.brandingOriginal;
        if (want && img.getAttribute("src") !== want) img.src = want;
      });
    };
    const start = () => {
      swap();
      remapInlineStyles();
      if (!_logoObserver) {
        _logoObserver = new MutationObserver(() => { swap(); remapInlineStyles(); });
        _logoObserver.observe(document.body, { childList: true, subtree: true });
      }
    };
    if (document.body) start();
    else document.addEventListener("DOMContentLoaded", start);
  }

  let _titleOriginal = null;
  function applyName(appName) {
    if (_titleOriginal === null) _titleOriginal = document.title;
    if (!appName) { document.title = _titleOriginal; return; }
    document.title = _titleOriginal.split("AIOTI Solar SCADA").join(appName).split("AIOTI").join(appName);
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
    } else if (st) st.remove();
  }

  /* =========================================================
     LAYOUTS — estilos estruturais prontos (editáveis por cima)
  ========================================================= */
  const LAYOUTS = {
    default: { name: "Padrão",   desc: "O visual atual da plataforma" },
    compact: { name: "Compacto", desc: "Mais denso: cards menores, mais usinas na tela" },
    solid:   { name: "Sólido",   desc: "Sem brilhos e transparências — foco total no dado" },
    glass:   { name: "Vidro",    desc: "Superfícies translúcidas com desfoque" },
  };
  const LAYOUT_CSS = `
  /* — Compacto — */
  html[data-brd-layout="compact"] .plant-card{padding:10px 12px;border-radius:10px;}
  html[data-brd-layout="compact"] .plant-card__stat{padding:4px 6px;}
  html[data-brd-layout="compact"] .plant-card__stat-value{font-size:.82rem;}
  html[data-brd-layout="compact"] .plant-card__chart{height:96px;}
  html[data-brd-layout="compact"] .plants-grid{gap:12px;grid-template-columns:repeat(auto-fill,minmax(300px,1fr));}
  html[data-brd-layout="compact"] .top-summary{padding-top:6px;padding-bottom:6px;}
  html[data-brd-layout="compact"] .card,html[data-brd-layout="compact"] .panel{border-radius:10px;}
  /* — Sólido — */
  html[data-brd-layout="solid"] .plant-card,html[data-brd-layout="solid"] .card,
  html[data-brd-layout="solid"] .panel,html[data-brd-layout="solid"] .modal,
  html[data-brd-layout="solid"] .topbar,html[data-brd-layout="solid"] .sidebar{
    box-shadow:none !important;backdrop-filter:none !important;}
  html[data-brd-layout="solid"] *{text-shadow:none !important;}
  html[data-brd-layout="solid"] .plant-card{background:var(--bg-card,#071009) !important;}
  html[data-brd-layout="solid"] body::before,html[data-brd-layout="solid"] body::after{display:none !important;}
  /* — Vidro — */
  html[data-brd-layout="glass"] .plant-card,html[data-brd-layout="glass"] .card,
  html[data-brd-layout="glass"] .panel{
    background:rgba(255,255,255,.045) !important;backdrop-filter:blur(14px) saturate(1.15);
    -webkit-backdrop-filter:blur(14px) saturate(1.15);border-color:rgba(255,255,255,.12) !important;}
  html[data-brd-layout="glass"] .topbar{backdrop-filter:blur(10px);-webkit-backdrop-filter:blur(10px);
    background:rgba(0,0,0,.35) !important;}
  @media (prefers-reduced-motion: reduce){
    html[data-brd-layout] *{transition-duration:.01ms !important;}
  }`;

  function applyLayout(layout) {
    const l = LAYOUTS[layout] ? layout : "default";
    if (!document.getElementById("brandingLayoutCss")) {
      const st = document.createElement("style");
      st.id = "brandingLayoutCss";
      st.textContent = LAYOUT_CSS;
      document.head.appendChild(st);
    }
    if (l === "default") delete document.documentElement.dataset.brdLayout;
    else document.documentElement.dataset.brdLayout = l;
  }

  let _effective = null;
  function applyBranding(b) {
    // cada etapa protegida: uma falha não pode impedir as seguintes
    // (senão o restore fica pela metade — CSS antigo preso, charts sujos)
    const safe = (fn) => { try { fn(); } catch (e) { console.warn("[branding] etapa falhou:", e); } };
    _effective = b || null;
    safe(() => computeTargets(b && b.colors));
    safe(() => { window.__BRAND_PALETTE = buildSeriesPalette(b && b.colors); });
    safe(() => { if (window.__refreshDsPalette) window.__refreshDsPalette(); });
    safe(() => applyLayout(b && b.colors && b.colors.layout));
    safe(() => applyGradient(b && b.colors));
    safe(recolorCharts);
    // tokens que o remap não alcança (triplet cru + meta)
    safe(() => {
      const p = b && b.colors && parseHex(b.colors.primary);
      if (p) {
        document.documentElement.style.setProperty("--neon-rgb", `${p[0]}, ${p[1]}, ${p[2]}`);
        document.querySelectorAll('meta[name="theme-color"]').forEach((m) => m.setAttribute("content", b.colors.primary));
      } else {
        document.documentElement.style.removeProperty("--neon-rgb");
      }
    });
    safe(() => applyLogo(b && b.logo_data));
    safe(() => applyName(b && b.app_name));
    safe(() => applyRobotSkin(b && b.robot_skin));
    window.__BRANDING = b || null;
    window.__BRANDING_PDF_FOOTER =
      (b && b.texts && b.texts.pdf_footer) ||
      (b && b.app_name ? "Gerado automaticamente pela plataforma " + b.app_name : DEFAULT_PDF_FOOTER);
    scheduleRemap(true);
  }

  /* =========================================================
     BOOT — pessoal > empresa > padrão
  ========================================================= */
  function readJson(key) {
    try { return JSON.parse(localStorage.getItem(key) || "null"); } catch { return null; }
  }
  function effectiveSaved() {
    return readJson(keyUser()) || readJson(keyCompany()) || null;
  }

  function boot() {
    const u = getUser();
    if (!u.username) return; // login: tema padrão
    const saved = effectiveSaved();
    if (saved) {
      _effective = saved;
      computeTargets(saved.colors);
      applyCachedRemap(); // css pronto do último acesso (zero flash)
      // stylesheets ainda carregando: refaz quando o DOM estiver de pé
      document.addEventListener("DOMContentLoaded", () => applyBranding(saved));
      if (document.readyState !== "loading") applyBranding(saved);
    }
    // rede: atualiza os espelhos (empresa + tema pessoal do usuário no banco)
    fetch(API_BASE + "/branding", { headers: authHeaders(), cache: "no-store" })
      .then((r) => (r.ok ? r.json() : null))
      .then((data) => {
        if (!data) return;
        if (data.branding) localStorage.setItem(keyCompany(), JSON.stringify(data.branding));
        else localStorage.removeItem(keyCompany());
        // "user_branding" só existe na Lambda nova; Lambda antiga = não mexe
        // no tema pessoal local (comportamento antigo preservado)
        if ("user_branding" in data) {
          if (data.user_branding) {
            localStorage.setItem(keyUser(), JSON.stringify(data.user_branding));
          } else {
            // servidor sem tema pessoal: se este aparelho tem um tema legado
            // (era só-local), sobe pro banco UMA vez — vira "por usuário"
            const legacy = readJson(keyUser());
            if (legacy) {
              fetch(API_BASE + "/branding", {
                method: "POST",
                headers: authHeaders({ "Content-Type": "application/json" }),
                body: JSON.stringify(Object.assign({ scope: "user" }, legacy)),
              }).catch(() => { });
            }
          }
        }
        const eff = effectiveSaved();
        const cur = JSON.stringify(_effective);
        if (JSON.stringify(eff) !== cur) applyBranding(eff);
      })
      .catch(() => { });
  }

  /* =========================================================
     PAINEL — dock lateral com preview ao vivo
  ========================================================= */
  // TEMAS — paletas completas e harmonizadas (2-3 matizes que combinam) com
  // degradê de fundo (bg -> bg2). Selecionáveis e editáveis por cima.
  const THEMES = [
    { name: "AIOTI",      primary: "#39e58c", accent: "#a8f060", bg: "#050e07", bg2: "#04141c", alarm: "#ff4f4f" },
    { name: "Nebulosa",   primary: "#a78bfa", accent: "#f472b6", bg: "#0a0714", bg2: "#140b22", alarm: "#ff5c77" },
    { name: "Oceano",     primary: "#4da3ff", accent: "#2dd4bf", bg: "#040a14", bg2: "#052030", alarm: "#ff5c5c" },
    { name: "Boreal",     primary: "#22d3ee", accent: "#a78bfa", bg: "#030f12", bg2: "#0c0a1e", alarm: "#ff6b6b" },
    { name: "Entardecer", primary: "#fb923c", accent: "#f472b6", bg: "#120a04", bg2: "#1c0612", alarm: "#ff5252" },
    { name: "Tropical",   primary: "#a3e635", accent: "#2dd4bf", bg: "#0a1204", bg2: "#03181a", alarm: "#ff5252" },
    { name: "Dourado",    primary: "#f5b93d", accent: "#4da3ff", bg: "#120c03", bg2: "#0a1020", alarm: "#ff5252" },
    { name: "Meia-noite", primary: "#7dd3fc", accent: "#c4b5fd", bg: "#05070d", bg2: "#0b0a18", alarm: "#ff6363" },
  ];

  // cada controle sabe QUAIS partes da tela ele muda (flash de destaque)
  const AFFECTS = {
    primary: [".sidebar", ".topbar", ".status-pill", ".sidebar-btn", ".os-sidebar", "#plantHeader", ".plant-topbar"],
    accent:  ["canvas", ".mini-chart"],
    bg:      [".layout", ".plant-card", ".main", ".os-board", "body"],
    alarm:   [".tk-badge", ".alarms-btn", ".alarm-badge", '[class*="alarm"]'],
    logo:    [".sidebar-logo", ".os-sidebar-logo", ".logo-area"],
    name:    ["title"],
  };

  const PANEL_CSS = `
  :root{--brd-z-dock:11000;--brd-z-flash:10990;}
  .brd-dock{position:fixed;top:0;right:0;height:100dvh;width:min(392px,100vw);
    z-index:var(--brd-z-dock);display:flex;flex-direction:column;
    background:rgba(7,13,10,.97);border-left:1px solid rgba(255,255,255,.09);
    box-shadow:-24px 0 64px rgba(0,0,0,.55);
    font-family:'Roboto','Segoe UI',sans-serif;color:#dfeee6;
    transform:translateX(100%);transition:transform .28s cubic-bezier(.22,1,.36,1);}
  .brd-dock.brd-open{transform:translateX(0);}
  .brd-head{display:flex;align-items:center;gap:10px;padding:16px 18px 12px;
    border-bottom:1px solid rgba(255,255,255,.07);flex-shrink:0;}
  .brd-head h3{margin:0;font-family:'Exo 2',sans-serif;font-size:.98rem;font-weight:700;
    letter-spacing:.02em;flex:1;}
  .brd-head .brd-live{font-family:'JetBrains Mono',monospace;font-size:.6rem;letter-spacing:.12em;
    padding:3px 8px;border-radius:999px;border:1px solid rgba(255,255,255,.14);opacity:.85;}
  .brd-live::before{content:"";display:inline-block;width:6px;height:6px;border-radius:50%;
    background:currentColor;margin-right:5px;animation:brd-blink 1.6s ease-in-out infinite;}
  @keyframes brd-blink{50%{opacity:.25}}
  .brd-x{background:none;border:none;color:inherit;font-size:1rem;cursor:pointer;
    width:34px;height:34px;border-radius:8px;}
  .brd-x:hover{background:rgba(255,255,255,.08);}
  .brd-body{flex:1;overflow-y:auto;padding:6px 18px 18px;scrollbar-width:thin;}
  .brd-sec{padding:14px 0 4px;}
  .brd-sec+.brd-sec{border-top:1px solid rgba(255,255,255,.06);}
  .brd-sec>h4{margin:0 0 2px;font-size:.78rem;font-weight:600;letter-spacing:.03em;}
  .brd-sec>.brd-hint{font-size:.68rem;opacity:.55;margin:0 0 10px;line-height:1.35;}
  .brd-row{display:flex;align-items:center;gap:12px;padding:7px 0;}
  .brd-row .brd-meta{flex:1;min-width:0;}
  .brd-row .brd-meta b{display:block;font-size:.76rem;font-weight:600;}
  .brd-row .brd-meta span{display:block;font-size:.65rem;opacity:.55;margin-top:1px;}
  .brd-swatch{position:relative;width:44px;height:32px;border-radius:8px;flex-shrink:0;
    border:1px solid rgba(255,255,255,.18);cursor:pointer;overflow:hidden;
    transition:transform .15s ease-out, box-shadow .15s ease-out;}
  .brd-swatch:hover{transform:scale(1.06);}
  .brd-swatch:focus-within{box-shadow:0 0 0 2px rgba(255,255,255,.35);}
  .brd-swatch input[type=color]{position:absolute;inset:-6px;width:auto;height:auto;
    border:none;padding:0;cursor:pointer;}
  .brd-hex{width:76px;flex-shrink:0;background:rgba(0,0,0,.35);color:inherit;
    border:1px solid rgba(255,255,255,.12);border-radius:7px;padding:6px 7px;
    font-family:'JetBrains Mono',monospace;font-size:.68rem;text-transform:lowercase;}
  .brd-hex:focus{outline:none;border-color:rgba(255,255,255,.35);}
  .brd-themes{display:grid;grid-template-columns:repeat(4,1fr);gap:8px;margin:4px 0 10px;}
  .brd-theme{padding:0;border:1px solid rgba(255,255,255,.12);border-radius:10px;overflow:hidden;
    background:rgba(0,0,0,.25);color:inherit;font-family:inherit;cursor:pointer;text-align:center;
    transition:border-color .15s ease-out, transform .15s ease-out;}
  .brd-theme:hover{transform:translateY(-2px);border-color:rgba(255,255,255,.35);}
  .brd-theme.brd-on{border-color:rgba(255,255,255,.65);box-shadow:0 0 0 1px rgba(255,255,255,.35);}
  .brd-theme .brd-th-prev{display:block;height:34px;position:relative;}
  .brd-theme .brd-th-prev::after{content:"";position:absolute;left:8px;right:26px;bottom:6px;height:3px;
    border-radius:2px;background:rgba(255,255,255,.55);}
  .brd-theme .brd-th-dot{position:absolute;right:7px;bottom:4px;width:8px;height:8px;border-radius:50%;}
  .brd-theme .brd-th-name{display:block;font-size:.6rem;padding:4px 2px 5px;opacity:.85;
    white-space:nowrap;overflow:hidden;text-overflow:ellipsis;}
  .brd-field{margin:8px 0;}
  .brd-field label{display:block;font-size:.66rem;opacity:.7;margin-bottom:4px;letter-spacing:.03em;}
  .brd-field input[type=text],.brd-field input[type=number]{width:100%;box-sizing:border-box;
    background:rgba(0,0,0,.35);border:1px solid rgba(255,255,255,.12);border-radius:8px;
    color:inherit;padding:8px 10px;font-size:.78rem;font-family:inherit;}
  .brd-field input:focus{outline:none;border-color:rgba(255,255,255,.35);}
  .brd-logo-row{display:flex;align-items:center;gap:12px;}
  .brd-logo-prev{width:52px;height:52px;object-fit:contain;border-radius:10px;flex-shrink:0;
    border:1px dashed rgba(255,255,255,.22);background:rgba(0,0,0,.3);padding:4px;}
  .brd-upl{flex:1;display:inline-flex;align-items:center;justify-content:center;gap:7px;
    padding:9px 10px;border-radius:8px;border:1px solid rgba(255,255,255,.14);cursor:pointer;
    font-size:.72rem;background:rgba(0,0,0,.25);transition:border-color .15s ease-out;}
  .brd-upl:hover{border-color:rgba(255,255,255,.32);}
  .brd-check{display:flex;align-items:center;gap:9px;font-size:.74rem;padding:7px 0;cursor:pointer;}
  .brd-check input{accent-color:#8ab4a0;width:15px;height:15px;}
  .brd-scope{display:flex;border:1px solid rgba(255,255,255,.14);border-radius:9px;overflow:hidden;margin:4px 0 2px;}
  .brd-scope button{flex:1;padding:8px 6px;border:none;background:transparent;color:inherit;
    font-size:.68rem;font-family:inherit;cursor:pointer;opacity:.6;transition:all .15s ease-out;}
  .brd-scope button.brd-on{opacity:1;background:rgba(255,255,255,.10);font-weight:600;}
  .brd-layouts{display:grid;grid-template-columns:1fr 1fr;gap:8px;}
  .brd-layout{display:flex;flex-direction:column;gap:6px;padding:10px;cursor:pointer;text-align:left;
    border:1px solid rgba(255,255,255,.12);border-radius:10px;background:rgba(0,0,0,.25);
    color:inherit;font-family:inherit;transition:border-color .15s ease-out, transform .15s ease-out;}
  .brd-layout:hover{transform:translateY(-1px);border-color:rgba(255,255,255,.3);}
  .brd-layout.brd-on{border-color:rgba(255,255,255,.55);background:rgba(255,255,255,.07);}
  .brd-layout b{font-size:.72rem;display:flex;align-items:center;gap:6px;}
  .brd-layout b .brd-lyk{width:8px;height:8px;border-radius:50%;border:1px solid rgba(255,255,255,.4);}
  .brd-layout.brd-on b .brd-lyk{background:#fff;}
  .brd-layout span{font-size:.6rem;opacity:.55;line-height:1.3;}
  .brd-restore{background:none;border:none;color:#ff9d9d;opacity:.75;font-size:.68rem;cursor:pointer;
    padding:8px 0;font-family:inherit;text-decoration:underline;text-underline-offset:3px;}
  .brd-restore:hover{opacity:1;}
  .brd-foot{flex-shrink:0;border-top:1px solid rgba(255,255,255,.08);padding:12px 18px
    calc(12px + env(safe-area-inset-bottom));background:rgba(0,0,0,.25);}
  .brd-status{font-size:.64rem;opacity:.6;margin-bottom:8px;min-height:1em;
    font-family:'JetBrains Mono',monospace;}
  .brd-actions{display:flex;gap:8px;}
  .brd-btn{flex:1;padding:10px;border-radius:9px;cursor:pointer;font-size:.76rem;font-family:inherit;
    border:1px solid rgba(255,255,255,.15);background:rgba(0,0,0,.25);color:inherit;
    transition:transform .12s ease-out, filter .15s ease-out;}
  .brd-btn:hover{filter:brightness(1.2);}
  .brd-btn:active{transform:scale(.97);}
  .brd-btn[disabled]{opacity:.45;cursor:default;}
  .brd-btn.brd-primary{background:#e8f5ee;color:#0a1710;font-weight:700;border:none;}
  .brd-btn.brd-ghost{flex:0 0 auto;padding:10px 12px;}
  .brd-flash{box-shadow:0 0 0 2px rgba(255,255,255,.85), 0 0 22px 4px rgba(255,255,255,.35) !important;
    transition:box-shadow .5s ease-out !important;}
  @media (max-width:640px){
    .brd-dock{top:auto;bottom:0;height:auto;max-height:78dvh;width:100vw;
      border-left:none;border-top:1px solid rgba(255,255,255,.12);border-radius:16px 16px 0 0;
      transform:translateY(100%);}
    .brd-dock.brd-open{transform:translateY(0);}
  }
  @media (prefers-reduced-motion: reduce){
    .brd-dock{transition:none;}
    .brd-live::before{animation:none;}
    .brd-preset,.brd-swatch,.brd-btn{transition:none;}
  }`;

  function ensurePanelCss() {
    if (document.getElementById("brandingPanelCss")) return;
    const st = document.createElement("style");
    st.id = "brandingPanelCss";
    st.textContent = PANEL_CSS;
    document.head.appendChild(st);
  }

  function flash(kind) {
    const sels = AFFECTS[kind];
    if (!sels) return;
    const seen = [];
    for (const sel of sels) {
      try {
        document.querySelectorAll(sel).forEach((el, i) => {
          if (i > 5 || el.closest(".brd-dock")) return;
          seen.push(el);
        });
      } catch { }
    }
    seen.forEach((el) => el.classList.add("brd-flash"));
    setTimeout(() => seen.forEach((el) => el.classList.remove("brd-flash")), 750);
  }

  function fileToDataUrl(file, cb) {
    if (file.type === "image/svg+xml" && file.size < 300000) {
      const fr = new FileReader();
      fr.onload = () => cb(fr.result);
      fr.readAsDataURL(file);
      return;
    }
    const img = new Image();
    const url = URL.createObjectURL(file);
    img.onload = () => {
      const MAX = 512, scale = Math.min(1, MAX / Math.max(img.width, img.height));
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

  const isAdmin = () => {
    const u = getUser();
    return u.is_superuser === true || u.role_key === "admin_customer";
  };

  let _dock = null;

  function openPanel() {
    if (_dock) { closePanel(false); return; }
    ensurePanelCss();
    const u = getUser();
    const admin = isAdmin();
    const before = _effective ? JSON.parse(JSON.stringify(_effective)) : null; // p/ descartar
    const base = _effective || {};
    const c = base.colors || {};
    let draftLogo = base.logo_data || null;
    let scope = admin ? (readJson(keyUser()) ? "user" : "company") : "user";
    let dirty = false;

    const dock = document.createElement("aside");
    dock.className = "brd-dock";
    dock.setAttribute("role", "dialog");
    dock.setAttribute("aria-label", "Personalização da plataforma");
    dock.innerHTML = `
      <div class="brd-head">
        <h3><i class="fa-solid fa-palette"></i> Personalização</h3>
        <span class="brd-live">AO VIVO</span>
        <button class="brd-x" id="brdClose" title="Fechar (Esc)"><i class="fa-solid fa-xmark"></i></button>
      </div>
      <div class="brd-body">
        <div class="brd-sec">
          <h4>Onde aplicar</h4>
          ${admin ? `
          <div class="brd-scope" id="brdScope">
            <button data-s="company" class="${scope === "company" ? "brd-on" : ""}">Toda a empresa</button>
            <button data-s="user" class="${scope === "user" ? "brd-on" : ""}">Só para mim (meu usuário)</button>
          </div>
          ${u.is_superuser ? `<div class="brd-field" id="brdCustWrap"><label>Cliente (ID) — superusuário</label>
            <input type="number" id="brdCustomer" value="${u.customer_id || 1}" min="1"></div>` : ""}
          ` : `<p class="brd-hint" style="margin:2px 0 0;">Personalização pessoal — fica no seu usuário e vale em qualquer aparelho.
               O padrão da empresa continua para os demais.</p>`}
        </div>
        <div class="brd-sec">
          <h4>Identidade</h4>
          <div class="brd-field"><label>Nome da plataforma <i style="opacity:.5">(títulos e relatórios)</i></label>
            <input type="text" id="brdName" maxlength="60" placeholder="AIOTI Solar SCADA"
                   value="${(base.app_name || "").replace(/"/g, "&quot;")}"></div>
          <div class="brd-field"><label>Logo <i style="opacity:.5">(menu, login dos modais, tickets)</i></label>
            <div class="brd-logo-row">
              <img id="brdLogoPrev" class="brd-logo-prev" src="${draftLogo || "assets/logo-aioti.png"}" alt="logo">
              <label class="brd-upl"><i class="fa-solid fa-upload"></i> Enviar imagem
                <input type="file" id="brdLogoFile" hidden accept="image/png,image/jpeg,image/svg+xml"></label>
            </div></div>
        </div>
        <div class="brd-sec">
          <h4>Temas</h4>
          <p class="brd-hint">Combinações prontas de cores que conversam entre si, com degradê de fundo — escolha uma e ajuste o que quiser por cima.</p>
          <div class="brd-themes" id="brdThemes">
            ${THEMES.map((t, i) => `
              <button class="brd-theme" data-i="${i}" title="${t.name}">
                <span class="brd-th-prev" style="background:linear-gradient(140deg, ${t.bg} 0%, ${t.bg2} 55%, ${t.primary} 140%);">
                  <span class="brd-th-dot" style="background:${t.accent}"></span></span>
                <span class="brd-th-name">${t.name}</span></button>`).join("")}
          </div>
          <div class="brd-row"><div class="brd-meta"><b>Cor principal</b><span>Menu, botões, indicadores e curvas dos gráficos</span></div>
            <input class="brd-hex" id="brdPrimaryHex" value="${c.primary || "#39e58c"}" maxlength="7" spellcheck="false">
            <span class="brd-swatch"><input type="color" id="brdPrimary" value="${c.primary || "#39e58c"}"></span></div>
          <div class="brd-row"><div class="brd-meta"><b>Destaque</b><span>Segunda cor das séries e realces</span></div>
            <input class="brd-hex" id="brdAccentHex" value="${c.accent || "#a8f060"}" maxlength="7" spellcheck="false">
            <span class="brd-swatch"><input type="color" id="brdAccent" value="${c.accent || "#a8f060"}"></span></div>
          <div class="brd-row"><div class="brd-meta"><b>Fundo</b><span>Tom base do plano de fundo e superfícies</span></div>
            <input class="brd-hex" id="brdBgHex" value="${c.bg || "#050e07"}" maxlength="7" spellcheck="false">
            <span class="brd-swatch"><input type="color" id="brdBg" value="${c.bg || "#050e07"}"></span></div>
          <div class="brd-row"><div class="brd-meta"><b>Fundo do degradê</b><span>Segunda cor do fundo — cria a transição</span></div>
            <input class="brd-hex" id="brdBg2Hex" value="${c.bg2 || "#04141c"}" maxlength="7" spellcheck="false">
            <span class="brd-swatch"><input type="color" id="brdBg2" value="${c.bg2 || "#04141c"}"></span></div>
          <div class="brd-row"><div class="brd-meta"><b>Alarme</b><span>Estados críticos e avisos vermelhos</span></div>
            <input class="brd-hex" id="brdAlarmHex" value="${c.alarm || "#ff4f4f"}" maxlength="7" spellcheck="false">
            <span class="brd-swatch"><input type="color" id="brdAlarm" value="${c.alarm || "#ff4f4f"}"></span></div>
        </div>
        <div class="brd-sec">
          <h4>Layout</h4>
          <p class="brd-hint">Estilos prontos — escolha um e edite as cores por cima como quiser.</p>
          <div class="brd-layouts" id="brdLayouts">
            ${Object.entries(LAYOUTS).map(([k, l]) => `
              <button class="brd-layout" data-l="${k}"><b><span class="brd-lyk"></span>${l.name}</b>
                <span>${l.desc}</span></button>`).join("")}
          </div>
        </div>
        <div class="brd-sec">
          <h4>Ajustes</h4>
          <label class="brd-check"><input type="checkbox" id="brdRobot" ${base.robot_skin === "neutral" ? "checked" : ""}>
            Robô assistente neutro (sem as cores AIOTI)</label>
          <div class="brd-field"><label>Rodapé dos relatórios PDF</label>
            <input type="text" id="brdPdf" maxlength="300" placeholder="${DEFAULT_PDF_FOOTER}"
                   value="${((base.texts || {}).pdf_footer || "").replace(/"/g, "&quot;")}"></div>
          <button class="brd-restore" id="brdReset">Restaurar padrão ${admin ? "AIOTI (toda a empresa)" : "da empresa"}</button>
        </div>
      </div>
      <div class="brd-foot">
        <div class="brd-status" id="brdStatus">pré-visualizando — nada salvo ainda</div>
        <div class="brd-actions">
          <button class="brd-btn brd-primary" id="brdSave"><i class="fa-solid fa-floppy-disk"></i> Salvar</button>
          <button class="brd-btn brd-ghost" id="brdUndo" title="Desfazer última alteração" disabled><i class="fa-solid fa-rotate-left"></i></button>
          <button class="brd-btn" id="brdDiscard">Descartar</button>
        </div>
      </div>`;
    document.body.appendChild(dock);
    _dock = dock;
    requestAnimationFrame(() => dock.classList.add("brd-open"));

    const $ = (id) => dock.querySelector("#" + id);

    let draftLayout = (base.colors || {}).layout || "default";
    const markLayout = () => dock.querySelectorAll(".brd-layout").forEach((b) =>
      b.classList.toggle("brd-on", b.dataset.l === draftLayout));
    markLayout();

    function draft() {
      return {
        app_name: $("brdName").value.trim() || null,
        logo_data: draftLogo,
        colors: {
          primary: $("brdPrimary").value, accent: $("brdAccent").value,
          bg: $("brdBg").value, bg2: $("brdBg2").value, alarm: $("brdAlarm").value,
          layout: draftLayout,
        },
        texts: $("brdPdf").value.trim() ? { pdf_footer: $("brdPdf").value.trim() } : {},
        robot_skin: $("brdRobot").checked ? "neutral" : "default",
      };
    }

    // histórico p/ Desfazer (agrupa mudanças num intervalo de 600ms)
    let hist = [], applied = JSON.stringify(draft()), lastPush = 0;
    function setInputs(d) {
      $("brdName").value = d.app_name || "";
      $("brdPdf").value = (d.texts && d.texts.pdf_footer) || "";
      $("brdRobot").checked = d.robot_skin === "neutral";
      const c = d.colors || {};
      [["Primary", c.primary], ["Accent", c.accent], ["Bg", c.bg], ["Bg2", c.bg2], ["Alarm", c.alarm]].forEach(([id, val]) => {
        if (val) { $("brd" + id).value = val; $("brd" + id + "Hex").value = val; }
      });
      draftLogo = d.logo_data || null;
      $("brdLogoPrev").src = draftLogo || "assets/logo-aioti.png";
      draftLayout = c.layout || "default";
      markLayout();
    }
    function preview(kind) {
      dirty = true;
      const now = Date.now();
      if (now - lastPush > 600) {
        hist.push(applied);
        if (hist.length > 40) hist.shift();
        lastPush = now;
        $("brdUndo").disabled = false;
      }
      applied = JSON.stringify(draft());
      $("brdStatus").textContent = "pré-visualizando — nada salvo ainda";
      applyBranding(draft());
      if (kind) flash(kind);
      try { markThemes(); } catch { /* ainda montando o painel */ }
    }
    $("brdUndo").addEventListener("click", () => {
      if (!hist.length) return;
      const d = JSON.parse(hist.pop());
      applied = JSON.stringify(d);
      setInputs(d);
      applyBranding(d);
      $("brdStatus").textContent = "alteração desfeita";
      $("brdUndo").disabled = hist.length === 0;
    });
    $("brdLayouts").addEventListener("click", (e) => {
      const b = e.target.closest(".brd-layout");
      if (!b) return;
      draftLayout = b.dataset.l;
      markLayout();
      preview("bg");
    });

    // swatch <-> hex sincronizados
    [["Primary", "primary"], ["Accent", "accent"], ["Bg", "bg"], ["Bg2", "bg"], ["Alarm", "alarm"]].forEach(([id, kind]) => {
      const sw = $("brd" + id), hx = $("brd" + id + "Hex");
      sw.addEventListener("input", () => { hx.value = sw.value; preview(kind); });
      hx.addEventListener("change", () => {
        if (/^#[0-9a-fA-F]{6}$/.test(hx.value.trim())) { sw.value = hx.value.trim(); preview(kind); }
        else hx.value = sw.value;
      });
    });
    $("brdName").addEventListener("input", () => preview("name"));
    $("brdPdf").addEventListener("input", () => preview());
    $("brdRobot").addEventListener("change", () => preview());
    const markThemes = () => {
      const d = draft().colors;
      dock.querySelectorAll(".brd-theme").forEach((b) => {
        const t = THEMES[+b.dataset.i];
        b.classList.toggle("brd-on",
          t.primary === d.primary && t.accent === d.accent && t.bg === d.bg && t.bg2 === d.bg2);
      });
    };
    markThemes();
    $("brdThemes").addEventListener("click", (e) => {
      const btn = e.target.closest(".brd-theme");
      if (!btn) return;
      const t = THEMES[+btn.dataset.i];
      [["Primary", t.primary], ["Accent", t.accent], ["Bg", t.bg], ["Bg2", t.bg2], ["Alarm", t.alarm]].forEach(([id, val]) => {
        $("brd" + id).value = val; $("brd" + id + "Hex").value = val;
      });
      preview("primary"); flash("bg");
      markThemes();
    });
    $("brdLogoFile").addEventListener("change", (e) => {
      const f = e.target.files && e.target.files[0];
      if (!f) return;
      fileToDataUrl(f, (dataUrl) => {
        if (dataUrl.length > 500000) { alert("Logo muito grande mesmo comprimida — use uma imagem menor."); return; }
        draftLogo = dataUrl;
        $("brdLogoPrev").src = dataUrl;
        preview("logo");
      });
    });
    const scopeEl = $("brdScope");
    if (scopeEl) scopeEl.addEventListener("click", (e) => {
      const b = e.target.closest("button[data-s]");
      if (!b) return;
      scope = b.dataset.s;
      scopeEl.querySelectorAll("button").forEach((x) => x.classList.toggle("brd-on", x === b));
      const cw = $("brdCustWrap");
      if (cw) cw.style.display = scope === "company" ? "" : "none";
    });

    function closePanel_(revert) {
      dock.classList.remove("brd-open");
      document.removeEventListener("keydown", onKey);
      setTimeout(() => { dock.remove(); if (_dock === dock) _dock = null; }, 300);
      if (revert) applyBranding(before || effectiveSaved());
    }
    window.__brdClose = closePanel_;
    const onKey = (e) => { if (e.key === "Escape") closePanel_(dirty); };
    document.addEventListener("keydown", onKey);
    $("brdClose").addEventListener("click", () => closePanel_(dirty));
    $("brdDiscard").addEventListener("click", () => closePanel_(true));

    $("brdSave").addEventListener("click", () => {
      const body = draft();
      $("brdSave").disabled = true;
      $("brdStatus").textContent = "salvando…";
      if (scope === "user") {
        // cache local primeiro (boot sem flash e funciona mesmo offline)…
        localStorage.setItem(keyUser(), JSON.stringify(body));
        applyBranding(body);
        dirty = false;
        // …e persiste no banco: o tema segue o usuário em qualquer aparelho
        fetch(API_BASE + "/branding", {
          method: "POST",
          headers: authHeaders({ "Content-Type": "application/json" }),
          body: JSON.stringify(Object.assign({ scope: "user" }, body)),
        })
          .then((r) => {
            $("brdStatus").textContent = r.ok
              ? "salvo ✓ — no seu usuário, vale em qualquer aparelho"
              : "salvo neste aparelho ✓ — não sincronizou com o servidor";
          })
          .catch(() => {
            $("brdStatus").textContent = "salvo neste aparelho ✓ — sem conexão com o servidor";
          })
          .finally(() => { $("brdSave").disabled = false; });
        return;
      }
      if (u.is_superuser && $("brdCustomer")) body.customer_id = parseInt($("brdCustomer").value, 10);
      fetch(API_BASE + "/branding", {
        method: "POST",
        headers: authHeaders({ "Content-Type": "application/json" }),
        body: JSON.stringify(body),
      })
        .then((r) => r.json().then((j) => ({ ok: r.ok, j })))
        .then(({ ok, j }) => {
          if (!ok) throw new Error((j && j.error) || "erro ao salvar");
          const mine = !body.customer_id || body.customer_id === u.customer_id;
          if (mine) {
            localStorage.setItem(keyCompany(), JSON.stringify(body));
            localStorage.removeItem(keyUser());
            // tema da empresa passa a valer pro admin: apaga o pessoal dele
            // também no banco (senão o pessoal antigo volta em outro aparelho)
            fetch(API_BASE + "/branding", {
              method: "POST",
              headers: authHeaders({ "Content-Type": "application/json" }),
              body: JSON.stringify({ scope: "user", reset: true }),
            }).catch(() => { });
          }
          $("brdStatus").textContent = mine ? "salvo ✓ — vale para toda a empresa"
                                            : "salvo ✓ no cliente " + body.customer_id;
          dirty = false;
          if (!mine) applyBranding(effectiveSaved());
        })
        .catch((err) => { $("brdStatus").textContent = "não salvou: " + err.message; })
        .finally(() => { $("brdSave").disabled = false; });
    });

    // pós-restauração: painel tem que ESPELHAR o estado restaurado, senão os
    // inputs seguem com o tema antigo e qualquer mexida re-aplica ele inteiro
    function syncPanelTo(b) {
      const t0 = THEMES[0]; // AIOTI padrão
      setInputs(b || { app_name: null, logo_data: null, robot_skin: "default", texts: {},
        colors: { primary: t0.primary, accent: t0.accent, bg: t0.bg, bg2: t0.bg2, alarm: t0.alarm, layout: "default" } });
      applied = JSON.stringify(draft());
      hist = [];
      $("brdUndo").disabled = true;
      try { markThemes(); } catch { }
    }

    $("brdReset").addEventListener("click", () => {
      if (scope === "user") {
        localStorage.removeItem(keyUser());
        // apaga também no banco (senão volta no próximo login/aparelho)
        fetch(API_BASE + "/branding", {
          method: "POST",
          headers: authHeaders({ "Content-Type": "application/json" }),
          body: JSON.stringify({ scope: "user", reset: true }),
        }).catch(() => { });
        const company = readJson(keyCompany());
        applyBranding(company);
        syncPanelTo(company);
        $("brdStatus").textContent = "voltou ao padrão da empresa";
        dirty = false;
        return;
      }
      if (!confirm("Restaurar o tema padrão AIOTI para todos os usuários da empresa?")) return;
      const body = { reset: true };
      if (u.is_superuser && $("brdCustomer")) body.customer_id = parseInt($("brdCustomer").value, 10);
      fetch(API_BASE + "/branding", {
        method: "POST",
        headers: authHeaders({ "Content-Type": "application/json" }),
        body: JSON.stringify(body),
      })
        .then((r) => { if (!r.ok) throw new Error("erro"); })
        .then(() => {
          localStorage.removeItem(keyCompany());
          const mine = readJson(keyUser());
          applyBranding(mine);
          syncPanelTo(mine);
          $("brdStatus").textContent = mine
            ? "tema da empresa restaurado — sua personalização pessoal continua valendo"
            : "tema padrão AIOTI restaurado";
          dirty = false;
        })
        .catch(() => { $("brdStatus").textContent = "não consegui restaurar — tente de novo"; });
    });
  }
  function closePanel(revert) {
    if (window.__brdClose) window.__brdClose(revert);
  }

  function injectSidebarButton() {
    const u = getUser();
    if (!u.username) return;
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
  window.__openBrandingPanel = openPanel; // acesso via console/atalho em outras páginas

  /* ---------- start ---------- */
  boot();
  if (document.readyState === "loading") document.addEventListener("DOMContentLoaded", injectSidebarButton);
  else injectSidebarButton();
})();
