/* ======================================================================
   NOTIFY SOUND — sons de notificação in-app da plataforma AIOTI
   Gera os sons via WebAudio (sem arquivos de áudio; funciona offline).
   Respeita a política de autoplay (destrava no primeiro toque/clique —
   obrigatório no iOS e Android) e tem preferência de mudo persistida.
   API global: NotifySound.play("critical"|"warning"|"info"|"ticket"),
               NotifySound.enabled(), NotifySound.setEnabled(bool)
   ====================================================================== */
(function () {
  "use strict";

  const PREF_KEY = "aioti_sound_enabled"; // "1" (default) | "0"
  let audioCtx = null;

  function enabled() {
    return localStorage.getItem(PREF_KEY) !== "0";
  }

  function setEnabled(on) {
    localStorage.setItem(PREF_KEY, on ? "1" : "0");
    updateMuteBtn();
    if (on) play("info"); // feedback imediato ao reativar
  }

  function ensureCtx() {
    if (!audioCtx) {
      try {
        audioCtx = new (window.AudioContext || window.webkitAudioContext)();
      } catch (_) { return null; }
    }
    return audioCtx;
  }

  // iOS/Android exigem gesto do usuário antes de tocar áudio
  function unlock() {
    const c = ensureCtx();
    if (c && c.state === "suspended") c.resume().catch(() => {});
  }
  document.addEventListener("pointerdown", unlock, { once: true });

  function tone(freq, at, dur, vol, type) {
    const c = ensureCtx();
    if (!c) return;
    const osc = c.createOscillator();
    const gain = c.createGain();
    osc.type = type || "sine";
    osc.frequency.value = freq;
    gain.gain.setValueAtTime(0.0001, at);
    gain.gain.exponentialRampToValueAtTime(vol, at + 0.015);
    gain.gain.exponentialRampToValueAtTime(0.0001, at + dur);
    osc.connect(gain);
    gain.connect(c.destination);
    osc.start(at);
    osc.stop(at + dur + 0.05);
  }

  function play(kind) {
    if (!enabled()) return;
    const c = ensureCtx();
    if (!c) return;
    if (c.state === "suspended") { c.resume().catch(() => {}); }
    if (c.state !== "running") return; // sem gesto ainda — não força

    const t = c.currentTime + 0.02;
    switch (kind) {
      case "critical": // tri-tom urgente descendo e voltando
        tone(880, t, 0.16, 0.20, "square");
        tone(622, t + 0.18, 0.16, 0.20, "square");
        tone(880, t + 0.36, 0.24, 0.22, "square");
        break;
      case "warning": // dois tons médios
        tone(740, t, 0.14, 0.16);
        tone(587, t + 0.17, 0.20, 0.16);
        break;
      case "ticket": // arpejo (mesmo espírito do som antigo de tickets)
        tone(880, t, 0.28, 0.14);
        tone(1108.73, t + 0.12, 0.28, 0.14);
        tone(1318.51, t + 0.24, 0.30, 0.14);
        break;
      default: // "info": ding suave ascendente
        tone(587, t, 0.10, 0.12);
        tone(880, t + 0.11, 0.18, 0.12);
    }
  }

  // ── Botão de mudo no painel de notificações (se existir na página) ──
  function updateMuteBtn() {
    const btn = document.getElementById("notifSoundToggle");
    if (!btn) return;
    const on = enabled();
    btn.innerHTML = on ? '<i class="fa-solid fa-volume-high"></i>' : '<i class="fa-solid fa-volume-xmark"></i>';
    btn.title = on ? "Sons de notificação: ligados (clique para silenciar)" : "Sons de notificação: desligados (clique para ativar)";
    btn.style.opacity = on ? "0.9" : "0.45";
  }

  function injectMuteBtn() {
    if (document.getElementById("notifSoundToggle")) return;
    const closeBtn = document.getElementById("notifPanelClose");
    if (!closeBtn || !closeBtn.parentElement) return;
    const btn = document.createElement("button");
    btn.id = "notifSoundToggle";
    btn.type = "button";
    btn.style.cssText = "background:none;border:none;color:#39e58c;cursor:pointer;font-size:13px;padding:2px 6px;margin-right:4px;transition:opacity .2s;";
    btn.addEventListener("click", (e) => {
      e.stopPropagation();
      setEnabled(!enabled());
    });
    closeBtn.parentElement.insertBefore(btn, closeBtn);
    updateMuteBtn();
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", injectMuteBtn);
  } else {
    injectMuteBtn();
  }

  window.NotifySound = { play, enabled, setEnabled };
})();
