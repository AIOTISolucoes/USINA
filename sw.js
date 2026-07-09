const CACHE_NAME = "aioti-v2";
const SHELL = [
  "/resumo.html",
  "/plant.html",
  "/os.html",
  "/index.html",
  "/css/style.css",
  "/css/layout.css",
  "/css/plant.css",
  "/css/login.css",
  "/os.css",
  "/js/app.js",
  "/js/plant.js",
  "/js/login.js",
  "/js/pwa.js",
  "/js/help_tour.js",
  "/js/notify_sound.js",
  "/manifest.json",
];

self.addEventListener("install", (e) => {
  e.waitUntil(
    caches.open(CACHE_NAME).then((c) => c.addAll(SHELL))
  );
  self.skipWaiting();
});

self.addEventListener("activate", (e) => {
  e.waitUntil(
    caches.keys().then((keys) =>
      Promise.all(keys.filter((k) => k !== CACHE_NAME).map((k) => caches.delete(k)))
    )
  );
  self.clients.claim();
});

self.addEventListener("fetch", (e) => {
  if (e.request.method !== "GET") return;
  const url = new URL(e.request.url);
  if (url.origin !== location.origin) return;

  e.respondWith(
    fetch(e.request)
      .then((res) => {
        const clone = res.clone();
        caches.open(CACHE_NAME).then((c) => c.put(e.request, clone));
        return res;
      })
      .catch(() => caches.match(e.request))
  );
});

// ── Push Notifications ──
self.addEventListener("push", (e) => {
  let data = { title: "AIOTI Solar", body: "Nova notificação", url: "/resumo.html" };
  try {
    data = Object.assign(data, e.data.json());
  } catch (_) {}

  e.waitUntil(
    self.registration.showNotification(data.title, {
      body: data.body,
      icon: "/img/icon-192.png",
      badge: "/img/icon-192.png",
      tag: data.tag || "aioti-default",
      data: { url: data.url },
      vibrate: [200, 100, 200, 100, 300],
      silent: false,          // garante o som padrão do sistema (Android/iOS)
      renotify: true,         // mesma tag volta a tocar/vibrar em alarme repetido
      requireInteraction: data.priority === "high",
    })
  );
});

self.addEventListener("notificationclick", (e) => {
  e.notification.close();
  const target = e.notification.data?.url || "/resumo.html";
  e.waitUntil(
    clients.matchAll({ type: "window", includeUncontrolled: true }).then((list) => {
      for (const c of list) {
        if (new URL(c.url).pathname === target && "focus" in c) return c.focus();
      }
      return clients.openWindow(target);
    })
  );
});
