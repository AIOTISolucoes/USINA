const CACHE_NAME = "aioti-v2";

self.addEventListener("install", (e) => {
  const base = self.registration.scope;
  const SHELL = [
    "resumo.html",
    "plant.html",
    "os.html",
    "index.html",
    "css/style.css",
    "css/layout.css",
    "css/plant.css",
    "css/login.css",
    "os.css",
    "js/app.js",
    "js/plant.js",
    "js/login.js",
    "js/pwa.js",
    "manifest.json",
  ].map((f) => new URL(f, base).href);

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
  const base = self.registration.scope;
  let data = { title: "AIOTI Solar", body: "Nova notificação", url: "resumo.html" };
  try {
    data = Object.assign(data, e.data.json());
  } catch (_) {}

  const iconUrl = new URL("img/icon-192.png", base).href;
  const clickUrl = data.url.startsWith("http") ? data.url : new URL(data.url, base).href;

  e.waitUntil(
    self.registration.showNotification(data.title, {
      body: data.body,
      icon: iconUrl,
      badge: iconUrl,
      tag: data.tag || "aioti-default",
      data: { url: clickUrl },
      vibrate: [200, 100, 200],
      requireInteraction: data.priority === "high",
    })
  );
});

self.addEventListener("notificationclick", (e) => {
  e.notification.close();
  const target = e.notification.data?.url || new URL("resumo.html", self.registration.scope).href;
  e.waitUntil(
    clients.matchAll({ type: "window", includeUncontrolled: true }).then((list) => {
      for (const c of list) {
        if (c.url === target && "focus" in c) return c.focus();
      }
      return clients.openWindow(target);
    })
  );
});
