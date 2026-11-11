const V = "mg-media-v34";
const SHELL = ["./", "./index.html", "./manifest.webmanifest", "./icons/icon-192.png", "./icons/icon-512.png", "./icons/maskable-512.png", "./icons/logo-mg.png", "./sync.js", "./icons/maskable-192.png", "./icons/monochrome-512.png", "./icons/apple-touch-icon.png", "./icons/favicon-48.png", "./icons/shortcut-96.png", "./icons/bg-habitacle.webp", "./icons/bg-habitacle-flou.webp"];
// Les fichiers sont toujours redemandés au réseau (cache: "reload") : sinon un hébergement qui garde les fichiers quelques minutes
// (GitHub Pages : 10 min) ferait copier d'anciens fichiers dans le cache de la nouvelle version.
self.addEventListener("install", e => { e.waitUntil(caches.open(V).then(c => Promise.all(SHELL.map(u => fetch(new Request(u, { cache: "reload" })).then(r => { if (!r.ok) throw new Error(u + " " + r.status); return c.put(u, r); })))).then(() => self.skipWaiting())); });
self.addEventListener("activate", e => { e.waitUntil(caches.keys().then(k => Promise.all(k.filter(x => x !== V).map(x => caches.delete(x)))).then(() => self.clients.claim())); });
self.addEventListener("fetch", e => {
  const r = e.request;
  if (r.method !== "GET" || new URL(r.url).origin !== location.origin) return;
  if (new URL(r.url).pathname.endsWith("/sw.js")) return; // toujours depuis le réseau : sert à détecter les nouvelles versions
  e.respondWith(caches.match(r, { ignoreSearch: true }).then(hit => hit || fetch(r).then(res => {
    const copy = res.clone(); caches.open(V).then(c => c.put(r, copy)); return res;
  }).catch(() => caches.match("./index.html"))));
});
