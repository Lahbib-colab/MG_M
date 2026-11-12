const V = "mg-media-v41";
// Fichiers INDISPENSABLES : tout ou rien. S'il en manque un, la nouvelle version n'est pas installée (l'ancienne continue de marcher)
// et elle est retentée à la vérification suivante.
const CORE = ["./", "./index.html", "./sync.js", "./player.js", "./manifest.webmanifest"];
// Fichiers FACULTATIFS (icônes, images de fond) : gardés s'ils sont là ; leur absence ne bloque jamais une mise à jour.
const EXTRA = ["./icons/icon-192.png", "./icons/icon-512.png", "./icons/maskable-512.png", "./icons/logo-mg.png", "./icons/maskable-192.png", "./icons/monochrome-512.png", "./icons/apple-touch-icon.png", "./icons/favicon-48.png", "./icons/shortcut-96.png", "./icons/bg-habitacle.webp", "./icons/bg-habitacle-flou.webp"];
const get = u => fetch(new Request(u, { cache: "reload" })).then(r => { if (!r.ok) throw new Error(u + " " + r.status); return r; });
// Les fichiers sont toujours redemandés au réseau (cache: "reload") : sinon un hébergement qui garde les fichiers quelques minutes
// (GitHub Pages : 10 min) ferait copier d'anciens fichiers dans le cache de la nouvelle version.
// Contrôle de cohérence : index.html doit porter le même numéro de build que ce sw.js, sinon une ancienne page serait figée
// dans le nouveau cache (et la mise à jour serait proposée à chaque ouverture). Dans ce cas l'installation échoue et sera retentée.
self.addEventListener("install", e => e.waitUntil((async () => {
  const c = await caches.open(V);
  try {
    const core = await Promise.all(CORE.map(async u => [u, await get(u)]));
    const want = +V.match(/(\d+)$/)[1], html = await core.find(x => x[0] === "./index.html")[1].clone().text(), m = html.match(/APP_BUILD=(\d+)/);
    if (!m || +m[1] !== want) throw new Error("index.html est en build " + (m ? m[1] : "?") + " au lieu de " + want);
    for (const [u, r] of core) await c.put(u, r);
    await Promise.all(EXTRA.map(u => get(u).then(r => c.put(u, r)).catch(() => {})));
  } catch (err) { await caches.delete(V); throw err; }
  await self.skipWaiting();
})()));
self.addEventListener("activate", e => { e.waitUntil(caches.keys().then(k => Promise.all(k.filter(x => x !== V).map(x => caches.delete(x)))).then(() => self.clients.claim())); });
self.addEventListener("fetch", e => {
  const r = e.request, u = new URL(r.url);
  if (r.method !== "GET" || u.origin !== location.origin) return;
  if (u.pathname.endsWith("/sw.js") || u.searchParams.has("_chk")) return; // toujours depuis le réseau : détection des nouvelles versions et diagnostic
  e.respondWith(caches.match(r, { ignoreSearch: true }).then(hit => hit || fetch(r).then(res => {
    const copy = res.clone(); caches.open(V).then(c => c.put(r, copy)); return res;
  }).catch(() => caches.match("./index.html"))));
});
