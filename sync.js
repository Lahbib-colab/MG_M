/*
 * MG Divertissement – module de synchronisation double tablette (v1.0)
 * Module INDÉPENDANT : il observe les lecteurs existants (#vid, #aud) par addEventListener,
 * ne remplace aucun gestionnaire, ne modifie pas la base IndexedDB « mg-media » (hors ajout d'un
 * média reçu, via la fonction existante dbPut) et n'utilise que des clés « mg.sync.* » et la base « mg-sync ».
 * Le volume n'est JAMAIS transmis : aucun message du protocole ne le prévoit.
 */
(function () {
  "use strict";
  if (typeof vid === "undefined" || typeof aud === "undefined" || typeof dbAll === "undefined" || typeof modal === "undefined") {
    console.warn("[MGSync] application hôte introuvable : module désactivé");
    return;
  }
  const VER = "1.0";
  const $q = s => document.querySelector(s);
  const sleep = ms => new Promise(r => setTimeout(r, ms));
  const nowL = () => performance.timeOrigin + performance.now();
  const jget = (k, d) => { try { const v = JSON.parse(localStorage.getItem(k)); return v == null ? d : v; } catch { return d; } };
  const jset = (k, v) => { try { localStorage.setItem(k, JSON.stringify(v)); } catch { /* quota */ } };
  const uid = () => (crypto.randomUUID ? crypto.randomUUID() : "d" + Math.random().toString(16).slice(2) + Date.now().toString(16));
  const E = s => String(s).replace(/[&<>"]/g, c => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;" }[c]));
  const hex = u8 => [...u8].map(b => b.toString(16).padStart(2, "0")).join("");
  const sha = async buf => new Uint8Array(await crypto.subtle.digest("SHA-256", buf));
  const CH = 262144, WIN = 16;

  /* ---------------------------------------------------------------- configuration locale du module */
  const C = Object.assign({ url: "", id: "", token: "", name: "", auto: true, follow: true, allowShare: true, calib: 0, indicator: true }, jget("mg.sync.cfg", {}));
  if (!C.id) C.id = uid();
  const saveC = () => jset("mg.sync.cfg", C);
  /* Relais du serveur (/proxy) pour l'extraction de liens : adresse, identifiant et jeton de cette tablette, ou null si elle n'est pas appairée. */
  window.MGSYNC_PROXY = () => (C.url && C.token && C.id) ? { base: C.url.replace(/^wss:/i, "https:").replace(/^ws:/i, "http:").replace(/\/sync\/?$/, "/proxy"), id: C.id, token: C.token } : null;
  saveC();
  const myName = () => C.name || "Tablette " + C.id.slice(0, 4);

  /* ---------------------------------------------------------------- journal (200 dernières entrées) */
  const LOG = jget("mg.sync.log", []);
  let logT = 0;
  function log(l, m, d) {
    LOG.push({ t: new Date().toISOString(), l, m, d }); if (LOG.length > 200) LOG.shift();
    clearTimeout(logT); logT = setTimeout(() => jset("mg.sync.log", LOG), 800);
    if (l === "err") console.warn("[MGSync]", m, d || "");
  }

  /* ---------------------------------------------------------------- état */
  const S = {
    status: "off", ws: null, want: false, tries: 0, offset: 0, rtt: 0, jitter: 0, lost: 0, samples: [], pend: new Map(), pingN: 0,
    master: null, peers: [], mode: "independent", shared: true, session: null, anc: null, el: null, item: null, sessionFp: null,
    applying: 0, hold: 0, lastAnchor: 0, lastDrift: 0, fastUntil: 0, syncing: false, drift: 0, peerDrift: null, lastSeek: 0, lastKick: 0, seekLag: 150, pendingOpen: false, blocked: false,
    peerLib: [], xf: new Map(), maxMB: 1024, pairCode: "", hint: ""
  };
  const srv = () => nowL() + S.offset;
  const me = () => S.peers.find(p => p.id === C.id);
  const peer = () => S.peers.find(p => p.id !== C.id);
  const isMaster = () => S.master === C.id;
  const syncActive = () => S.status === "online" && S.mode === "synced" && C.follow;
  // Événements provoqués par le module lui-même : chacun est « attendu » puis consommé, pour ne jamais confondre une action de l'utilisateur avec une action du module.
  const ex = { play: [], pause: [], seeked: [] };
  const expect = (...evs) => { const t = nowL() + 2500; for (const e of evs) ex[e].push(t); };
  const consume = ev => { const q = ex[ev], n = nowL(); while (q.length && q[0] < n) q.shift(); if (q.length) { q.shift(); return true; } return false; };
  const doPlay = el => { if (el.paused) expect("play"); return el.play(); };
  const doPause = el => { if (!el.paused) expect("pause"); el.pause(); };
  const doSeek = (el, pos) => { if (isFinite(pos) && Math.abs(el.currentTime - pos) > 0.001) { expect("seeked"); el.currentTime = Math.max(0, pos); } };

  /* ---------------------------------------------------------------- base « mg-sync » (empreintes, morceaux de transfert) */
  let _sdb;
  const sdb = () => _sdb || (_sdb = new Promise((ok, ko) => {
    const r = indexedDB.open("mg-sync", 1);
    r.onupgradeneeded = () => { const d = r.result; d.createObjectStore("fp"); d.createObjectStore("chunks"); d.createObjectStore("meta"); };
    r.onsuccess = () => ok(r.result); r.onerror = () => ko(r.error);
  }));
  const sop = async (st, mode, fn) => {
    const d = await sdb();
    return new Promise((ok, ko) => { const t = d.transaction(st, mode), s = t.objectStore(st); let res; const q = fn(s); if (q) q.onsuccess = () => res = q.result; t.oncomplete = () => ok(res); t.onerror = () => ko(t.error); t.onabort = () => ko(t.error); });
  };
  const sget = (st, k) => sop(st, "readonly", s => s.get(k));
  const sput = (st, k, v) => sop(st, "readwrite", s => s.put(v, k));
  const sdel = (st, k) => sop(st, "readwrite", s => s.delete(k));
  const skeys = (st, range) => sop(st, "readonly", s => s.getAllKeys(range));
  const sclear = (st, range) => sop(st, "readwrite", s => s.delete(range));

  /* ---------------------------------------------------------------- identité des contenus (empreinte) */
  const fpCache = new Map();
  let INDEX = new Map(), INV = [], invRev = "", pubRev = "";
  async function fpBlob(b) {
    const sz = b.size, A = new Uint8Array(await b.slice(0, CH).arrayBuffer()), B = sz > CH ? new Uint8Array(await b.slice(Math.max(CH, sz - CH)).arrayBuffer()) : new Uint8Array(0);
    const H = new TextEncoder().encode(sz + "|"), all = new Uint8Array(H.length + A.length + B.length);
    all.set(H); all.set(A, H.length); all.set(B, H.length + A.length);
    return hex(await sha(all)).slice(0, 40);
  }
  async function fpOf(it) {
    const key = it.id + "|" + it.blob.size + "|" + it.name;
    if (fpCache.has(key)) return fpCache.get(key);
    let f = await sget("fp", key).catch(() => null);
    if (!f) { f = await fpBlob(it.blob); sput("fp", key, f).catch(() => { }); }
    fpCache.set(key, f); return f;
  }
  async function scanLib() {
    const out = [], idx = new Map();
    for (const c of ["films", "musique", "livres"]) {
      let l = []; try { l = await dbAll(c); } catch { /* base indisponible */ }
      for (const it of l) { if (!it.blob) continue; /* flux en ligne : pas de fichier à partager */ const f = await fpOf(it); idx.set(f, it); out.push({ fp: f, cat: c, name: it.name, size: it.blob.size }); }
    }
    INDEX = idx; INV = out;
    invRev = hex(await sha(new TextEncoder().encode(out.map(x => x.fp).sort().join()))).slice(0, 12);
    return out;
  }
  async function findLocal(fp) { let it = INDEX.get(fp); if (!it) { await scanLib(); it = INDEX.get(fp); } return it || null; }
  async function publishLib(force) {
    if (S.status !== "online") return;
    await scanLib();
    if (!force && invRev === pubRev) return;
    pubRev = invRev; send({ t: "lib", rev: invRev, items: INV });
  }

  /* ---------------------------------------------------------------- connexion */
  function send(o) { const w = S.ws; if (w && w.readyState === 1) { w.send(JSON.stringify(o)); return true; } return false; }
  const cmd = (op, a) => send({ t: "cmd", op, a: a || {} });
  function setStatus(s) { S.status = s; render(); indicator(); }

  function connect(code) {
    if (S.ws || !C.url) { if (!C.url) toast("Indiquez l’adresse du serveur dans le panneau Synchronisation"); return; }
    S.want = true; S.pairCode = code || ""; setStatus("connecting");
    let ws;
    try { ws = new WebSocket(C.url); } catch (e) { log("err", "Adresse invalide", String(e)); S.hint = "Adresse invalide"; S.want = false; setStatus("off"); return; }
    ws.binaryType = "arraybuffer"; S.ws = ws; S.hint = "";
    ws.onopen = () => {
      log("info", "Socket ouverte", C.url);
      if (!C.token) send({ t: "pair", id: C.id, name: myName(), code: S.pairCode }); else send({ t: "hello", id: C.id, token: C.token, name: myName(), v: 1 });
    };
    ws.onmessage = ev => { try { if (typeof ev.data === "string") onText(JSON.parse(ev.data)); else onBin(ev.data); } catch (e) { log("err", "Message illisible", String(e)); } };
    ws.onerror = () => { S.hint = /^wss:/.test(C.url) ? "Connexion impossible. Vérifiez l’adresse, le serveur et le certificat (ca.crt installé sur la tablette)." : "Connexion impossible. Vérifiez l’adresse et le serveur."; };
    ws.onclose = () => {
      const was = S.status; S.ws = null; clearInterval(S.pingT); S.pend.clear();
      S.peers = S.peers.map(p => p.id === C.id ? p : { ...p, online: false });
      if (S.want) { setStatus("connecting"); const d = Math.min(10000, 500 * 2 ** S.tries) + Math.random() * 300; S.tries++; log("warn", "Connexion perdue, nouvel essai", Math.round(d)); setTimeout(() => { if (S.want && !S.ws) connect(); }, d); }
      else setStatus("off");
      if (was === "online") toast("Synchronisation : connexion perdue, lecture locale conservée");
    };
  }
  function disconnect() { S.want = false; clearTimeout(S.retry); if (S.ws) { try { S.ws.close(); } catch { /* ignore */ } } S.ws = null; setStatus("off"); }

  /* ---------------------------------------------------------------- horloge commune (type NTP) */
  function startPings() {
    clearInterval(S.pingT); S.samples = []; S.pingN = 0;
    const tick = () => {
      if (S.pend.size > 3) { log("warn", "Pings sans réponse"); try { S.ws && S.ws.close(); } catch { /* ignore */ } return; }
      const c = nowL(); S.pend.set(c, 1); send({ t: "ping", c, r: Math.round(S.rtt) }); S.pingN++;
    };
    tick(); let n = 0;
    S.pingT = setInterval(() => { tick(); if (++n === 8) { clearInterval(S.pingT); S.pingT = setInterval(tick, 2000); } }, 200);
  }
  function onPong(m) {
    S.pend.delete(m.c);
    const t1 = nowL(), rtt = t1 - m.c, off = m.s + rtt / 2 - t1;
    S.samples.push({ rtt, off }); if (S.samples.length > 12) S.samples.shift();
    const best = Math.min(...S.samples.map(x => x.rtt));
    if (S.samples.length < 3 || Math.abs(S.offset) < 1e-9) S.offset = S.samples.reduce((a, x) => a + x.off, 0) / S.samples.length;
    else if (rtt <= best * 1.5 + 2) S.offset = S.offset * 0.7 + off * 0.3;
    const r = S.samples.map(x => x.rtt).sort((a, b) => a - b); S.rtt = r[r.length >> 1];
    S.jitter = r.reduce((a, x) => a + Math.abs(x - S.rtt), 0) / r.length;
    if (S.pingN % 3 === 0) render();
  }
  function quality() {
    if (S.status !== "online") return { bars: 0, txt: "Hors ligne" };
    if (S.rtt < 20 && S.jitter < 8) return { bars: 4, txt: "Excellente" };
    if (S.rtt < 45 && S.jitter < 20) return { bars: 3, txt: "Bonne" };
    if (S.rtt < 100) return { bars: 2, txt: "Moyenne" };
    return { bars: 1, txt: "Faible" };
  }

  /* ---------------------------------------------------------------- messages du serveur */
  function onText(m) {
    switch (m.t) {
      case "paired": C.token = m.token; saveC(); toast("Tablette appairée"); log("info", "Appairage réussi"); return;
      case "err":
        log("err", "Erreur serveur", m);
        if (m.code === "unauthorized") { C.token = ""; saveC(); S.want = false; S.hint = "Cette tablette n’est plus appairée. Reconnectez-la avec le code affiché par le serveur."; try { S.ws.close(); } catch { /* ignore */ } }
        else if (m.code === "code" || m.code === "full" || m.code === "rate") { S.want = false; S.hint = m.msg || "Erreur"; toast(S.hint); try { S.ws.close(); } catch { /* ignore */ } }
        render(); return;
      case "welcome": return onWelcome(m);
      case "pong": return onPong(m);
      case "presence": return onPresence(m);
      case "mode": return onMode(m);
      case "open": return onOpen(m);
      case "start": return onStart(m);
      case "play": return onPlay(m);
      case "pause": return onPause(m);
      case "seek": return onSeek(m);
      case "rate": if (S.anc) { S.anc.pos = m.pos; S.anc.at = m.at; S.anc.rate = m.rate; } return;
      case "anchor": if (S.anc && syncActive()) { S.anc.pos = m.pos; S.anc.at = m.at; S.anc.playing = m.playing; } return;
      case "queue": if (S.session) S.session.queue = m.queue; return;
      case "stop": return onStop();
      case "state": return applyState(m.session);
      case "peerdrift": S.peerDrift = m.d; render(); return;
      case "peerlib": S.peerLib = m.items || []; render(); return;
      case "game": if (window.MGSync && typeof window.MGSync.onGame === "function") window.MGSync.onGame(m.g, m.from); return;
      case "ack": return;
      case "nack": log("warn", "Commande refusée", m); if (!/conflit/.test(m.reason || "")) toast("Synchronisation : " + (m.reason || "commande refusée")); if (syncActive()) setTimeout(() => send({ t: "getstate" }), 450); return;
      case "xfer:req": case "xfer:ok": case "xfer:go": case "xfer:ack": case "xfer:end": case "xfer:deny": case "xfer:abort": case "xfer:done": case "xfer:offer": return onXfer(m);
      default: return;
    }
  }
  function onWelcome(m) {
    S.tries = 0; S.master = m.master; S.mode = m.session.mode; S.shared = m.session.shared; S.session = m.session; S.maxMB = m.maxTransferMB || 1024;
    S.offset = m.time - nowL(); setStatus("online"); startPings();
    log("info", "Connecté", { master: m.master, mode: S.mode });
    send({ t: "follow", v: C.follow });
    publishLib(true).then(() => { if (S.mode === "synced") rejoinOrPublish(); });
  }
  function onPresence(m) {
    const prevMaster = S.master;
    S.peers = m.devices; S.master = m.master; S.mode = m.mode; S.shared = m.shared;
    if (prevMaster && prevMaster !== m.master) toast(isMaster() ? "Cette tablette est maintenant la tablette maître" : "L’autre tablette est maintenant maître");
    render(); indicator();
  }
  function onMode(m) {
    S.mode = m.mode; S.session = m.session; render(); indicator();
    if (m.mode === "synced") rejoinOrPublish();
    else { S.sessionFp = null; S.anc = null; }
    toast(m.mode === "synced" ? "Mode synchronisé activé" : "Mode indépendant : chaque tablette est libre");
  }
  async function rejoinOrPublish() {
    const s = S.session;
    if (s && s.content) { send({ t: "getstate" }); return; }
    const cp = currentPlay();
    if (cp && !cp.el.paused) sendOpen(cp.it, await fpOf(cp.it), cp.el);
  }

  /* ---------------------------------------------------------------- lecteurs : accès et utilitaires */
  const vOpen = () => { const o = $q("#ovV"); return !!(o && o.classList.contains("on")); };
  function itemOf(el) { if (el === vid) { const f = vOpen() && typeof curFilm !== "undefined" ? curFilm : null; return f && f.blob ? f : null; /* un flux en ligne (m3u8) n'a pas de fichier : il n'est pas synchronisé */ } return typeof cur !== "undefined" && aud.src ? cur : null; }
  function currentPlay() { const v = itemOf(vid); if (v) return { el: vid, it: v }; const a = itemOf(aud); if (a && !aud.paused) return { el: aud, it: a }; return null; }
  const kindOf = it => (META[it.cat].ty === "video" ? "video" : "audio");
  const once = (el, ev, ms) => new Promise(r => { const h = () => { el.removeEventListener(ev, h); r(true); }; el.addEventListener(ev, h); setTimeout(() => { el.removeEventListener(ev, h); r(false); }, ms || 5000); });
  async function seekTo(el, pos) {
    if (!isFinite(pos) || pos < 0) pos = 0;
    if (Math.abs(el.currentTime - pos) < 0.03) return;
    const t0 = nowL(); expect("seeked");
    const p = once(el, "seeked", 4000); el.currentTime = pos; await p;
    S.seekLag = S.seekLag * 0.7 + (nowL() - t0) * 0.3;
  }
  function runAt(sat, fn) {
    const target = sat - S.offset;
    (function tick() { const r = target - nowL(); if (r > 16) return void setTimeout(tick, r - 12); while (target - nowL() > 0.5) { /* attente active < 12 ms */ } fn(Math.max(0, nowL() - target)); })();
  }
  function blocked() {
    if (S.blocked) return; S.blocked = true; log("warn", "Lecture automatique bloquée par le navigateur");
    modal(`<h3>Rejoindre la lecture</h3><p class="msub">Touchez le bouton pour démarrer la lecture synchronisée sur cette tablette.</p><div class="mact"><button class="btn" data-go>Rejoindre</button><button class="btn ghost" data-close>Plus tard</button></div>`, m => {
      const prev = m.parentElement.onclick; m.parentElement.onclick = e => { prev(e); if (e.target.closest("[data-go]")) { S.blocked = false; send({ t: "getstate" }); } if (e.target.closest("[data-close]")) S.blocked = false; };
    });
  }

  /* ---------------------------------------------------------------- ouverture d'un contenu (commune) */
  async function loadPaused(it, pos) {
    S.applying++;
    try {
      const kind = kindOf(it), el = kind === "video" ? vid : aud, cp = itemOf(el);
      const same = cp && cp.id === it.id && el.src && (kind === "audio" || vOpen());
      el.muted = true; S.el = el; S.item = it;
      if (!same) {
        if (kind === "audio" && vOpen()) { history.back(); await sleep(250); }
        if (kind === "video" && !aud.paused) aud.pause();
        openItem(it);
        await once(el, "loadedmetadata", 8000);
      }
      el.pause(); await seekTo(el, pos || 0);
      if (el.readyState < 3) await once(el, "canplay", 4000);
      setTimeout(() => { if (S.el === el && el.muted && !(S.anc && S.anc.playing)) el.muted = false; }, 14000);
    } finally { S.applying--; }
  }
  async function onOpen(m) {
    S.session = { ...(S.session || {}), content: m.content, queue: m.queue, playing: false, pos: m.pos };
    S.sessionFp = m.content.fp; S.pendingOpen = true; S.anc = null; render();
    if (!syncActive()) { S.pendingOpen = false; send({ t: "ready", seq: m.seq, have: false, reason: "détachée" }); return; }
    const it = await findLocal(m.content.fp);
    if (!it || !vis(it)) { S.pendingOpen = false; send({ t: "ready", seq: m.seq, have: false, reason: !it ? "absent" : "interdit" }); if (!it) promptMissing(m.content); render(); return; }
    try { await loadPaused(it, m.pos || 0); send({ t: "ready", seq: m.seq, have: true }); }
    catch (e) { log("err", "Chargement impossible", String(e)); send({ t: "ready", seq: m.seq, have: false, reason: "erreur" }); S.pendingOpen = false; }
  }
  function onStart(m) {
    S.pendingOpen = false; render();
    if (!syncActive() || !S.el || (m.missing || []).includes(C.id)) return;
    S.anc = { pos: m.pos, at: m.at, playing: m.play !== false, rate: 1 };
    const el = S.el;
    if (!S.anc.playing) { el.muted = false; return; }
    S.fastUntil = nowL() + 7000;
    runAt(m.at, late => {
      el.muted = false;
      if (late > 30 || C.calib) doSeek(el, m.pos + late / 1000 + C.calib / 1000);
      doPlay(el).catch(blocked);
    });
  }
  async function onPlay(m) {
    if (!syncActive() || !S.el) return;
    S.hold = 0;
    S.anc = { pos: m.pos, at: m.at, playing: true, rate: m.rate || 1 };
    const el = S.el; doPause(el);
    await seekTo(el, m.pos + C.calib / 1000); S.fastUntil = nowL() + 7000;
    runAt(m.at, late => { el.muted = false; if (late > 30) doSeek(el, m.pos + late / 1000 + C.calib / 1000); doPlay(el).catch(blocked); });
  }
  function onPause(m) {
    if (!syncActive() || !S.el) return;
    S.hold = 0;
    S.anc = { pos: m.pos, at: m.at, playing: false, rate: 1 };
    const el = S.el;
    runAt(m.at, () => { doPause(el); doSeek(el, m.pos); });
  }
  function onSeek(m) { if (!syncActive() || !S.el) return; S.hold = 0; S.anc = { pos: m.pos, at: srv(), playing: false, rate: 1 }; doSeek(S.el, m.pos); }
  function onStop() {
    if (!syncActive()) return;
    S.sessionFp = null; S.anc = null;
    if (S.el === vid && vOpen()) history.back(); else if (S.el === aud) doPause(aud);
    S.el = null; render();
  }
  async function applyState(ses) {
    S.session = ses;
    if (!syncActive() || !ses || !ses.content) return;
    const it = await findLocal(ses.content.fp);
    if (!it || !vis(it)) { promptMissing(ses.content); return; }
    S.sessionFp = ses.content.fp; S.syncing = true; render();
    const exp = ses.playing ? ses.pos + (srv() - ses.posAt) / 1000 * ses.rate : ses.pos;
    const el = kindOf(it) === "video" ? vid : aud, cp = itemOf(el), have = cp && cp.id === it.id && el.src && (el === aud || vOpen());
    if (!have) await loadPaused(it, exp); else { S.el = el; S.item = it; }
    S.anc = { pos: ses.pos, at: ses.posAt, playing: ses.playing, rate: ses.rate };
    if (ses.playing) {
      const T = srv() + 700; S.applying++;
      try { await seekTo(el, ses.pos + (T - ses.posAt) / 1000 * ses.rate + C.calib / 1000); } finally { S.applying--; }
      runAt(T, () => { el.muted = false; doPlay(el).catch(blocked); });
    } else { el.muted = false; doPause(el); await seekTo(el, ses.pos); }
  }

  /* ---------------------------------------------------------------- actions locales -> commandes */
  async function sendOpen(it, fp, el) {
    const q = INV.filter(x => x.cat === it.cat).sort((a, b) => a.name.localeCompare(b.name, "fr", { numeric: true })).map(x => x.fp).slice(0, 500);
    cmd("open", { fp, cat: it.cat, name: it.name, size: it.blob.size, pos: el.currentTime, queue: q });
  }
  async function onLocal(el, ev) {
    if (consume(ev)) return;
    if (!syncActive() || S.applying) return;
    if (ev === "seeked" || ev === "play" || ev === "pause") S.hold = nowL() + 2500;
    const it = itemOf(el); if (!it) return;
    if (ev === "pause" && (el.ended || el.readyState === 0)) return;
    let fp; try { fp = await fpOf(it); } catch { return; }
    if (ev === "play") { if (fp !== S.sessionFp) return sendOpen(it, fp, el); return cmd("play", { pos: el.currentTime - C.calib / 1000 }); }
    if (fp !== S.sessionFp) return;
    if (ev === "pause") return cmd("pause", { pos: el.currentTime - C.calib / 1000 });
    if (ev === "seeked") { clearTimeout(S.seekT); S.seekT = setTimeout(() => { if (!syncActive() || S.applying) return; return el.paused ? cmd("seek", { pos: el.currentTime }) : cmd("play", { pos: el.currentTime - C.calib / 1000 }); }, 300); }
  }
  for (const el of [vid, aud]) for (const ev of ["play", "pause", "seeked"]) el.addEventListener(ev, () => onLocal(el, ev));
  vid.addEventListener("ended", () => { if (syncActive() && S.el === vid && isMaster()) log("info", "Fin de lecture"); });

  /* ---------------------------------------------------------------- correction continue de la dérive */
  setInterval(() => {
    if (!syncActive() || !S.el || S.applying || isMaster() || nowL() < S.hold) return;
    const a = S.anc, el = S.el; if (!a || !a.playing) { if (S.syncing) { S.syncing = false; render(); } return; }
    if (el.paused) { if (nowL() - S.lastKick > 3000 && !S.blocked && el.readyState >= 2) { S.lastKick = nowL(); doPlay(el).catch(blocked); } return; }
    if (el.seeking || el.readyState < 3) { S.syncing = true; return; }
    const exp = a.pos + (srv() - a.at) / 1000 * a.rate + C.calib / 1000, d = el.currentTime - exp, ad = Math.abs(d);
    S.drift = S.drift * 0.6 + d * 1000 * 0.4;
    const was = S.syncing;
    const fast = nowL() < S.fastUntil;
    if (ad > (fast ? 0.25 : 0.7)) { if (nowL() - S.lastSeek > (fast ? 1200 : 2500)) { S.lastSeek = nowL(); doSeek(el, exp + S.seekLag / 1000 + 0.05); S.syncing = true; log("warn", "Repositionnement", { ecart: Math.round(d * 1000) }); } }
    else if (ad > 0.04) { S.syncing = true; el.playbackRate = a.rate * (1 + Math.max(-0.08, Math.min(0.08, -d * 0.8))); }
    else { if (el.playbackRate !== a.rate) el.playbackRate = a.rate; S.syncing = false; }
    if (was !== S.syncing) { render(); indicator(); }
  }, 250);
  setInterval(() => {
    if (!syncActive() || S.status !== "online") return;
    const n = nowL();
    if (isMaster() && S.el && !S.el.paused && S.anc && S.anc.playing && !S.applying && n - S.lastAnchor > (n < S.fastUntil ? 400 : 1900)) { S.lastAnchor = n; send({ t: "anchor", pos: S.el.currentTime - C.calib / 1000, at: srv(), playing: true }); }
    if (!isMaster() && S.anc && S.anc.playing && n - S.lastDrift > 1900) { S.lastDrift = n; send({ t: "drift", d: S.drift }); }
  }, 200);

  /* ---------------------------------------------------------------- contenu absent / bibliothèque commune */
  function promptMissing(content) {
    if (S.missingShown === content.fp) return; S.missingShown = content.fp;
    const p = peer(), has = (S.peerLib || []).some(x => x.fp === content.fp);
    modal(`<h3>Contenu absent</h3><p class="msub">« ${E(content.name.replace(/\.[^.]+$/, ""))} » n’est pas sur cette tablette. Cette tablette reste en lecture indépendante pour ce contenu.</p>${has ? `<div class="mact"><button class="btn" data-ask>Demander à l’autre tablette</button><button class="btn ghost" data-close>Plus tard</button></div>` : `<div class="mact"><button class="btn ghost" data-close>Fermer</button></div>`}`, m => {
      const prev = m.parentElement.onclick; m.parentElement.onclick = e => { prev(e); if (e.target.closest("[data-ask]")) { closeModal(); requestXfer(content.fp); } };
    });
    log("warn", "Contenu absent", content.name);
  }
  const libDiff = () => {
    const mine = new Set(INV.map(x => x.fp)), theirs = new Set((S.peerLib || []).map(x => x.fp));
    return { common: INV.filter(x => theirs.has(x.fp)).length, onlyHere: INV.filter(x => !theirs.has(x.fp)), onlyPeer: (S.peerLib || []).filter(x => !mine.has(x.fp) && x.cat !== "photos") };
  };

  /* ---------------------------------------------------------------- transferts de fichiers */
  const rid = () => crypto.getRandomValues(new Uint32Array(1))[0];
  function toBuf(...arrs) { let n = 0; for (const a of arrs) n += a.length; const o = new Uint8Array(n); let p = 0; for (const a of arrs) { o.set(a, p); p += a.length; } return o; }
  async function requestXfer(fp) {
    const p = peer(), meta = (S.peerLib || []).find(x => x.fp === fp);
    if (!p || !p.online || !meta) return toast("L’autre tablette n’est pas disponible");
    if (INDEX.has(fp)) return toast("Ce contenu est déjà présent sur cette tablette");
    if (meta.size > S.maxMB * 1048576) return toast("Fichier trop volumineux (maximum " + S.maxMB + " Mo)");
    try { const est = await navigator.storage.estimate(); if (est.quota && est.usage + meta.size * 2 > est.quota) return toast("Espace de stockage insuffisant"); } catch { /* estimation indisponible */ }
    const tid = rid();
    S.xf.set(tid, { tid, dir: "in", fp, name: meta.name, cat: meta.cat, size: meta.size, to: p.id, got: 0, total: Math.ceil(meta.size / CH), state: "demande" });
    send({ t: "xfer:req", tid, to: p.id, fp, name: meta.name, cat: meta.cat, size: meta.size }); render();
  }
  function offerXfer(fp) {
    const p = peer(), it = INV.find(x => x.fp === fp); if (!p || !p.online || !it) return toast("L’autre tablette n’est pas disponible");
    send({ t: "xfer:offer", tid: 0, to: p.id, fp, name: it.name, cat: it.cat, size: it.size }); toast("Proposition envoyée à l’autre tablette");
  }
  async function onXfer(m) {
    const tid = m.tid >>> 0;
    if (m.t === "xfer:offer") {
      modal(`<h3>Contenu proposé</h3><p class="msub">L’autre tablette propose « ${E(m.name.replace(/\.[^.]+$/, ""))} » (${sizeOf(m.size)}). Voulez-vous le recevoir ?</p><div class="mact"><button class="btn" data-yes>Recevoir</button><button class="btn ghost" data-close>Refuser</button></div>`, mm => {
        const prev = mm.parentElement.onclick; mm.parentElement.onclick = e => { prev(e); if (e.target.closest("[data-yes]")) { closeModal(); S.peerLib = S.peerLib.some(x => x.fp === m.fp) ? S.peerLib : [...S.peerLib, { fp: m.fp, cat: m.cat, name: m.name, size: m.size }]; requestXfer(m.fp); } };
      }); return;
    }
    if (m.t === "xfer:req") {                                  // nous sommes l'émetteur
      const it = C.allowShare ? INDEX.get(m.fp) : null;
      if (!C.allowShare) return send({ t: "xfer:deny", tid, to: m.from, why: "Le partage de fichiers est désactivé sur l’autre tablette" });
      if (!it) return send({ t: "xfer:deny", tid, to: m.from, why: "Fichier introuvable sur l’autre tablette" });
      S.xf.set(tid, { tid, dir: "out", it, fp: m.fp, name: m.name, size: it.blob.size, to: m.from, total: Math.ceil(it.blob.size / CH), got: 0, state: "attente" });
      send({ t: "xfer:ok", tid, to: m.from, size: it.blob.size }); render(); return;
    }
    const x = S.xf.get(tid); if (!x) return;
    if (m.t === "xfer:ok" && x.dir === "in") {                 // nous sommes le récepteur : reprise éventuelle
      const keys = await skeys("chunks", IDBKeyRange.bound(tid + ":", tid + ":\uffff")).catch(() => []); const have = new Set(keys.map(k => +String(k).split(":")[1]));
      let start = 0; while (have.has(start)) start++;
      x.got = start; x.state = "réception"; x.hashes = []; await sput("meta", "x" + tid, { fp: x.fp, name: x.name, cat: x.cat, size: x.size, to: x.to }).catch(() => { });
      send({ t: "xfer:go", tid, to: x.to, start }); render(); return;
    }
    if (m.t === "xfer:go" && x.dir === "out") { x.state = "envoi"; sendFile(x, m.start || 0); render(); return; }
    if (m.t === "xfer:ack" && x.dir === "out") { x.acked = Math.max(x.acked || -1, m.i); render(); return; }
    if (m.t === "xfer:end" && x.dir === "in") return finishXfer(x, m);
    if (m.t === "xfer:done" && x.dir === "out") { x.state = "terminé"; S.xf.delete(tid); toast("Transfert terminé"); render(); return; }
    if (m.t === "xfer:deny" || m.t === "xfer:abort") { x.state = "interrompu"; x.err = m.why || "Interrompu"; x.abort = true; log("warn", "Transfert interrompu", m.why); toast(x.err); render(); return; }
  }
  async function sendFile(x, start) {
    const size = x.it.blob.size, hashes = []; x.acked = start - 1;
    try {
      for (let i = 0; i < x.total && !x.abort; i++) {
        const buf = await x.it.blob.slice(i * CH, Math.min(size, (i + 1) * CH)).arrayBuffer(), h = await sha(buf); hashes.push(h);
        if (i < start) continue;
        while (!x.abort && (i - x.acked > WIN || (S.ws && S.ws.bufferedAmount > 8e6))) await sleep(15);
        if (x.abort) break;
        if (!S.ws || S.ws.readyState !== 1) { x.abort = true; x.err = "Connexion perdue"; break; }
        const f = new Uint8Array(41 + buf.byteLength), dv = new DataView(f.buffer); f[0] = 1; dv.setUint32(1, x.tid); dv.setUint32(5, i); f.set(h, 9); f.set(new Uint8Array(buf), 41);
        S.ws.send(f.buffer); x.got = i + 1; if (i % 8 === 0) render();
      }
      if (!x.abort) send({ t: "xfer:end", tid: x.tid, to: x.to, total: x.total, digest: hex(await sha(toBuf(...hashes))) });
      else { x.state = "interrompu"; render(); }
    } catch (e) { x.abort = true; x.state = "interrompu"; x.err = String(e); log("err", "Envoi", String(e)); render(); }
  }
  async function onBin(ab) {
    const u = new Uint8Array(ab); if (u.length < 41 || u[0] !== 1) return;
    const dv = new DataView(ab), tid = dv.getUint32(1), idx = dv.getUint32(5), h = hex(u.subarray(9, 41)), data = ab.slice(41), x = S.xf.get(tid);
    if (!x || x.dir !== "in") return;
    const real = hex(await sha(data));
    if (real !== h) { x.abort = true; x.state = "interrompu"; x.err = "Contrôle d’intégrité échoué (morceau " + idx + ")"; send({ t: "xfer:abort", tid, to: x.to, why: x.err }); log("err", x.err); render(); return; }
    await sput("chunks", tid + ":" + idx, { b: new Blob([data]), h }).catch(e => { x.abort = true; log("err", "Stockage", String(e)); });
    x.got = Math.max(x.got, idx + 1);
    if (idx % 4 === 3 || x.got >= x.total) send({ t: "xfer:ack", tid, to: x.to, i: idx });
    if (idx % 8 === 0) render();
  }
  async function finishXfer(x, m) {
    x.state = "vérification"; render();
    try {
      const recs = []; for (let i = 0; i < x.total; i++) { const r = await sget("chunks", x.tid + ":" + i); if (!r) throw new Error("morceau " + i + " manquant"); recs.push(r); }
      const digest = hex(await sha(toBuf(...recs.map(r => { const a = new Uint8Array(32); for (let k = 0; k < 32; k++) a[k] = parseInt(r.h.substr(k * 2, 2), 16); return a; }))));
      if (digest !== m.digest) throw new Error("empreinte globale différente");
      const final = new Blob(recs.map(r => r.b), { type: "" });
      if (final.size !== x.size) throw new Error("taille inattendue");
      if ((await fpBlob(final)) !== x.fp) throw new Error("le fichier reçu ne correspond pas à la demande");
      const mime = x.cat === "films" ? (/\.mp4$/i.test(x.name) ? "video/mp4" : "video/webm") : "";
      const file = mime ? new Blob([final], { type: mime }) : final;
      const id = await dbPut({ cat: x.cat, name: x.name, blob: file, type: file.type });
      if (META[x.cat].ty === "audio" && typeof extractAudio === "function") await extractAudio(id, file, null, x.name).catch(() => { });
      if (META[x.cat].ty === "book" && typeof importBookMeta === "function") await importBookMeta(id, file).catch(() => { });
      await sclear("chunks", IDBKeyRange.bound(x.tid + ":", x.tid + ":\uffff")).catch(() => { }); await sdel("meta", "x" + x.tid).catch(() => { });
      send({ t: "xfer:done", tid: x.tid, to: x.to }); x.state = "terminé"; S.xf.delete(x.tid);
      await publishLib(true); toast("Transfert terminé : " + x.name.replace(/\.[^.]+$/, ""));
      try { if (typeof counts === "function") counts(); const lib = $q("#v-lib"); if (lib && lib.classList.contains("on") && typeof renderLib === "function") renderLib(); } catch { /* interface non critique */ }
      if (S.session && S.session.content && S.session.content.fp === x.fp) send({ t: "getstate" });
    } catch (e) { x.state = "interrompu"; x.err = "Vérification échouée : " + e.message; log("err", x.err); send({ t: "xfer:abort", tid: x.tid, to: x.to, why: x.err }); toast(x.err); }
    render();
  }

  /* ---------------------------------------------------------------- panneau dans les Réglages */
  const css = document.createElement("style"); css.id = "mgs-css";
  css.textContent = `
  .mgs-dot{width:14px;height:14px;border-radius:50%;display:block}.mgs-gn{background:#34d399;box-shadow:0 0 10px #34d39980}.mgs-or{background:#fbbf24;box-shadow:0 0 10px #fbbf2480}.mgs-rd{background:#f87171;box-shadow:0 0 10px #f8717180}.mgs-bl{background:#60a5fa;box-shadow:0 0 10px #60a5fa80}
  .mgs-chip{display:inline-flex;align-items:center;gap:6px;padding:4px 12px;border-radius:999px;font-size:14px;background:rgba(96,165,250,.18);color:#60a5fa;border:1px solid rgba(96,165,250,.4);white-space:nowrap}
  .mgs-bars{display:inline-flex;gap:3px;align-items:flex-end;height:18px}.mgs-bars i{width:5px;border-radius:2px;background:var(--track)}.mgs-bars i.on{background:#34d399}
  .mgs-act{display:flex;flex-wrap:wrap;gap:8px;padding:12px 0;border-top:1px solid var(--line)}.mgs-act .btn{padding:.55em 1.1em;font-size:15px}
  .mgs-in{background:var(--chip);color:var(--txt);border:1px solid var(--line);border-radius:12px;padding:10px 12px;font-size:16px;min-width:0;width:100%}
  .mgs-list{padding:8px 0;border-top:1px solid var(--line)}.mgs-li{display:flex;align-items:center;gap:10px;padding:6px 0;font-size:15px}.mgs-li span{flex:1;min-width:0;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
  .mgs-pg{height:6px;border-radius:6px;background:var(--track);overflow:hidden;margin-top:4px}.mgs-pg i{display:block;height:100%;background:var(--acc2)}
  .mgs-ind{position:absolute;top:max(2vh,12px);left:2.4vw;z-index:4;display:flex;align-items:center;gap:8px;padding:7px 14px;border-radius:999px;background:rgba(0,0,0,.55);color:#fff;font-size:14px;border:1px solid rgba(255,255,255,.18);backdrop-filter:blur(8px);cursor:pointer}
  .mini .mgs-ind{position:static;background:var(--chip);color:var(--txt);border-color:var(--line);flex:none}
  .mgs-ind i{width:9px;height:9px;border-radius:50%}`;
  document.head.appendChild(css);

  const colOf = c => "mgs-" + c;
  function stat() {
    if (S.status === "off") return { txt: "Déconnecté", col: "rd", sub: S.hint || "Aucune connexion au serveur de synchronisation" };
    if (S.status === "connecting") return { txt: "Connexion en cours…", col: "or", sub: S.hint || C.url };
    const p = peer();
    if (S.mode === "synced" && C.follow && (S.syncing || S.pendingOpen)) return { txt: "Synchronisation en cours…", col: "or", sub: "Alignement des lectures" };
    if (!p || !p.online) return { txt: "Connecté, l’autre tablette est absente", col: "or", sub: "En attente de l’autre tablette" };
    return { txt: "Connecté et synchronisé", col: "gn", sub: S.mode === "synced" ? "Les deux tablettes partagent la même lecture" : "Chaque tablette est libre" };
  }
  const msTxt = v => (v == null || !isFinite(v) ? "—" : "± " + Math.round(Math.abs(v)) + " ms");
  function panelHTML() {
    const st = stat(), q = quality(), p = peer(), m = me(), D = libDiff();
    const bars = [1, 2, 3, 4].map(i => `<i class="${i <= q.bars ? "on" : ""}" style="height:${5 + i * 3}px"></i>`).join("");
    const SIc = k => (typeof SI !== "undefined" && SI[k]) || "", Ic = k => (typeof ICON !== "undefined" && ICON[k]) || "";
    const WIFI = '<svg class="ico" viewBox="0 0 24 24"><path d="M2.5 9a14 14 0 0 1 19 0M5.5 12.5a9.5 9.5 0 0 1 13 0M8.6 16a5 5 0 0 1 6.8 0"/><circle cx="12" cy="19.2" r=".6" fill="currentColor"/></svg>';
    const icFor = l => ({ "Autre tablette": Ic("duo"), "Qualité de connexion": WIFI, "Décalage de lecture estimé": Ic("clock"), "Mode": SIc("ref"), "Suivre la session": SIc("pl"), "Contrôle partagé": Ic("duo"), "Compensation audio de cette tablette": SIc("vol"), "Partage de fichiers": SIc("dl"), "Indicateur dans le lecteur": SIc("spark"), "Bibliothèque commune": SIc("db") })[l] || SIc("info");
    const row = (ic, l, sub, ctl, st2) => `<div class="srow${ic || icFor(l) ? "" : " ni"}"${st2 ? ` style="${st2}"` : ""}>${ic || icFor(l) ? `<span class="si">${ic || icFor(l)}</span>` : ""}<div class="sl"><b>${l}</b>${sub ? `<small>${sub}</small>` : ""}</div><div class="sctl${/class="seg"|type="range"|<input|<\/button>\s*<button/.test(ctl || "") ? " wide" : ""}">${ctl || ""}</div></div>`;
    const sw = (k, on) => `<button class="sw ${on ? "on" : ""}" data-sy="${k}" role="switch" aria-checked="${!!on}"></button>`;
    const dot = c => `<i class="mgs-dot ${colOf(c)}"></i>`;
    const roleTxt = id => (id === S.master ? "Maître" : "Secondaire");
    const xfers = [...S.xf.values()].map(x => `<div class="mgs-li"><div style="flex:1;min-width:0"><div style="display:flex;gap:8px"><span>${x.dir === "in" ? "Réception" : "Envoi"} · ${E(x.name.replace(/\.[^.]+$/, ""))}</span><span style="flex:none;color:var(--mut)">${x.state}${x.err ? " – " + E(x.err) : ""}</span></div><div class="mgs-pg"><i style="width:${x.total ? Math.min(100, x.got / x.total * 100) : 0}%"></i></div></div><button class="btn ghost" data-sy="cancel:${x.tid}" style="padding:.4em .9em;font-size:14px">${x.state === "interrompu" ? "Fermer" : "Annuler"}</button></div>`).join("");
    const miss = D.onlyPeer.slice(0, 6).map(x => `<div class="mgs-li"><span>${E(x.name.replace(/\.[^.]+$/, ""))}</span><button class="btn ghost" data-sy="get:${x.fp}" style="padding:.4em .9em;font-size:14px">Recevoir</button></div>`).join("");
    const mine = D.onlyHere.slice(0, 4).map(x => `<div class="mgs-li"><span>${E(x.name.replace(/\.[^.]+$/, ""))}</span><button class="btn ghost" data-sy="offer:${x.fp}" style="padding:.4em .9em;font-size:14px">Envoyer</button></div>`).join("");
    const I = k => (typeof ICON !== "undefined" && ICON[k]) || "";
    return `<h3>Synchronisation des tablettes</h3>
      ${row(dot(st.col), E(st.txt), E(st.sub), S.mode === "synced" ? `<span class="mgs-chip"><i class="mgs-dot mgs-bl" style="width:9px;height:9px"></i>Mode synchronisé</span>` : "")}
      <div class="srow"><span class="si">${SIc("screen")}</span><div class="sl"><b>Cette tablette</b><small>${m ? roleTxt(C.id) : "—"}</small><input class="mgs-in" id="mgs-name" maxlength="30" value="${E(myName())}" aria-label="Nom de cette tablette" style="margin-top:6px"></div></div>
      ${row("", "Autre tablette", p ? (p.online ? roleTxt(p.id) + " · " + (p.follow ? "suit la session" : "détachée") : "Hors ligne") : "Pas encore appairée", `<b>${p ? E(p.name) : "—"}</b>`)}
      ${row("", "Qualité de connexion", S.status === "online" ? Math.round(S.rtt) + " ms · gigue " + Math.round(S.jitter) + " ms" : "", `<span class="mgs-bars">${bars}</span><span>${q.txt}</span>`)}
      ${row("", "Décalage de lecture estimé", S.mode === "synced" ? "Mesuré entre les deux lectures" : "Actif en mode synchronisé", `<b>${isMaster() ? msTxt(S.peerDrift) : msTxt(S.drift)}</b>`)}
      ${row("", "Mode", "Indépendant : chacun sa lecture. Synchronisé : même film, mêmes commandes.", `<div class="seg"><button class="${S.mode !== "synced" ? "on" : ""}" data-sy="mode:independent">Indépendant</button><button class="${S.mode === "synced" ? "on" : ""}" data-sy="mode:synced">Synchronisé</button></div>`)}
      ${row("", "Suivre la session", "Désactivez pour reprendre le contrôle individuel", sw("follow", C.follow))}
      ${isMaster() ? row("", "Contrôle partagé", "L’autre tablette peut aussi lancer, pauser et avancer", sw("shared", S.shared)) : ""}
      ${row("", "Compensation audio de cette tablette", "Retard de sortie du son, en ms (Bluetooth, haut-parleurs)", `<button class="btn ghost" data-sy="calib:-10">${I("minus")}</button><b style="min-width:64px;text-align:center">${C.calib} ms</b><button class="btn ghost" data-sy="calib:10">${I("plus")}</button>`)}
      ${row("", "Partage de fichiers", "Autoriser l’autre tablette à demander des contenus", sw("share", C.allowShare))}
      ${row("", "Indicateur dans le lecteur", "Petite pastille discrète pendant la lecture", sw("ind", C.indicator))}
      <div class="srow"><span class="si">${Ic("cpu")}</span><div class="sl"><b>Serveur</b><small>Adresse affichée au démarrage du serveur</small><input class="mgs-in" id="mgs-url" inputmode="url" autocapitalize="off" placeholder="wss://192.168.43.1:8443/sync" value="${E(C.url)}" aria-label="Adresse du serveur" style="margin-top:6px"></div></div>
      <div class="mgs-act"><button class="btn" data-sy="connect">${S.want ? "Reconnecter" : "Connecter"}</button><button class="btn ghost" data-sy="disconnect">Déconnecter</button><button class="btn ghost" data-sy="syncnow">Synchroniser maintenant</button><button class="btn ghost" data-sy="swap">Échanger les rôles</button></div>
      ${row("", "Bibliothèque commune", S.status === "online" ? D.common + " en commun · " + D.onlyHere.length + " seulement ici · " + D.onlyPeer.length + " seulement sur l’autre" : "Disponible une fois connecté", "")}
      ${miss || mine ? `<div class="mgs-list">${miss}${mine}</div>` : ""}
      ${xfers ? `<div class="mgs-list">${xfers}</div>` : ""}
      <div class="mgs-act"><button class="btn ghost" data-sy="log">Journal</button><button class="btn ghost" data-sy="resume">Reprendre un transfert</button></div>`;
  }
  let renderT = 0;
  function render() { clearTimeout(renderT); renderT = setTimeout(() => { const c = $q("#sync-card"); if (!c) return; const ae = document.activeElement; if (ae && c.contains(ae) && ae.tagName === "INPUT") return; c.innerHTML = panelHTML(); }, 80); }
  function mount(host) {
    if (host.querySelector("#sync-card")) return;
    const sec = document.createElement("section"); sec.className = "sg"; sec.id = "sync-card"; sec.innerHTML = panelHTML();
    if (host.matches("[data-slot]")) host.appendChild(sec); else host.insertBefore(sec, host.children[2] || null);
    if (!C.url && /^https?:/.test(location.protocol)) fetch("/healthz", { cache: "no-store" }).then(r => r.json()).then(j => { if (j && j.mgsync && !C.url) { C.url = (location.protocol === "https:" ? "wss://" : "ws://") + location.host + "/sync"; saveC(); render(); } }).catch(() => { });
    scanLib().then(() => { if (S.status === "online") publishLib(); render(); }).catch(() => { });
  }
  new MutationObserver(() => { const g = document.querySelector("#setBody [data-slot=sync]") || document.querySelector("#setBody .sgrid"); if (g) mount(g); }).observe(document.getElementById("setBody") || document.body, { childList: true, subtree: true });

  document.addEventListener("click", async e => {
    const b = e.target.closest("#sync-card [data-sy]"); if (!b) return;
    const k = b.dataset.sy;
    if (k === "connect") {
      C.url = ($q("#mgs-url") ? $q("#mgs-url").value : C.url).trim(); saveC();
      if (S.ws) { S.want = true; try { S.ws.close(); } catch { /* ignore */ } return; }
      if (!C.token) askCode(); else connect();
    } else if (k === "disconnect") disconnect();
    else if (k === "syncnow") { if (S.status !== "online") return toast("Non connecté"); if (S.mode !== "synced") return toast("Passez en mode synchronisé"); if (isMaster() && S.el) { const el = S.el; cmd(el.paused ? "seek" : "play", { pos: el.currentTime - C.calib / 1000 }); } else send({ t: "getstate" }); }
    else if (k === "swap") send({ t: "role:swap" });
    else if (k.startsWith("mode:")) { if (S.status !== "online") return toast("Connectez d’abord les tablettes"); cmd("mode", { mode: k.slice(5) }); }
    else if (k === "follow") { C.follow = !C.follow; saveC(); b.classList.toggle("on", C.follow); send({ t: "follow", v: C.follow }); if (C.follow && S.mode === "synced") send({ t: "getstate" }); else if (!C.follow && S.el) { S.el.playbackRate = 1; } indicator(); }
    else if (k === "shared") cmd("shared", { v: !S.shared });
    else if (k === "share") { C.allowShare = !C.allowShare; saveC(); b.classList.toggle("on", C.allowShare); }
    else if (k === "ind") { C.indicator = !C.indicator; saveC(); b.classList.toggle("on", C.indicator); indicator(); }
    else if (k.startsWith("calib:")) { C.calib = Math.max(-500, Math.min(500, C.calib + +k.slice(6))); saveC(); render(); }
    else if (k.startsWith("get:")) requestXfer(k.slice(4));
    else if (k.startsWith("offer:")) offerXfer(k.slice(6));
    else if (k.startsWith("cancel:")) { const x = S.xf.get(+k.slice(7)); if (x) { x.abort = true; if (x.state !== "interrompu") send({ t: "xfer:abort", tid: x.tid, to: x.to, why: "Annulé" }); S.xf.delete(x.tid); render(); } }
    else if (k === "resume") resumePending();
    else if (k === "log") openLog();
  });
  document.addEventListener("change", e => {
    if (e.target.id === "mgs-name") { C.name = e.target.value.trim().slice(0, 30); saveC(); send({ t: "rename", name: myName() }); }
    if (e.target.id === "mgs-url") { C.url = e.target.value.trim(); saveC(); }
  });
  function askCode() {
    modal(`<h3>Appairer cette tablette</h3><p class="msub">Saisissez le code à 6 chiffres affiché sur le serveur (Termux), ou dans le fichier <code>data/pairing-code.txt</code>.</p><div class="mrow" style="cursor:default"><input class="ptxt" id="mgs-code" inputmode="numeric" maxlength="6" placeholder="000000" style="text-align:center;font-size:26px;letter-spacing:.3em"></div><div class="mact"><button class="btn" data-ok>Associer</button><button class="btn ghost" data-close>Annuler</button></div>`, m => {
      const prev = m.parentElement.onclick; setTimeout(() => { const i = $q("#mgs-code"); i && i.focus(); }, 100);
      m.parentElement.onclick = e => { prev(e); if (e.target.closest("[data-ok]")) { const c = $q("#mgs-code").value.replace(/\D/g, ""); if (c.length !== 6) return toast("Le code comporte 6 chiffres"); closeModal(); connect(c); } };
    });
  }
  async function resumePending() {
    const keys = await skeys("meta").catch(() => []); const k = keys.find(x => /^x\d+$/.test(x));
    if (!k) return toast("Aucun transfert à reprendre");
    const meta = await sget("meta", k); if (!meta) return;
    const p = peer(); if (!p || !p.online) return toast("L’autre tablette n’est pas disponible");
    const tid = +k.slice(1); S.xf.set(tid, { tid, dir: "in", fp: meta.fp, name: meta.name, cat: meta.cat, size: meta.size, to: p.id, got: 0, total: Math.ceil(meta.size / CH), state: "reprise" });
    send({ t: "xfer:req", tid, to: p.id, fp: meta.fp, name: meta.name, cat: meta.cat, size: meta.size }); render();
  }
  function openLog() {
    const rows = LOG.slice(-80).reverse().map(x => `<div class="mgs-li" style="align-items:flex-start"><span style="white-space:normal">${E(x.t.slice(11, 19))} · <b>${E(x.l)}</b> · ${E(x.m)}${x.d ? " " + E(JSON.stringify(x.d)).slice(0, 120) : ""}</span></div>`).join("") || '<p class="mempty">Journal vide.</p>';
    modal(`<h3>Journal de synchronisation</h3><div style="max-height:46vh;overflow:auto;margin:8px 0">${rows}</div><div class="mact"><button class="btn" data-exp>Exporter</button><button class="btn ghost" data-clr>Effacer</button><button class="btn ghost" data-close>Fermer</button></div>`, m => {
      const prev = m.parentElement.onclick; m.parentElement.onclick = e => { prev(e);
        if (e.target.closest("[data-clr]")) { LOG.length = 0; jset("mg.sync.log", []); openLog(); }
        if (e.target.closest("[data-exp]")) { const a = document.createElement("a"); a.href = URL.createObjectURL(new Blob([JSON.stringify({ version: VER, device: myName(), log: LOG }, null, 1)], { type: "application/json" })); a.download = "mg-sync-journal.json"; a.click(); setTimeout(() => URL.revokeObjectURL(a.href), 4000); } };
    });
  }

  /* ---------------------------------------------------------------- indicateur discret dans le lecteur */
  function indicator() {
    const on = C.indicator && S.mode === "synced" && (S.status !== "off" || S.sessionFp);
    let st;
    if (S.status !== "online") st = { c: "#f87171", t: "Hors synchro" };
    else if (!C.follow) st = { c: "#60a5fa", t: "Détachée · toucher pour rejoindre" };
    else if (S.syncing || S.pendingOpen) st = { c: "#fbbf24", t: "Synchronisation…" };
    else st = { c: "#34d399", t: "Synchro" + (isMaster() ? (S.peerDrift != null ? " · " + msTxt(S.peerDrift) : "") : " · " + msTxt(S.drift)) };
    for (const host of [$q("#ovV"), $q("#mini")]) {
      if (!host) continue; let el = host.querySelector(".mgs-ind");
      if (!on) { if (el) el.remove(); continue; }
      if (!el) { el = document.createElement("div"); el.className = "mgs-ind"; el.setAttribute("role", "button"); el.title = "Toucher pour détacher ou rejoindre la session"; el.addEventListener("click", ev => { ev.stopPropagation(); C.follow = !C.follow; saveC(); send({ t: "follow", v: C.follow }); if (C.follow && S.mode === "synced") send({ t: "getstate" }); else if (S.el) S.el.playbackRate = 1; toast(C.follow ? "Session rejointe" : "Lecture individuelle : vous contrôlez cette tablette"); indicator(); render(); }); host.appendChild(el); }
      el.innerHTML = `<i style="background:${st.c}"></i><span>${E(st.t)}</span>`;
    }
  }
  setInterval(indicator, 1500);

  /* ---------------------------------------------------------------- reconnexion et cycle de vie */
  window.addEventListener("online", () => { if (S.want && !S.ws) { S.tries = 0; connect(); } });
  document.addEventListener("visibilitychange", () => { if (!document.hidden && S.want && !S.ws) { S.tries = 0; connect(); } });
  setTimeout(() => { if (C.auto && C.url && C.token) connect(); }, 1500);
  setInterval(() => { if (S.status === "online") publishLib().catch(() => { }); }, 15000);

  /* ---------------------------------------------------------------- interface de test / diagnostic */
  window.MGSync = {
    version: VER, state: S, cfg: C, log: LOG, connect, disconnect, scanLib, fpOf, requestXfer, offerXfer,
    setMode: m => cmd("mode", { mode: m }), swapRoles: () => send({ t: "role:swap" }), syncNow: () => send({ t: "getstate" }),
    setFollow: v => { C.follow = !!v; saveC(); send({ t: "follow", v: C.follow }); if (v && S.mode === "synced") send({ t: "getstate" }); },
    srv, quality, inv: () => INV, peer, me, publish: () => publishLib(false), onGame: null,
    gameSend: g => send({ t: "game", g }), peerOnline: () => { const p = peer(); return S.status === "online" && !!(p && p.online); }
  };
  log("info", "Module chargé", { v: VER, id: C.id.slice(0, 8) });
})();
