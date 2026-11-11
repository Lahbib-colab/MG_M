/*
 * MG Divertissement – player.js
 *  1. Analyse des listes M3U (chaînes, logos, groupes) et des playlists HLS (qualités, direct ou VOD).
 *  2. Extraction de vidéos m3u8 + jaquettes depuis un lien : page web, liste M3U, fichier m3u8, JSON.
 *  3. Réseau avec repli : accès direct, puis relais du serveur Termux (si la tablette est appairée), puis relais personnalisé.
 *  4. Lecteur HLS renforcé (lecture native du navigateur) : chargement, reconnexion automatique, détection de blocage,
 *     choix de la qualité, retour au direct, messages d'erreur clairs.
 * Aucune dépendance. Les parties sans navigateur (analyse, extraction sans DOM) se testent sous Node.
 */
(function (root) {
  "use strict";
  const MGP = {};

  /* ============================================================ outils */
  const decodeEnt = s => String(s).replace(/&amp;/g, "&").replace(/&quot;/g, '"').replace(/&#0?39;|&apos;/g, "'").replace(/&lt;/g, "<").replace(/&gt;/g, ">").replace(/&#x2F;/gi, "/");
  const absUrl = (u, base) => { try { u = decodeEnt(String(u).trim()); if (!u || /^(javascript|data|blob|about):/i.test(u)) return ""; return new URL(u, base || undefined).href; } catch { return ""; } };
  const isHttp = u => /^https?:\/\//i.test(u || "");
  const hostOf = u => { try { return new URL(u).hostname.replace(/^www\./, ""); } catch { return ""; } };
  const clean = s => decodeEnt(String(s || "")).replace(/\s+/g, " ").trim().slice(0, 140);
  /* Un lien de flux HLS : extension .m3u8 ou .m3u (avec ou sans paramètres), ou « m3u8 » dans l'adresse. */
  const isM3u8 = u => /\.m3u8?(?:[?#&]|$)|m3u8/i.test(String(u || ""));
  MGP.isHls = u => /\.m3u8(?:[?#&]|$)|m3u8/i.test(String(u || ""));
  MGP.hostOf = hostOf;

  /* « Mon film | MonSite » ou « MonSite - Mon film » -> « Mon film » (seulement si l'autre partie ressemble au nom du site). */
  function cleanTitle(t, site, host) {
    t = clean(t); if (!t) return "";
    const h = String(host || "").toLowerCase().replace(/^www\./, ""), names = [site, h, h.replace(/\.[a-z]{2,}$/i, "")].filter(Boolean).map(x => String(x).toLowerCase());
    const hit = x => names.some(n => n && (x.toLowerCase().includes(n) || n.includes(x.toLowerCase())));
    let m = t.match(/^(.*?)\s+[|\-–—·»:]\s+([^|\-–—·»:]{2,40})$/); if (m && hit(m[2])) return m[1].trim();
    m = t.match(/^([^|\-–—·»:]{2,40})\s+[|\-–—·»:]\s+(.*)$/); if (m && hit(m[1])) return m[2].trim();
    return t;
  }
  function titleFromUrl(u) {
    try {
      const segs = new URL(u).pathname.split("/").filter(Boolean).map(s => { try { return decodeURIComponent(s); } catch { return s; } });
      let n = segs.pop() || ""; n = n.replace(/\.[a-z0-9]{2,5}$/i, "");
      while (/^(index|master|playlist|manifest|chunklist|main|stream|live|hls|video|default|prog_index|mono|stereo)$/i.test(n) && segs.length) n = segs.pop();
      n = n.replace(/[-_.+]+/g, " ").trim();
      return n ? n.charAt(0).toUpperCase() + n.slice(1) : hostOf(u);
    } catch { return ""; }
  }
  MGP.titleFromUrl = titleFromUrl;

  /* ============================================================ analyse M3U / HLS */
  function parseAttrs(s) { const o = {}, re = /([A-Za-z0-9_-]+)=("([^"]*)"|[^,\s]*)/g; let m; while ((m = re.exec(s))) o[m[1].toLowerCase()] = m[3] !== undefined ? m[3] : m[2]; return o; }
  function parseExtinf(line) {
    const body = line.slice(8); let q = false, ci = -1;
    for (let i = 0; i < body.length; i++) { const c = body[i]; if (c === '"') q = !q; else if (c === "," && !q) { ci = i; break; } }
    const meta = ci < 0 ? body : body.slice(0, ci), name = ci < 0 ? "" : body.slice(ci + 1).trim();
    const a = parseAttrs(meta.replace(/^\s*-?[\d.]+/, ""));
    return { dur: parseFloat(meta), name: name || a["tvg-name"] || "", logo: a["tvg-logo"] || a["logo"] || "", group: a["group-title"] || "", id: a["tvg-id"] || "" };
  }
  const levelLabel = v => v.h ? v.h + "p" : v.bw ? (v.bw / 1e6).toFixed(1).replace(".", ",") + " Mb/s" : "Qualité";
  MGP.levelLabel = levelLabel;

  function plTitle(L) {
    const pl = L.find(l => l.startsWith("#PLAYLIST:")); if (pl) return clean(pl.slice(10));
    for (const l of L) if (l.startsWith("#EXT-X-SESSION-DATA:")) { const a = parseAttrs(l.slice(20)); if (/title|name/i.test(a["data-id"] || "") && a.value) return clean(a.value); }
    return "";
  }
  /* Retourne { kind: "master" | "media" | "list" | "unknown", ... } */
  function parseM3U(text, base) {
    text = String(text || "").replace(/^\uFEFF/, "");
    const L = text.split(/\r?\n/).map(l => l.trim());
    const tags = L.filter(l => l.startsWith("#EXT"));
    if (!tags.length && !L.some(l => isHttp(l))) return { kind: "unknown" };
    if (L.some(l => l.startsWith("#EXT-X-STREAM-INF"))) {
      const variants = [], media = [];
      for (let i = 0; i < L.length; i++) {
        if (L[i].startsWith("#EXT-X-STREAM-INF:")) {
          const a = parseAttrs(L[i].slice(18)); let j = i + 1; while (j < L.length && (!L[j] || L[j].startsWith("#"))) j++;
          const url = absUrl(L[j] || "", base); if (!url) continue;
          const r = (a.resolution || "").match(/(\d+)x(\d+)/);
          variants.push({ url, bw: +a.bandwidth || 0, avg: +a["average-bandwidth"] || 0, w: r ? +r[1] : 0, h: r ? +r[2] : 0, codecs: a.codecs || "", fps: +a["frame-rate"] || 0 });
        } else if (L[i].startsWith("#EXT-X-MEDIA:")) { const a = parseAttrs(L[i].slice(13)); media.push({ type: a.type, name: a.name, lang: a.language, uri: a.uri ? absUrl(a.uri, base) : "", def: a.default === "YES" }); }
      }
      const seen = new Set(), uniq = variants.filter(v => !seen.has(v.url) && seen.add(v.url));
      uniq.sort((x, y) => (y.h - x.h) || (y.bw - x.bw)); uniq.forEach(v => v.label = levelLabel(v));
      return { kind: "master", variants: uniq, media, title: plTitle(L) };
    }
    if (L.some(l => /^#EXT-X-(TARGETDURATION|MEDIA-SEQUENCE|ENDLIST|PLAYLIST-TYPE|MAP|KEY|PROGRAM-DATE-TIME)/.test(l))) {
      const ended = L.some(l => l.startsWith("#EXT-X-ENDLIST")), vod = L.some(l => /^#EXT-X-PLAYLIST-TYPE:VOD/.test(l));
      let dur = 0, n = 0; for (const l of L) if (l.startsWith("#EXTINF:")) { dur += parseFloat(l.slice(8)) || 0; n++; }
      const key = L.find(l => l.startsWith("#EXT-X-KEY:")), km = key ? (parseAttrs(key.slice(11)).method || "NONE") : "NONE";
      return { kind: "media", live: !(ended || vod), duration: dur, segments: n, encrypted: km !== "NONE", method: km, title: plTitle(L) };
    }
    const entries = []; let pend = null, grp = "";
    for (const l of L) {
      if (!l) continue;
      if (l.startsWith("#EXTINF:")) pend = parseExtinf(l);
      else if (l.startsWith("#EXTGRP:")) grp = l.slice(8).trim();
      else if (l.startsWith("#")) continue;
      else { const url = absUrl(l, base); if (!isHttp(url)) { pend = null; continue; } entries.push({ url, name: clean(pend && pend.name), logo: pend && pend.logo ? absUrl(pend.logo, base) : "", group: clean((pend && pend.group) || grp), id: (pend && pend.id) || "", dur: pend ? pend.dur : 0 }); pend = null; }
    }
    return entries.length ? { kind: "list", entries } : { kind: "unknown" };
  }
  MGP.parseM3U = parseM3U;

  /* ============================================================ extraction de vidéos + jaquettes */
  const IMG_KEY = /^(image|images|img|poster|posterurl|poster_url|thumbnail|thumbnails|thumbnailurl|thumbnail_url|thumb|thumburl|thumb_url|cover|coverurl|cover_url|jaquette|logo|icon|artwork|picture|still|preview|previewimage|screenshot|stream_icon|backdrop|banner|imageurl|image_url|tvg-logo)$/i;
  const TITLE_KEY = /^(title|name|label|titre|nom|headline|episodetitle|episode_title|tvg-name|video_title|videotitle|programme)$/i;
  const IMG_EXT = /\.(?:jpe?g|png|webp|gif|avif|bmp|svg)(?:[?#]|$)/i;
  const looksUrl = s => typeof s === "string" && s.length > 3 && s.length < 2000 && (/^(https?:)?\/\//i.test(s) || /^\.{0,2}\//.test(s) || IMG_EXT.test(s));
  const pickStr = v => typeof v === "string" ? v : Array.isArray(v) ? (v.map(pickStr).find(Boolean) || "") : (v && typeof v === "object") ? pickStr(v.url || v.src || v.href || v.contentUrl || v.large || v.medium || v.small || "") : "";

  function walkJson(node, base, out, stack) {
    stack = stack || [];
    if (typeof node === "string") {
      if (isM3u8(node) && /^(https?:)?\/\/|^\.{0,2}\/|\.m3u8/i.test(node) && node.length < 2500) out.push({ raw: node, ctx: stack.filter(x => x && typeof x === "object" && !Array.isArray(x)).slice().reverse() });
      return;
    }
    if (Array.isArray(node)) { for (const x of node) walkJson(x, base, out, stack); return; }
    if (node && typeof node === "object") { stack.push(node); for (const k of Object.keys(node)) walkJson(node[k], base, out, stack); stack.pop(); }
  }
  function fromJson(data, base) {
    const hits = []; walkJson(data, base, hits, []);
    const items = [];
    for (const h of hits) {
      const url = absUrl(h.raw, base); if (!isHttp(url)) continue;
      let image = "", title = "";
      for (const o of h.ctx) {
        for (const k of Object.keys(o)) {
          if (!image && IMG_KEY.test(k)) { const v = pickStr(o[k]); if (looksUrl(v)) image = absUrl(v, base); }
          if (!title && TITLE_KEY.test(k) && typeof o[k] === "string" && o[k].trim() && !isM3u8(o[k])) title = clean(o[k]);
        }
        if (image && title) break;
      }
      items.push({ url, title, image });
    }
    return items;
  }

  /* Cherche, dans un texte quelconque (script, JSON mal formé, page sans DOM), les adresses m3u8 et, autour d'elles, une image et un titre. */
  /* Une adresse peut apparaître telle quelle, ou échappée dans du JSON (https:\/\/hôte\/a.m3u8, \u002F, \u0026). */
  const CH = String.raw`(?:[^\s"'<>\\)\]]|\\/|\\u002[Ff]|\\u0026)`;
  const URL_IN_TEXT = new RegExp(String.raw`(?:https?:)?(?:\\?/){2}` + CH + String.raw`+?\.m3u8?(?:\?` + CH + String.raw`*)?|(?:\.{0,2}/|[A-Za-z0-9_-]+/)` + CH + String.raw`*?\.m3u8(?:\?` + CH + String.raw`*)?`, "gi");
  const unesc = s => s.replace(/\\u002[Ff]/g, "/").replace(/\\\//g, "/").replace(/\\u0026/g, "&").replace(/\\+$/, "");
  function scanText(text, base) {
    const out = [];
    URL_IN_TEXT.lastIndex = 0; let m;
    while ((m = URL_IN_TEXT.exec(text))) {
      const raw = unesc(m[0]); let url = absUrl(raw.startsWith("//") ? "https:" + raw : raw, base); if (!isHttp(url)) continue;
      const a = Math.max(0, m.index - 700), b = Math.min(text.length, m.index + m[0].length + 700), win = text.slice(a, b), at = m.index - a;
      const best = (re, valid) => { let bm, d = 1e9, v = ""; re.lastIndex = 0; while ((bm = re.exec(win))) { const val = unesc(bm[2]); if (valid && !valid(val)) continue; const dist = Math.abs(bm.index - at); if (dist < d) { d = dist; v = val; } } return v; };
      const image = best(/["']?(poster|image|thumbnail|thumb|cover|preview|logo|jaquette|artwork|img)["']?\s*[:=]\s*["']([^"'\s]+)["']/gi, v => looksUrl(v));
      const title = best(/["']?(title|name|titre|label)["']?\s*[:=]\s*["']([^"']{2,140})["']/gi, v => !isM3u8(v) && !looksUrl(v));
      out.push({ url, title: clean(title), image: image ? absUrl(image, base) : "" });
    }
    return out;
  }

  /* ---- DOM : associe chaque vidéo à son image et à son titre en restant dans son conteneur ---- */
  const LAZY = ["data-src", "data-lazy-src", "data-original", "data-lazy", "data-img", "data-image", "data-thumb", "data-thumbnail", "data-poster", "data-cover", "data-bg", "poster", "src"];
  function srcsetFirst(v) { return (String(v || "").split(",")[0] || "").trim().split(/\s+/)[0] || ""; }
  function imgOfEl(el) {
    if (!el || !el.getAttribute) return "";
    for (const k of LAZY) { const v = el.getAttribute(k); if (v && (looksUrl(v)) && (el.tagName === "IMG" || k !== "src") && !/^data:/i.test(v)) return v; }
    const ss = el.getAttribute("srcset") || el.getAttribute("data-srcset"); if (ss) return srcsetFirst(ss);
    const st = el.getAttribute("style"); if (st) { const m = st.match(/url\(["']?([^"')]+)/i); if (m) return m[1]; }
    return "";
  }
  const BADIMG = /logo|icon|avatar|sprite|flag|emoji|pixel|spacer|blank|loader|spinner|badge|button|btn_/i;
  function imageIn(node) {
    let v = imgOfEl(node); if (v) return v;
    const pv = node.querySelector && node.querySelector("video[poster]"); if (pv) return pv.getAttribute("poster");
    const list = node.querySelectorAll ? node.querySelectorAll("img,picture source,[data-poster],[data-thumb],[data-bg],[style*='background']") : [];
    for (const el of list) {
      const w = +(el.getAttribute("width") || 999), h = +(el.getAttribute("height") || 999); if (w < 40 || h < 40) continue;
      const hint = (el.getAttribute("alt") || "") + " " + (el.className || "") + " " + (el.getAttribute("src") || ""); if (el.tagName === "IMG" && BADIMG.test(hint)) continue;
      v = imgOfEl(el); if (v) return v;
    }
    return "";
  }
  function titleIn(node, self) {
    for (const k of ["data-title", "title", "aria-label", "data-name", "alt"]) { const v = node.getAttribute && node.getAttribute(k); if (v && v.trim().length > 1 && !isM3u8(v)) return v; }
    if (self) return "";
    const q = node.querySelector && node.querySelector("[itemprop=name],h1,h2,h3,h4,h5,[class*=title],[class*=name],a[title],img[alt]");
    if (q) { const t = q.getAttribute("title") || q.getAttribute("alt") || q.textContent; if (t && t.trim().length > 1 && !isM3u8(t)) return t; }
    if (/^(A|LI|FIGURE|ARTICLE)$/.test(node.tagName)) { const t = (node.textContent || "").replace(/\s+/g, " ").trim(); if (t.length > 1 && t.length < 160 && !isM3u8(t)) return t; }
    return "";
  }
  function fromDom(html, pageUrl) {
    let doc; try { doc = new DOMParser().parseFromString(html, "text/html"); } catch { return null; }
    const bh = doc.querySelector("base[href]"), base = bh ? (absUrl(bh.getAttribute("href"), pageUrl) || pageUrl) : pageUrl;
    const items = [], re = new RegExp(URL_IN_TEXT.source, "gi");
    const urlsOf = s => { const r = []; re.lastIndex = 0; let m; while ((m = re.exec(s))) { const raw = unesc(m[0]); const u = absUrl(raw.startsWith("//") ? "https:" + raw : raw, base); if (isHttp(u)) r.push(u); } return r; };
    const distinctIn = node => new Set(urlsOf(node.innerHTML || "")).size;
    /* 1) attributs des éléments : video/source/a/div[data-src]/meta… */
    for (const el of doc.querySelectorAll("*")) {
      if (el.tagName === "SCRIPT" || el.tagName === "STYLE") continue;
      for (const at of el.attributes) {
        if (!/m3u8/i.test(at.value)) continue;
        for (const u of urlsOf(at.value)) {
          let image = "", title = "";
          const own = el.closest ? el.closest("video") : null; if (own && own.getAttribute("poster")) image = own.getAttribute("poster");
          title = titleIn(el, true); if (!image) image = imgOfEl(el);
          let node = el;
          for (let d = 0; node && node.tagName !== "BODY" && d < 7 && (!image || !title); d++, node = node.parentElement) {
            if (node !== el && distinctIn(node) > 1) break;
            if (!image) image = imageIn(node); if (!title) title = titleIn(node, false);
          }
          items.push({ url: u, title: clean(title), image: image ? absUrl(image, base) : "" });
        }
      }
    }
    /* 2) scripts : JSON (JSON-LD, __NEXT_DATA__, config de lecteur) puis recherche dans le texte */
    for (const sc of doc.querySelectorAll("script")) {
      const t = sc.textContent || ""; if (!/m3u8/i.test(t)) continue;
      const type = (sc.getAttribute("type") || "").toLowerCase(); let done = false;
      if (/json/.test(type) || /^\s*[\[{]/.test(t)) { try { items.push(...fromJson(JSON.parse(t), base)); done = true; } catch { /* pas du JSON pur */ } }
      if (!done) items.push(...scanText(t, base));
    }
    /* 3) métadonnées de la page : og:title / og:image, JSON-LD (Movie, VideoObject, Episode…), h1, <title> */
    const mc = n => { const e = doc.querySelector(n); return e ? e.getAttribute("content") || e.getAttribute("href") || "" : ""; };
    const site = mc('meta[property="og:site_name"]') || mc('meta[name="application-name"]');
    let mImg = mc('meta[property="og:image"]') || mc('meta[property="og:image:url"]') || mc('meta[name="twitter:image"]') || mc('link[rel="image_src"]'), mTitle = mc('meta[property="og:title"]') || mc('meta[name="twitter:title"]');
    for (const sc of doc.querySelectorAll('script[type="application/ld+json"]')) {
      try { const walk = n => { if (Array.isArray(n)) return n.forEach(walk); if (n && typeof n === "object") { if (/^(Movie|VideoObject|Episode|TVEpisode|TVSeries|CreativeWork)$/i.test([].concat(n["@type"] || []).join(" ").split(" ").find(x => /^(Movie|VideoObject|Episode|TVEpisode|TVSeries|CreativeWork)$/i.test(x)) || "")) { if (!mTitle && typeof n.name === "string") mTitle = n.name; const im = pickStr(n.image) || pickStr(n.thumbnailUrl); if (!mImg && im) mImg = im; } Object.values(n).forEach(walk); } }; walk(JSON.parse(sc.textContent)); } catch { /* JSON-LD invalide */ }
    }
    if (!mTitle) { const h1 = doc.querySelector("h1"); mTitle = (doc.title || "") || (h1 ? h1.textContent : ""); }
    const meta = { title: cleanTitle(mTitle, site, hostOf(pageUrl)), image: mImg && !/^data:/i.test(mImg) ? absUrl(mImg, base) : "", site };
    const uniq = new Set(items.map(i => i.url));
    if (uniq.size === 1) for (const i of items) { if (!i.image && meta.image) i.image = meta.image; if (!i.title && meta.title) i.title = meta.title; }
    /* 4) pages liées (iframe / lecteur intégré) : renvoyées à l'appelant pour une analyse en un niveau */
    const frames = [...doc.querySelectorAll("iframe[src],iframe[data-src]")].map(f => absUrl(f.getAttribute("src") || f.getAttribute("data-src"), base)).filter(isHttp);
    return { items, frames: [...new Set(frames)].slice(0, 4), pageTitle: clean(doc.title || ""), meta };
  }

  function finalize(items, pageUrl) {
    const map = new Map();
    for (const it of items) {
      if (!it || !isHttp(it.url)) continue;
      const prev = map.get(it.url);
      if (!prev) map.set(it.url, { url: it.url, title: it.title || "", image: it.image || "", group: it.group || "", auto: !!it.auto });
      else { if (!prev.title && it.title) prev.title = it.title; if (!prev.image && it.image) prev.image = it.image; if (!prev.group && it.group) prev.group = it.group; }
    }
    const out = [...map.values()];
    out.forEach(i => { if (!i.title) { i.title = titleFromUrl(i.url) || hostOf(i.url); i.auto = true; } });
    const count = {}; out.forEach(i => count[i.title] = (count[i.title] || 0) + 1);
    const seen = {}; out.forEach(i => { if (count[i.title] > 1) { seen[i.title] = (seen[i.title] || 0) + 1; i.title += " (" + seen[i.title] + ")"; } });
    return out;
  }

  /* Point d'entrée : texte reçu (page, liste, m3u8, JSON) -> { kind, items, frames, note } */
  function extract(text, o) {
    o = o || {}; const url = o.url || "", ct = String(o.contentType || "").toLowerCase(), t = String(text || "").trim();
    if (!t) return { kind: "empty", items: [], frames: [], note: "Le contenu est vide." };
    if (/^#EXTM3U/i.test(t) || (/^#EXT/m.test(t) && /\.m3u8?(?:[?#]|$)/i.test(url)) || /mpegurl/.test(ct)) {
      const p = parseM3U(t, url);
      if (p.kind === "list") return { kind: "list", items: finalize(p.entries.map(e => ({ url: e.url, title: e.name, image: e.logo, group: e.group })), url), frames: [], note: "" };
      if (p.kind === "master" || p.kind === "media") return { kind: "hls", items: finalize([{ url, title: p.title || o.title || "" }], url), frames: [], note: p.kind === "master" ? "Flux unique avec " + p.variants.length + " qualité(s)." : "Flux unique." };
    }
    if (/json/.test(ct) || /^[\[{]/.test(t)) { try { const items = finalize(fromJson(JSON.parse(t), url), url); return { kind: "json", items, frames: [], note: items.length ? "" : "Aucun lien m3u8 dans ces données." }; } catch { /* continue : texte */ } }
    let items = [], frames = [], pageTitle = "";
    const d = (typeof DOMParser !== "undefined") ? fromDom(t, url) : null;
    let meta = null; if (d) { items = d.items; frames = d.frames; pageTitle = d.pageTitle; meta = d.meta; } else items = scanText(t, url);
    items = finalize(items, url);
    const note = items.length ? "" : "Aucun lien .m3u8 dans le code de cette page.";
    return { kind: "html", items, frames, pageTitle, note, meta };
  }
  MGP.extract = extract;
  MGP.finalize = items => finalize(items, "");
  /* Une seule vidéo trouvée : elle prend le titre et l'image de la page qui la contient (le titre déduit de l'adresse est remplacé). */
  MGP.applyMeta = (items, meta) => { if (meta && items.length === 1) { const i = items[0]; if (meta.title && (!i.title || i.auto)) { i.title = meta.title; i.auto = false; } if (!i.image && meta.image) i.image = meta.image; } return items; };

  /* ============================================================ réseau avec repli */
  MGP.net = { proxy: null, tpl: null };   // proxy() -> { base, id, token } | null ; tpl() -> "https://relais/?u={url}"
  const why = e => e && e.status ? "réponse " + e.status : e && e.name === "AbortError" ? "délai dépassé" : "accès refusé par le navigateur (CORS) ou pas de connexion";
  async function one(u, as, ms) {
    const c = new AbortController(), t = setTimeout(() => c.abort(), ms || 12000);
    try {
      const r = await fetch(u, { signal: c.signal, credentials: "omit", referrerPolicy: "no-referrer", cache: "no-store" });
      if (!r.ok) { const e = new Error("HTTP " + r.status); e.status = r.status; throw e; }
      const ct = r.headers.get("content-type") || "";
      const data = as === "blob" ? await r.blob() : await r.text();
      return { data, ct, finalUrl: r.headers.get("x-final-url") || r.url || u };
    } finally { clearTimeout(t); }
  }
  /* as: "text" | "blob". Essaie dans l'ordre : direct, serveur Termux, relais personnalisé. */
  async function get(url, as, o) {
    o = o || {}; const reasons = [];
    try { return Object.assign(await one(url, as, o.timeout), { via: "direct" }); } catch (e) { reasons.push("accès direct : " + why(e)); }
    if (o.direct) { const e = new Error(reasons.join(" ; ")); e.reasons = reasons; throw e; }
    const px = MGP.net.proxy && MGP.net.proxy();
    if (px) { try { return Object.assign(await one(px.base + "?u=" + encodeURIComponent(url) + "&id=" + encodeURIComponent(px.id) + "&k=" + encodeURIComponent(px.token), as, o.timeout || 18000), { via: "serveur" }); } catch (e) { reasons.push("serveur MG : " + why(e)); } }
    else reasons.push("serveur MG : tablette non appairée");
    const tpl = MGP.net.tpl && MGP.net.tpl();
    if (tpl) { try { const u = /\{url\}/.test(tpl) ? tpl.replace("{url}", encodeURIComponent(url)) : tpl + encodeURIComponent(url); return Object.assign(await one(u, as, o.timeout || 18000), { via: "relais" }); } catch (e) { reasons.push("relais personnalisé : " + why(e)); } }
    const err = new Error("Impossible de récupérer cette adresse (" + reasons.join(" ; ") + ")"); err.reasons = reasons; throw err;
  }
  MGP.fetchText = (u, o) => get(u, "text", o);
  MGP.fetchBlob = async (u, o) => {
    const r = await get(u, "blob", o), b = r.data;
    if (!b || b.size < 120) throw new Error("image vide");
    if (!/^image\//i.test(b.type || "")) { const h = new Uint8Array(await b.slice(0, 12).arrayBuffer()), s = String.fromCharCode(...h); if (!(/^(\x89PNG|\xFF\xD8\xFF|GIF8|RIFF)/.test(s) || /<svg/i.test(await b.slice(0, 300).text()))) throw new Error("ce n'est pas une image"); }
    return r;
  };

  /* ============================================================ lecteur HLS renforcé (lecture native du navigateur) */
  const DELAYS = [1500, 3000, 5000, 8000, 12000, 15000];
  MGP.nativeHls = () => { try { return !!document.createElement("video").canPlayType("application/vnd.apple.mpegurl"); } catch { return false; } };
  const MSG = {
    1: "Lecture interrompue.", 2: "Problème de connexion avec le flux.", 3: "Le flux est endommagé ou son format n'est pas lisible.",
    4: "Flux introuvable, expiré, ou format non pris en charge par ce navigateur."
  };

  function Player(video, cb) { this.v = video; this.cb = cb || {}; this.state = "idle"; this.live = false; this.levels = null; this.level = -1; this.master = ""; this._h = []; this._t = []; this._id = 0; }
  Player.prototype._set = function (s, x) { this.state = s; if (this.cb.onstate) this.cb.onstate(s, x || {}); };
  Player.prototype._on = function (ev, fn) { const f = fn.bind(this); this.v.addEventListener(ev, f); this._h.push([ev, f]); };
  Player.prototype._clear = function () { this._h.forEach(([e, f]) => this.v.removeEventListener(e, f)); this._h = []; this._t.forEach(t => { clearTimeout(t); clearInterval(t); }); this._t = []; };
  Player.prototype.load = function (url, o) {
    o = o || {}; this._clear(); const id = ++this._id; this.url = this.src = url; this.live = !!o.live; this.hint = !!o.live; this.probeLive = undefined; this.attempt = 0; this.startAt = +o.start || 0; this.levels = null; this.level = -1; this.master = ""; this.stall = 0; this.lastT = -1; this.playedOnce = false;
    if (MGP.isHls(url) && !MGP.nativeHls()) { this._set("error", { fatal: true, title: "Ce navigateur ne lit pas le HLS (m3u8)", detail: "Utilisez une version récente de Chrome ou de Safari.", noRetry: true }); return; }
    const v = this.v;
    this._on("loadedmetadata", () => { this._applyLive(); if (this._pendingSeek && isFinite(v.duration) && !this.live) { try { v.currentTime = Math.min(this._pendingSeek, Math.max(0, v.duration - 3)); } catch { /* ignoré */ } } this._pendingSeek = 0; });
    this._on("waiting", () => { clearTimeout(this._w); this._w = setTimeout(() => { if (!v.paused && v.readyState < 3) this._set("buffering"); }, 700); });
    this._on("stalled", () => { clearTimeout(this._w); this._w = setTimeout(() => { if (!v.paused && v.readyState < 3) this._set("buffering"); }, 1200); });
    this._on("playing", () => { clearTimeout(this._w); this.playedOnce = true; this._set("playing"); clearTimeout(this._ok); this._ok = setTimeout(() => { this.attempt = 0; }, 6000); });
    this._on("canplay", () => { if (!v.paused && this.state !== "playing") this._set("playing"); });
    this._on("pause", () => { if (!v.ended && this.state !== "error" && this.state !== "reconnecting") this._set("paused"); });
    this._on("ended", () => { if (this.live) this._again("Le direct s'est arrêté."); else this._set("ended"); });
    this._on("error", () => { const e = v.error, c = e ? e.code : 0; if (c === 1) return; this._again(MSG[c] || "Erreur de lecture.", c); });
    this._t.push(setInterval(() => this._watch(), 3000));
    this._attach(); this._probe(url, id);
  };
  /* Direct ou non : la playlist fait foi (pas de #EXT-X-ENDLIST = direct) ; sinon la durée annoncée ; sinon l'indication de l'appelant. */
  Player.prototype._applyLive = function () {
    const v = this.v; let L;
    if (this.probeLive !== undefined) L = this.probeLive; else if (v.duration === Infinity) L = true; else if (v.readyState >= 1) L = false; else L = this.hint;
    if (L !== this.live) { this.live = L; if (this.cb.onlive) this.cb.onlive(L); }
  };
  Player.prototype._attach = function () {
    const v = this.v; this._pendingSeek = this.startAt || 0; this._set("loading");
    try { v.src = this.src; v.load(); } catch { /* ignoré */ }
    const p = v.play(); if (p && p.catch) p.catch(() => { /* démarrage automatique refusé : l'utilisateur appuiera sur lecture */ if (v.paused && this.state === "loading") this._set("paused"); });
  };
  /* Lit le « master » pour proposer les qualités et savoir s'il s'agit d'un direct (accès direct uniquement : sinon pas de menu). */
  Player.prototype._probe = async function (url, id) {
    if (!MGP.isHls(url)) return;
    try {
      const r = await get(url, "text", { direct: true, timeout: 7000 }); if (id !== this._id) return;
      const p = parseM3U(r.data, r.finalUrl || url);
      if (p.kind === "master") {
        if (p.variants.length > 1) { this.levels = p.variants; this.master = url; if (this.cb.onlevels) this.cb.onlevels(p.variants); }
        try { const r2 = await get(p.variants[0].url, "text", { direct: true, timeout: 6000 }); if (id !== this._id) return; const q = parseM3U(r2.data, r2.finalUrl || p.variants[0].url); if (q.kind === "media") { this.probeLive = q.live; this._applyLive(); } } catch { /* variante illisible : on se fie à la durée */ }
      } else if (p.kind === "media") { this.probeLive = p.live; this._applyLive(); if (p.encrypted && p.method !== "AES-128" && this.cb.onnote) this.cb.onnote("Flux chiffré (" + p.method + ") : la lecture peut échouer."); }
    } catch { /* pas d'accès aux playlists (CORS) : la lecture continue sans menu de qualité */ }
  };
  Player.prototype._again = function (reason, code) {
    if (this.state === "reconnecting") return;
    const max = (this.live || this.hint) ? DELAYS.length : 3;
    if (this.attempt >= max) { this._set("error", { fatal: true, title: this.playedOnce ? "Le flux s'est interrompu" : "Impossible de lire ce flux", detail: reason + ((this.live || this.hint) ? " Vérifiez l'adresse et la connexion." : ""), code }); return; }
    const delay = DELAYS[Math.min(this.attempt, DELAYS.length - 1)]; this.attempt++;
    this._set("reconnecting", { n: this.attempt, of: max, delay, reason });
    this._t.push(setTimeout(() => this._reload(), delay));
  };
  Player.prototype._reload = function () { const t = this.v.currentTime; this.startAt = !this.live && t > 2 ? t : (this.startAt || 0); this._attach(); if (this.probeLive === undefined || !this.levels) this._probe(this.url, this._id); };
  Player.prototype._watch = function () {
    const v = this.v; if (this.state !== "playing" && this.state !== "buffering") { this.stall = 0; return; }
    if (v.paused) { this.stall = 0; return; }
    if (Math.abs(v.currentTime - this.lastT) < 0.05) { this.stall += 3; } else { this.stall = 0; this.lastT = v.currentTime; }
    if (this.stall >= 15) { this.stall = 0; this._again("Plus d'image depuis 15 secondes."); }
  };
  Player.prototype.retry = function () { this.attempt = 0; this._reload(); };
  Player.prototype.setLevel = function (i) {
    if (!this.levels) return; this.level = i; const keep = this.live ? 0 : this.v.currentTime;
    this.src = i < 0 ? this.master : this.levels[i].url; this.startAt = keep; this.attempt = 0; this._attach(); if (this.cb.onlevel) this.cb.onlevel(i);
  };
  Player.prototype.behind = function () { const s = this.v.seekable; return this.live && s && s.length ? Math.max(0, s.end(s.length - 1) - this.v.currentTime) : 0; };
  Player.prototype.goLive = function () { const s = this.v.seekable; if (s && s.length) { try { this.v.currentTime = Math.max(0, s.end(s.length - 1) - 0.8); } catch { /* ignoré */ } } this.v.play().catch(() => { }); };
  Player.prototype.info = function () {
    const v = this.v, b = v.buffered; let ahead = 0; for (let i = 0; i < b.length; i++) if (v.currentTime >= b.start(i) && v.currentTime <= b.end(i)) ahead = b.end(i) - v.currentTime;
    const q = v.getVideoPlaybackQuality ? v.getVideoPlaybackQuality() : null;
    return { size: v.videoWidth ? v.videoWidth + "×" + v.videoHeight : "", ahead, dropped: q ? q.droppedVideoFrames : 0, live: this.live, behind: this.behind(), level: this.level, state: this.state, attempt: this.attempt };
  };
  Player.prototype.destroy = function (keepSrc) { this._id++; this._clear(); clearTimeout(this._w); clearTimeout(this._ok); this.state = "idle"; this.levels = null; if (!keepSrc) { try { this.v.removeAttribute("src"); this.v.load(); } catch { /* ignoré */ } } };
  MGP.Player = Player;

  root.MGP = MGP;
  if (typeof module !== "undefined" && module.exports) module.exports = MGP;
})(typeof window !== "undefined" ? window : globalThis);
