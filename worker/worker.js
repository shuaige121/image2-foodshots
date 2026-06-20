// img2-counter — shared cross-device coordinator for image generation.
//  Counter:  POST /report {device,date,count} · GET /day · GET /week
//  Cooldown: POST /gen {device} · GET /check         (sliding window, see below)
//  Lock/state machine (prevents concurrent codex runs that race the OAuth refresh):
//    POST /claim     {device, task, ttl?}  -> acquire the single generation lease (who/when/what)
//    POST /heartbeat {lease_id, ttl?}      -> extend your lease during a long batch
//    POST /release   {lease_id}            -> give it back
//    GET  /state                           -> {lease: who/when/what, free, cooldown}
//  Image staging + audit (R2 = transient store, NAS = durable archive, prune R2 after NAS acks):
//    POST /upload   {device,thread_id,name,prompt,date,png_b64} -> stash PNG in R2 + catalog row (status:staged)
//    GET  /pending  ?limit=                -> rows still staged in R2, for the NAS puller to fetch
//    GET  /img      ?key=                  -> stream a staged PNG from R2 (puller downloads this)
//    POST /ack      {catKey,r2key,nas_path}-> NAS confirmed durable -> DELETE R2 object + mark archived
//    GET  /audit    ?date=                 -> the catalog for a day: who/when/what prompt/where now
//  One worker = one source of truth for "who is generating + the cooldown + what images exist".
const THRESH = 12;
const WINDOW_MIN = 45;
const WINDOW_MS = WINDOW_MIN * 60000;
const PROMPT_MAX = 2000;
const PNG_MAX = 12 * 1024 * 1024; // ~8MB image after b64 decode
const CAT_TTL = 60 * 60 * 24 * 120; // catalog rows live 120 days

// R2 object key / catalog key are built ONLY from sanitized segments — no caller string
// reaches the key raw, so no path-traversal or prefix-injection.
const seg = (s, max = 80) => String(s == null ? "" : s).replace(/[^A-Za-z0-9._-]/g, "_").slice(0, max) || "x";
const sgtDate = (now) => new Date(now + 8 * 3600 * 1000).toISOString().slice(0, 10);

export default {
  async fetch(req, env) {
    const auth = req.headers.get("authorization") || "";
    if (auth !== `Bearer ${env.AUTH_TOKEN}`) return new Response("unauthorized", { status: 401 });
    const url = new URL(req.url);
    const now = Date.now();

    // ---------- cooldown (sliding window, event-anchored) ----------
    const getRecent = async () => {
      const raw = await env.KV.get("gen:recent");
      const cutoff = now - WINDOW_MS - 600000;
      return (raw ? JSON.parse(raw) : []).filter((t) => t >= cutoff).sort((a, b) => a - b);
    };
    const cooldownStatus = async () => {
      const win = (await getRecent()).filter((t) => t >= now - WINDOW_MS);
      const n = win.length;
      const hit = n >= THRESH;
      const cdUntil = hit ? win[n - THRESH] + WINDOW_MS : 0;
      const inCd = now < cdUntil;
      return {
        safe: !inCd, in_cooldown: inCd, cooldown_until: inCd ? cdUntil : 0,
        cooldown_until_iso: inCd ? new Date(cdUntil).toISOString() : null,
        cooldown_remaining_sec: inCd ? Math.ceil((cdUntil - now) / 1000) : 0,
        recent_in_window: n, threshold: THRESH, window_min: WINDOW_MIN,
      };
    };

    // ---------- lease / state machine ----------
    const getLease = async () => {
      const raw = await env.KV.get("lease");
      if (!raw) return null;
      const l = JSON.parse(raw);
      return now < l.expires_at ? l : null; // expired => free
    };

    if (req.method === "POST" && url.pathname === "/claim") {
      const b = await req.json().catch(() => ({}));
      const device = b.device || "unknown";
      const task = b.task || "";
      const ttl = Math.min(Math.max(parseInt(b.ttl || "600", 10), 30), 1800); // 30s..30min
      const cur = await getLease();
      if (cur && cur.device !== device) {
        return Response.json({
          granted: false, holder: cur.device, task: cur.task, started_at: cur.started_at,
          expires_at: cur.expires_at, wait_sec: Math.ceil((cur.expires_at - now) / 1000),
        });
      }
      const lease_id = cur && cur.device === device ? cur.lease_id : crypto.randomUUID();
      const lease = {
        device, task, lease_id,
        started_at: cur && cur.device === device ? cur.started_at : now,
        expires_at: now + ttl * 1000,
      };
      await env.KV.put("lease", JSON.stringify(lease), { expirationTtl: ttl + 120 });
      // inherited=true means this device already held the lease (nested caller) -> caller must NOT release it.
      return Response.json({ granted: true, inherited: !!(cur && cur.device === device), ...lease });
    }
    if (req.method === "POST" && url.pathname === "/heartbeat") {
      const b = await req.json().catch(() => ({}));
      const ttl = Math.min(Math.max(parseInt(b.ttl || "600", 10), 30), 1800);
      const cur = await getLease();
      if (cur && cur.lease_id === b.lease_id) {
        cur.expires_at = now + ttl * 1000;
        await env.KV.put("lease", JSON.stringify(cur), { expirationTtl: ttl + 120 });
        return Response.json({ ok: true, expires_at: cur.expires_at });
      }
      return Response.json({ ok: false, reason: cur ? "lease rotated/held by other" : "no active lease" });
    }
    if (req.method === "POST" && url.pathname === "/release") {
      const b = await req.json().catch(() => ({}));
      const cur = await getLease();
      if (cur && cur.lease_id === b.lease_id) {
        await env.KV.delete("lease");
        return Response.json({ ok: true });
      }
      return Response.json({ ok: false, reason: cur ? "not your lease" : "no active lease" });
    }
    if (req.method === "GET" && url.pathname === "/state") {
      const cur = await getLease();
      return Response.json({
        free: !cur,
        lease: cur ? {
          device: cur.device, task: cur.task,
          started_at: cur.started_at, started_iso: new Date(cur.started_at).toISOString(),
          expires_at: cur.expires_at, age_sec: Math.round((now - cur.started_at) / 1000),
          remaining_sec: Math.ceil((cur.expires_at - now) / 1000),
        } : null,
        cooldown: await cooldownStatus(),
        now,
      });
    }

    // ---------- cooldown endpoints ----------
    if (req.method === "POST" && url.pathname === "/gen") {
      const arr = await getRecent();
      arr.push(now);
      await env.KV.put("gen:recent", JSON.stringify(arr), { expirationTtl: 7200 });
      return Response.json(await cooldownStatus());
    }
    if (req.method === "GET" && url.pathname === "/check") return Response.json(await cooldownStatus());

    // ---------- image staging (R2) + audit catalog (KV) ----------
    if (req.method === "POST" && url.pathname === "/upload") {
      const b = await req.json().catch(() => ({}));
      if (!b.png_b64 || typeof b.png_b64 !== "string") return new Response("missing png_b64", { status: 400 });
      // reject oversized payloads BEFORE atob, so a huge string can't force a big decode (isolate DoS)
      if (b.png_b64.length > Math.ceil(PNG_MAX / 3) * 4 + 16) return new Response("payload too large", { status: 413 });
      let bytes;
      try { bytes = Uint8Array.from(atob(b.png_b64), (c) => c.charCodeAt(0)); }
      catch { return new Response("bad base64", { status: 400 }); }
      if (bytes.length < 100 || bytes.length > PNG_MAX) return new Response("size out of range", { status: 413 });
      if (!(bytes[0] === 0x89 && bytes[1] === 0x50 && bytes[2] === 0x4e && bytes[3] === 0x47))
        return new Response("not a png", { status: 415 });
      const device = seg(b.device || "unknown", 40);
      const thread_id = seg(b.thread_id || "x", 60);
      const name = seg(b.name || "img", 80);
      const date = /^\d{4}-\d{2}-\d{2}$/.test(b.date || "") ? b.date : sgtDate(now);
      const prompt = String(b.prompt || "").slice(0, PROMPT_MAX);
      const r2key = `img/${date}/${device}/${thread_id}__${name}.png`;
      const catKey = `cat:${date}:${device}:${thread_id}:${name}`;
      // Invariant: object before catalog — a 'staged' row ALWAYS has its R2 object, so the puller's
      // /img never 404s a staged key. Cost: a KV.put failure orphans the object (no row) — harmless,
      // GC'd by a future reconcile sweep; never a false 'staged' that wedges the puller.
      await env.R2.put(r2key, bytes, { httpMetadata: { contentType: "image/png" } });
      const entry = { device, thread_id, name, prompt, r2key, size: bytes.length, ts: now, status: "staged" };
      await env.KV.put(catKey, JSON.stringify(entry), { expirationTtl: CAT_TTL });
      return Response.json({ ok: true, r2key, catKey, size: bytes.length });
    }
    if (req.method === "GET" && url.pathname === "/pending") {
      const limit = Math.min(parseInt(url.searchParams.get("limit") || "200", 10), 1000);
      const pending = [];
      let cursor; // page through ALL cat: keys — KV.list caps at 1000/call, so staged rows
      while (pending.length < limit) { // beyond the first page would otherwise never be pulled
        const list = await env.KV.list({ prefix: "cat:", cursor });
        for (const k of list.keys) {
          if (pending.length >= limit) break;
          const v = await env.KV.get(k.name);
          if (!v) continue;
          const e = JSON.parse(v);
          if (e.status === "staged") pending.push({ catKey: k.name, ...e });
        }
        if (list.list_complete || !list.cursor) break;
        cursor = list.cursor;
      }
      return Response.json({ count: pending.length, pending });
    }
    if (req.method === "GET" && url.pathname === "/img") {
      const key = url.searchParams.get("key") || "";
      if (!key.startsWith("img/")) return new Response("bad key", { status: 400 });
      const obj = await env.R2.get(key);
      if (!obj) return new Response("not found (archived/pruned?)", { status: 404 });
      return new Response(obj.body, { headers: { "content-type": "image/png" } });
    }
    if (req.method === "POST" && url.pathname === "/ack") {
      const b = await req.json().catch(() => ({}));
      const { catKey, r2key } = b;
      const nas_path = String(b.nas_path || "").slice(0, 400);
      if (!catKey || !r2key) return new Response("need catKey + r2key", { status: 400 });
      const v = await env.KV.get(catKey);
      if (!v) return new Response("no such catalog entry", { status: 404 });
      const e = JSON.parse(v);
      if (e.r2key !== r2key) return new Response("r2key mismatch", { status: 409 });
      // Mark archived in the catalog FIRST: a crash between the two awaits must never leave a
      // pruned object still flagged 'staged' (which /pending would re-emit forever, /img 404ing).
      e.status = "archived"; e.nas_path = nas_path; e.archived_ts = now;
      await env.KV.put(catKey, JSON.stringify(e), { expirationTtl: CAT_TTL });
      try { await env.R2.delete(r2key); } catch (_) { /* best-effort prune; leftover object is a harmless orphan a sweep GCs */ }
      return Response.json({ ok: true, archived: catKey });
    }
    if (req.method === "GET" && url.pathname === "/audit") {
      const date = url.searchParams.get("date") || sgtDate(now);
      const list = await env.KV.list({ prefix: `cat:${date}:` });
      const items = [];
      for (const k of list.keys) {
        const v = await env.KV.get(k.name);
        if (!v) continue;
        const e = JSON.parse(v);
        items.push({
          catKey: k.name, device: e.device, thread_id: e.thread_id, name: e.name,
          prompt: e.prompt, status: e.status, r2key: e.r2key, nas_path: e.nas_path || null,
          size: e.size, ts: e.ts,
        });
      }
      return Response.json({ date, count: items.length, items });
    }

    // ---------- daily counter ----------
    if (req.method === "POST" && url.pathname === "/report") {
      const b = await req.json().catch(() => null);
      const { device, date, count } = b || {};
      if (!device || !/^\d{4}-\d{2}-\d{2}$/.test(date || "") || typeof count !== "number") {
        return new Response("bad request", { status: 400 });
      }
      await env.KV.put(`c:${date}:${device}`, String(Math.max(0, Math.floor(count))), { expirationTtl: 60 * 60 * 24 * 40 });
      return Response.json({ ok: true, device, date, count });
    }
    if (req.method === "GET" && url.pathname === "/day") {
      const date = url.searchParams.get("date") || sgtDate(now);
      const list = await env.KV.list({ prefix: `c:${date}:` });
      const devices = {}; let total = 0;
      for (const k of list.keys) {
        const v = parseInt((await env.KV.get(k.name)) || "0", 10);
        devices[k.name.slice(`c:${date}:`.length)] = v; total += v;
      }
      return Response.json({ date, total, devices });
    }
    if (req.method === "GET" && url.pathname === "/week") {
      const end = url.searchParams.get("end") || sgtDate(now);
      const endMs = Date.parse(end + "T00:00:00Z");
      const days = {}; let total = 0;
      for (let i = 0; i < 7; i++) {
        const d = new Date(endMs - i * 86400 * 1000).toISOString().slice(0, 10);
        const list = await env.KV.list({ prefix: `c:${d}:` });
        let day = 0;
        for (const k of list.keys) day += parseInt((await env.KV.get(k.name)) || "0", 10);
        if (day > 0) days[d] = day; total += day;
      }
      return Response.json({ end, total, days });
    }

    return new Response("not found", { status: 404 });
  },
};
