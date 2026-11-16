'use strict';

const express = require('express');
const crypto = require('crypto');
const http = require('http');
const https = require('https');
const { URL } = require('url');

const app = express();
app.disable('x-powered-by');
app.use(express.json({ limit: '256kb' }));

const PORT = Number(process.env.PORT || 3000);
const IDLE_CLOSE_MS = Math.max(1000, Number(process.env.STREAM_IDLE_CLOSE_MS || 10000));
const MAX_BUFFER = Math.max(256 * 1024, Number(process.env.STREAM_MAX_BUFFER_BYTES || 2 * 1024 * 1024));
const HLS_MAX_BYTES = Math.max(4 * 1024 * 1024, Number(process.env.HLS_CACHE_MAX_BYTES || 64 * 1024 * 1024));
const HLS_TTL = Math.max(1000, Number(process.env.HLS_CACHE_TTL_MS || 30000));

const sessions = new Map();
const hlsCache = new Map();
let hlsCacheBytes = 0;
let adminConfig = { vastTagUrl: process.env.VAST_TAG_URL || '', bannerHtml: process.env.BANNER_HTML || '' };

function providerConfig() {
  return {
    type: (process.env.IPTV_PROVIDER || 'xtream').toLowerCase(),
    base: (process.env.XTREAM_BASE_URL || '').replace(/\/+$/, ''),
    username: process.env.XTREAM_USERNAME || '',
    password: process.env.XTREAM_PASSWORD || '',
    m3u: process.env.M3U_URL || ''
  };
}
function safeProviderUrl(raw) {
  const cfg = providerConfig();
  try {
    const u = new URL(raw);
    const base = cfg.base ? new URL(cfg.base) : null;
    return ['http:', 'https:'].includes(u.protocol) && base &&
      u.hostname.toLowerCase() === base.hostname.toLowerCase();
  } catch { return false; }
}
function xtreamUrl(action, extra = {}) {
  const c = providerConfig();
  const u = new URL(c.base);
  u.searchParams.set('username', c.username);
  u.searchParams.set('password', c.password);
  if (action) u.searchParams.set('action', action);
  for (const [k, v] of Object.entries(extra)) u.searchParams.set(k, String(v));
  return u.toString();
}
async function providerJson(action, extra) {
  const c = providerConfig();
  if (!c.base || !c.username || !c.password) throw new Error('IPTV provider is not configured');
  const r = await fetch(xtreamUrl(action, extra), { signal: AbortSignal.timeout(20000) });
  if (!r.ok) throw new Error('IPTV provider returned HTTP ' + r.status);
  return r.json();
}
function adminAuth(req, res, next) {
  const expected = process.env.ADMIN_PASSWORD;
  if (!expected) return res.status(503).json({ error: 'Set ADMIN_PASSWORD in the server environment first' });
  const header = req.headers.authorization || '';
  if (!header.startsWith('Basic ')) {
    res.setHeader('WWW-Authenticate', 'Basic realm="JokStream Admin"');
    return res.status(401).end('Authentication required');
  }
  let decoded = '';
  try { decoded = Buffer.from(header.slice(6), 'base64').toString('utf8'); } catch {}
  const supplied = decoded.slice(decoded.indexOf(':') + 1);
  const a = Buffer.from(supplied);
  const b = Buffer.from(expected);
  if (a.length !== b.length || !crypto.timingSafeEqual(a, b)) return res.status(401).end('Invalid credentials');
  next();
}
function jsonError(res, err) {
  res.status(502).json({ error: err.message || 'Provider request failed' });
}

// --- Shared continuous MPEG-TS fan-out ---
function closeSession(key, s, reason) {
  if (sessions.get(key) !== s) return;
  sessions.delete(key);
  s.closed = true;
  if (s.idleTimer) clearTimeout(s.idleTimer);
  s.upstream?.destroy();
  s.response?.destroy();
  for (const client of [...s.clients]) {
    try { client.end(); } catch {}
  }
  s.clients.clear();
  console.log('[stream]', reason, 'viewers=0');
}
function detachClient(key, s, res) {
  if (!s.clients.delete(res)) return;
  if (!s.clients.size && !s.closed) {
    s.idleTimer = setTimeout(() => {
      if (!s.clients.size) closeSession(key, s, 'idle timeout');
    }, IDLE_CLOSE_MS);
    s.idleTimer.unref?.();
  }
}
function attachClient(key, s, res) {
  if (s.idleTimer) clearTimeout(s.idleTimer);
  s.idleTimer = null;
  s.clients.add(res);
  res.on('close', () => detachClient(key, s, res));
  res.on('error', () => detachClient(key, s, res));
}
function startTsSession(key, sourceUrl) {
  const s = { clients: new Set(), upstream: null, response: null, idleTimer: null, closed: false };
  sessions.set(key, s);
  const parsed = new URL(sourceUrl);
  const transport = parsed.protocol === 'https:' ? https : http;
  const req = transport.get(parsed, { headers: { 'User-Agent': 'JokStream/2.0', Accept: '*/*' }, timeout: 20000 }, upstreamRes => {
    if (s.closed) return upstreamRes.destroy();
    if ([301,302,303,307,308].includes(upstreamRes.statusCode) && upstreamRes.headers.location) {
      const next = new URL(upstreamRes.headers.location, parsed).toString();
      upstreamRes.resume();
      if (!safeProviderUrl(next)) return closeSession(key, s, 'redirect host rejected');
      sessions.delete(key);
      s.closed = true;
      return startTsSession(key, next);
    }
    if (upstreamRes.statusCode !== 200) {
      upstreamRes.resume();
      return closeSession(key, s, 'upstream HTTP ' + upstreamRes.statusCode);
    }
    const ct = String(upstreamRes.headers['content-type'] || '').toLowerCase();
    if (ct.includes('mpegurl')) {
      upstreamRes.resume();
      return closeSession(key, s, 'HLS requested through TS path');
    }
    s.response = upstreamRes;
    upstreamRes.on('data', chunk => {
      for (const client of [...s.clients]) {
        if (client.destroyed || client.writableEnded) { detachClient(key, s, client); continue; }
        if (client.writableLength + chunk.length > MAX_BUFFER) {
          client.destroy();
          detachClient(key, s, client);
          continue;
        }
        client.write(chunk);
      }
    });
    upstreamRes.on('end', () => closeSession(key, s, 'upstream ended'));
    upstreamRes.on('error', () => closeSession(key, s, 'upstream error'));
  });
  s.upstream = req;
  req.on('timeout', () => req.destroy(new Error('Upstream timeout')));
  req.on('error', () => closeSession(key, s, 'connection error'));
  return s;
}

// --- HLS segment request coalescing + bounded RAM cache ---
function cacheGet(key) {
  const item = hlsCache.get(key);
  if (!item) return null;
  if (Date.now() - item.time > HLS_TTL) {
    hlsCache.delete(key); hlsCacheBytes -= item.body.length; return null;
  }
  return item;
}
function cacheSet(key, body, type) {
  if (body.length > HLS_MAX_BYTES / 4) return;
  while (hlsCacheBytes + body.length > HLS_MAX_BYTES && hlsCache.size) {
    const oldestKey = hlsCache.keys().next().value;
    const old = hlsCache.get(oldestKey);
    hlsCache.delete(oldestKey); hlsCacheBytes -= old.body.length;
  }
  hlsCache.set(key, { body, type, time: Date.now() });
  hlsCacheBytes += body.length;
}
const inFlight = new Map();
async function fetchHlsResource(url) {
  const key = url;
  const cached = cacheGet(key);
  if (cached) return cached;
  if (inFlight.has(key)) return inFlight.get(key);
  const promise = (async () => {
    if (!safeProviderUrl(url)) throw new Error('Provider host rejected');
    const r = await fetch(url, { headers: { 'User-Agent': 'JokStream/2.0' }, signal: AbortSignal.timeout(20000), redirect: 'manual' });
    if ([301,302,303,307,308].includes(r.status) && r.headers.get('location')) {
      const next = new URL(r.headers.get('location'), url).toString();
      if (!safeProviderUrl(next)) throw new Error('Redirect host rejected');
      return fetchHlsResource(next);
    }
    if (!r.ok) throw new Error('Provider HTTP ' + r.status);
    const type = r.headers.get('content-type') || 'application/octet-stream';
    const body = Buffer.from(await r.arrayBuffer());
    if (type.toLowerCase().includes('mpegurl') || /\.m3u8(?:$|\?)/i.test(url)) {
      const text = body.toString('utf8').split(/\r?\n/).map(line => {
        const trimmed = line.trim();
        if (!trimmed || trimmed.startsWith('#')) {
          return trimmed.replace(/URI="([^"]+)"/g, (_, uri) => 'URI="' + '/api/hls-resource?url=' + encodeURIComponent(new URL(uri, url).toString()) + '"');
        }
        const absolute = new URL(trimmed, url).toString();
        return '/api/hls-resource?url=' + encodeURIComponent(absolute);
      }).join('\n');
      return { body: Buffer.from(text), type: 'application/vnd.apple.mpegurl' };
    }
    cacheSet(key, body, type);
    return { body, type };
  })();
  inFlight.set(key, promise);
  try { return await promise; } finally { inFlight.delete(key); }
}

app.get('/api/health', (req, res) => res.json({
  status: 'ok',
  activeTsChannels: sessions.size,
  activeTsViewers: [...sessions.values()].reduce((n, s) => n + s.clients.size, 0),
  hlsCacheEntries: hlsCache.size,
  hlsCacheBytes
}));

app.get('/api/live', async (req, res) => {
  try {
    const c = providerConfig();
    if (c.type === 'm3u') return res.json({ items: await loadM3UItems('live') });
    const data = await providerJson('get_live_streams');
    res.json({ items: Array.isArray(data) ? data : [] });
  } catch (e) { jsonError(res, e); }
});
app.get('/api/movies', async (req, res) => {
  try {
    const data = await providerJson('get_vod_streams');
    res.json({ items: Array.isArray(data) ? data : [] });
  } catch (e) { jsonError(res, e); }
});
app.get('/api/series', async (req, res) => {
  try {
    const data = await providerJson('get_series');
    res.json({ items: Array.isArray(data) ? data : [] });
  } catch (e) { jsonError(res, e); }
});
async function loadM3UItems() {
  const c = providerConfig();
  if (!c.m3u) throw new Error('M3U_URL is not configured');
  const u = new URL(c.m3u);
  if (!['http:','https:'].includes(u.protocol)) throw new Error('Invalid M3U URL');
  const r = await fetch(u, { signal: AbortSignal.timeout(30000) });
  if (!r.ok) throw new Error('M3U fetch failed');
  const text = await r.text();
  const items = [];
  let current = null;
  for (const line of text.split(/\r?\n/)) {
    if (line.startsWith('#EXTINF:')) {
      const name = line.split(',').slice(1).join(',').trim();
      current = { name, stream_id: String(items.length + 1), stream_type: 'live' };
    } else if (line.trim() && !line.startsWith('#') && current) {
      current.stream_url = line.trim(); items.push(current); current = null;
    }
  }
  return items;
}

// TS route uses server-side provider credentials and numeric Xtream IDs.
app.get('/api/stream/:id', (req, res) => {
  const id = String(req.params.id);
  if (!/^[a-zA-Z0-9_-]{1,100}$/.test(id)) return res.status(400).end('Invalid stream ID');
  const c = providerConfig();
  let source;
  if (c.type === 'xtream') {
    if (!c.base || !c.username || !c.password) return res.status(503).json({ error: 'IPTV is not configured' });
    source = c.base + '/live/' + encodeURIComponent(c.username) + '/' + encodeURIComponent(c.password) + '/' + encodeURIComponent(id) + '.ts';
  } else {
    return res.status(400).json({ error: 'M3U stream URL proxy requires a configured server-side playlist mapping' });
  }
  const key = crypto.createHash('sha256').update(source).digest('hex');
  let s = sessions.get(key);
  if (!s || s.closed) s = startTsSession(key, source);
  res.status(200).set({
    'Content-Type': 'video/mp2t',
    'Cache-Control': 'no-store',
    'X-Accel-Buffering': 'no'
  });
  attachClient(key, s, res);
});

app.get('/api/hls-resource', async (req, res) => {
  try {
    const raw = String(req.query.url || '');
    if (!raw || raw.length > 4096 || !safeProviderUrl(raw)) return res.status(403).end('URL rejected');
    const result = await fetchHlsResource(raw);
    res.set('Content-Type', result.type).set('Cache-Control', 'no-store').send(result.body);
  } catch (e) { res.status(502).json({ error: e.message }); }
});

app.get('/api/admin/config', adminAuth, (req, res) => {
  res.json({ vastTagUrl: adminConfig.vastTagUrl, bannerHtml: adminConfig.bannerHtml });
});
app.post('/api/admin/config', adminAuth, (req, res) => {
  const vastTagUrl = String(req.body.vastTagUrl || '');
  const bannerHtml = String(req.body.bannerHtml || '');
  if (vastTagUrl && !/^https:\/\//i.test(vastTagUrl)) return res.status(400).json({ error: 'VAST URL must use HTTPS' });
  if (vastTagUrl.length > 4096 || bannerHtml.length > 20000) return res.status(400).json({ error: 'Setting too large' });
  adminConfig = { vastTagUrl, bannerHtml };
  res.json({ success: true });
});
app.get('/api/ads', (req, res) => res.json({ vastTagUrl: adminConfig.vastTagUrl, bannerHtml: adminConfig.bannerHtml }));

app.use(express.static('public'));
app.get('*', (req, res) => res.sendFile(require('path').join(process.cwd(), 'public', 'index.html')));

const server = app.listen(PORT, () => console.log('JokStream V2 listening on ' + PORT));
function shutdown() {
  for (const [key, s] of sessions) closeSession(key, s, 'server shutdown');
  server.close(() => process.exit(0));
  setTimeout(() => process.exit(1), 8000).unref();
}
process.on('SIGTERM', shutdown);
process.on('SIGINT', shutdown);
