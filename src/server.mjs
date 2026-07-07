// Local dashboard for the GSC index checker.
//   node src/server.mjs   →  http://localhost:4500  (or: npm run dashboard)
//
// Data lives in gsc.db (SQLite). The dashboard has three tabs:
//   • Pages   — every sitemap URL + index status, re-check / reindex / copy
//   • Logs    — structured error/warn/info feed (filterable)
//   • Console — raw live process stream (Server-Sent Events)

import http from 'node:http';
import { createReadStream } from 'node:fs';
import { exec } from 'node:child_process';
import {
  listSites,
  pickSite,
  readSitemapUrls,
  inspectUrl,
  inspectFull,
  requestReindex,
  publishUrlNotification,
  getIndexNotificationStatus,
  listSitesDetailed,
  listSitemaps,
  submitSitemap,
  searchAnalytics,
  gscInspectLink,
  clientEmail,
  authMode,
  findOAuthClient,
  beginReconnect,
  isAuthError,
} from './gsc.mjs';
import {
  getPageMap,
  savePage,
  setMeta,
  getMeta,
  log,
  logBus,
  getLogs,
  clearLogs,
  errorCount,
  historyFor,
  migrateFromJsonIfEmpty,
  exportResultsJson,
} from './db.mjs';

const PORT = Number(process.env.PORT) || 4500;
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

// Open a URL in the default browser, cross-platform (no-op when GSC_NO_OPEN set).
function openBrowser(url) {
  if (process.env.GSC_NO_OPEN) return; // headless / CI
  const cmd =
    process.platform === 'darwin' ? `open "${url}"`
    : process.platform === 'win32' ? `start "" "${url}"`
    : `xdg-open "${url}"`;
  exec(cmd, () => {});
}

migrateFromJsonIfEmpty();

let SITE = null;
// Tracks the single in-flight scan so we never run two at once (which used to
// interleave and re-scan the same pages) and can report progress / be stopped.
let scan = null; // { total, done, indexed, notIndexed, errors, cancel, siteUrl }
async function resolveSite() {
  if (SITE) return SITE;
  try {
    const sites = await listSites();
    const siteUrl = pickSite(sites, getMeta('siteUrl'));
    if (sites.length === 0)
      log({ level: 'warn', event: 'auth', message: `No Search Console properties for ${clientEmail()} — sign in with an owner account (npm run login)` });
    else
      log({ level: 'info', event: 'auth', message: `Authenticated as ${clientEmail()} · ${sites.length} properties · using ${siteUrl}` });
    if (siteUrl) setMeta('siteUrl', siteUrl);
    const resolved = { siteUrl, sites };
    // Only cache a successful resolution — so after `npm run login` the running
    // dashboard recovers on the next request instead of needing a restart.
    if (sites.length > 0) SITE = resolved;
    return resolved;
  } catch (err) {
    const m = err.errors?.[0]?.message || err.message;
    log({ level: 'error', event: 'auth', message: `Could not list properties: ${m}` });
    throw err;
  }
}

// Read a property's sitemap URLs, cached per property for the server session so
// switching between properties stays snappy (restart to pick up new sitemaps).
const _sitemapCache = new Map(); // siteUrl -> urls[]
async function sitemapUrls(siteUrl) {
  if (_sitemapCache.has(siteUrl)) return _sitemapCache.get(siteUrl);
  const urls = await readSitemapUrls(siteUrl);
  _sitemapCache.set(siteUrl, urls);
  log({ level: 'info', event: 'sitemap', message: `Loaded ${urls.length} URLs for ${siteUrl}` });
  return urls;
}

function json(res, code, obj) {
  res.writeHead(code, { 'content-type': 'application/json' });
  res.end(JSON.stringify(obj));
}
async function body(req) {
  const chunks = [];
  for await (const c of req) chunks.push(c);
  return chunks.length ? JSON.parse(Buffer.concat(chunks).toString()) : {};
}

const server = http.createServer(async (req, res) => {
  try {
    const url = new URL(req.url, `http://localhost:${PORT}`);
    const p = url.pathname;

    if (p === '/') {
      res.writeHead(200, { 'content-type': 'text/html; charset=utf-8' });
      const stream = createReadStream(new URL('../public/dashboard.html', import.meta.url));
      // pipe() does not forward source errors; handle them so a missing/locked
      // dashboard.html can't crash the whole process with an uncaught error.
      stream.on('error', (e) => {
        log({ level: 'error', event: 'server', message: 'Could not read dashboard.html: ' + e.message });
        res.end();
      });
      stream.pipe(res);
      return;
    }

    // ---- static brand assets (favicon + logo) served from public/ ----
    if (p === '/favicon.ico' || p === '/logo.svg') {
      const file = p === '/logo.svg' ? '../public/logo.svg' : '../public/favicon.ico';
      const type = p === '/logo.svg' ? 'image/svg+xml' : 'image/x-icon';
      res.writeHead(200, { 'content-type': type, 'cache-control': 'max-age=86400' });
      const s = createReadStream(new URL(file, import.meta.url));
      s.on('error', () => res.end());
      s.pipe(res);
      return;
    }

    // ---- live console stream (SSE) ----
    if (p === '/api/stream') {
      res.writeHead(200, {
        'content-type': 'text/event-stream',
        'cache-control': 'no-cache',
        connection: 'keep-alive',
      });
      res.write('retry: 2000\n\n');
      const onLog = (entry) => res.write(`data: ${JSON.stringify(entry)}\n\n`);
      logBus.on('log', onLog);
      const ping = setInterval(() => res.write(': ping\n\n'), 25000);
      req.on('close', () => {
        clearInterval(ping);
        logBus.off('log', onLog);
      });
      return;
    }

    // ---- page data ----
    if (p === '/api/data') {
      let siteUrl, sites;
      try {
        ({ siteUrl, sites } = await resolveSite());
      } catch (err) {
        if (!isAuthError(err)) throw err;
        const m = err.errors?.[0]?.message || err.message;
        log({ level: 'warn', event: 'auth', message: `Google sign-in expired/invalid: ${m}` });
        return json(res, 200, {
          authExpired: true,
          authError: m,
          bot: clientEmail(),
          authMode: authMode(),
          hasOAuthClient: !!findOAuthClient(),
          hasAccess: false,
          errorCount: errorCount(),
          sites: [],
          rows: [],
        });
      }
      const pages = getPageMap(siteUrl);
      const urls = await sitemapUrls(siteUrl);
      const rows = urls.map((u) => ({
        url: u,
        gscLink: gscInspectLink(siteUrl, u),
        ...(pages[u] || { isIndexed: null, coverageState: 'Not checked yet', verdict: null }),
      }));
      return json(res, 200, {
        siteUrl,
        sites,
        bot: clientEmail(),
        authMode: authMode(),
        hasOAuthClient: !!findOAuthClient(),
        hasAccess: sites.length > 0,
        updatedAt: getMeta('updatedAt'),
        errorCount: errorCount(),
        rows,
      });
    }

    // ---- reconnect Google account (in-place OAuth re-login) ----
    if (p === '/api/reconnect' && req.method === 'POST') {
      if (!findOAuthClient())
        return json(res, 200, { ok: false, message: 'No OAuth client JSON found in the project root.' });
      try {
        const authUrl = beginReconnect(5500);
        openBrowser(authUrl); // local tool → open the consent page in the user's browser
        SITE = null; // re-resolve once the new token lands
        log({ level: 'info', event: 'auth', message: 'Reconnect started — waiting for Google sign-in (:5500)' });
        return json(res, 200, { ok: true, authUrl });
      } catch (e) {
        return json(res, 200, { ok: false, message: e.message });
      }
    }

    // ---- scan progress / stop (read-only status + cancel a running scan) ----
    if (p === '/api/scan-status') {
      return json(res, 200, scan
        ? { running: true, total: scan.total, done: scan.done, indexed: scan.indexed, notIndexed: scan.notIndexed, errors: scan.errors, cancelling: scan.cancel }
        : { running: false });
    }
    if (p === '/api/scan-stop' && req.method === 'POST') {
      if (!scan) return json(res, 200, { ok: false, message: 'No scan is running.' });
      scan.cancel = true;
      log({ level: 'info', event: 'run', message: 'Stop requested — finishing the current page, then stopping.' });
      return json(res, 200, { ok: true, stopping: true });
    }

    // ---- check one/many URLs (single scan at a time; cancellable) ----
    if (p === '/api/check' && req.method === 'POST') {
      const { urls } = await body(req);
      if (!Array.isArray(urls) || !urls.length)
        return json(res, 400, { error: 'Body must be { "urls": [ ...one or more URLs ] }' });
      if (scan) return json(res, 409, { ok: false, busy: true, message: 'A scan is already running — stop it first.' });

      // Claim the lock synchronously (before any await) to avoid a race.
      scan = { total: urls.length, done: 0, indexed: 0, notIndexed: 0, errors: 0, cancel: false, siteUrl: null };
      const out = [];
      try {
        const { siteUrl } = await resolveSite();
        scan.siteUrl = siteUrl;
        const prev = getPageMap(siteUrl);
        log({ level: 'info', event: 'run', message: `Checking ${urls.length} URL(s) on ${siteUrl}` });
        for (let i = 0; i < urls.length; i++) {
          if (scan.cancel) { log({ level: 'warn', event: 'run', message: `Scan stopped by user at ${i}/${urls.length}` }); break; }
          const u = urls[i];
          try {
            const r = await inspectUrl(siteUrl, u);
            const wasIndexed = prev[u]?.isIndexed;
            savePage(siteUrl, u, r);
            if (wasIndexed === true && !r.isIndexed)
              log({ level: 'info', event: 'deindex', url: u, message: `DE-INDEXED: ${r.coverageState}` });
            log({ level: 'debug', event: 'check', url: u, message: `${r.verdict} · ${r.coverageState}`, raw: r });
            r.isIndexed ? scan.indexed++ : scan.notIndexed++;
            out.push({ url: u, ...r });
          } catch (err) {
            const m = err.errors?.[0]?.message || err.message;
            scan.errors++;
            if (err.code === 429) {
              log({ level: 'warn', event: 'ratelimit', url: u, message: '429 quota hit — stopping' });
              out.push({ url: u, error: m });
              break;
            }
            log({ level: 'error', event: 'check', url: u, message: m });
            out.push({ url: u, error: m });
          }
          scan.done++;
          if (i < urls.length - 1 && !scan.cancel) await sleep(250);
        }
        exportResultsJson();
        const cancelled = scan.cancel;
        log({ level: 'info', event: 'run', message: `Done — ${out.filter((x) => !x.error).length}/${urls.length} ok${cancelled ? ' (stopped)' : ''}` });
        return json(res, 200, { results: out, cancelled });
      } finally {
        scan = null; // release the lock no matter what
      }
    }

    // ---- reindex / notify removed (Indexing API publish) ----
    if (p === '/api/reindex' && req.method === 'POST') {
      const { url: target, type } = await body(req);
      const t = type === 'URL_DELETED' ? 'URL_DELETED' : 'URL_UPDATED';
      const result = await publishUrlNotification(target, t);
      log({ level: result.ok ? 'info' : 'error', event: 'reindex', url: target, message: `${t}: ${result.message}` });
      return json(res, 200, result);
    }

    // ---- full URL inspection (URL Inspection API) ----
    if (p === '/api/inspect') {
      const { siteUrl } = await resolveSite();
      const u = url.searchParams.get('url');
      try {
        const data = await inspectFull(siteUrl, u);
        log({ level: 'debug', event: 'inspect', url: u, message: 'Full inspection fetched', raw: data });
        return json(res, 200, { ok: true, result: data });
      } catch (err) {
        const m = err.errors?.[0]?.message || err.message;
        log({ level: 'error', event: 'inspect', url: u, message: m });
        return json(res, 200, { ok: false, message: m });
      }
    }

    // ---- index notification status (Indexing API getMetadata) ----
    if (p === '/api/index-status') {
      const u = url.searchParams.get('url');
      const r = await getIndexNotificationStatus(u);
      log({ level: r.ok ? 'debug' : 'warn', event: 'index-status', url: u, message: r.ok ? 'Fetched notify metadata' : r.message, raw: r.data });
      return json(res, 200, r);
    }

    // ---- properties (sites.list) ----
    if (p === '/api/sites') {
      const sites = await listSitesDetailed();
      const { siteUrl } = await resolveSite();
      log({ level: 'debug', event: 'sites', message: `Listed ${sites.length} properties` });
      return json(res, 200, { sites, active: siteUrl });
    }

    // ---- switch active property ----
    if (p === '/api/site' && req.method === 'POST') {
      const { siteUrl } = await body(req);
      setMeta('siteUrl', siteUrl);
      SITE = null; // reset cache
      log({ level: 'info', event: 'site', message: `Active property switched to ${siteUrl}` });
      return json(res, 200, { ok: true, siteUrl });
    }

    // ---- sitemaps (sitemaps.list) ----
    if (p === '/api/sitemaps') {
      const { siteUrl } = await resolveSite();
      try {
        const sitemaps = await listSitemaps(siteUrl);
        log({ level: 'debug', event: 'sitemaps', message: `Listed ${sitemaps.length} sitemaps for ${siteUrl}` });
        return json(res, 200, { ok: true, sitemaps });
      } catch (err) {
        const m = err.errors?.[0]?.message || err.message;
        log({ level: 'error', event: 'sitemaps', message: m });
        return json(res, 200, { ok: false, message: m });
      }
    }

    // ---- submit sitemap (sitemaps.submit) ----
    if (p === '/api/sitemaps/submit' && req.method === 'POST') {
      const { feedpath } = await body(req);
      const { siteUrl } = await resolveSite();
      const r = await submitSitemap(siteUrl, feedpath);
      log({ level: r.ok ? 'info' : 'error', event: 'sitemap-submit', message: r.message });
      return json(res, 200, r);
    }

    // ---- search analytics (searchanalytics.query) ----
    if (p === '/api/analytics' && req.method === 'POST') {
      const { url: target, days, dimensions } = await body(req);
      const { siteUrl } = await resolveSite();
      try {
        const rows = await searchAnalytics(siteUrl, { url: target, days, dimensions });
        log({ level: 'debug', event: 'analytics', url: target || null, message: `Search Analytics: ${rows.length} rows (${days || 28}d)` });
        return json(res, 200, { ok: true, rows });
      } catch (err) {
        const m = err.errors?.[0]?.message || err.message;
        log({ level: 'error', event: 'analytics', message: m });
        return json(res, 200, { ok: false, message: m });
      }
    }

    // ---- history for one URL (on the active property) ----
    if (p === '/api/history') {
      const { siteUrl } = await resolveSite();
      const u = url.searchParams.get('url');
      return json(res, 200, { url: u, history: historyFor(siteUrl, u, 50) });
    }

    // ---- logs ----
    if (p === '/api/logs') {
      const level = url.searchParams.get('level') || 'all';
      const limit = Number(url.searchParams.get('limit') || 300);
      return json(res, 200, { logs: getLogs({ level, limit }), errorCount: errorCount() });
    }
    if (p === '/api/logs/clear' && req.method === 'POST') {
      clearLogs();
      log({ level: 'info', event: 'logs', message: 'Logs cleared' });
      return json(res, 200, { ok: true });
    }

    res.writeHead(404).end('Not found');
  } catch (err) {
    log({ level: 'error', event: 'server', message: err.message });
    json(res, 500, { error: err.message });
  }
});

server.listen(PORT, () => {
  const link = `http://localhost:${PORT}`;
  console.log(`\n🟢 GSC dashboard running → ${link}\n`);
  log({ level: 'info', event: 'server', message: `Dashboard started on ${link}` });
  openBrowser(link); // no-op if GSC_NO_OPEN=1 (headless/CI)
});
